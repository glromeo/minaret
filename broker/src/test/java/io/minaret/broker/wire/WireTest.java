package io.minaret.broker.wire;

import io.minaret.messaging.Destination;
import io.minaret.messaging.Header;
import io.minaret.messaging.wire.BodyEncoding;
import io.minaret.messaging.wire.WireFrame;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.UUID;
import java.util.function.Function;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The Broker's byte codec pinned against the literals C#'s WireTests and runtime's WireTest pin.
 * Where an expected frame is needed, it is rendered by runtime's string codec,
 * so this also checks that the two implementations agree.
 *
 * Frame.parse follows the receive path, not Wire.TryParseHeader: a message with no \0 is a header-only
 * frame. So the three C# "missing body separator" rows reappear here as whichever error the header
 * itself earns.
 */
class WireTest {

    private static final ByteBufAllocator ALLOC = PooledByteBufAllocator.DEFAULT;
    private static final UUID PINNED = UUID.fromString("00112233-4455-6677-8899-aabbccddeeff");

    private static ByteBuf bytes(String text) {
        return Unpooled.copiedBuffer(text, StandardCharsets.UTF_8);
    }

    private static Frame parsed(String raw) {
        Frame frame = new Frame();
        assertNull(frame.parse(bytes(raw)), raw);
        return frame;
    }

    private static String error(String raw) {
        return new Frame().parse(bytes(raw));
    }

    /** Renders with out, answers the M01 text after the WebSocket header, and releases the buffer. */
    private static String render(Function<ByteBufAllocator, ByteBuf> out) {
        ByteBuf buf = out.apply(ALLOC);
        try {
            return FrameWriter.payload(buf).toString(StandardCharsets.UTF_8);
        } finally {
            buf.release();
        }
    }

    private static String client(Header header, String body) {
        return io.minaret.messaging.wire.Wire.serializeText(header, body);
    }

    private static Header header(UUID from, Destination to, String type, UUID replyTo, long time, Integer ttl) {
        return new Header(from, to, type, replyTo, time, ttl, BodyEncoding.RAW, null, "");
    }

    @ParameterizedTest
    @CsvSource(delimiter = ';', value = {
            "'';unknown frame tag",
            "not a frame at all;unknown frame tag",
            "M01|no-nul-separator;truncated header",
            "M02||t|||||\0body;unknown frame tag",
            "XX||t|||||\0body;unknown frame tag",
            "M01|too|few\0body;truncated header",
            "M01|xyz|t|||||\0body;malformed guid",
            "M01||t|notanumber||||\0body;malformed time",
            "M01||t|99999999999999999999||||\0body;malformed time",
            "M01||t||notanumber|||\0body;malformed ttl",
            "M01||||||||\0body;empty type",
            "M01||t|||||brotli\0body;unknown body encoding",
            "M01|/|dispatch|||||\0{};destination path must be /channel/topic",
            "M01|//echo|dispatch|||||\0{};destination path must be /channel/topic",
            "M01|/LAYER_1/|dispatch|||||\0{};destination path must be /channel/topic",
    })
    void parse_rejectsMalformedFrames(String raw, String expected) {
        assertEquals(expected, error(raw));
    }

    @Test
    void parse_headerOnlyFrame_hasAnEmptyBody() {
        Frame frame = parsed("M01||client.ping|||||");
        assertTrue(frame.typeIs("client.ping".getBytes(StandardCharsets.US_ASCII)));
        assertEquals(0, frame.bodyLengthActual());
    }

    @Test
    void parse_aHeaderPastTheLimit_isFatal_notADrop() {
        String raw = "M01||" + "t".repeat(Wire.MAX_HEADER_BYTES) + "|||||\0{}";
        Frame frame = new Frame();
        assertEquals(Frame.FATAL_HEADER_TOO_LONG, frame.parse(bytes(raw)));
        assertTrue(frame.fatal());

        assertEquals("unknown frame tag", frame.parse(bytes("M02||t|||||\0{}")));
        assertFalse(frame.fatal());
    }

    @Test
    void parse_everyFieldSet() {
        UUID to = UUID.randomUUID();
        UUID from = UUID.randomUUID();
        UUID replyTo = UUID.randomUUID();
        Frame frame = parsed(client(header(from, Destination.ofId(to), "greeting", replyTo, 1769472000000L, 60), "\"hi\""));

        assertEquals(Frame.ToKind.ID, frame.toKind());
        assertEquals(to, frame.to().toUUID());
        assertEquals("greeting", frame.type());
        assertEquals(1769472000000L, frame.time());
        assertTrue(frame.hasTtl());
        assertEquals(60, frame.ttl());
        assertEquals(from, frame.from().toUUID());
        assertEquals(replyTo, frame.replyTo().toUUID());
        assertFalse(frame.gzip());
        assertTrue(frame.hasBodyLength());
        assertEquals(4, frame.bodyLength());
        assertEquals(4, frame.bodyLengthActual());
        assertEquals(0, frame.extrasLength());
    }

    @Test
    void parse_everyOptionalFieldAbsent() {
        Frame frame = parsed("M01||register|||||\0{}");
        assertEquals(Frame.ToKind.EMPTY, frame.toKind());
        assertNull(frame.from());
        assertNull(frame.replyTo());
        assertFalse(frame.hasTtl());
        assertFalse(frame.hasBodyLength());
        assertEquals(0, frame.time());
    }

    @Test
    void parse_explicitZeroTtl_isDistinctFromAbsentTtl() {
        Frame zero = parsed("M01||t||0|||\0{}");
        assertTrue(zero.hasTtl());
        assertEquals(0, zero.ttl());
        assertFalse(parsed("M01||t|||||\0{}").hasTtl());
    }

    @Test
    void parse_acceptsNegativeTtl() {
        assertEquals(-5, parsed("M01||t||-5|||\0body").ttl());
    }

    @Test
    void parse_pathDestination_splitsOnTheFirstSeparatorOnly() {
        Frame frame = parsed("M01|/C1/a/b|dispatch|||||\0\"hi\"");
        assertEquals(Frame.ToKind.PATH, frame.toKind());
        assertEquals("C1", frame.channel());
        assertEquals("a/b", frame.topic());
    }

    @Test
    void guids_areBigEndianBase64Url_bothWays() {
        Frame frame = parsed("M01|ABEiM0RVZneImaq7zN3u_w|t|||||\0{}");
        assertEquals(PINNED, frame.to().toUUID());

        String out = render(alloc -> FrameWriter.originate(alloc, Uuid.of(PINNED), "t".getBytes(StandardCharsets.US_ASCII), 0, null, false, "{}".getBytes(StandardCharsets.UTF_8)));
        assertEquals("ABEiM0RVZneImaq7zN3u_w", out.split("\\|")[1]);
    }

    @Test
    void guids_rejectACharacterOutsideTheAlphabet() {
        assertEquals("malformed guid", error("M01|ABEiM0RVZneImaq7zN3u+w|t|||||\0{}"));
    }

    /**
     * The relay re-renders the header from parsed values. Re-stamped with the values it already
     * carries, it must reproduce the client's frame byte for byte, including no trailing separator
     * when there are no extras.
     */
    @Test
    void relay_withTheSameValues_isByteIdenticalToTheClientsFrame() {
        UUID to = UUID.randomUUID();
        UUID from = UUID.randomUUID();
        String raw = client(header(from, Destination.ofId(to), "greeting", UUID.randomUUID(), 1769472000000L, 60), "\"hi\"");
        Frame frame = parsed(raw);

        assertEquals(raw, render(alloc -> FrameWriter.relay(alloc, frame, frame.to(), frame.time(), frame.ttl(), frame.from())));
    }

    @Test
    void relay_preservesUnknownTrailingFields() {
        Frame frame = parsed("M01||bulk|1769472000000|60|||||x|somefuturevalue\0{\"a\":1}");
        UUID to = UUID.randomUUID();

        String forwarded = render(alloc -> FrameWriter.relay(alloc, frame, Uuid.of(to), frame.time(), frame.ttl(), null));

        WireFrame reparsed = io.minaret.messaging.wire.Wire.parseHeader(forwarded).frame();
        assertEquals("x|somefuturevalue", reparsed.header().extras());
        assertEquals(to, reparsed.header().to().id());
        assertEquals("{\"a\":1}", reparsed.body());
    }

    /**
     * Uses a body whose plaintext is under the threshold. A relay that inflated and re-encoded it
     * would emit it as raw and be caught; a large plaintext would make the check vacuous.
     */
    @Test
    void relay_forwardsACompressedBody_withoutInflatingIt() throws Exception {
        String gzipped = gzipped("small but compressed");
        Frame frame = parsed("M01||bulk|1||||gzip\0" + gzipped);
        assertTrue(frame.gzip());

        String forwarded = render(alloc -> FrameWriter.relay(alloc, frame, Uuid.of(UUID.randomUUID()), frame.time(), 30, null));

        WireFrame reparsed = io.minaret.messaging.wire.Wire.parseHeader(forwarded).frame();
        assertEquals(BodyEncoding.GZIP, reparsed.header().encoding());
        assertEquals(30, reparsed.header().ttl());
        assertEquals(gzipped, reparsed.body());
        assertEquals("small but compressed", io.minaret.messaging.wire.Wire.parseBody(reparsed));
    }

    @Test
    void originate_matchesTheClientsRendering() {
        UUID to = UUID.randomUUID();
        UUID replyTo = UUID.randomUUID();
        byte[] body = "{\"ok\":true}".getBytes(StandardCharsets.UTF_8);

        String out = render(alloc -> FrameWriter.originate(alloc, Uuid.of(to), "client.registered".getBytes(StandardCharsets.US_ASCII), 1769472000000L, Uuid.of(replyTo), false, body));

        String expected = client(header(null, Destination.ofId(to), "client.registered", replyTo, 1769472000000L, null), "{\"ok\":true}");
        assertEquals(expected, out);
    }

    /** The back-filled WebSocket header: an unmasked final text frame, in each of the three length forms. */
    @ParameterizedTest
    @ValueSource(ints = {10, 125 - 40, 200, 70_000})
    void originate_backFillsAValidWebSocketHeader(int bodySize) {
        byte[] body = "a".repeat(bodySize).getBytes(StandardCharsets.UTF_8);
        ByteBuf buf = FrameWriter.originate(ALLOC, null, "t".getBytes(StandardCharsets.US_ASCII), 0, null, false, body);
        try {
            int payload = FrameWriter.payload(buf).readableBytes();
            int at = buf.readerIndex();
            assertEquals((byte) 0x81, buf.getByte(at));
            int length = buf.getByte(at + 1) & 0xff;
            assertEquals(0, length & 0x80, "a server frame must not be masked");
            if (payload < 126) {
                assertEquals(payload, length);
            } else if (payload <= 0xffff) {
                assertEquals(126, length);
                assertEquals(payload, buf.getUnsignedShort(at + 2));
            } else {
                assertEquals(127, length);
                assertEquals(payload, buf.getLong(at + 2));
            }
            assertEquals(payload, buf.readableBytes() - (payload < 126 ? 2 : payload <= 0xffff ? 4 : 10));
        } finally {
            buf.release();
        }
    }

    @Test
    void json_encode_compressesAtTheThreshold_measuredOnTheInput() {
        Json json = new Json(io.micronaut.serde.ObjectMapper.getDefault());
        String under = "a".repeat(Wire.COMPRESSION_THRESHOLD_BYTES - 3);
        assertFalse(json.encode(under).gzip()); // quoted: threshold - 1 bytes
        assertTrue(json.encode(under + "a").gzip());
    }

    @Test
    void json_read_inflatesAGzippedBody() throws Exception {
        Json json = new Json(io.micronaut.serde.ObjectMapper.getDefault());
        Frame frame = parsed("M01||t|||||gzip\0" + gzipped("\"hello\""));
        assertEquals("hello", json.read(frame, String.class));
        assertNotNull(frame.buffer());
    }

    private static String gzipped(String text) throws Exception {
        ByteArrayOutputStream target = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(target)) {
            gzip.write(text.getBytes(StandardCharsets.UTF_8));
        }
        return Base64.getUrlEncoder().withoutPadding().encodeToString(target.toByteArray());
    }

}
