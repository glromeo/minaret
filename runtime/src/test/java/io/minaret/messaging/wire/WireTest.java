package io.minaret.messaging.wire;

import com.fasterxml.jackson.databind.JsonNode;
import io.minaret.messaging.Destination;
import io.minaret.messaging.Header;
import io.minaret.messaging.Verbs;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins this codec to the same literals broker's WireTest and javascript/test/wire.test.js pin. The
 * protocol is written three times; these assertions are what make that checkable
 * rather than merely hoped for, so a literal here should be changed only alongside its counterparts.
 */
class WireTest {

    /**
     * The single most important assertion in this file. Java's UUID accessors are already RFC-4122
     * order. If this drifts, every frame still parses locally and no frame routes between
     * runtimes.
     */
    @Test
    void guidEncodingMatchesTheOtherRuntimes() {
        UUID guid = UUID.fromString("00112233-4455-6677-8899-aabbccddeeff");

        assertEquals("ABEiM0RVZneImaq7zN3u_w", Guids.encode(guid));
        assertEquals(guid, Guids.decode("ABEiM0RVZneImaq7zN3u_w"));
    }

    @Test
    void registerHandshakeRendersTheBrokerAddressedShape() {
        UUID routeId = UUID.randomUUID();
        Header header = Header.of(Verbs.CLIENT_REGISTER).withFrom(routeId).withTime(1769472000000L);

        String frame = Wire.serializeMessage(header, new Body("x"));

        assertTrue(
                frame.startsWith("M01||client.register|1769472000000||" + Guids.encode(routeId) + "||"),
                frame
        );
    }

    /**
     * Eight segments, so eight separators: to, type, time, ttl, from, replyTo, enc, bodyLength. The JS
     * client renders M01||register||||||2\0{} too.
     *
     * Asserted as a whole-string equality rather than a prefix, so a seven-separator frame cannot
     * pass.
     */
    @Test
    void emptyDestinationAndNoOptionalFieldsRenderAsBareSeparators() {
        String frame = Wire.writeHeaderAndBody(Header.of("register"), "");

        assertEquals("M01||register||||||\0", frame);
    }

    @Test
    void serializedFrameStampsBodyLength() {
        String frame = Wire.serializeText(Header.of("register"), "{}");

        assertEquals("M01||register||||||2\0{}", frame);
    }

    @Test
    void headerRoundTripsEveryField() {
        UUID from = UUID.randomUUID();
        UUID replyTo = UUID.randomUUID();
        UUID topicId = UUID.randomUUID();

        Header header = new Header(
                from, Destination.ofId(topicId), Verbs.DISPATCH, replyTo,
                1769472000000L, 60, BodyEncoding.RAW, 3, ""
        );

        WireFrame.Parsed parsed = Wire.parseHeader(Wire.writeHeaderAndBody(header, "\"x\""));

        assertTrue(parsed.isOk(), parsed.error());
        assertEquals(header, parsed.frame().header());
        assertEquals("\"x\"", parsed.frame().body());
    }

    @Test
    void pathDestinationKeepsSlashesInTheTopicHalf() {
        WireFrame.Parsed parsed = Wire.parseHeader("M01|/LAYER_1/ticks/fast|publish|||||\0\"x\"");

        assertTrue(parsed.isOk(), parsed.error());
        assertEquals("LAYER_1", parsed.frame().header().to().channel());
        assertEquals("ticks/fast", parsed.frame().header().to().topic());
    }

    @Test
    void emptyPathHalvesAreRejected() {
        assertDropped("M01|/|dispatch|||||\0{}", "destination path must be /channel/topic");
        assertDropped("M01|//echo|dispatch|||||\0{}", "destination path must be /channel/topic");
        assertDropped("M01|/LAYER_1/|dispatch|||||\0{}", "destination path must be /channel/topic");
    }

    @Test
    void unknownTrailingFieldsSurviveAsExtras() {
        // Segments 1-8 (to..bodyLength) are the known M01 fields; everything past bodyLength is a field
        // a later version added, carried through so a newer client's frame is not corrupted by an older
        // relay.
        WireFrame.Parsed parsed = Wire.parseHeader("M01||bulk|1769472000000|60|||||x|somefuturevalue\0{\"a\":1}");

        assertTrue(parsed.isOk(), parsed.error());
        assertEquals("x|somefuturevalue", parsed.frame().header().extras());
        assertNull(parsed.frame().header().bodyLength());
        assertEquals(60, parsed.frame().header().ttl());
    }

    @Test
    void extrasRoundTripThroughARelay() {
        WireFrame.Parsed parsed = Wire.parseHeader("M01||bulk|1|||||3|somefuturevalue\0\"x\"");

        assertTrue(parsed.isOk(), parsed.error());
        assertEquals("somefuturevalue", parsed.frame().header().extras());
        assertEquals(3, parsed.frame().header().bodyLength());

        assertTrue(Wire.writeHeader(parsed.frame().header()).endsWith("|somefuturevalue"));
    }

    @Test
    void absentBodyLengthIsNullNotZero() {
        WireFrame.Parsed parsed = Wire.parseHeader("M01||bulk|1||||\0\"x\"");

        assertTrue(parsed.isOk(), parsed.error());
        assertNull(parsed.frame().header().bodyLength());
    }

    @Test
    void negativeTtlParsesAsAlreadyExpired() {
        WireFrame.Parsed parsed = Wire.parseHeader("M01||t||-5|||\0body");

        assertTrue(parsed.isOk(), parsed.error());
        assertEquals(-5, parsed.frame().header().ttl());
    }

    /**
     * Written out rather than parameterized through CsvSource: several of these frames end in the body
     * separator, and any per-argument trimming would silently eat it and turn the case into a different
     * assertion than the one intended.
     */
    @Test
    void malformedFramesAreDroppableNotFatal() {
        assertDropped("M01|no-nul-separator", "missing body separator");
        assertDropped("M01||x|||||", "missing body separator");
        assertDropped("M02||x|||||\0body", "unknown frame tag");
        assertDropped("M01|too|few\0body", "truncated header");
        assertDropped("M01|xyz|t|||||\0body", "malformed guid");
        assertDropped("M01|ABEiM0RVZneImaq7zN3u|x|||||\0", "malformed guid");
        assertDropped("M01||t|notanumber||||\0body", "malformed time");
        assertDropped("M01||t|99999999999999999999||||\0body", "malformed time");
        assertDropped("M01||t||notanumber|||\0body", "malformed ttl");
        assertDropped("M01||||||||\0body", "empty type");
        assertDropped("M01|||||||\0", "empty type");
        assertDropped("M01||t|||||brotli\0body", "unknown body encoding");
        assertDropped("M01||x|||||deflate|\0", "unknown body encoding");
        assertDropped("M01||x||||||-1\0", "malformed body length");
    }

    private static void assertDropped(String raw, String expected) {
        WireFrame.Parsed parsed = Wire.parseHeader(raw);

        assertFalse(parsed.isOk(), () -> "expected '" + raw + "' to be rejected");
        assertEquals(expected, parsed.error(), () -> "for frame '" + raw + "'");
    }

    /**
     * The threshold is on the input size, never the compressed output — the assertion that keeps three
     * deflate implementations from framing the same message differently.
     */
    @Test
    void bodiesAtOrAboveTheThresholdAreGzipped() {
        String big = "x".repeat(Wire.COMPRESSION_THRESHOLD_BYTES);
        Wire.Prepared prepared = Wire.prepareBody(Header.of("bulk"), big);

        assertEquals(BodyEncoding.GZIP, prepared.header().encoding());
        assertEquals(prepared.body().length(), prepared.header().bodyLength());

        WireFrame.Parsed parsed = Wire.parseHeader(Wire.writeHeaderAndBody(prepared.header(), prepared.body()));

        assertTrue(parsed.isOk(), parsed.error());
        assertEquals("\"" + big + "\"", Wire.parseBody(parsed.frame()));
    }

    @Test
    void bodiesBelowTheThresholdStayRaw() {
        Wire.Prepared prepared = Wire.prepareBody(Header.of("small"), new Body("x"));

        assertEquals(BodyEncoding.RAW, prepared.header().encoding());
        assertEquals("{\"value\":\"x\"}", prepared.body());
        assertEquals(13, prepared.header().bodyLength());
    }

    @Test
    void undecodableBodyYieldsEmptyRatherThanThrowing() {
        WireFrame.Parsed parsed = Wire.parseHeader("M01||bulk|1||||gzip\0not-actually-gzip");

        assertTrue(parsed.isOk(), parsed.error());
        assertEquals("", Wire.parseBody(parsed.frame()));
    }

    @Test
    void nonJsonBodyParsesAsNullRatherThanThrowing() {
        WireFrame.Parsed parsed = Wire.parseHeader("M01||t|||||\0not json");

        assertTrue(parsed.isOk(), parsed.error());
        assertNull(Wire.parseBodyAsJson(parsed.frame()));
    }

    @Test
    void jsonBodyIsReadableAsATree() {
        WireFrame.Parsed parsed = Wire.parseHeader("M01||t|||||\0{\"a\":1}");

        assertTrue(parsed.isOk(), parsed.error());
        JsonNode body = Wire.parseBodyAsJson(parsed.frame());
        assertNotNull(body);
        assertEquals(1, body.get("a").asInt());
    }

    @Test
    void delimitersInHeaderFieldsAreRejectedOnSerialize() {
        assertThrows(IllegalArgumentException.class, () -> Wire.writeHeader(Header.of("bad|type")));
        assertThrows(IllegalArgumentException.class, () -> Wire.writeHeader(Header.of("t", Destination.ofPath("a/b", "c"))));
        assertThrows(IllegalArgumentException.class, () -> Wire.writeHeader(Header.of("t", Destination.ofPath("", "c"))));
        assertThrows(IllegalArgumentException.class, () -> Wire.serializeText(Header.of("t"), "has\0separator"));
    }

    private record Body(String value) {
    }

}
