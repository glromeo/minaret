package io.minaret.messaging.wire;

import com.fasterxml.jackson.databind.JsonNode;
import io.minaret.messaging.Destination;
import io.minaret.messaging.Header;
import io.minaret.util.Json;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.UUID;
import java.util.zip.ZipException;

/**
 * The M01 wire protocol codec: M01|to|type|time|ttl|from|replyTo|enc|bodyLength[|extras...]\0body — a
 * \0-split, |-delimited text frame. Browsers are first-class clients, so binary frames are out.
 *
 * The format exists so the Broker can route a frame by reading only its header: it scans to the body
 * separator, parses the prefix, and never looks past it. Fields are in Broker read-order — to decides
 * control-plane versus forward and is the routing key, type is the verb that says which namespace to is
 * in, and time/ttl are the whole expiry check. from and replyTo are passthrough the Broker distrusts,
 * and enc/bodyLength describe the body, which the Broker never touches.
 *
 * An empty segment means the field is absent. There is deliberately no way to express "time genuinely
 * 0". bodyLength is the one optional field: a sender that knows its exact body byte count stamps it as a
 * receive-side buffer-sizing hint, and an older sender that omits it produces an equally valid frame.
 *
 * One of three implementations of this format, with broker's Frame/FrameWriter and
 * javascript/src/messaging/utils/wire.js. A change here has to land in all of them, and WireTest
 * pins the same literals their tests pin — that is what turns "written three times"
 * into something checkable. The wire-protocol skill lists them.
 */
public final class Wire {

    public static final String TAG = "M01";
    public static final char FIELD_SEPARATOR = '|';
    public static final char BODY_SEPARATOR = '\0';

    /**
     * Bodies at or above this many UTF-8 bytes are gzipped. The decision is made on the input size, never
     * on the compressed output — an output-size comparison would make two runtimes with different deflate
     * implementations frame the same message differently, which is precisely what this codec existing
     * three times cannot afford.
     */
    public static final int COMPRESSION_THRESHOLD_BYTES = 1024;

    private static final String GZIP_TOKEN = "gzip";

    /**
     * Separates — and, as a leading character, introduces — the segments of an unresolved /channel/topic
     * destination. Safe as a discriminator because base64url's alphabet (A-Z a-z 0-9 - _) excludes it, so
     * a leading '/' can never begin an encoded UUID.
     */
    private static final char PATH_SEPARATOR = '/';

    /**
     * to, type, time, ttl, from, replyTo, and the tail holding enc/bodyLength/extras together.
     */
    private static final int SPLIT_FIELDS = 7;

    private static final Base64.Encoder BASE64URL = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder BASE64URL_DECODER = Base64.getUrlDecoder();

    private Wire() {
    }

    /**
     * Parses the header of raw without reading past the body separator.
     *
     * WebSocketConnection calls it for every inbound frame, once its fragments are stitched.
     *
     * Answers a failure rather than throwing: a receive loop treats a malformed frame as droppable and
     * never fatal, and untrusted input should not cost an exception.
     */
    public static WireFrame.Parsed parseHeader(String raw) {
        int separator = raw.indexOf(BODY_SEPARATOR);
        if (separator < 0) return WireFrame.Parsed.failed("missing body separator");

        String head = raw.substring(0, separator);
        String body = raw.substring(separator + 1);

        return parseHeaderFields(head, body);
    }

    private static WireFrame.Parsed parseHeaderFields(String head, String body) {
        if (!head.startsWith(TAG) || head.length() < TAG.length() + 1 || head.charAt(TAG.length()) != FIELD_SEPARATOR)
            return WireFrame.Parsed.failed("unknown frame tag");

        // Every field but the last must be separator-terminated; the trailing one runs to the end of the
        // header, and anything past it is a field a later version added. A limit of SPLIT_FIELDS is what
        // leaves enc/bodyLength/extras together in the final element, to be peeled off one at a time
        // below — which is how a frame from an older sender that never stamped bodyLength still parses.
        String[] fields = head.substring(TAG.length() + 1).split("\\|", SPLIT_FIELDS);
        if (fields.length < SPLIT_FIELDS) return WireFrame.Parsed.failed("truncated header");

        String type = fields[1];
        if (type.isEmpty()) return WireFrame.Parsed.failed("empty type");

        Destination to;
        try {
            to = readDestination(fields[0]);
        } catch (MalformedField e) {
            return WireFrame.Parsed.failed(e.getMessage());
        }

        long time;
        Integer ttl;
        UUID from;
        UUID replyTo;
        BodyEncoding encoding;
        Integer bodyLength = null;
        String extras = "";

        try {
            time = readTime(fields[2]);
            ttl = readTtl(fields[3]);
            from = readGuid(fields[4]);
            replyTo = readGuid(fields[5]);

            String tail = fields[6];
            int encodingEnd = tail.indexOf(FIELD_SEPARATOR);
            encoding = readEncoding(encodingEnd < 0 ? tail : tail.substring(0, encodingEnd));

            if (encodingEnd >= 0) {
                tail = tail.substring(encodingEnd + 1);
                int bodyLengthEnd = tail.indexOf(FIELD_SEPARATOR);
                bodyLength = readBodyLength(bodyLengthEnd < 0 ? tail : tail.substring(0, bodyLengthEnd));

                if (bodyLengthEnd >= 0) extras = tail.substring(bodyLengthEnd + 1);
            }
        } catch (MalformedField e) {
            return WireFrame.Parsed.failed(e.getMessage());
        }

        Header header = new Header(from, to, type, replyTo, time, ttl, encoding, bodyLength, extras);
        return WireFrame.Parsed.ok(new WireFrame(header, body));
    }

    /**
     * Decodes an already-parsed frame's body, reversing its encoding. Total by design — an absent or
     * undecodable body yields an empty string rather than throwing into a receive loop.
     */
    public static String parseBody(WireFrame frame) {
        String body = frame.body();
        if (body.isEmpty()) return "";
        if (frame.header().encoding() != BodyEncoding.GZIP) return body;

        try {
            return Gzip.decompressUtf8(BASE64URL_DECODER.decode(body));
        } catch (IllegalArgumentException | ZipException e) {
            return "";
        }
    }

    /**
     * Decodes the body and parses it as JSON, answering null when it is empty or is not valid JSON.
     */
    public static JsonNode parseBodyAsJson(WireFrame frame) {
        return Json.tryParse(parseBody(frame), JsonNode.class);
    }

    /**
     * Decodes the body and deserializes it as T, answering null when it is empty or is not valid JSON
     * for T.
     *
     * Router uses it for the registration ack's ClientInfo; ChannelBase's ack helpers use it for every
     * BrokerAck.
     */
    public static <T> T parseBodyAs(WireFrame frame, Class<T> type) {
        return Json.tryParse(parseBody(frame), type);
    }

    /**
     * JSON-serializes body and compresses it past the threshold.
     *
     * Every send starts here, through Router.sendMessage. Answers the header re-stamped with the
     * encoding *and* bodyLength it produced, so the caller holds one value already agreeing with the
     * body rather than two facts to keep in step.
     *
     * bodyLength is stamped even though nothing in this runtime reads it — a WebSocket already
     * delivers whole messages — purely so a frame from this client is byte-identical to the others'.
     * That is what makes the three implementations comparable at all.
     */
    public static Prepared prepareBody(Header header, Object body) {
        String json = Json.stringify(body);
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);

        if (bytes.length < COMPRESSION_THRESHOLD_BYTES)
            return new Prepared(header.withBody(BodyEncoding.RAW, bytes.length), json);

        String compressed = BASE64URL.encodeToString(Gzip.compress(bytes));
        // The length stamped is the compressed body's own byte count — base64url is ASCII, so its char count is its byte count.
        return new Prepared(header.withBody(BodyEncoding.GZIP, compressed.length()), compressed);
    }

    /**
     * Serializes a header and an already-textual body into one frame, compressing when the body is at or
     * above the threshold.
     *
     * Named apart from a body-serializing overload deliberately: one overloaded name would silently
     * JSON-encode a body that was already text.
     */
    public static String serializeText(Header header, String body) {
        if (body.indexOf(BODY_SEPARATOR) >= 0)
            throw new IllegalArgumentException("Body must not contain a body separator ('\\0').");

        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        if (bytes.length < COMPRESSION_THRESHOLD_BYTES)
            return writeHeaderAndBody(header.withBody(BodyEncoding.RAW, bytes.length), body);

        String compressed = BASE64URL.encodeToString(Gzip.compress(bytes));
        return writeHeaderAndBody(header.withBody(BodyEncoding.GZIP, compressed.length()), compressed);
    }

    public static String serializeMessage(Header header, Object body) {
        Prepared prepared = prepareBody(header, body);
        return writeHeaderAndBody(prepared.header(), prepared.body());
    }

    /**
     * Renders header around an already wire-ready body — still compressed when header.encoding says so.
     *
     * Connection.send calls it, on what prepareBody produced.
     *
     * Validates the header only: a body out of prepareBody or serializeText is already known
     * well-formed, and re-checking per send would repeat work the frame already passed.
     */
    public static String writeHeaderAndBody(Header header, String body) {
        return writeHeader(header) + BODY_SEPARATOR + body;
    }

    /**
     * Renders everything up to but not including the body separator. Rejects delimiters in type, the
     * destination and extras rather than escaping them — which is what keeps the parser a plain scan on
     * the Broker's routing hot path.
     */
    public static String writeHeader(Header header) {
        validate(header);

        StringBuilder out = new StringBuilder(96);
        out.append(TAG);
        out.append(FIELD_SEPARATOR);
        appendDestination(out, header.to());
        out.append(FIELD_SEPARATOR).append(header.type());
        out.append(FIELD_SEPARATOR);
        if (header.time() != 0) out.append(header.time());
        out.append(FIELD_SEPARATOR);
        if (header.ttl() != null) out.append(header.ttl().intValue());
        out.append(FIELD_SEPARATOR);
        if (header.from() != null) out.append(Guids.encode(header.from()));
        out.append(FIELD_SEPARATOR);
        if (header.replyTo() != null) out.append(Guids.encode(header.replyTo()));
        out.append(FIELD_SEPARATOR);
        if (header.encoding() == BodyEncoding.GZIP) out.append(GZIP_TOKEN);
        out.append(FIELD_SEPARATOR);
        if (header.bodyLength() != null) out.append(header.bodyLength().intValue());
        if (!header.extras().isEmpty()) out.append(FIELD_SEPARATOR).append(header.extras());

        return out.toString();
    }

    private static void validate(Header header) {
        if (header.type().isEmpty()) throw new IllegalArgumentException("Type must not be empty.");
        if (containsAny(header.type(), FIELD_SEPARATOR, BODY_SEPARATOR))
            throw new IllegalArgumentException("Type must not contain '|' or '\\0'.");

        // A path destination shares the to segment with base64url UUIDs, so it needs the same delimiter
        // freedom type does. An empty half would render a path that parses back as neither one thing nor
        // the other ("//topic" reads as an absent channel), so reject it rather than round-trip it. The
        // channel name additionally cannot contain the path separator, since the first '/' after the
        // marker is what splits the two halves; the topic may, so a later /channel/topic/sub form stays
        // representable.
        Destination to = header.to();
        if (to.isPath()) {
            if (to.channel().isEmpty() || to.topic() == null || to.topic().isEmpty())
                throw new IllegalArgumentException("Destination channel and topic must not be empty.");

            if (containsAny(to.channel(), FIELD_SEPARATOR, BODY_SEPARATOR, PATH_SEPARATOR))
                throw new IllegalArgumentException("Destination channel must not contain '|', '\\0' or '/'.");

            if (containsAny(to.topic(), FIELD_SEPARATOR, BODY_SEPARATOR))
                throw new IllegalArgumentException("Destination topic must not contain '|' or '\\0'.");
        }

        if (header.extras().indexOf(BODY_SEPARATOR) >= 0)
            throw new IllegalArgumentException("Extras must not contain a body separator ('\\0').");
    }

    private static boolean containsAny(String value, char... chars) {
        for (int i = 0; i < value.length(); i++)
            for (char c : chars)
                if (value.charAt(i) == c) return true;

        return false;
    }

    private static void appendDestination(StringBuilder out, Destination value) {
        if (value.isPath()) {
            out.append(PATH_SEPARATOR).append(value.channel()).append(PATH_SEPARATOR).append(value.topic());
            return;
        }

        if (value.id() != null) out.append(Guids.encode(value.id()));
    }

    /**
     * Parses the to segment: empty means Broker-addressed, a leading '/' means an unresolved
     * /channel/topic path, anything else must be a base64url UUID.
     *
     * Both path segments must be non-empty. A channel name cannot contain '/' (it is the separator), so
     * the split is the first one after the leading marker and any '/' in the remainder belongs to the
     * topic name — which keeps a later /channel/topic/sub form parseable by this same rule.
     */
    private static Destination readDestination(String segment) throws MalformedField {
        if (segment.isEmpty()) return Destination.EMPTY;

        if (segment.charAt(0) == PATH_SEPARATOR) {
            String rest = segment.substring(1);
            int split = rest.indexOf(PATH_SEPARATOR);

            if (split <= 0 || split == rest.length() - 1)
                throw new MalformedField("destination path must be /channel/topic");

            return Destination.ofPath(rest.substring(0, split), rest.substring(split + 1));
        }

        return Destination.ofId(readGuid(segment));
    }

    private static UUID readGuid(String segment) throws MalformedField {
        if (segment.isEmpty()) return null;

        UUID value = Guids.decode(segment);
        if (value == null) throw new MalformedField("malformed guid");

        return value;
    }

    private static long readTime(String segment) throws MalformedField {
        if (segment.isEmpty()) return 0;
        try {
            return Long.parseLong(segment);
        } catch (NumberFormatException e) {
            throw new MalformedField("malformed time");
        }
    }

    private static Integer readTtl(String segment) throws MalformedField {
        if (segment.isEmpty()) return null;
        try {
            // Negative TTLs parse: BrokerPolicy honors them as already-expired.
            return Integer.valueOf(segment);
        } catch (NumberFormatException e) {
            throw new MalformedField("malformed ttl");
        }
    }

    private static BodyEncoding readEncoding(String segment) throws MalformedField {
        if (segment.isEmpty()) return BodyEncoding.RAW;
        if (segment.equals(GZIP_TOKEN)) return BodyEncoding.GZIP;

        throw new MalformedField("unknown body encoding");
    }

    private static Integer readBodyLength(String segment) throws MalformedField {
        if (segment.isEmpty()) return null;

        // Unsigned, unlike time and ttl: a sign here is malformed, not a negative length.
        char first = segment.charAt(0);
        if (first == '-' || first == '+') throw new MalformedField("malformed body length");

        try {
            return Integer.valueOf(segment);
        } catch (NumberFormatException e) {
            throw new MalformedField("malformed body length");
        }
    }

    /**
     * A header and body that already agree with each other — what {@link #prepareBody} answers so a
     * caller holds one self-consistent value instead of a body plus a separate encoding to remember
     * about it.
     */
    public record Prepared(Header header, String body) {
    }

    /**
     * Signals a field that could not be parsed, carrying the same wording the other two runtimes use so
     * a droppable frame is described identically wherever it is dropped. Package-private and never
     * escapes: every entry point converts it into a failed parse result.
     */
    private static final class MalformedField extends Exception {

        MalformedField(String message) {
            super(message);
        }

    }

}
