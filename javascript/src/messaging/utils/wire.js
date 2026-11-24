import { stringify } from "../../utils/json.js";
import { Destination } from "../message.js";
import { decodeBase64Url, encodeBase64Url } from "./base64url.js";
import { decodeGuid, encodeGuid, GUID_CHARS } from "./guid.js";

/**
 * The wire protocol codec, mirroring runtime's Wire:
 *
 *   M01|to|type|time|ttl|from|replyTo|enc|bodyLength[|extras...]\0body
 *
 * A change to framing or field order in Wire.java has to land here too. test/wire.test.js pins the
 * same literals WireTest pins, which is what makes that checkable rather than hoped for.
 *
 * The format exists so the Broker can route by reading only the header: it scans to the body
 * separator, parses the prefix, and never looks past it. Fields sit in Broker read-order — `to`
 * decides control-plane vs forward and is the routing key, `type` says which namespace `to` is in,
 * time and ttl are the whole expiry check. `from` and `replyTo` are passthrough the Broker distrusts,
 * and enc and bodyLength describe a body it never touches.
 *
 * An empty segment means the field is absent, so there is deliberately no way to say "time genuinely
 * 0". An explicit ttl of 0 does serialize as "0" and parse back as 0, not null; the two must not be
 * collapsed.
 *
 * bodyLength is stamped but never read. It is a receive-side sizing hint for a fragmented socket.
 * Stamping it costs nothing and keeps frames byte-identical to the other implementations', which is
 * what makes them comparable at all.
 *
 * One thing differs from Wire.java, a place a change needs thought rather than transcription: body
 * decoding is async. gzip goes through DecompressionStream, the only inflate available without a
 * dependency, and it is stream-shaped. Header parsing stays synchronous, which is the half that
 * matters: nothing awaits anything to learn where a frame is going. Router's promise chain is what
 * keeps wire order despite the await.
 */

export const TAG = "M01";
export const FIELD_SEPARATOR = "|";
export const BODY_SEPARATOR = "\0";

/**
 * Bodies at or above this many UTF-8 bytes are gzipped.
 *
 * Decided on the *input* size, never the compressed output. An output-size comparison would have
 * this client and runtime frame the same message differently, since their deflate
 * implementations differ.
 */
export const COMPRESSION_THRESHOLD_BYTES = 1024;

/**
 * Body encoding, carried in the frame's enc segment.
 *
 * `prepareBody` decides it, `tryParseBody` reverses it. On the header rather than threaded as a
 * parameter, because it describes *this message*: a header built to send already knows how its body
 * is encoded, and a received one already carries how its body arrived.
 */
export const BodyEncoding = Object.freeze({
    Raw: "raw",
    Gzip: "gzip"
});

const GZIP_TOKEN = "gzip";

/**
 * Separates — and, as a leading character, introduces — the segments of an unresolved /channel/topic
 * destination.
 *
 * Safe as the discriminator because base64url's alphabet excludes it, so a leading '/' can never
 * begin an encoded Guid. `readDestination` is what relies on that.
 */
const PATH_SEPARATOR = "/";

/** tag + to, type, time, ttl, from, replyTo, enc. Everything past that is optional. */
const MIN_SEGMENTS = 8;

const ENCODER = new TextEncoder();
const DECODER = new TextDecoder();

// ── Parsing ─────────────────────────────────────────────────────────────────────────────────────

/**
 * Parses the header of raw without reading past the body separator, answering `{ frame, error }`
 * with exactly one of the two set.
 *
 * WebSocketConnection's message handler calls it for every inbound frame.
 *
 * Non-throwing by design, like runtime's Wire.parseHeader: a receive loop treats a malformed frame as
 * droppable and never fatal, so a bad frame must not cost an exception thrown out of a socket
 * callback.
 */
export function tryParseHeader(raw) {
    if (!raw.startsWith(`${TAG}${FIELD_SEPARATOR}`)) return { frame: null, error: "unknown frame tag" };

    const separator = raw.indexOf(BODY_SEPARATOR);
    if (separator < 0) return { frame: null, error: "missing body separator" };

    const segments = raw.substring(0, separator).split(FIELD_SEPARATOR);
    if (segments.length < MIN_SEGMENTS) return { frame: null, error: "truncated header" };

    const type = segments[2];
    if (type === "") return { frame: null, error: "empty type" };

    try {
        return {
            frame: {
                header: Object.freeze({
                    to: readDestination(segments[1]),
                    type,
                    time: readInteger(segments[3], "time") ?? 0,
                    ttl: readInteger(segments[4], "ttl"),
                    from: readGuid(segments[5]),
                    replyTo: readGuid(segments[6]),
                    encoding: readEncoding(segments[7]),
                    // Absent past an older sender's enc segment, exactly as if the field never
                    // existed. That is the whole point of it being optional.
                    bodyLength: segments.length > 8 ? readBodyLength(segments[8]) : null,
                    // Everything past the known fields travels verbatim, so a frame from a newer
                    // sender survives a round trip through this client intact.
                    extras: segments.slice(9).join(FIELD_SEPARATOR)
                }),
                body: raw.substring(separator + 1)
            },
            error: null
        };
    } catch (error) {
        return { frame: null, error: error instanceof Error ? error.message : String(error) };
    }
}

/**
 * Decodes an already-parsed frame's body, reversing its encoding.
 *
 * Total by design: an absent or undecodable body yields an empty string rather than throwing into a
 * receive loop.
 */
export async function parseBody(frame) {
    const { body } = frame;
    if (body === "") return "";
    if (frame.header.encoding !== BodyEncoding.Gzip) return body;

    try {
        return DECODER.decode(await gunzip(decodeBase64Url(body)));
    } catch {
        return "";
    }
}

/**
 * Decodes the body and parses it as JSON, answering undefined when it is empty or isn't valid JSON.
 *
 * The client's whole read path: Router's frame handler and every channel handler reach it.
 *
 * undefined as the failure signal is unambiguous because it is not a JSON value — a body of `null`
 * parses to `null`.
 */
export async function tryParseBody(frame) {
    const body = await parseBody(frame);
    if (body === "") return undefined;

    try {
        return JSON.parse(body);
    } catch {
        return undefined;
    }
}

// ── Serializing ─────────────────────────────────────────────────────────────────────────────────

/**
 * JSON-serializes body and compresses it past the threshold.
 *
 * Every send starts here, through Router.sendMessage. Answers the header re-stamped with the
 * encoding *and* bodyLength it produced, so the caller holds one value already agreeing with the
 * body rather than two facts to keep in step.
 *
 * The author of the encoding decision, so whatever header.encoding carried in is discarded rather
 * than consulted.
 */
export async function prepareBody(header, body) {
    const json = stringify(body);
    const bytes = ENCODER.encode(json);

    if (bytes.length < COMPRESSION_THRESHOLD_BYTES)
        return { header: { ...header, encoding: BodyEncoding.Raw, bodyLength: bytes.length }, body: json };

    // base64url is ASCII, so its char count *is* its byte count on the wire — which is what
    // bodyLength means for a compressed body.
    const compressed = encodeBase64Url(await gzip(bytes));
    return { header: { ...header, encoding: BodyEncoding.Gzip, bodyLength: compressed.length }, body: compressed };
}

/**
 * Renders header around an already wire-ready body — still compressed when header.encoding says so —
 * into one frame.
 *
 * Connection.send calls it, on what prepareBody produced.
 *
 * Validates the header only: a body out of prepareBody is JSON, and
 * JSON.stringify escapes U+0000, so there is nothing left to reject.
 */
export function writeHeaderAndBody(header, body) {
    return `${writeHeader(header)}${BODY_SEPARATOR}${body}`;
}

/**
 * Serializes a header and an already-textual body into one frame, compressing past the threshold.
 * header.encoding is overwritten with what this call decides.
 *
 * Named apart from serializeMessage because one name would bind a string body
 * to the JSON path and silently re-encode text that was already text.
 */
export async function serializeText(header, body) {
    if (body.includes(BODY_SEPARATOR)) throw new Error("Body must not contain a body separator ('\\0').");

    const bytes = ENCODER.encode(body);
    if (bytes.length < COMPRESSION_THRESHOLD_BYTES)
        return writeHeaderAndBody({ ...header, encoding: BodyEncoding.Raw, bodyLength: bytes.length }, body);

    const compressed = encodeBase64Url(await gzip(bytes));
    return writeHeaderAndBody({ ...header, encoding: BodyEncoding.Gzip, bodyLength: compressed.length }, compressed);
}

/** Serializes a header plus a body JSON-serialized from an arbitrary value. */
export async function serializeMessage(header, body) {
    const prepared = await prepareBody(header, body);
    return writeHeaderAndBody(prepared.header, prepared.body);
}

/**
 * Renders everything up to but not including the body separator.
 *
 * Validation rejects delimiters rather than escaping them, which is what keeps the Broker's parse a
 * plain scan. `|` stays unconstrained *inside* the body, since it only has meaning before the `\0`.
 */
function writeHeader(header) {
    validateHeaderFields(header);

    const segments = [
        TAG,
        writeDestination(header.to),
        header.type,
        header.time === 0 ? "" : String(header.time),
        header.ttl === null || header.ttl === undefined ? "" : String(header.ttl),
        writeGuid(header.from),
        writeGuid(header.replyTo),
        header.encoding === BodyEncoding.Gzip ? GZIP_TOKEN : "",
        header.bodyLength === null || header.bodyLength === undefined ? "" : String(header.bodyLength)
    ];

    if (header.extras) segments.push(header.extras);

    return segments.join(FIELD_SEPARATOR);
}

function validateHeaderFields(header) {
    if (!header.type) throw new Error("Type must not be empty.");
    if (/[|\0]/.test(header.type)) throw new Error("Type must not contain '|' or '\\0'.");

    // A path destination shares the `to` segment with base64url Guids, so it needs the same
    // delimiter discipline type does. An empty half would render a path parsing back as neither
    // thing — "//echo" reads as an absent channel — so it is rejected rather than round-tripped. The
    // channel additionally cannot contain '/', since the first one after the marker splits the
    // halves; the topic may, which keeps a later /channel/topic/sub form representable.
    if (header.to.channel !== null && header.to.channel !== undefined) {
        if (!header.to.channel || !header.to.topic) throw new Error("Destination channel and topic must not be empty.");
        if (/[|\0/]/.test(header.to.channel)) throw new Error("Destination channel must not contain '|', '\\0' or '/'.");
        if (/[|\0]/.test(header.to.topic)) throw new Error("Destination topic must not contain '|' or '\\0'.");
    }

    if (header.extras.includes(BODY_SEPARATOR)) throw new Error("Extras must not contain a body separator ('\\0').");
}

// ── Field codecs ────────────────────────────────────────────────────────────────────────────────

function writeDestination(to) {
    if (to.channel !== null && to.channel !== undefined) return `${PATH_SEPARATOR}${to.channel}${PATH_SEPARATOR}${to.topic}`;

    return writeGuid(to.id);
}

/**
 * Parses the `to` segment: empty is Broker-addressed, a leading '/' is an unresolved path, anything
 * else must be a base64url Guid.
 *
 * Both path halves must be non-empty, and the split is at the *first* '/' after the marker — a
 * channel name cannot contain one, so any later '/' belongs to the topic.
 */
function readDestination(segment) {
    if (segment === "") return Destination.empty;

    if (segment.startsWith(PATH_SEPARATOR)) {
        const rest = segment.substring(1);
        const split = rest.indexOf(PATH_SEPARATOR);
        if (split <= 0 || split === rest.length - 1) throw new Error("destination path must be /channel/topic");

        return Destination.fromPath(rest.substring(0, split), rest.substring(split + 1));
    }

    return Destination.fromId(readGuid(segment));
}

function writeGuid(value) {
    return value === null || value === undefined ? "" : encodeGuid(value);
}

function readGuid(segment) {
    if (segment === "") return null;

    try {
        return decodeGuid(segment);
    } catch {
        throw new Error("malformed guid");
    }
}

/** Decimal, invariant, leading sign allowed — a negative ttl must parse (already-expired). */
function readInteger(segment, field) {
    if (segment === "") return null;
    if (!/^-?\d+$/.test(segment)) throw new Error(`malformed ${field}`);

    return Number(segment);
}

/** Unsigned, unlike time/ttl — a negative byte count describes nothing. */
function readBodyLength(segment) {
    if (segment === "") return null;
    if (!/^\d+$/.test(segment)) throw new Error("malformed body length");

    return Number(segment);
}

function readEncoding(segment) {
    if (segment === "") return BodyEncoding.Raw;
    if (segment === GZIP_TOKEN) return BodyEncoding.Gzip;

    throw new Error("unknown body encoding");
}

// ── Compression ─────────────────────────────────────────────────────────────────────────────────

/**
 * Compression is app-level, deliberately not permessage-deflate: transport compression would force
 * the Broker to inflate a body it never reads and re-deflate it on every hop, whereas this way the
 * body crosses the Broker as an opaque span and only the recipient inflates.
 *
 * CompressionStream/DecompressionStream are the platform's own gzip, present in browsers and in
 * Node, which is what keeps this package dependency-free. They are the reason the body half of this
 * codec is async at all.
 */
async function gzip(bytes) {
    return through(bytes, new CompressionStream("gzip"));
}

async function gunzip(bytes) {
    return through(bytes, new DecompressionStream("gzip"));
}

async function through(bytes, transform) {
    const writer = transform.writable.getWriter();
    void writer.write(bytes);
    void writer.close();

    return new Uint8Array(await new Response(transform.readable).arrayBuffer());
}
