package io.minaret.broker.wire;

import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

/**
 * One received frame, parsed in place: a flyweight over the message's pooled buffer that records
 * where each header field lies and decodes only what routing reads — the ids into probe Uuids and
 * the numbers into primitives. Nothing is copied and no String is built.
 *
 * Session owns one and re-parses it for every message on its connection. That is safe because a
 * Netty channel handles its messages one at a time on its own event loop. The one asynchronous verb,
 * channel.resolve, copies out what it needs before it yields. Offsets index the buffer passed to
 * parse, which stays valid only until Session releases it.
 *
 * The grammar and error strings are those of C#'s TryParseHeaderFields and ReceiveFrameAsync,
 * checked in the same order, so the two Brokers drop the same frames with the same reason.
 * WireTest pins the literals.
 */
public final class Frame {

    /** parse's answer for a header past MAX_HEADER_BYTES; fatal() is true alongside it. */
    public static final String FATAL_HEADER_TOO_LONG = "header exceeds " + Wire.MAX_HEADER_BYTES + " bytes";

    /** What the To field holds: nothing (a Broker-addressed verb), a 22-char id, or a /channel/topic path. */
    public enum ToKind { EMPTY, ID, PATH }

    private ByteBuf buf;
    private boolean fatal;

    private ToKind toKind;
    private final Uuid to = new Uuid();
    private int channelStart, channelEnd, topicStart, topicEnd;

    private int typeStart, typeEnd;

    private long time;
    private boolean hasTtl;
    private int ttl;

    private boolean hasFrom;
    private final Uuid from = new Uuid();

    private boolean hasReplyTo;
    private final Uuid replyTo = new Uuid();

    private boolean gzip;
    private boolean hasBodyLength;
    private int bodyLength;

    private int extrasStart, extrasEnd;
    private int bodyStart, bodyEnd;

    /**
     * Parses message, which is the whole WebSocket message from readerIndex to writerIndex.
     *
     * null means the frame parsed; otherwise the answer is why not. When fatal() is then true, the
     * header outgrew MAX_HEADER_BYTES, which C# raises as a WebSocketException that ends the
     * connection. Otherwise the frame is dropped.
     *
     * A message with no body separator is a header-only frame, as in ReceiveFrameAsync, not a
     * "missing body separator" error.
     */
    public String parse(ByteBuf message) {
        buf = message;
        fatal = false;
        int start = message.readerIndex();
        int end = message.writerIndex();

        int separator = message.indexOf(start, end, Wire.BODY);
        int headEnd = separator < 0 ? end : separator;
        if (headEnd - start > Wire.MAX_HEADER_BYTES) {
            fatal = true;
            return FATAL_HEADER_TOO_LONG;
        }

        bodyStart = separator < 0 ? end : separator + 1;
        bodyEnd = end;

        if (headEnd - start < 4 || !startsWith(start, Wire.TAG) || message.getByte(start + 3) != Wire.FIELD)
            return "unknown frame tag";

        // Six separators bound the first six fields; the seventh range runs to headEnd and holds
        // enc, bodyLength and extras, peeled off below. That is SplitAny into seven ranges.
        int f0 = start + 4;
        int f1 = next(f0, headEnd);
        int f2 = f1 < 0 ? -1 : next(f1, headEnd);
        int f3 = f2 < 0 ? -1 : next(f2, headEnd);
        int f4 = f3 < 0 ? -1 : next(f3, headEnd);
        int f5 = f4 < 0 ? -1 : next(f4, headEnd);
        int f6 = f5 < 0 ? -1 : next(f5, headEnd);
        if (f6 < 0) return "truncated header";

        typeStart = f1;
        typeEnd = f2 - 1;
        if (typeEnd == typeStart) return "empty type";

        String error = readDestination(f0, f1 - 1);
        if (error != null) return error;

        error = readTime(f2, f3 - 1);
        if (error != null) return error;

        error = readTtl(f3, f4 - 1);
        if (error != null) return error;

        hasFrom = f5 - 1 > f4;
        if (hasFrom && !readGuid(f4, f5 - 1, from)) return "malformed guid";

        hasReplyTo = f6 - 1 > f5;
        if (hasReplyTo && !readGuid(f5, f6 - 1, replyTo)) return "malformed guid";

        int encodingEnd = message.indexOf(f6, headEnd, Wire.FIELD);
        error = readEncoding(f6, encodingEnd < 0 ? headEnd : encodingEnd);
        if (error != null) return error;

        hasBodyLength = false;
        extrasStart = extrasEnd = headEnd;
        if (encodingEnd >= 0) {
            int lengthStart = encodingEnd + 1;
            int lengthEnd = message.indexOf(lengthStart, headEnd, Wire.FIELD);
            error = readBodyLength(lengthStart, lengthEnd < 0 ? headEnd : lengthEnd);
            if (error != null) return error;

            if (lengthEnd >= 0) extrasStart = lengthEnd + 1;
        }

        return null;
    }

    /** Whether the last parse failed in a way the connection cannot survive. */
    public boolean fatal() {
        return fatal;
    }

    public ByteBuf buffer() {
        return buf;
    }

    public ToKind toKind() {
        return toKind;
    }

    /** The To id, a probe overwritten by the next parse; meaningful only when toKind() is ID. */
    public Uuid to() {
        return to;
    }

    /** Time as sent, 0 when absent. */
    public long time() {
        return time;
    }

    public boolean hasTtl() {
        return hasTtl;
    }

    public int ttl() {
        return ttl;
    }

    /** The sender's From claim, a probe, or null when absent. */
    public Uuid from() {
        return hasFrom ? from : null;
    }

    /** The ReplyTo correlation id, a probe, or null when absent. Copy it before storing it. */
    public Uuid replyTo() {
        return hasReplyTo ? replyTo : null;
    }

    public boolean gzip() {
        return gzip;
    }

    public boolean hasBodyLength() {
        return hasBodyLength;
    }

    /** The declared bodyLength field; bodyLengthActual() is what arrived. */
    public int bodyLength() {
        return bodyLength;
    }

    public boolean typeIs(byte[] verb) {
        int length = typeEnd - typeStart;
        if (length != verb.length) return false;
        for (int i = 0; i < length; i++)
            if (buf.getByte(typeStart + i) != verb[i]) return false;
        return true;
    }

    public int typeStart() {
        return typeStart;
    }

    public int typeLength() {
        return typeEnd - typeStart;
    }

    /** The verb as text. Logging and tests only; the dispatch compares bytes with typeIs. */
    public String type() {
        return buf.toString(typeStart, typeEnd - typeStart, StandardCharsets.UTF_8);
    }

    /** The path's channel half. Allocates; read only on the path-addressed slow path. */
    public String channel() {
        return buf.toString(channelStart, channelEnd - channelStart, StandardCharsets.UTF_8);
    }

    /** The path's topic half: everything after the first '/' past the marker. Allocates. */
    public String topic() {
        return buf.toString(topicStart, topicEnd - topicStart, StandardCharsets.UTF_8);
    }

    public int extrasStart() {
        return extrasStart;
    }

    public int extrasLength() {
        return extrasEnd - extrasStart;
    }

    public int bodyStart() {
        return bodyStart;
    }

    public int bodyLengthActual() {
        return bodyEnd - bodyStart;
    }

    private int next(int from, int headEnd) {
        int at = buf.indexOf(from, headEnd, Wire.FIELD);
        return at < 0 ? -1 : at + 1;
    }

    private boolean startsWith(int index, byte[] prefix) {
        for (int i = 0; i < prefix.length; i++)
            if (buf.getByte(index + i) != prefix[i]) return false;
        return true;
    }

    private String readDestination(int start, int end) {
        if (start == end) {
            toKind = ToKind.EMPTY;
            return null;
        }

        if (buf.getByte(start) == Wire.PATH) {
            int rest = start + 1;
            int split = buf.indexOf(rest, end, Wire.PATH);
            if (split <= rest || split == end - 1) return "destination path must be /channel/topic";

            toKind = ToKind.PATH;
            channelStart = rest;
            channelEnd = split;
            topicStart = split + 1;
            topicEnd = end;
            return null;
        }

        if (!readGuid(start, end, to)) return "malformed guid";
        toKind = ToKind.ID;
        return null;
    }

    private boolean readGuid(int start, int end, Uuid target) {
        return end - start == Guids.ENCODED_LENGTH && Guids.decode(buf, start, target);
    }

    private String readTime(int start, int end) {
        time = 0;
        if (start == end) return null;

        // long.TryParse with AllowLeadingSign: one optional sign, at least one digit, no overflow.
        boolean negative = false;
        int i = start;
        byte first = buf.getByte(i);
        if (first == '-' || first == '+') {
            negative = first == '-';
            i++;
        }
        if (i == end) return "malformed time";

        long value = 0;
        for (; i < end; i++) {
            int digit = buf.getByte(i) - '0';
            if (digit < 0 || digit > 9) return "malformed time";
            if (value < (Long.MIN_VALUE + digit) / 10) return "malformed time";
            value = value * 10 - digit;
        }

        if (!negative) {
            if (value == Long.MIN_VALUE) return "malformed time";
            value = -value;
        }

        time = value;
        return null;
    }

    private String readTtl(int start, int end) {
        hasTtl = false;
        if (start == end) return null;

        // Negative TTLs parse: Policy honors them as already expired.
        boolean negative = false;
        int i = start;
        byte first = buf.getByte(i);
        if (first == '-' || first == '+') {
            negative = first == '-';
            i++;
        }
        if (i == end) return "malformed ttl";

        long value = 0;
        for (; i < end; i++) {
            int digit = buf.getByte(i) - '0';
            if (digit < 0 || digit > 9) return "malformed ttl";
            value = value * 10 + digit;
            if (value > (negative ? -(long) Integer.MIN_VALUE : Integer.MAX_VALUE)) return "malformed ttl";
        }

        hasTtl = true;
        ttl = (int) (negative ? -value : value);
        return null;
    }

    private String readEncoding(int start, int end) {
        gzip = false;
        if (start == end) return null;
        if (end - start == Wire.GZIP.length && startsWith(start, Wire.GZIP)) {
            gzip = true;
            return null;
        }
        return "unknown body encoding";
    }

    private String readBodyLength(int start, int end) {
        if (start == end) return null;

        // NumberStyles.None: digits only, no sign.
        long value = 0;
        for (int i = start; i < end; i++) {
            int digit = buf.getByte(i) - '0';
            if (digit < 0 || digit > 9) return "malformed body length";
            value = value * 10 + digit;
            if (value > Integer.MAX_VALUE) return "malformed body length";
        }

        // Rejected on the header as well as capped at the aggregator, so an oversized frame is
        // dropped for its declared size.
        if (value > Wire.MAX_BODY_BYTES) return "body length exceeds the maximum frame size";

        hasBodyLength = true;
        bodyLength = (int) value;
        return null;
    }

}
