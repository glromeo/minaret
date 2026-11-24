package io.minaret.broker.wire;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

/**
 * Renders outbound frames, each into one pooled direct buffer holding the finished socket bytes: the
 * WebSocket frame header, then the M01 header, then the body.
 *
 * The WebSocket header is written here rather than left to Netty's encoder, so a frame is a single
 * buffer with no TextWebSocketFrame wrapper. A fanout then hands every subscriber a
 * retainedDuplicate of that one buffer (a pooled view, no copy), and the write goes straight past
 * WebSocket08FrameEncoder, which only encodes WebSocketFrame messages. Server-to-client frames are
 * never masked (RFC 6455 §5.1), so the header is a pure function of the length. WS_RESERVE bytes are
 * reserved in front and the header is back-filled once the length is known.
 *
 * Field order and rendering follow C#'s WriteHeaderBytes: empty means absent, a Time of 0 is not
 * written, a TTL is written whenever it is set, and extras ride after the bodyLength field.
 */
public final class FrameWriter {

    private static final int WS_RESERVE = 10;

    private static final int MAX_LONG_CHARS = 20;
    private static final int MAX_INT_CHARS = 11;

    /** The one long whose negation overflows, so writeLong cannot render it digit by digit. */
    private static final byte[] LONG_MIN = Wire.ascii(Long.toString(Long.MIN_VALUE));

    private FrameWriter() {
    }

    /**
     * The relay: frame's header re-stamped by Policy (To, Time, TTL, From) with everything else
     * carried through — type and extras as raw bytes, replyTo/enc/bodyLength re-rendered from their
     * parsed values, which is what C#'s `header with { ... }` re-render produces. The body is copied
     * across byte for byte and never decoded.
     *
     * Every forwarding path renders through this: Session's forwardToRoute, dispatch, notify and
     * publish, the last once for its whole fanout.
     */
    public static ByteBuf relay(ByteBufAllocator alloc, Frame frame, Uuid to, long time, int ttl, Uuid from) {
        ByteBuf in = frame.buffer();
        int bodyLength = frame.bodyLengthActual();
        int bound = WS_RESERVE + 4
                + Guids.ENCODED_LENGTH + 1
                + frame.typeLength() + 1
                + MAX_LONG_CHARS + 1
                + MAX_INT_CHARS + 1
                + Guids.ENCODED_LENGTH + 1
                + Guids.ENCODED_LENGTH + 1
                + Wire.GZIP.length + 1
                + MAX_INT_CHARS
                + 1 + frame.extrasLength()
                + 1 + bodyLength;

        ByteBuf out = begin(alloc, bound);

        Guids.encode(out, to.msb, to.lsb);
        out.writeByte(Wire.FIELD);
        out.writeBytes(in, frame.typeStart(), frame.typeLength());
        out.writeByte(Wire.FIELD);
        if (time != 0) writeLong(out, time);
        out.writeByte(Wire.FIELD);
        writeLong(out, ttl);
        out.writeByte(Wire.FIELD);
        if (from != null) Guids.encode(out, from.msb, from.lsb);
        out.writeByte(Wire.FIELD);
        Uuid replyTo = frame.replyTo();
        if (replyTo != null) Guids.encode(out, replyTo.msb, replyTo.lsb);
        out.writeByte(Wire.FIELD);
        if (frame.gzip()) out.writeBytes(Wire.GZIP);
        out.writeByte(Wire.FIELD);
        if (frame.hasBodyLength()) writeLong(out, frame.bodyLength());
        if (frame.extrasLength() > 0) {
            out.writeByte(Wire.FIELD);
            out.writeBytes(in, frame.extrasStart(), frame.extrasLength());
        }
        out.writeByte(Wire.BODY);
        out.writeBytes(in, frame.bodyStart(), bodyLength);

        return finish(out);
    }

    /**
     * A Broker-originated frame, always with From absent: every ack, topic.resolved, event and
     * event.replay. body is wire-ready, meaning already gzipped and base64url-encoded when gzip says
     * so (see Json.encode).
     *
     * Reached through Session.sendToSelf and Session.emit.
     */
    public static ByteBuf originate(ByteBufAllocator alloc, Uuid to, byte[] type, long time, Uuid replyTo, boolean gzip, byte[] body) {
        int bound = WS_RESERVE + 4
                + Guids.ENCODED_LENGTH + 1
                + type.length + 1
                + MAX_LONG_CHARS + 1
                + 1 // ttl, absent
                + 1 // from, absent
                + Guids.ENCODED_LENGTH + 1
                + Wire.GZIP.length + 1
                + MAX_INT_CHARS
                + 1 + body.length;

        ByteBuf out = begin(alloc, bound);

        if (to != null) Guids.encode(out, to.msb, to.lsb);
        out.writeByte(Wire.FIELD);
        out.writeBytes(type);
        out.writeByte(Wire.FIELD);
        if (time != 0) writeLong(out, time);
        out.writeByte(Wire.FIELD); // ttl
        out.writeByte(Wire.FIELD); // from
        out.writeByte(Wire.FIELD);
        if (replyTo != null) Guids.encode(out, replyTo.msb, replyTo.lsb);
        out.writeByte(Wire.FIELD);
        if (gzip) out.writeBytes(Wire.GZIP);
        out.writeByte(Wire.FIELD);
        writeLong(out, body.length);
        out.writeByte(Wire.BODY);
        out.writeBytes(body);

        return finish(out);
    }

    /** The M01 frame inside a buffer from this class, with the WebSocket header skipped. Tests only. */
    public static ByteBuf payload(ByteBuf frame) {
        int index = frame.readerIndex();
        int length = frame.getByte(index + 1) & 0x7f;
        int header = length < 126 ? 2 : length == 126 ? 4 : 10;
        return frame.slice(index + header, frame.readableBytes() - header);
    }

    private static ByteBuf begin(ByteBufAllocator alloc, int bound) {
        ByteBuf out = alloc.directBuffer(bound);
        out.writerIndex(WS_RESERVE);
        out.writeBytes(Wire.TAG);
        out.writeByte(Wire.FIELD);
        return out;
    }

    /** Back-fills the WebSocket header (FIN, text opcode, unmasked length) in front of the payload. */
    private static ByteBuf finish(ByteBuf out) {
        long length = out.writerIndex() - WS_RESERVE;
        int start;
        if (length < 126) {
            start = WS_RESERVE - 2;
            out.setByte(start + 1, (int) length);
        } else if (length <= 0xFFFF) {
            start = WS_RESERVE - 4;
            out.setByte(start + 1, 126);
            out.setShort(start + 2, (int) length);
        } else {
            start = 0;
            out.setByte(start + 1, 127);
            out.setLong(start + 2, length);
        }
        out.setByte(start, 0x81);
        return out.readerIndex(start);
    }

    /** Decimal ASCII, no allocation: digits are counted first, then written right to left. */
    static void writeLong(ByteBuf out, long value) {
        if (value == Long.MIN_VALUE) {
            out.writeBytes(LONG_MIN);
            return;
        }
        if (value < 0) {
            out.writeByte('-');
            value = -value;
        }

        int digits = 1;
        for (long v = value; v >= 10; v /= 10) digits++;

        int at = out.writerIndex() + digits;
        out.ensureWritable(digits);
        for (int i = 1; i <= digits; i++) {
            out.setByte(at - i, (int) ('0' + value % 10));
            value /= 10;
        }
        out.writerIndex(at);
    }

}
