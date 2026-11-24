package io.minaret.broker.wire;

import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * The header encoding of a 128-bit id: base64url over its 16 bytes in RFC-4122 (big-endian) order,
 * unpadded, so exactly 22 characters. The same encoding as runtime's Guids, pinned by the
 * shared literal
 * 00112233-4455-6677-8899-aabbccddeeff ⇄ ABEiM0RVZneImaq7zN3u_w.
 *
 * Works straight between buffer bytes and the two longs, with no byte[] or String in between. 22
 * sextets are 132 bits: the first 64 are msb, the next 64 lsb, and the last 4 are padding, ignored on
 * read.
 *
 * Frame.parse reads every to/from/replyTo segment through decode. FrameWriter writes every id it
 * stamps — From, the resolved To, an echoed ReplyTo — through encode.
 */
public final class Guids {

    public static final int ENCODED_LENGTH = 22;

    private static final byte[] ALPHABET =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_".getBytes(StandardCharsets.US_ASCII);

    private static final byte[] SEXTET = new byte[128];

    static {
        Arrays.fill(SEXTET, (byte) -1);
        for (int i = 0; i < ALPHABET.length; i++) SEXTET[ALPHABET[i]] = (byte) i;
    }

    private Guids() {
    }

    /** Decodes 22 bytes at index into target. False when any byte is outside the base64url alphabet. */
    public static boolean decode(ByteBuf buf, int index, Uuid target) {
        long msb = 0;
        long lsb = 0;
        for (int i = 0; i < 10; i++) {
            int s = sextet(buf.getByte(index + i));
            if (s < 0) return false;
            msb = (msb << 6) | s;
        }

        int pivot = sextet(buf.getByte(index + 10));
        if (pivot < 0) return false;
        msb = (msb << 4) | (pivot >>> 2);
        lsb = pivot & 0x3;

        for (int i = 11; i < 21; i++) {
            int s = sextet(buf.getByte(index + i));
            if (s < 0) return false;
            lsb = (lsb << 6) | s;
        }

        int last = sextet(buf.getByte(index + 21));
        if (last < 0) return false;
        lsb = (lsb << 2) | (last >>> 4);

        target.set(msb, lsb);
        return true;
    }

    /** Writes the 22-byte encoding at the buffer's writer index. */
    public static void encode(ByteBuf buf, long msb, long lsb) {
        for (int shift = 58; shift >= 4; shift -= 6) buf.writeByte(ALPHABET[(int) (msb >>> shift) & 0x3f]);
        buf.writeByte(ALPHABET[(int) (((msb & 0xf) << 2) | (lsb >>> 62))]);
        for (int shift = 56; shift >= 2; shift -= 6) buf.writeByte(ALPHABET[(int) (lsb >>> shift) & 0x3f]);
        buf.writeByte(ALPHABET[(int) ((lsb & 0x3) << 4)]);
    }

    private static int sextet(byte b) {
        return b < 0 ? -1 : SEXTET[b];
    }

}
