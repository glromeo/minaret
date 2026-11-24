package io.minaret.messaging.wire;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.UUID;

/**
 * The wire encoding for a UUID: base64url over its 16 raw bytes in RFC-4122 (big-endian) order,
 * unpadded, so exactly 22 characters instead of 36.
 *
 * This is the one place the three runtimes are easiest to get silently wrong, and the reason it is a
 * type of its own rather than two inline calls. C# must ask for big-endian explicitly, because
 * Guid.ToByteArray() is little-endian for the first three fields — self-consistent within C#, but it
 * would not match anyone else. Java needs no such correction: getMostSignificantBits/
 * getLeastSignificantBits are already the RFC-4122 byte order, and ByteBuffer is big-endian by default.
 *
 * Pinned by the same literal WireTests and wire.test.js pin:
 * 00112233-4455-6677-8899-aabbccddeeff encodes to ABEiM0RVZneImaq7zN3u_w.
 */
public final class Guids {

    /**
     * base64url of 16 bytes, unpadded. A segment of any other length is not a UUID and is rejected
     * without even attempting to decode it.
     */
    public static final int ENCODED_LENGTH = 22;

    private static final int BYTES = 16;
    private static final Base64.Encoder ENCODER = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder DECODER = Base64.getUrlDecoder();

    private Guids() {
    }

    public static String encode(UUID value) {
        byte[] bytes = ByteBuffer.allocate(BYTES)
                .putLong(value.getMostSignificantBits())
                .putLong(value.getLeastSignificantBits())
                .array();

        return ENCODER.encodeToString(bytes);
    }

    /**
     * Decodes a to/from/replyTo segment, answering null when it is not a well-formed encoded UUID.
     * Callers distinguish "absent" (an empty segment, handled before this is reached) from
     * "malformed" (this answering null), because the two are a valid frame and a droppable one
     * respectively.
     */
    public static UUID decode(String segment) {
        if (segment.length() != ENCODED_LENGTH) return null;

        byte[] bytes;
        try {
            bytes = DECODER.decode(segment);
        } catch (IllegalArgumentException e) {
            return null;
        }

        if (bytes.length != BYTES) return null;

        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        return new UUID(buffer.getLong(), buffer.getLong());
    }

}
