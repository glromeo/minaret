package io.minaret.broker.wire;

import java.nio.charset.StandardCharsets;

/**
 * The M01 constants, as Runtime/Messaging/Utils/Wire.cs declares them. The frame is
 * M01|to|type|time|ttl|from|replyTo|enc|bodyLength[|extras...]\0body; field-by-field rules live on
 * Frame (read) and FrameWriter (write), the rationale in the wire-protocol skill.
 */
public final class Wire {

    public static final byte FIELD = '|';
    public static final byte BODY = 0;
    public static final byte PATH = '/';

    public static final byte[] TAG = ascii("M01");
    public static final byte[] GZIP = ascii("gzip");

    /** Compared against the *uncompressed* body size, never the compressed output. */
    public static final int COMPRESSION_THRESHOLD_BYTES = 1024;

    /** A header longer than this ends the connection rather than dropping the frame, as in C#. */
    public static final int MAX_HEADER_BYTES = 8 * 1024;

    public static final int MAX_BODY_BYTES = 8 * 1024 * 1024;

    /** The largest WebSocket message the Broker accepts: a maximal header, its separator, a maximal body. */
    public static final int MAX_MESSAGE_BYTES = MAX_HEADER_BYTES + 1 + MAX_BODY_BYTES;

    private Wire() {
    }

    public static byte[] ascii(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

}
