package io.minaret.messaging.wire;

import io.minaret.messaging.Header;

/**
 * A parsed header plus the body exactly as it arrived — still base64url'd gzip when the header says so.
 *
 * The body is a String rather than bytes, matching the JavaScript client and diverging from the
 * Broker's byte path. That path exists so the Broker can relay a body it never decodes and stitch socket fragments;
 * nothing here relays, because a client is always its body's actual consumer.
 */
public record WireFrame(Header header, String body) {

    /**
     * The outcome of parsing a frame: exactly one of frame and error is set. A parse failure is
     * droppable, never connection-fatal, which is why this is a returned value rather than an exception
     * — untrusted input should not cost a stack unwind on the receive loop.
     */
    public record Parsed(WireFrame frame, String error) {

        public static Parsed ok(WireFrame frame) {
            return new Parsed(frame, null);
        }

        public static Parsed failed(String error) {
            return new Parsed(null, error);
        }

        public boolean isOk() {
            return frame != null;
        }

    }

}
