package io.minaret.messaging.wire;

/**
 * How a body is encoded on the wire, carried in the frame's enc segment so the receive side can reverse
 * it. Lives on Header itself rather than as a parameter threaded alongside it — see that record for why.
 */
public enum BodyEncoding {

    RAW,

    /**
     * gzipped, then base64url'd. Still compressed when relayed: the Broker never inflates a body it does
     * not read.
     */
    GZIP

}
