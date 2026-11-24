package io.minaret.broker.routing;

import io.minaret.broker.wire.Frame;

/**
 * C#'s BrokerPolicy.Resolve: how long a frame may live, and when it was sent, resolved once per
 * data-plane frame. The rationale is in CLAUDE.md "BrokerPolicy".
 *
 * A mutable struct rather than a returned tuple. Session owns one and reads ttl, time and expired
 * straight after resolve, so the resolution allocates nothing. The From re-stamp that C# also does
 * here is Session's: it is just "the sender's RouteId", which Session already holds.
 */
final class Policy {

    static final int DEFAULT_TTL_SECONDS = 30;

    /** The resolved TTL. An absent TTL or an explicit 0 becomes the default; a negative one is kept and has already expired. */
    int ttl;

    /** The resolved Time, in the sender's clock. An unstamped 0 means "now" rather than 1970. */
    long time;

    boolean expired;

    void resolve(Frame frame, long clockOffsetMs, long nowMs) {
        ttl = !frame.hasTtl() || frame.ttl() == 0 ? DEFAULT_TTL_SECONDS : frame.ttl();
        long sentMs = frame.time() == 0 ? nowMs : frame.time() + clockOffsetMs;
        time = sentMs - clockOffsetMs;
        expired = nowMs > sentMs + ttl * 1000L;
    }

}
