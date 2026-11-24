package io.minaret.broker.routing;

import io.minaret.broker.wire.Uuid;

import java.security.SecureRandom;

/**
 * RFC 9562 version-7 ids: 48 bits of Unix milliseconds, then random bits. The JDK has
 * no equivalent.
 *
 * IdentityRegistry mints a RouteId this way for an identity seen for the first time. ChannelRegistry
 * mints a TopicId this way for every new topic. Both are time-ordered, so ids read in creation order
 * in logs and on the monitor.
 */
final class UuidV7 {

    private static final SecureRandom RANDOM = new SecureRandom();

    private UuidV7() {
    }

    static Uuid next() {
        long millis = System.currentTimeMillis();
        long msb = (millis << 16) | 0x7000L | (RANDOM.nextInt() & 0x0FFFL);
        long lsb = (RANDOM.nextLong() & 0x3FFFFFFFFFFFFFFFL) | 0x8000000000000000L;
        return new Uuid(msb, lsb);
    }

}
