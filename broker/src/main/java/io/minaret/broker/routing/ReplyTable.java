package io.minaret.broker.routing;

import io.minaret.broker.wire.Uuid;
import io.minaret.broker.wire.UuidTable;

/**
 * correlation id → the route awaiting its reply: ChannelRegistry's pendingReplies.
 *
 * Session.handleDispatch claims an id on the requester's event loop and takes it on the provider's,
 * so the table is shared across threads. It is split into stripes, each an UuidTable under its own
 * monitor. Two loops rarely meet on one stripe, and neither a claim nor a take allocates: the id is
 * stored as its two longs, never copied into a key object, and the route is the session's own
 * RouteId instance.
 */
final class ReplyTable {

    private static final int STRIPE_BITS = 6;

    @SuppressWarnings("unchecked")
    private final UuidTable<Uuid>[] stripes = new UuidTable[1 << STRIPE_BITS];

    ReplyTable() {
        for (int i = 0; i < stripes.length; i++) stripes[i] = new UuidTable<>();
    }

    /** Claims correlationId for route, answering whether route now holds it. Re-claiming by the holder is idempotent. */
    boolean claim(Uuid correlationId, Uuid route) {
        UuidTable<Uuid> stripe = stripe(correlationId);
        synchronized (stripe) {
            Uuid holder = stripe.putIfAbsent(correlationId, route);
            return holder == null || holder.equals(route);
        }
    }

    /** Consumes the claim: the route awaiting correlationId, or null. */
    Uuid take(Uuid correlationId) {
        UuidTable<Uuid> stripe = stripe(correlationId);
        synchronized (stripe) {
            return stripe.remove(correlationId);
        }
    }

    /** Drops every claim route holds, when its connection ends. */
    void removeRoute(Uuid route) {
        for (UuidTable<Uuid> stripe : stripes)
            synchronized (stripe) {
                stripe.removeValue(route);
            }
    }

    /** By the hash's top bits, so a stripe's own slots, picked by the low bits, still spread. */
    private UuidTable<Uuid> stripe(Uuid id) {
        return stripes[(int) (UuidTable.hash(id.msb(), id.lsb()) >>> (64 - STRIPE_BITS))];
    }

}
