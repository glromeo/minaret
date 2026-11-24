package io.minaret.broker.routing;

import io.minaret.broker.routing.ChannelRegistry.SubscriptionNode;
import io.minaret.broker.wire.Uuid;
import io.minaret.broker.wire.UuidTable;
import jakarta.inject.Singleton;

import java.util.ArrayList;
import java.util.List;

/**
 * RouteId → the Session currently holding it, through a Route that outlives any one session.
 *
 * Every routing path calls get with one of the frame's probe Uuids: one lock-free UuidTable read
 * plus one volatile field, no allocation. The table is copy-on-write, replaced under this monitor
 * only when a RouteId is seen for the first time.
 *
 * A Route is never removed. RouteIds are stable per identity (IdentityRegistry never forgets one
 * either), so the table is bounded by the identities ever seen, and a reference to a Route can
 * never go stale. That is what lets a SubscriptionNode hold its subscriber's Route, and the fanout
 * read route.session instead of hashing the RouteId once per subscriber.
 */
@Singleton
public final class ConnectionRegistry {

    private final BrokerMetrics metrics;
    private volatile UuidTable<Route> routes = new UuidTable<>();

    public ConnectionRegistry(BrokerMetrics metrics) {
        this.metrics = metrics;
    }

    /**
     * One RouteId's anchor. session is the connection holding it now, null between connections.
     * subscriptions heads the RouteId's own list of SubscriptionNodes; it belongs to
     * ChannelRegistry and is guarded by ChannelRegistry's lock.
     */
    static final class Route {

        final Uuid id;
        volatile Session session;
        SubscriptionNode subscriptions;

        private Route(Uuid id) {
            this.id = id;
        }

    }

    /** An unconditional overwrite: a newer session under the same RouteId replaces the older one. */
    synchronized void register(Uuid routeId, Session session) {
        route(routeId).session = session;
        metrics.connectionRegistered();
    }

    /**
     * Clears the route only if it is still this session, and answers whether it was. Teardown gates
     * every RouteId-keyed cleanup on that answer, so a superseded session cannot tear down its
     * replacement's tables.
     */
    synchronized boolean unregister(Uuid routeId, Session session) {
        Route route = routes.get(routeId);
        if (route == null || route.session != session) return false;
        route.session = null;
        metrics.connectionUnregistered();
        return true;
    }

    Session get(Uuid routeId) {
        Route route = routes.get(routeId);
        return route == null ? null : route.session;
    }

    /** The Route for routeId, created on first sight. ChannelRegistry.subscribe anchors a node on it. */
    Route route(Uuid routeId) {
        Route route = routes.get(routeId);
        return route != null ? route : create(routeId);
    }

    /** The Route for routeId, or null if it has never been seen. */
    Route existingRoute(Uuid routeId) {
        return routes.get(routeId);
    }

    /** Every session that has registered, for the client.connected replay. */
    List<Session> registered() {
        UuidTable<Route> snapshot = routes;
        List<Session> registered = new ArrayList<>();
        for (int i = 0, n = snapshot.capacity(); i < n; i++) {
            Route route = snapshot.valueAt(i);
            Session session = route == null ? null : route.session;
            if (session != null && session.clientInfo() != null) registered.add(session);
        }
        return registered;
    }

    private synchronized Route create(Uuid routeId) {
        Route route = routes.get(routeId);
        if (route != null) return route;

        route = new Route(routeId.copy());
        UuidTable<Route> next = routes.copy();
        next.put(route.id, route);
        routes = next;
        return route;
    }

}
