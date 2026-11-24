package io.minaret.broker.routing;

import io.minaret.broker.contract.Contract.Event;
import io.minaret.broker.contract.EventFilter;
import io.minaret.broker.wire.Uuid;
import jakarta.inject.Singleton;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * event name → routeId → the set of filters that route subscribed with. A set because subscribing
 * is additive. The rationale is in CLAUDE.md "Events".
 */
@Singleton
public final class EventRegistry {

    private final ConcurrentMap<String, ConcurrentMap<Uuid, Set<EventFilter>>> subscribers = new ConcurrentHashMap<>();

    /**
     * Idempotent for an identical filter. Session.handleEventSubscribe calls it under
     * ChannelRegistry's lock, so taking the snapshot and adding the subscriber are one atomic step.
     */
    void subscribe(String name, Uuid routeId, EventFilter filter) {
        subscribers.computeIfAbsent(name, ignored -> new ConcurrentHashMap<>())
                .computeIfAbsent(routeId, ignored -> ConcurrentHashMap.newKeySet())
                .add(filter);
    }

    /** The routes with at least one filter matching occurrence, each listed once. */
    List<Uuid> subscribersOf(String name, Event occurrence) {
        Map<Uuid, Set<EventFilter>> byRoute = subscribers.get(name);
        if (byRoute == null || byRoute.isEmpty()) return List.of();

        List<Uuid> matched = new ArrayList<>();
        for (Map.Entry<Uuid, Set<EventFilter>> entry : byRoute.entrySet())
            for (EventFilter filter : entry.getValue())
                if (filter.matches(occurrence)) {
                    matched.add(entry.getKey());
                    break;
                }
        return matched;
    }

    void removeConnection(Uuid routeId) {
        for (Map<Uuid, Set<EventFilter>> byRoute : subscribers.values()) byRoute.remove(routeId);
    }

}
