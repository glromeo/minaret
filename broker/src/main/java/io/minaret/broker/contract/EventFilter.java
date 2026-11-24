package io.minaret.broker.contract;

import io.minaret.broker.contract.Contract.ClientEvent;
import io.minaret.broker.contract.Contract.ClientInfo;
import io.minaret.broker.contract.Contract.Event;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * One event.subscribe's narrowing, the port of C#'s EventFilter record struct.
 *
 * EventRegistry keeps a set of these per route, so two subscriptions narrowing the same way must
 * collapse. A record gives that for free: Map.equals compares contents, which is what C# had to
 * normalise its Dictionary into sorted arrays to get. where is copied, so a caller's map cannot
 * change a filter already in a set.
 *
 * Session.handleEventSubscribe applies one to the replay, and EventRegistry.subscribersOf applies
 * it to every live occurrence, with the same matches, so the two can never disagree.
 */
public record EventFilter(String channel, String topic, Map<String, String> where) {

    /** Not Map.copyOf, which throws on the null values a JSON where may carry. */
    public EventFilter {
        where = where == null || where.isEmpty() ? Map.of() : Collections.unmodifiableMap(new HashMap<>(where));
    }

    /**
     * A client event narrows on where against the client's properties. Any other narrows on channel,
     * then topic. An occurrence that carries no channel always matches.
     */
    public boolean matches(Event occurrence) {
        if (occurrence instanceof ClientEvent client) return matchesProperties(client.client());
        if (channel == null || occurrence.channel() == null) return true;
        return channel.equals(occurrence.channel()) && (topic == null || topic.equals(occurrence.topic()));
    }

    private boolean matchesProperties(ClientInfo client) {
        if (where.isEmpty()) return true;
        if (client == null) return false;

        Map<String, String> properties = client.properties();
        for (Map.Entry<String, String> wanted : where.entrySet())
            if (!properties.containsKey(wanted.getKey()) || !Objects.equals(properties.get(wanted.getKey()), wanted.getValue())) return false;
        return true;
    }

}
