package io.minaret.broker.routing;

import io.minaret.broker.contract.Contract.ChannelEvent;
import io.minaret.broker.contract.Contract.ClientEvent;
import io.minaret.broker.contract.Contract.ClientInfo;
import io.minaret.broker.contract.Contract.TopicEvent;
import io.minaret.broker.contract.EventFilter;
import io.minaret.broker.wire.Uuid;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static io.minaret.broker.contract.EventNames.CHANNEL_REGISTERED;
import static io.minaret.broker.contract.EventNames.CHANNEL_REMOVED;
import static io.minaret.broker.contract.EventNames.CLIENT_CONNECTED;
import static io.minaret.broker.contract.EventNames.TOPIC_REGISTERED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The port of Broker.Tests' EventRegistryTests. C#'s FiltersFor is not ported, so its two tests pin EventFilter equality instead. */
class EventRegistryTest {

    private static final EventFilter ANY = new EventFilter(null, null, null);

    private static Uuid newId() {
        return Uuid.of(UUID.randomUUID());
    }

    private static EventFilter channel(String channel) {
        return new EventFilter(channel, null, null);
    }

    private static EventFilter where(String... pairs) {
        Map<String, String> where = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) where.put(pairs[i], pairs[i + 1]);
        return new EventFilter(null, null, where);
    }

    private static ChannelEvent onChannel(String channel) {
        return new ChannelEvent(CHANNEL_REGISTERED, channel, UUID.randomUUID());
    }

    private static TopicEvent onTopic(String channel, String topic) {
        return new TopicEvent(TOPIC_REGISTERED, channel, topic, UUID.randomUUID(), null);
    }

    private static ClientEvent onClient(String... pairs) {
        Map<String, String> properties = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) properties.put(pairs[i], pairs[i + 1]);
        return new ClientEvent(CLIENT_CONNECTED, UUID.randomUUID(), new ClientInfo(null, null, 0, properties));
    }

    @Test
    void subscribersOf_anEventNobodySubscribedTo_isEmpty() {
        assertTrue(new EventRegistry().subscribersOf(CHANNEL_REGISTERED, onChannel("LAYER_1")).isEmpty());
    }

    @Test
    void subscribe_isIdempotent_soAReSubscribeDoesNotDeliverTwice() {
        EventRegistry registry = new EventRegistry();
        Uuid route = newId();
        registry.subscribe(CHANNEL_REGISTERED, route, ANY);
        registry.subscribe(CHANNEL_REGISTERED, route, ANY);
        assertEquals(List.of(route), registry.subscribersOf(CHANNEL_REGISTERED, onChannel("LAYER_1")));
    }

    @Test
    void subscribe_keepsEventNamesIndependent() {
        EventRegistry registry = new EventRegistry();
        Uuid registered = newId();
        Uuid removed = newId();
        registry.subscribe(CHANNEL_REGISTERED, registered, ANY);
        registry.subscribe(CHANNEL_REMOVED, removed, ANY);
        assertEquals(List.of(registered), registry.subscribersOf(CHANNEL_REGISTERED, onChannel("LAYER_1")));
        assertEquals(List.of(removed), registry.subscribersOf(CHANNEL_REMOVED, onChannel("LAYER_1")));
    }

    @Test
    void removeConnection_dropsTheRouteFromEveryEventItSubscribedTo() {
        EventRegistry registry = new EventRegistry();
        Uuid leaving = newId();
        Uuid staying = newId();
        registry.subscribe(CHANNEL_REGISTERED, leaving, ANY);
        registry.subscribe(CHANNEL_REMOVED, leaving, ANY);
        registry.subscribe(CHANNEL_REGISTERED, staying, ANY);

        registry.removeConnection(leaving);

        assertTrue(registry.subscribersOf(CHANNEL_REMOVED, onChannel("LAYER_1")).isEmpty());
        assertEquals(List.of(staying), registry.subscribersOf(CHANNEL_REGISTERED, onChannel("LAYER_1")));
    }

    @Test
    void aChannelFilter_admitsOnlyThatChannel() {
        EventRegistry registry = new EventRegistry();
        Uuid watcher = newId();
        registry.subscribe(CHANNEL_REGISTERED, watcher, channel("LAYER_1"));
        assertEquals(List.of(watcher), registry.subscribersOf(CHANNEL_REGISTERED, onChannel("LAYER_1")));
        assertTrue(registry.subscribersOf(CHANNEL_REGISTERED, onChannel("OTHER")).isEmpty());
    }

    @Test
    void aTopicFilter_admitsOnlyThatTopicOnThatChannel() {
        EventRegistry registry = new EventRegistry();
        Uuid watcher = newId();
        registry.subscribe(TOPIC_REGISTERED, watcher, new EventFilter("LAYER_1", "echo", null));
        assertEquals(List.of(watcher), registry.subscribersOf(TOPIC_REGISTERED, onTopic("LAYER_1", "echo")));
        assertTrue(registry.subscribersOf(TOPIC_REGISTERED, onTopic("LAYER_1", "run")).isEmpty());
        assertTrue(registry.subscribersOf(TOPIC_REGISTERED, onTopic("OTHER", "echo")).isEmpty());
    }

    @Test
    void aChannelFilterWithNoTopic_admitsEveryTopicOnThatChannel() {
        EventRegistry registry = new EventRegistry();
        Uuid watcher = newId();
        registry.subscribe(TOPIC_REGISTERED, watcher, channel("LAYER_1"));
        assertEquals(List.of(watcher), registry.subscribersOf(TOPIC_REGISTERED, onTopic("LAYER_1", "echo")));
        assertEquals(List.of(watcher), registry.subscribersOf(TOPIC_REGISTERED, onTopic("LAYER_1", "run")));
        assertTrue(registry.subscribersOf(TOPIC_REGISTERED, onTopic("OTHER", "echo")).isEmpty());
    }

    @Test
    void twoFiltersOnOneName_areAdditive_notLastWins() {
        EventRegistry registry = new EventRegistry();
        Uuid watcher = newId();
        registry.subscribe(CHANNEL_REGISTERED, watcher, channel("A"));
        registry.subscribe(CHANNEL_REGISTERED, watcher, channel("B"));
        assertEquals(List.of(watcher), registry.subscribersOf(CHANNEL_REGISTERED, onChannel("A")));
        assertEquals(List.of(watcher), registry.subscribersOf(CHANNEL_REGISTERED, onChannel("B")));
        assertTrue(registry.subscribersOf(CHANNEL_REGISTERED, onChannel("C")).isEmpty());
    }

    @Test
    void aRouteWhoseFiltersBothMatch_isDeliveredToOnce() {
        EventRegistry registry = new EventRegistry();
        Uuid watcher = newId();
        registry.subscribe(TOPIC_REGISTERED, watcher, channel("LAYER_1"));
        registry.subscribe(TOPIC_REGISTERED, watcher, new EventFilter("LAYER_1", "echo", null));
        assertEquals(List.of(watcher), registry.subscribersOf(TOPIC_REGISTERED, onTopic("LAYER_1", "echo")));
    }

    @Test
    void aFilteredSubscriptionToAClientEvent_stillMatches() {
        EventRegistry registry = new EventRegistry();
        Uuid watcher = newId();
        registry.subscribe(CLIENT_CONNECTED, watcher, channel("LAYER_1"));
        assertEquals(List.of(watcher), registry.subscribersOf(CLIENT_CONNECTED, new ClientEvent(CLIENT_CONNECTED, UUID.randomUUID(), null)));
    }

    @Test
    void aPropertyFilter_admitsOnlyClientsDeclaringThatProperty() {
        EventRegistry registry = new EventRegistry();
        Uuid watcher = newId();
        registry.subscribe(CLIENT_CONNECTED, watcher, where("role", "worker"));
        assertEquals(List.of(watcher), registry.subscribersOf(CLIENT_CONNECTED, onClient("role", "worker")));
        assertTrue(registry.subscribersOf(CLIENT_CONNECTED, onClient("role", "scheduler")).isEmpty());
        assertTrue(registry.subscribersOf(CLIENT_CONNECTED, onClient("region", "eu")).isEmpty());
    }

    @Test
    void severalPropertyConstraints_mustAllHold() {
        EventRegistry registry = new EventRegistry();
        Uuid watcher = newId();
        registry.subscribe(CLIENT_CONNECTED, watcher, where("role", "worker", "region", "eu"));
        assertEquals(List.of(watcher), registry.subscribersOf(CLIENT_CONNECTED, onClient("role", "worker", "region", "eu")));
        assertTrue(registry.subscribersOf(CLIENT_CONNECTED, onClient("role", "worker", "region", "us")).isEmpty());
        assertTrue(registry.subscribersOf(CLIENT_CONNECTED, onClient("role", "worker")).isEmpty());
    }

    @Test
    void aClientDeclaringExtraProperties_stillMatches() {
        EventRegistry registry = new EventRegistry();
        Uuid watcher = newId();
        registry.subscribe(CLIENT_CONNECTED, watcher, where("role", "worker"));
        assertEquals(List.of(watcher), registry.subscribersOf(CLIENT_CONNECTED, onClient("role", "worker", "region", "eu", "version", "3")));
    }

    @Test
    void theSamePropertyFilter_isOneFilter_regardlessOfEntryOrder() {
        EventFilter forward = where("role", "worker", "region", "eu");
        EventFilter backward = where("region", "eu", "role", "worker");
        assertEquals(forward, backward);
        assertEquals(forward.hashCode(), backward.hashCode());
    }

    @Test
    void anUnnarrowedSubscription_matchesAClientWithNoProperties() {
        EventRegistry registry = new EventRegistry();
        Uuid watcher = newId();
        registry.subscribe(CLIENT_CONNECTED, watcher, ANY);
        assertEquals(List.of(watcher), registry.subscribersOf(CLIENT_CONNECTED, onClient()));
    }

}
