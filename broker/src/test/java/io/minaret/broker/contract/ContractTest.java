package io.minaret.broker.contract;

import io.minaret.broker.contract.Contract.ChannelEvent;
import io.minaret.broker.contract.Contract.ClientEvent;
import io.minaret.broker.contract.Contract.TopicEvent;
import io.micronaut.core.beans.BeanIntrospection;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Event's channel() and topic() defaults exist for EventFilter.matches. They are not record
 * components, so they must not become serialized properties: an event body must not carry
 * "channel":null.
 */
class ContractTest {

    private static final Map<Class<?>, List<String>> PROPERTIES = Map.of(
            ClientEvent.class, List.of("name", "routeId", "client"),
            ChannelEvent.class, List.of("name", "channel", "providerRouteId"),
            TopicEvent.class, List.of("name", "channel", "topic", "topicId", "subscriberRouteId"));

    @ParameterizedTest
    @ValueSource(classes = {ClientEvent.class, ChannelEvent.class, TopicEvent.class})
    void events_serializeOnlyTheirRecordComponents(Class<?> type) {
        assertEquals(PROPERTIES.get(type), List.of(BeanIntrospection.getIntrospection(type).getPropertyNames()));
    }

}
