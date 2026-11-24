package io.minaret.broker.contract;

import io.minaret.broker.contract.Contract.BrokerAck;
import io.minaret.broker.contract.Contract.BrokerConnectionInfo;
import io.minaret.broker.contract.Contract.ChannelEvent;
import io.minaret.broker.contract.Contract.ChannelName;
import io.minaret.broker.contract.Contract.ChannelTopic;
import io.minaret.broker.contract.Contract.ClientEvent;
import io.minaret.broker.contract.Contract.ClientInfo;
import io.minaret.broker.contract.Contract.EventReplay;
import io.minaret.broker.contract.Contract.EventSubscription;
import io.minaret.broker.contract.Contract.TopicEvent;
import io.minaret.messaging.ControlBodies;
import io.minaret.messaging.Events;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.RecordComponent;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Pins broker/contract to runtime/messaging, the two hand-kept Java copies of the wire contract.
 *
 * Nothing else compares them. A verb or event name renamed on one side still compiles, and every
 * broker test that builds its frames with runtime's codec would only catch the drift if it happened
 * to exercise that verb. Here, drift fails by name: each vocabulary is compared constant by constant
 * (name → wire value), so a constant added, dropped or respelled on either side shows up in the diff.
 * Each shared body is compared by record component, which is its JSON property list, since neither
 * side renames properties.
 */
class ContractParityTest {

    @Test
    void verbs_matchRuntime() {
        assertEquals(constants(io.minaret.messaging.Verbs.class), constants(Verbs.class));
    }

    @Test
    void eventNames_matchRuntime() {
        assertEquals(constants(Events.Names.class), constants(EventNames.class));
    }

    @Test
    void closeCodes_matchRuntime() {
        assertEquals(constants(io.minaret.messaging.CloseCodes.class), constants(CloseCodes.class));
    }

    /**
     * Broker record, runtime record. The broker folds the control requests into two shapes by what
     * they carry (ChannelName, ChannelTopic), while runtime declares one record per request, so those
     * pair many-to-one.
     */
    static Stream<Arguments> bodies() {
        return Stream.of(
                Arguments.of(ClientInfo.class, io.minaret.messaging.ClientInfo.class),
                Arguments.of(BrokerAck.class, ControlBodies.BrokerAck.class),
                Arguments.of(ChannelName.class, ControlBodies.ChannelRegistration.class),
                Arguments.of(ChannelName.class, ControlBodies.ChannelResolution.class),
                Arguments.of(ChannelName.class, ControlBodies.TopicListRequest.class),
                Arguments.of(ChannelTopic.class, ControlBodies.TopicRegistration.class),
                Arguments.of(ChannelTopic.class, ControlBodies.TopicSubscription.class),
                Arguments.of(ChannelTopic.class, ControlBodies.TopicUnsubscription.class),
                Arguments.of(EventSubscription.class, Events.Subscription.class),
                Arguments.of(ClientEvent.class, Events.ClientEvent.class),
                Arguments.of(ChannelEvent.class, Events.ChannelEvent.class),
                Arguments.of(TopicEvent.class, Events.TopicEvent.class),
                Arguments.of(EventReplay.class, Events.Replay.class),
                Arguments.of(BrokerConnectionInfo.class, io.minaret.discovery.BrokerConnectionInfo.class));
    }

    @ParameterizedTest
    @MethodSource("bodies")
    void body_hasRuntimesProperties(Class<? extends Record> broker, Class<? extends Record> runtime) {
        assertEquals(components(runtime), components(broker));
    }

    /**
     * name → wire value of every public static final field. The broker's verbs are pre-encoded ASCII
     * bytes, compared on the frame path without decoding, so they are decoded here.
     */
    private static Map<String, Object> constants(Class<?> type) {
        Map<String, Object> constants = new TreeMap<>();
        for (Field field : type.getFields()) {
            int modifiers = field.getModifiers();
            if (!Modifier.isStatic(modifiers) || !Modifier.isFinal(modifiers)) continue;
            try {
                Object value = field.get(null);
                constants.put(field.getName(), value instanceof byte[] bytes ? new String(bytes, StandardCharsets.US_ASCII) : value);
            } catch (IllegalAccessException e) {
                throw new AssertionError(e);
            }
        }
        assertFalse(constants.isEmpty(), type + " declares no constants: wrong class?");
        return constants;
    }

    private static List<String> components(Class<? extends Record> type) {
        return Arrays.stream(type.getRecordComponents()).map(RecordComponent::getName).toList();
    }

}
