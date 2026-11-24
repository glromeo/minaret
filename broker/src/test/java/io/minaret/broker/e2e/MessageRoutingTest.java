package io.minaret.broker.e2e;

import io.minaret.messaging.ClientInfo;
import io.minaret.messaging.ControlBodies.BrokerAck;
import io.minaret.messaging.ControlBodies.ChannelRegistration;
import io.minaret.messaging.ControlBodies.ChannelResolution;
import io.minaret.messaging.ControlBodies.TopicRegistration;
import io.minaret.messaging.ControlBodies.TopicSubscription;
import io.minaret.messaging.Destination;
import io.minaret.messaging.Header;
import io.minaret.messaging.wire.BodyEncoding;
import io.minaret.messaging.wire.Wire;
import io.minaret.messaging.wire.WireFrame;
import io.micronaut.runtime.server.EmbeddedServer;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.micronaut.test.support.TestPropertyProvider;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.net.URI;
import java.nio.file.Files;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.minaret.broker.e2e.LiveSocket.bodyOf;
import static io.minaret.broker.e2e.LiveSocket.header;
import static io.minaret.broker.e2e.LiveSocket.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The port of Broker.Tests' MessageRoutingTests: the real server, the real pipeline, real sockets.
 * One Broker for the class, in a realm of its own, for the reason BrokerFactory gives: the Default
 * realm's pipe is held by whatever Broker the developer is running.
 */
@MicronautTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MessageRoutingTest implements TestPropertyProvider {

    static final int DEFAULT_TTL_SECONDS = 30;

    @Inject
    EmbeddedServer server;

    /** Plain ws on a random port: TLS is Micronaut's concern, not the Broker's, and needs no certificate here. */
    static Map<String, String> brokerProperties() {
        try {
            return Map.of(
                    "micronaut.server.ssl.enabled", "false",
                    "micronaut.server.port", "-1",
                    "messagebus.realm", "TEST" + UUID.randomUUID().toString().replace("-", ""),
                    "messagebus.idl.path", Files.createTempDirectory("idl").toString()
            );
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    @Override
    public Map<String, String> getProperties() {
        return brokerProperties();
    }

    private LiveSocket connect() throws Exception {
        return new LiveSocket(URI.create("ws://localhost:" + server.getPort() + "/msgbus"));
    }

    private static ClientInfo newClient() {
        return new ClientInfo(UUID.randomUUID(), UUID.randomUUID().toString(), 0, Map.of());
    }

    private static String greeting(ClientInfo from, ClientInfo to, long time, Integer ttl, String body) {
        return Wire.serializeMessage(header(from.routeId(), Destination.ofId(to.routeId()), "greeting", time, ttl), body);
    }

    private static String control(UUID from, String verb, Object body) {
        return Wire.serializeMessage(new Header(from, Destination.EMPTY, verb, UUID.randomUUID(), 0, null, BodyEncoding.RAW, null, ""), body);
    }

    @Test
    void broker_routesMessage_betweenTwoRegisteredClients() throws Exception {
        try (LiveSocket sender = connect(); LiveSocket receiver = connect()) {
            ClientInfo senderInfo = newClient();
            ClientInfo receiverInfo = newClient();
            sender.register(senderInfo);
            receiver.register(receiverInfo);

            sender.send(greeting(senderInfo, receiverInfo, System.currentTimeMillis(), null, "hello receiver"));

            String received = receiver.receive();
            WireFrame frame = parse(received);
            assertEquals(senderInfo.routeId(), frame.header().from());
            assertEquals(receiverInfo.routeId(), frame.header().to().id());
            assertEquals("greeting", frame.header().type());
            assertEquals("hello receiver", bodyOf(received));
        }
    }

    @Test
    void broker_dropsMessage_whenTtlExpired() throws Exception {
        try (LiveSocket sender = connect(); LiveSocket receiver = connect()) {
            ClientInfo senderInfo = newClient();
            ClientInfo receiverInfo = newClient();
            sender.register(senderInfo);
            receiver.register(receiverInfo);

            sender.send(greeting(senderInfo, receiverInfo, System.currentTimeMillis() - 60_000, 5, "too late"));
            sender.send(greeting(senderInfo, receiverInfo, System.currentTimeMillis(), null, "still on time"));

            assertEquals("still on time", bodyOf(receiver.receive()));
            receiver.assertNoMoreMessages();
        }
    }

    @Test
    void broker_dropsMessage_whenTtlOmitted_andDefaultTtlExpired() throws Exception {
        try (LiveSocket sender = connect(); LiveSocket receiver = connect()) {
            ClientInfo senderInfo = newClient();
            ClientInfo receiverInfo = newClient();
            sender.register(senderInfo);
            receiver.register(receiverInfo);

            sender.send(greeting(senderInfo, receiverInfo, System.currentTimeMillis() - (DEFAULT_TTL_SECONDS + 5) * 1000L, null, "too late"));
            sender.send(greeting(senderInfo, receiverInfo, System.currentTimeMillis(), null, "still on time"));

            assertEquals("still on time", bodyOf(receiver.receive()));
            receiver.assertNoMoreMessages();
        }
    }

    @Test
    void broker_usesRegistrationClockOffset_soSkewedSenderIsNotFalselyExpired() throws Exception {
        try (LiveSocket sender = connect(); LiveSocket receiver = connect()) {
            ClientInfo senderInfo = newClient();
            ClientInfo receiverInfo = newClient();
            long skewMs = TimeUnit.MINUTES.toMillis(10);

            ClientInfo enriched = sender.register(senderInfo, System.currentTimeMillis() - skewMs);
            assertTrue(Math.abs(enriched.clockOffsetMs() - skewMs) <= 5000, "offset " + enriched.clockOffsetMs());
            receiver.register(receiverInfo);

            sender.send(greeting(senderInfo, receiverInfo, System.currentTimeMillis() - skewMs, 30, "fresh despite skew"));

            assertEquals("fresh despite skew", bodyOf(receiver.receive()));
        }
    }

    @Test
    void broker_sendsAckBeforeRegistering_soRegistrationIsObservableOnceAcked() throws Exception {
        try (LiveSocket sender = connect(); LiveSocket receiver = connect()) {
            ClientInfo senderInfo = newClient();
            ClientInfo receiverInfo = newClient();
            sender.register(senderInfo);
            assertEquals(receiverInfo.routeId(), receiver.register(receiverInfo).routeId());

            sender.send(greeting(senderInfo, receiverInfo, System.currentTimeMillis(), null, "routable immediately after ack"));

            assertEquals("routable immediately after ack", bodyOf(receiver.receive()));
        }
    }

    @Test
    void broker_unregistersRoute_whenClientDisconnects_soNoDeadSocketLingers() throws Exception {
        try (LiveSocket sender = connect()) {
            ClientInfo senderInfo = newClient();
            sender.register(senderInfo);
            ClientInfo goneInfo = newClient();
            try (LiveSocket gone = connect()) {
                gone.register(goneInfo);
                gone.closeNormally();
            }
            Thread.sleep(200);

            sender.send(greeting(senderInfo, goneInfo, System.currentTimeMillis(), null, "nobody home"));
            sender.send(greeting(senderInfo, senderInfo, System.currentTimeMillis(), null, "still alive"));

            assertEquals("still alive", bodyOf(sender.receive()));
        }
    }

    @Test
    void broker_appliesDefaultTime_whenSenderLeavesItUnset() throws Exception {
        try (LiveSocket sender = connect(); LiveSocket receiver = connect()) {
            ClientInfo senderInfo = newClient();
            ClientInfo receiverInfo = newClient();
            sender.register(senderInfo);
            receiver.register(receiverInfo);

            sender.send(greeting(senderInfo, receiverInfo, 0, null, "unstamped but deliverable"));

            assertEquals("unstamped but deliverable", bodyOf(receiver.receive()));
        }
    }

    @Test
    void broker_relaysResolvedTtlAndTime_whenSenderOmitsThem() throws Exception {
        try (LiveSocket sender = connect(); LiveSocket receiver = connect()) {
            ClientInfo senderInfo = newClient();
            ClientInfo receiverInfo = newClient();
            sender.register(senderInfo);
            receiver.register(receiverInfo);

            sender.send(greeting(senderInfo, receiverInfo, 0, null, "resolved by policy"));

            String received = receiver.receive();
            WireFrame frame = parse(received);
            assertEquals(DEFAULT_TTL_SECONDS, frame.header().ttl());
            assertNotEquals(0, frame.header().time());
            assertEquals("resolved by policy", bodyOf(received));
        }
    }

    @Test
    void broker_preservesUnknownHeaderFields_soNewerClientsSurviveAnOlderBroker() throws Exception {
        try (LiveSocket sender = connect(); LiveSocket receiver = connect()) {
            ClientInfo senderInfo = newClient();
            ClientInfo receiverInfo = newClient();
            sender.register(senderInfo);
            receiver.register(receiverInfo);

            String plain = Wire.serializeText(header(senderInfo.routeId(), Destination.ofId(receiverInfo.routeId()), "greeting", System.currentTimeMillis(), null), "\"carries an extra field\"");
            int separator = plain.indexOf('\0');
            sender.send(plain.substring(0, separator) + "|somefuturevalue" + plain.substring(separator));

            String received = receiver.receive();
            assertEquals("somefuturevalue", parse(received).header().extras());
            assertEquals("carries an extra field", bodyOf(received));
        }
    }

    @Test
    void broker_deliversCompressedBody_intact() throws Exception {
        try (LiveSocket sender = connect(); LiveSocket receiver = connect()) {
            ClientInfo senderInfo = newClient();
            ClientInfo receiverInfo = newClient();
            sender.register(senderInfo);
            receiver.register(receiverInfo);

            String body = "a".repeat(Wire.COMPRESSION_THRESHOLD_BYTES * 4);
            String sent = Wire.serializeMessage(header(senderInfo.routeId(), Destination.ofId(receiverInfo.routeId()), "bulk", System.currentTimeMillis(), null), body);
            sender.send(sent);

            String received = receiver.receive();
            WireFrame frame = parse(received);
            assertEquals(BodyEncoding.GZIP, frame.header().encoding());
            assertEquals(parse(sent).body(), frame.body());
            assertEquals(body, bodyOf(received));
        }
    }

    @Test
    void channel_providerRegisters_clientResolvesDispatchesAndReceivesReply() throws Exception {
        try (LiveSocket provider = connect(); LiveSocket consumer = connect()) {
            ClientInfo providerInfo = newClient();
            ClientInfo consumerInfo = newClient();
            provider.register(providerInfo);
            consumer.register(consumerInfo);

            provider.send(control(providerInfo.routeId(), "channel.register", new ChannelRegistration("LAYER_1")));
            WireFrame registered = provider.receiveFrame();
            assertEquals("channel.registered", registered.header().type());
            assertTrue(Wire.parseBodyAs(registered, BrokerAck.class).ok());

            consumer.send(control(consumerInfo.routeId(), "channel.resolve", new ChannelResolution("LAYER_1")));
            WireFrame resolved = consumer.receiveFrame();
            assertEquals("channel.resolved", resolved.header().type());
            BrokerAck resolvedAck = Wire.parseBodyAs(resolved, BrokerAck.class);
            assertTrue(resolvedAck.ok());
            assertEquals(providerInfo.routeId(), resolvedAck.routeId());

            consumer.send(Wire.serializeMessage(header(consumerInfo.routeId(), Destination.ofId(providerInfo.routeId()), "echo", System.currentTimeMillis(), null), "hello"));
            String providerReceived = provider.receive();
            WireFrame providerFrame = parse(providerReceived);
            assertEquals("echo", providerFrame.header().type());
            assertEquals(consumerInfo.routeId(), providerFrame.header().from());
            assertEquals("hello", bodyOf(providerReceived));

            provider.send(Wire.serializeMessage(header(providerInfo.routeId(), Destination.ofId(consumerInfo.routeId()), "echo", System.currentTimeMillis(), null), "echo: hello"));
            assertEquals("echo: hello", bodyOf(consumer.receive()));
        }
    }

    @Test
    void channel_publish_fansOutToAllSubscribedClients() throws Exception {
        try (LiveSocket provider = connect(); LiveSocket subscriberA = connect(); LiveSocket subscriberB = connect()) {
            ClientInfo providerInfo = newClient();
            provider.register(providerInfo);
            subscriberA.register(newClient());
            subscriberB.register(newClient());

            provider.send(control(providerInfo.routeId(), "channel.register", new ChannelRegistration("FANOUT")));
            provider.receive();
            provider.send(control(providerInfo.routeId(), "topic.register", new TopicRegistration("FANOUT", "ticks")));
            UUID topicId = Wire.parseBodyAs(provider.receiveFrame(), BrokerAck.class).topicId();
            assertNotNull(topicId);

            for (LiveSocket socket : new LiveSocket[]{subscriberA, subscriberB}) {
                socket.send(control(null, "channel.resolve", new ChannelResolution("FANOUT")));
                socket.receive();
                socket.send(control(null, "topic.subscribe", new TopicSubscription("FANOUT", "ticks")));
                assertEquals(topicId, Wire.parseBodyAs(socket.receiveFrame(), BrokerAck.class).topicId());
            }

            provider.send(Wire.serializeMessage(new Header(providerInfo.routeId(), Destination.ofId(topicId), "publish", UUID.randomUUID(), 0, null, BodyEncoding.RAW, null, ""), "tick 1"));

            String receivedA = subscriberA.receive();
            String receivedB = subscriberB.receive();
            assertEquals("tick 1", bodyOf(receivedA));
            assertEquals(receivedA, receivedB);
            WireFrame fanout = parse(receivedA);
            assertEquals("publish", fanout.header().type());
            assertEquals(topicId, fanout.header().to().id());
            assertEquals(providerInfo.routeId(), fanout.header().from());
            assertEquals("published", provider.receiveFrame().header().type());
        }
    }

    @Test
    void channel_publishByPath_resolvesTopicAndAcksItsId() throws Exception {
        try (LiveSocket provider = connect(); LiveSocket subscriber = connect()) {
            ClientInfo providerInfo = newClient();
            provider.register(providerInfo);
            subscriber.register(newClient());

            provider.send(control(providerInfo.routeId(), "channel.register", new ChannelRegistration("BY_NAME")));
            provider.receive();
            subscriber.send(control(null, "channel.resolve", new ChannelResolution("BY_NAME")));
            subscriber.receive();
            subscriber.send(control(null, "topic.subscribe", new TopicSubscription("BY_NAME", "ticks")));
            UUID topicId = Wire.parseBodyAs(subscriber.receiveFrame(), BrokerAck.class).topicId();
            assertNotNull(topicId);

            provider.send(Wire.serializeMessage(header(providerInfo.routeId(), Destination.ofPath("BY_NAME", "ticks"), "publish", 0, null), "tick 1"));

            String delivered = subscriber.receive();
            WireFrame fanout = parse(delivered);
            assertEquals(topicId, fanout.header().to().id());
            assertNull(fanout.header().to().channel());
            assertEquals("tick 1", bodyOf(delivered));

            WireFrame resolved = provider.receiveFrame();
            assertEquals("topic.resolved", resolved.header().type());
            assertEquals(topicId, Wire.parseBodyAs(resolved, BrokerAck.class).topicId());
        }
    }

    @Test
    void register_underAnIdentityAlreadyHeld_closesThePriorConnection_asSuperseded() throws Exception {
        try (LiveSocket prior = connect(); LiveSocket newer = connect()) {
            String identity = UUID.randomUUID().toString();
            prior.register(new ClientInfo(null, identity, 0, Map.of()));
            newer.register(new ClientInfo(null, identity, 0, Map.of()));

            assertEquals(4001, prior.closed.get(10, TimeUnit.SECONDS));
        }
    }

}
