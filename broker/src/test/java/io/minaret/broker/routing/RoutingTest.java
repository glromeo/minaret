package io.minaret.broker.routing;

import io.minaret.broker.routing.ChannelRegistry.Channel;
import io.minaret.broker.routing.TestBus.Socket;
import io.minaret.broker.wire.Uuid;
import io.minaret.messaging.ClientInfo;
import io.minaret.messaging.ControlBodies.BrokerAck;
import io.minaret.messaging.ControlBodies.ChannelRegistration;
import io.minaret.messaging.ControlBodies.ChannelResolution;
import io.minaret.messaging.ControlBodies.TopicRegistration;
import io.minaret.messaging.ControlBodies.TopicUnsubscription;
import io.minaret.messaging.Destination;
import io.minaret.messaging.Header;
import io.minaret.messaging.wire.BodyEncoding;
import io.minaret.messaging.wire.Wire;
import io.minaret.messaging.wire.WireFrame;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static io.minaret.broker.routing.TestBus.client;
import static io.minaret.broker.routing.TestBus.control;
import static io.minaret.broker.routing.TestBus.topicFrame;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The wire-level half of Broker.Tests' ChannelRegistryTests: dispatch, publish and notify routing
 * through real Sessions. A "seeded" socket is a recipient entered into the registry with no
 * handshake, as the C# tests' SeedConnection.
 */
class RoutingTest {

    private static Uuid id(UUID value) {
        return Uuid.of(value);
    }

    @Test
    void publish_toATopicOnSomeoneElsesChannel_isRejected() {
        TestBus bus = new TestBus();
        ClientInfo provider = client();
        ClientInfo intruder = client();
        bus.channels.registerChannel("LAYER_1", id(provider.routeId()));
        Uuid topicId = bus.channels.registerTopic(id(provider.routeId()), "LAYER_1", "echo");

        Socket socket = bus.registered(intruder);
        socket.send(topicFrame("publish", Destination.ofId(topicId.toUUID()), UUID.randomUUID(), "hi"));

        List<WireFrame> frames = socket.frames();
        WireFrame last = frames.get(frames.size() - 1);
        assertEquals("published", last.header().type());
        assertFalse(socket.body(last, BrokerAck.class).ok());
    }

    @Test
    void publish_fansOutToSubscriber() {
        TestBus bus = new TestBus();
        ClientInfo provider = client();
        UUID subscriberRouteId = UUID.randomUUID();
        Socket subscriber = bus.seeded(subscriberRouteId);
        bus.channels.registerChannel("LAYER_1", id(provider.routeId()));
        Uuid topicId = bus.channels.subscribe(id(provider.routeId()), "LAYER_1", "echo", id(subscriberRouteId));

        bus.registered(provider).send(topicFrame("publish", Destination.ofId(topicId.toUUID()), null, "hi"));

        assertEquals(1, subscriber.sent.size());
        WireFrame fanout = subscriber.frames().get(0);
        assertEquals("publish", fanout.header().type());
        assertEquals(provider.routeId(), fanout.header().from());
        assertEquals(topicId.toUUID(), fanout.header().to().id());
        assertEquals("\"hi\"", fanout.body());
    }

    @Test
    void topicUnsubscribe_acksAndStopsFanout() {
        TestBus bus = new TestBus();
        ClientInfo provider = client();
        ClientInfo subscriberInfo = client();
        bus.channels.registerChannel("LAYER_1", id(provider.routeId()));
        Uuid topicId = bus.channels.subscribe(id(provider.routeId()), "LAYER_1", "echo", id(subscriberInfo.routeId()));

        Socket subscriber = bus.registered(subscriberInfo);
        subscriber.send(control("channel.resolve", new ChannelResolution("LAYER_1")));
        subscriber.send(control("topic.unsubscribe", new TopicUnsubscription("LAYER_1", "echo")));

        List<WireFrame> frames = subscriber.frames();
        WireFrame last = frames.get(frames.size() - 1);
        assertEquals("topic.unsubscribed", last.header().type());
        BrokerAck ack = subscriber.body(last, BrokerAck.class);
        assertTrue(ack.ok());
        assertEquals(topicId.toUUID(), ack.topicId());
        assertNull(bus.channels.topic(topicId).head());

        int before = subscriber.sent.size();
        bus.registered(provider).send(topicFrame("publish", Destination.ofId(topicId.toUUID()), null, "hi"));
        assertEquals(before, subscriber.sent.size());
    }

    @Test
    void dispatch_byPath_fromAClientThatNeverSubscribed_reachesTheProvider() {
        TestBus bus = new TestBus();
        ClientInfo provider = client();
        ClientInfo clientInfo = client();
        bus.channels.registerChannel("LAYER_1", id(provider.routeId()));
        Uuid topicId = bus.channels.registerTopic(id(provider.routeId()), "LAYER_1", "echo");
        Socket providerSocket = bus.seeded(provider.routeId());

        Socket clientSocket = bus.registered(clientInfo);
        clientSocket.send(control("channel.resolve", new ChannelResolution("LAYER_1")));
        clientSocket.send(topicFrame("dispatch", Destination.ofPath("LAYER_1", "echo"), UUID.randomUUID(), "hello"));

        assertEquals(1, providerSocket.sent.size());
        WireFrame delivered = providerSocket.frames().get(0);
        assertEquals("dispatch", delivered.header().type());
        assertEquals(topicId.toUUID(), delivered.header().to().id());
        assertEquals(1, clientSocket.ofType("topic.resolved").size());
    }

    @Test
    void dispatch_byPath_afterItsProviderDied_isDropped_notReroutedToTheNameNewOwner() {
        TestBus bus = new TestBus();
        ClientInfo firstProvider = client();
        ClientInfo secondProvider = client();
        Channel first = bus.channels.registerChannel("LAYER_1", id(firstProvider.routeId()));
        bus.channels.registerTopic(id(firstProvider.routeId()), "LAYER_1", "echo");
        Socket secondSocket = bus.seeded(secondProvider.routeId());

        Socket clientSocket = bus.registered(client());
        clientSocket.send(control("channel.resolve", new ChannelResolution("LAYER_1")));

        bus.channels.removeConnection(id(firstProvider.routeId()));
        bus.channels.removeChannel(first);
        assertNotNull(bus.channels.registerChannel("LAYER_1", id(secondProvider.routeId())));
        bus.channels.registerTopic(id(secondProvider.routeId()), "LAYER_1", "echo");

        clientSocket.send(topicFrame("dispatch", Destination.ofPath("LAYER_1", "echo"), UUID.randomUUID(), "hello"));

        assertTrue(secondSocket.sent.isEmpty());
        assertTrue(clientSocket.ofType("topic.resolved").isEmpty());
    }

    @Test
    void topicUnsubscribe_whenNotSubscribed_isRejected() {
        TestBus bus = new TestBus();
        ClientInfo provider = client();
        bus.channels.registerChannel("LAYER_1", id(provider.routeId()));
        bus.channels.registerTopic(id(provider.routeId()), "LAYER_1", "echo");

        Socket socket = bus.registered(client());
        socket.send(control("channel.resolve", new ChannelResolution("LAYER_1")));
        socket.send(control("topic.unsubscribe", new TopicUnsubscription("LAYER_1", "echo")));

        List<WireFrame> frames = socket.frames();
        WireFrame last = frames.get(frames.size() - 1);
        assertEquals("topic.unsubscribed", last.header().type());
        assertFalse(socket.body(last, BrokerAck.class).ok());
    }

    @Test
    void dispatch_providerReply_goesToTheAwaitingClient_notBackToTheProvider() {
        TestBus bus = new TestBus();
        ClientInfo provider = client();
        UUID clientRouteId = UUID.randomUUID();
        UUID correlationId = UUID.randomUUID();
        Socket clientSocket = bus.seeded(clientRouteId);
        bus.channels.registerChannel("LAYER_1", id(provider.routeId()));
        Uuid topicId = bus.channels.registerTopic(id(provider.routeId()), "LAYER_1", "echo");
        bus.channels.subscribe(id(provider.routeId()), "LAYER_1", "echo", id(provider.routeId()));
        bus.channels.expectReply(id(correlationId), id(clientRouteId));

        Socket providerSocket = bus.registered(provider);
        providerSocket.send(topicFrame("dispatch", Destination.ofId(topicId.toUUID()), correlationId, "echo: hello"));

        assertEquals(1, clientSocket.sent.size());
        WireFrame delivered = clientSocket.frames().get(0);
        assertEquals(topicId.toUUID(), delivered.header().to().id());
        assertEquals(correlationId, delivered.header().replyTo());
        assertTrue(providerSocket.ofType("dispatch").isEmpty());
    }

    @Test
    void publish_byPath_resolvesTopicAndAcksTheId() {
        TestBus bus = new TestBus();
        ClientInfo provider = client();
        UUID subscriberRouteId = UUID.randomUUID();
        Socket subscriber = bus.seeded(subscriberRouteId);
        bus.channels.registerChannel("LAYER_1", id(provider.routeId()));
        Uuid topicId = bus.channels.subscribe(id(provider.routeId()), "LAYER_1", "echo", id(subscriberRouteId));

        Socket providerSocket = bus.registered(provider);
        providerSocket.send(topicFrame("publish", Destination.ofPath("LAYER_1", "echo"), null, "hi"));

        assertEquals(1, subscriber.sent.size());
        assertEquals(topicId.toUUID(), subscriber.frames().get(0).header().to().id());

        BrokerAck ack = providerSocket.body(providerSocket.single("topic.resolved"), BrokerAck.class);
        assertEquals(topicId.toUUID(), ack.topicId());
        assertEquals("LAYER_1", ack.channel());
        assertEquals(List.of("echo"), ack.topics());
    }

    @Test
    void oneConnection_registersTwoChannels_bothServeTheirOwnTopics() {
        TestBus bus = new TestBus();
        Socket socket = bus.registered(client());
        socket.send(control("channel.register", new ChannelRegistration("FIRST")));
        socket.send(control("channel.register", new ChannelRegistration("SECOND")));
        socket.send(control("topic.register", new TopicRegistration("FIRST", "echo")));
        socket.send(control("topic.register", new TopicRegistration("SECOND", "run")));

        List<WireFrame> channelAcks = socket.ofType("channel.registered");
        assertEquals(2, channelAcks.size());
        channelAcks.forEach(frame -> assertTrue(socket.body(frame, BrokerAck.class).ok()));

        List<WireFrame> topicAcks = socket.ofType("topic.registered");
        assertEquals(2, topicAcks.size());
        UUID first = socket.body(topicAcks.get(0), BrokerAck.class).topicId();
        UUID second = socket.body(topicAcks.get(1), BrokerAck.class).topicId();
        assertNotNull(first);
        assertNotNull(second);
        assertNotEquals(first, second);
    }

    @Test
    void dispatch_reusingACorrelationIdAnotherRouteAwaits_isDropped() {
        TestBus bus = new TestBus();
        UUID providerRouteId = UUID.randomUUID();
        UUID victimRouteId = UUID.randomUUID();
        UUID correlationId = UUID.randomUUID();
        Socket providerSocket = bus.seeded(providerRouteId);
        bus.channels.registerChannel("LAYER_1", id(providerRouteId));
        Uuid topicId = bus.channels.registerTopic(id(providerRouteId), "LAYER_1", "echo");
        assertTrue(bus.channels.expectReply(id(correlationId), id(victimRouteId)));

        bus.registered(client()).send(topicFrame("dispatch", Destination.ofId(topicId.toUUID()), correlationId, "steal"));

        assertTrue(providerSocket.ofType("dispatch").isEmpty());
        assertEquals(id(victimRouteId), bus.channels.takeReplyRoute(id(correlationId)));
    }

    @Test
    void forwardToRoute_fromAConnectionThatNeverRegistered_isDropped() {
        TestBus bus = new TestBus();
        UUID recipientRouteId = UUID.randomUUID();
        Socket recipient = bus.seeded(recipientRouteId);

        bus.open().send(Wire.serializeMessage(new Header(UUID.randomUUID(), Destination.ofId(recipientRouteId), "greeting", null, 0, null, BodyEncoding.RAW, null, ""), "hi"));

        assertTrue(recipient.sent.isEmpty());
    }

    @Test
    void forwardedFrame_carriesTheSendersRegisteredRouteId_notTheOneItClaimed() {
        TestBus bus = new TestBus();
        UUID recipientRouteId = UUID.randomUUID();
        Socket recipient = bus.seeded(recipientRouteId);
        ClientInfo sender = client();
        UUID forged = UUID.randomUUID();

        bus.registered(sender).send(Wire.serializeMessage(new Header(forged, Destination.ofId(recipientRouteId), "greeting", null, 0, null, BodyEncoding.RAW, null, ""), "hi"));

        assertEquals(1, recipient.sent.size());
        assertEquals(sender.routeId(), recipient.frames().get(0).header().from());
    }

    @Test
    void notify_fromTheProvider_reachesTheNamedClientOnly() {
        TestBus bus = new TestBus();
        ClientInfo provider = client();
        UUID recipientRouteId = UUID.randomUUID();
        Socket recipient = bus.seeded(recipientRouteId);
        Socket bystander = bus.seeded(UUID.randomUUID());
        bus.channels.registerChannel("LAYER_1", id(provider.routeId()));
        Uuid topicId = bus.channels.registerTopic(id(provider.routeId()), "LAYER_1", "news");

        bus.registered(provider).send(topicFrame("notify", Destination.ofId(topicId.toUUID()), recipientRouteId, "psst"));

        assertEquals(1, recipient.sent.size());
        assertEquals(recipientRouteId, recipient.frames().get(0).header().replyTo());
        assertTrue(bystander.sent.isEmpty());
    }

    @Test
    void notify_fromAnyoneButTheProvider_isDropped() {
        TestBus bus = new TestBus();
        UUID providerRouteId = UUID.randomUUID();
        UUID recipientRouteId = UUID.randomUUID();
        Socket recipient = bus.seeded(recipientRouteId);
        bus.channels.registerChannel("LAYER_1", id(providerRouteId));
        Uuid topicId = bus.channels.registerTopic(id(providerRouteId), "LAYER_1", "news");

        bus.registered(client()).send(topicFrame("notify", Destination.ofId(topicId.toUUID()), recipientRouteId, "spoof"));

        assertTrue(recipient.sent.isEmpty());
    }

    @Test
    void channelResolve_beforeTheProviderRegisters_isAckedOnceItDoes() {
        TestBus bus = new TestBus();
        Socket clientSocket = bus.registered(client());
        UUID correlation = UUID.randomUUID();
        clientSocket.send(control("channel.resolve", new ChannelResolution("LATE"), correlation));
        assertTrue(clientSocket.ofType("channel.resolved").isEmpty());

        ClientInfo provider = client();
        bus.registered(provider).send(control("channel.register", new ChannelRegistration("LATE")));

        WireFrame resolved = clientSocket.single("channel.resolved");
        assertEquals(correlation, resolved.header().replyTo());
        assertEquals(provider.routeId(), clientSocket.body(resolved, BrokerAck.class).routeId());
    }

    @Test
    void channelResolve_parkedWhenTheConnectionEnds_isCancelled() {
        TestBus bus = new TestBus();
        Socket clientSocket = bus.registered(client());
        clientSocket.send(control("channel.resolve", new ChannelResolution("NEVER")));
        clientSocket.close();

        bus.registered(client()).send(control("channel.register", new ChannelRegistration("NEVER")));

        assertTrue(clientSocket.ofType("channel.resolved").isEmpty());
    }

}
