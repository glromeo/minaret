package io.minaret.broker.routing;

import io.minaret.broker.routing.TestBus.Socket;
import io.minaret.broker.wire.Uuid;
import io.minaret.messaging.ClientInfo;
import io.minaret.messaging.ControlBodies.BrokerAck;
import io.minaret.messaging.ControlBodies.ChannelRegistration;
import io.minaret.messaging.ControlBodies.TopicSubscription;
import io.minaret.messaging.Events.ChannelEvent;
import io.minaret.messaging.Events.ClientEvent;
import io.minaret.messaging.Events.Names;
import io.minaret.messaging.Events.Replay;
import io.minaret.messaging.Events.Subscription;
import io.minaret.messaging.wire.WireFrame;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static io.minaret.broker.routing.TestBus.client;
import static io.minaret.broker.routing.TestBus.control;
import static io.minaret.broker.routing.TestBus.register;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The port of Broker.Tests' ClientSessionTests: handshake, teardown, identity, and the event plane,
 * one Session at a time on an EmbeddedChannel. A unit test rather than an e2e one for the same
 * reason as the C# original: it drives a teardown at moments no real client can produce on cue.
 */
class SessionTest {

    private static String subscribe(String name) {
        return subscribe(new Subscription(name, null, null, null));
    }

    private static String subscribe(Subscription subscription) {
        return control("event.subscribe", subscription);
    }

    private static String registerChannel(String name) {
        return control("channel.register", new ChannelRegistration(name));
    }

    private static <T> List<T> events(Socket socket, Class<T> type, String name) {
        return socket.ofType("event").stream()
                .filter(frame -> name.equals(io.minaret.messaging.wire.Wire.parseBodyAsJson(frame).path("name").asText()))
                .map(frame -> socket.body(frame, type))
                .toList();
    }

    private static Replay replay(Socket socket) {
        List<WireFrame> replays = socket.ofType("event.replay");
        return replays.isEmpty() ? null : socket.body(replays.get(0), Replay.class);
    }

    private static Uuid id(UUID value) {
        return Uuid.of(value);
    }

    @Test
    void connectionEnding_unregistersItsRoute_evenWhenTheAckNeverReachedTheClient() {
        TestBus bus = new TestBus();
        ClientInfo info = client();
        Socket socket = bus.openFailingWrites();
        socket.send(register(info));
        socket.close();

        assertTrue(socket.sent.isEmpty());
        assertNull(bus.connections.get(id(info.routeId())));
    }

    @Test
    void connectionDroppingBeforeRegistering_tearsDownCleanly_leavingNothingBehind() {
        TestBus bus = new TestBus();
        Socket socket = bus.open();
        socket.close();

        assertTrue(bus.connections.registered().isEmpty());
        assertNull(socket.session().clientInfo());
    }

    @Test
    void successfulRegistration_isRoutable_thenUnregisteredOnDisconnect() {
        TestBus bus = new TestBus();
        ClientInfo info = client();
        Socket socket = bus.registered(info);

        WireFrame ack = socket.single("client.registered");
        assertNull(ack.header().from());
        assertEquals(info.routeId(), ack.header().to().id());
        assertSame(socket.session(), bus.connections.get(id(info.routeId())));

        socket.close();
        assertNull(bus.connections.get(id(info.routeId())));
    }

    @Test
    void handleRegister_emptyIdentity_isRejectedAsMalformed() {
        TestBus bus = new TestBus();
        ClientInfo info = new ClientInfo(UUID.randomUUID(), "", 0, Map.of());
        // The client record defaults an empty identity, so the frame is written by hand.
        String frame = "M01||client.register|" + System.currentTimeMillis() + "||||||\0{\"routeId\":\"" + info.routeId() + "\",\"identity\":\"\"}";
        Socket socket = bus.open();
        socket.send(frame);

        assertTrue(socket.sent.isEmpty());
        assertNull(bus.connections.get(id(info.routeId())));
    }

    @Test
    void handleRegister_knownIdentity_reassignsThePreviouslyClaimedRouteId() {
        TestBus bus = new TestBus();
        Uuid assigned = bus.identities.resolveOrClaim("trading-app", null);

        bus.registered(new ClientInfo(null, "trading-app", 0, Map.of())).close();
        Socket second = bus.registered(new ClientInfo(UUID.randomUUID(), "trading-app", 0, Map.of()));

        assertEquals(assigned.toUUID(), second.single("client.registered").header().to().id());
    }

    @Test
    void handleRegister_knownIdentity_evictsThePriorLiveSession() {
        TestBus bus = new TestBus();
        Uuid assigned = bus.identities.resolveOrClaim("trading-app", null);

        Socket first = bus.registered(new ClientInfo(null, "trading-app", 0, Map.of()));
        assertSame(first.session(), bus.connections.get(assigned));

        Socket second = bus.registered(new ClientInfo(null, "trading-app", 0, Map.of()));

        assertEquals(4001, first.closedWith);
        first.close();
        assertSame(second.session(), bus.connections.get(assigned));
    }

    @Test
    void aTornDownSupersededSession_leavesTheReplacementsSubscriptionsIntact() {
        TestBus bus = new TestBus();
        Uuid routeId = bus.identities.resolveOrClaim("trading-app", null);

        Socket first = bus.registered(new ClientInfo(null, "trading-app", 0, Map.of()));
        Socket second = bus.registered(new ClientInfo(null, "trading-app", 0, Map.of()));
        second.send(registerChannel("LAYER_1"));
        second.send(control("topic.subscribe", new TopicSubscription("LAYER_1", "echo")));
        second.send(subscribe(Names.CLIENT_CONNECTED));

        UUID topicId = second.body(second.single("topic.subscribed"), BrokerAck.class).topicId();
        assertTrue(subscribers(bus, topicId).contains(routeId));

        assertEquals(4001, first.closedWith);
        first.close();

        assertTrue(subscribers(bus, topicId).contains(routeId));
        assertFalse(bus.events.subscribersOf(Names.CLIENT_CONNECTED,
                new io.minaret.broker.contract.Contract.ClientEvent(Names.CLIENT_CONNECTED, routeId.toUUID(), null)).isEmpty());
        assertEquals(routeId, bus.channels.channelProvider("LAYER_1"));
        assertTrue(events(second, ClientEvent.class, Names.CLIENT_DISCONNECTED).isEmpty());
    }

    @Test
    void handleRegister_unknownIdentity_acceptsAndClaimsTheClientProposedRouteId() {
        TestBus bus = new TestBus();
        UUID cachedFromBeforeTheRestart = UUID.randomUUID();
        Socket socket = bus.registered(new ClientInfo(cachedFromBeforeTheRestart, "trading-app", 0, Map.of()));

        assertEquals(cachedFromBeforeTheRestart, socket.single("client.registered").header().to().id());
    }

    @Test
    void eventSubscribe_isAcked_andTheAckEchoesTheRequestsCorrelationId() {
        TestBus bus = new TestBus();
        ClientInfo watcher = client();
        Socket socket = bus.registered(watcher);
        UUID correlation = UUID.randomUUID();
        socket.send(control("event.subscribe", new Subscription(Names.CHANNEL_REGISTERED, null, null, null), correlation));

        WireFrame ack = socket.single("event.subscribed");
        assertNull(ack.header().from());
        assertEquals(watcher.routeId(), ack.header().to().id());
        assertEquals(correlation, ack.header().replyTo());
    }

    @Test
    void anEventFiredByOneConnection_reachesASubscriberOnAnother() {
        TestBus bus = new TestBus();
        Socket watcher = bus.registered(client());
        watcher.send(subscribe(Names.CHANNEL_REGISTERED));

        ClientInfo provider = client();
        bus.registered(provider).send(registerChannel("LAYER_1"));

        List<ChannelEvent> occurrences = events(watcher, ChannelEvent.class, Names.CHANNEL_REGISTERED);
        assertEquals(1, occurrences.size());
        assertEquals("LAYER_1", occurrences.get(0).channel());
        assertEquals(provider.routeId(), occurrences.get(0).providerRouteId());
    }

    @Test
    void subscribingToAPresenceEvent_replaysWhatIsAlreadyLive_beforeTheAck() {
        TestBus bus = new TestBus();
        bus.channels.registerChannel("LAYER_1", Uuid.of(UUID.randomUUID()));

        Socket socket = bus.registered(client());
        socket.send(subscribe(Names.CHANNEL_REGISTERED));

        Replay replay = replay(socket);
        assertNotNull(replay);
        assertEquals(Names.CHANNEL_REGISTERED, replay.name());
        assertEquals(1, replay.channels().size());
        assertEquals("LAYER_1", replay.channels().get(0).channel());

        List<String> types = socket.frames().stream().map(frame -> frame.header().type()).toList();
        assertEquals(1, types.stream().filter("event.replay"::equals).count());
        assertTrue(types.indexOf("event.replay") < types.indexOf("event.subscribed"));
        assertTrue(socket.ofType("event").isEmpty());
    }

    @Test
    void aChannelNarrowedSubscription_replaysOnlyThatChannel() {
        TestBus bus = new TestBus();
        bus.channels.registerChannel("LAYER_1", Uuid.of(UUID.randomUUID()));
        bus.channels.registerChannel("OTHER", Uuid.of(UUID.randomUUID()));

        Socket socket = bus.registered(client());
        socket.send(subscribe(new Subscription(Names.CHANNEL_REGISTERED, "LAYER_1", null, null)));

        Replay replay = replay(socket);
        assertEquals(1, replay.channels().size());
        assertEquals("LAYER_1", replay.channels().get(0).channel());
    }

    @Test
    void aChannelNarrowedSubscription_receivesOnlyThatChannelsEvents() {
        TestBus bus = new TestBus();
        Socket watcher = bus.registered(client());
        watcher.send(subscribe(new Subscription(Names.CHANNEL_REGISTERED, "LAYER_1", null, null)));

        bus.registered(client()).send(registerChannel("OTHER"));
        ClientInfo wanted = client();
        bus.registered(wanted).send(registerChannel("LAYER_1"));

        List<ChannelEvent> occurrences = events(watcher, ChannelEvent.class, Names.CHANNEL_REGISTERED);
        assertEquals(1, occurrences.size());
        assertEquals("LAYER_1", occurrences.get(0).channel());
        assertEquals(wanted.routeId(), occurrences.get(0).providerRouteId());
    }

    @Test
    void aPropertyNarrowedSubscription_receivesOnlyMatchingClients_andTheirInfo() {
        TestBus bus = new TestBus();
        Socket watcher = bus.registered(client());
        watcher.send(subscribe(new Subscription(Names.CLIENT_CONNECTED, null, null, Map.of("role", "worker"))));

        bus.registered(new ClientInfo(UUID.randomUUID(), "scheduler", 0, Map.of("role", "scheduler")));
        ClientInfo worker = new ClientInfo(UUID.randomUUID(), "worker", 0, Map.of("role", "worker", "region", "eu"));
        bus.registered(worker);

        List<ClientEvent> occurrences = events(watcher, ClientEvent.class, Names.CLIENT_CONNECTED);
        assertEquals(1, occurrences.size());
        assertEquals(worker.routeId(), occurrences.get(0).routeId());
        assertEquals("eu", occurrences.get(0).client().properties().get("region"));
    }

    @Test
    void aPropertyNarrowedSubscription_replaysOnlyMatchingClients() {
        TestBus bus = new TestBus();
        ClientInfo scheduler = new ClientInfo(UUID.randomUUID(), "scheduler", 0, Map.of("role", "scheduler"));
        bus.registered(scheduler);
        bus.registered(new ClientInfo(UUID.randomUUID(), "worker", 0, Map.of("role", "worker")));

        Socket socket = bus.registered(client());
        socket.send(subscribe(new Subscription(Names.CLIENT_CONNECTED, null, null, Map.of("role", "scheduler"))));

        Replay replay = replay(socket);
        assertEquals(1, replay.clients().size());
        assertEquals(scheduler.routeId(), replay.clients().get(0).routeId());
    }

    @Test
    void replayOfManyTopics_isOneFrame_notOnePerTopic() {
        TestBus bus = new TestBus();
        Uuid provider = Uuid.of(UUID.randomUUID());
        bus.channels.registerChannel("LAYER_1", provider);
        for (String topic : List.of("echo", "run", "stop")) assertNotNull(bus.channels.registerTopic(provider, "LAYER_1", topic));

        Socket socket = bus.registered(client());
        socket.send(subscribe(Names.TOPIC_REGISTERED));

        Replay replay = replay(socket);
        assertEquals(List.of("echo", "run", "stop"), replay.topics().stream().map(topic -> topic.topic()).sorted().toList());
        assertEquals(1, socket.ofType("event.replay").size());
    }

    @Test
    void anEdgeTriggeredEvent_replaysNothingOnSubscribe() {
        TestBus bus = new TestBus();
        bus.channels.registerChannel("LAYER_1", Uuid.of(UUID.randomUUID()));

        Socket socket = bus.registered(client());
        socket.send(subscribe(Names.CHANNEL_REMOVED));

        assertTrue(socket.ofType("event").isEmpty());
        assertNull(replay(socket));
        assertEquals(1, socket.ofType("event.subscribed").size());
    }

    @Test
    void aDisconnectingProvider_emitsChannelRemoved_thenClientDisconnected() {
        TestBus bus = new TestBus();
        Socket watcher = bus.registered(client());
        watcher.send(subscribe(Names.CHANNEL_REMOVED));
        watcher.send(subscribe(Names.CLIENT_DISCONNECTED));

        ClientInfo provider = client();
        Socket providerSocket = bus.registered(provider);
        providerSocket.send(registerChannel("LAYER_1"));
        providerSocket.close();

        List<WireFrame> occurrences = watcher.ofType("event");
        assertEquals(2, occurrences.size());
        ChannelEvent removed = watcher.body(occurrences.get(0), ChannelEvent.class);
        assertEquals(Names.CHANNEL_REMOVED, removed.name());
        assertEquals("LAYER_1", removed.channel());
        ClientEvent disconnected = watcher.body(occurrences.get(1), ClientEvent.class);
        assertEquals(Names.CLIENT_DISCONNECTED, disconnected.name());
        assertEquals(provider.routeId(), disconnected.routeId());
    }

    @Test
    void aSubscriberThatDropped_isSkipped_withoutFailingTheOperationThatFiredTheEvent() {
        TestBus bus = new TestBus();
        Socket watcher = bus.registered(client());
        watcher.send(subscribe(Names.CHANNEL_REGISTERED));
        watcher.channel.close(); // gone, but its teardown has not run yet: still in every table

        ClientInfo provider = client();
        Socket providerSocket = bus.registered(provider);
        providerSocket.send(registerChannel("LAYER_1"));

        assertEquals(provider.routeId(), providerSocket.single("channel.registered").header().to().id());
    }

    private static List<Uuid> subscribers(TestBus bus, UUID topicId) {
        List<Uuid> routes = new java.util.ArrayList<>();
        ChannelRegistry.Topic topic = bus.channels.topic(Uuid.of(topicId));
        for (ChannelRegistry.SubscriptionNode node = topic.head(); node != null; node = node.next())
            if (!node.unlinked()) routes.add(node.route.id);
        return routes;
    }

}
