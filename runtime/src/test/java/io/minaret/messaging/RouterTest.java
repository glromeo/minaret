package io.minaret.messaging;

import com.fasterxml.jackson.databind.JsonNode;
import io.minaret.util.Json;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RouterTest {

    private static final int TIMEOUT_MS = 5000;

    @Test
    void connect_completesOnTheBrokersAck_andAdoptsTheEnrichedClientInfo() throws Exception {
        FakeConnection connection = new FakeConnection();
        connection.ackClockOffsetMs = 37;

        try (Router router = new Router(() -> connection)) {
            ClientInfo sent = ClientInfo.create();
            ClientInfo registered = router.connectTo(sent, connection).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

            assertNotNull(registered.routeId(), "the Broker must assign a RouteId when none was proposed");
            assertEquals(sent.identity(), registered.identity());
            assertEquals(37, registered.clockOffsetMs());
            assertEquals(registered, router.clientInfo());
        }
    }

    @Test
    void registerFrameIsBrokerAddressedAndCarriesTheWholeClientInfo() throws Exception {
        FakeConnection connection = new FakeConnection();

        try (Router router = new Router(() -> connection)) {
            ClientInfo info = ClientInfo.create(Map.of("role", "provider"));
            router.connectTo(info, connection).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

            FakeConnection.Sent register = connection.sent().get(0);

            assertEquals(Verbs.CLIENT_REGISTER, register.header().type());
            assertTrue(register.header().to().isEmpty(), "register must be Broker-addressed");
            assertEquals(info.routeId(), register.header().from());
            assertEquals(info, Json.tryParse(register.body(), ClientInfo.class));
        }
    }

    /**
     * There is no implicit registration timeout — bounding the wait is deliberately the caller's choice,
     * which on this side is orTimeout rather than a cancellation token.
     */
    @Test
    void registrationWaitsIndefinitelyUnlessTheCallerBoundsIt() {
        FakeConnection connection = new FakeConnection();
        connection.suppressRegistrationAck = true;

        try (Router router = new Router(() -> connection)) {
            CompletableFuture<ClientInfo> pending = router.connectTo(ClientInfo.create(), connection)
                    .orTimeout(200, TimeUnit.MILLISECONDS);

            assertThrows(java.util.concurrent.ExecutionException.class, () -> pending.get(TIMEOUT_MS, TimeUnit.MILLISECONDS));
        }
    }

    @Test
    void sendMessage_stampsFromAndTime_butRespectsWhatTheCallerSet() throws Exception {
        FakeConnection connection = new FakeConnection();

        try (Router router = new Router(() -> connection)) {
            ClientInfo registered = router.connectTo(ClientInfo.create(), connection).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
            connection.clearSent();

            router.sendMessage(Header.of("greeting"), "hello").get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

            Header stamped = connection.sent().get(0).header();
            assertEquals(registered.routeId(), stamped.from());
            assertTrue(stamped.time() > 0, "time should be stamped at send");

            connection.clearSent();

            UUID explicit = UUID.randomUUID();
            router.sendMessage(Header.of("greeting").withFrom(explicit).withTime(123), "hello")
                    .get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

            Header kept = connection.sent().get(0).header();
            assertEquals(explicit, kept.from());
            assertEquals(123, kept.time());
        }
    }

    @Test
    void sendBeforeConnect_fails() {
        FakeConnection connection = new FakeConnection();

        try (Router router = new Router(() -> connection)) {
            assertThrows(
                    java.util.concurrent.ExecutionException.class,
                    () -> router.sendMessage(Header.of("greeting"), "hello").get(TIMEOUT_MS, TimeUnit.MILLISECONDS)
            );
        }
    }

    @Test
    void unparseableBodyIsDroppedRatherThanBreakingTheMessageLoop() throws Exception {
        FakeConnection connection = new FakeConnection();

        try (Router router = new Router(() -> connection)) {
            router.connectTo(ClientInfo.create(), connection).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

            List<Message<JsonNode>> received = new ArrayList<>();
            CountDownLatch latch = new CountDownLatch(1);
            router.handleMessage().add(message -> {
                received.add(message);
                latch.countDown();
            });

            connection.raiseRaw("M01||greeting|1||" + io.minaret.messaging.wire.Guids.encode(UUID.randomUUID()) + "|||\0not json");
            connection.raise(Header.of("greeting").withFrom(UUID.randomUUID()), "hello");

            assertTrue(latch.await(TIMEOUT_MS, TimeUnit.MILLISECONDS), "the good frame must still arrive");
            assertEquals(1, received.size());
            assertEquals("hello", received.get(0).body().asText());
        }
    }

    /**
     * A late ack on a connection the Router has already replaced must not complete the new registration
     * and report the wrong clock offset.
     */
    @Test
    void staleConnectionCannotCompleteTheNewRegistration() throws Exception {
        FakeConnection first = new FakeConnection();
        FakeConnection second = new FakeConnection();
        second.ackClockOffsetMs = 99;

        try (Router router = new Router(() -> first)) {
            ClientInfo info = ClientInfo.create();
            router.connectTo(info, first).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
            ClientInfo registered = router.connectTo(info, second).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

            assertEquals(99, registered.clockOffsetMs());
            assertTrue(first.closed, "the replaced connection must be closed");

            // The stale connection raising its own ack must change nothing.
            first.raiseRegistrationAck(new ClientInfo(info.routeId(), info.identity(), -1, Map.of()));

            assertEquals(99, router.clientInfo().clockOffsetMs());
        }
    }

    @Test
    void eventsAreRoutedByNameAndNotThroughHandleMessage() throws Exception {
        FakeConnection connection = new FakeConnection();

        try (Router router = new Router(() -> connection)) {
            router.connectTo(ClientInfo.create(), connection).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

            List<Events.Event> seen = new ArrayList<>();
            List<Events.Origin> origins = new ArrayList<>();
            CountDownLatch latch = new CountDownLatch(1);

            router.on(Events.Names.CHANNEL_REGISTERED, (event, origin) -> {
                seen.add(event);
                origins.add(origin);
                latch.countDown();
            }).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

            List<Message<JsonNode>> throughHandleMessage = new ArrayList<>();
            router.handleMessage().add(throughHandleMessage::add);

            UUID provider = UUID.randomUUID();
            connection.raise(
                    Header.of(Verbs.EVENT),
                    new Events.ChannelEvent(Events.Names.CHANNEL_REGISTERED, "LAYER_1", provider)
            );

            assertTrue(latch.await(TIMEOUT_MS, TimeUnit.MILLISECONDS));
            assertEquals(1, seen.size());
            assertEquals(Events.Origin.LIVE, origins.get(0));
            assertEquals("LAYER_1", ((Events.ChannelEvent) seen.get(0)).channel());
            assertTrue(throughHandleMessage.isEmpty(), "events must not surface through handleMessage");
        }
    }

    /**
     * A replay frame carries the whole snapshot but fans out occurrence by occurrence, so a handler's
     * contract is one occurrence at a time regardless of framing — only the origin differs.
     */
    @Test
    void replayFansOutOccurrenceByOccurrenceWithReplayOrigin() throws Exception {
        FakeConnection connection = new FakeConnection();

        try (Router router = new Router(() -> connection)) {
            router.connectTo(ClientInfo.create(), connection).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

            List<Events.Origin> origins = new ArrayList<>();
            CountDownLatch latch = new CountDownLatch(2);

            router.on(Events.Names.CHANNEL_REGISTERED, (event, origin) -> {
                origins.add(origin);
                latch.countDown();
            }).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

            connection.raise(
                    Header.of(Verbs.EVENT_REPLAY),
                    new Events.Replay(
                            Events.Names.CHANNEL_REGISTERED,
                            null,
                            List.of(
                                    new Events.ChannelEvent(Events.Names.CHANNEL_REGISTERED, "A", UUID.randomUUID()),
                                    new Events.ChannelEvent(Events.Names.CHANNEL_REGISTERED, "B", UUID.randomUUID())
                            ),
                            null
                    )
            );

            assertTrue(latch.await(TIMEOUT_MS, TimeUnit.MILLISECONDS));
            assertEquals(List.of(Events.Origin.REPLAY, Events.Origin.REPLAY), origins);
        }
    }

    /**
     * Handlers survive a reconnect in memory; the Broker-side subscriptions do not. Re-subscribing is
     * what keeps a reconnected client from staying armed for events it no longer receives.
     */
    @Test
    void reconnectReSubscribesEveryEventSubscription() throws Exception {
        FakeConnection first = new FakeConnection();
        FakeConnection second = new FakeConnection();

        try (Router router = new Router(() -> first)) {
            router.connectTo(ClientInfo.create(), first).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
            router.on(Events.Names.CLIENT_CONNECTED, (event, origin) -> {
            }).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

            router.connectTo(ClientInfo.create(), second).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

            boolean resubscribed = second.sent().stream()
                    .anyMatch(sent -> Verbs.EVENT_SUBSCRIBE.equals(sent.header().type()));

            assertTrue(resubscribed, "the new connection must carry the subscription again");
        }
    }

    /**
     * The Broker evicted this client because another registered under its identity and took its RouteId.
     * Reconnecting would take the RouteId straight back and evict that connection, which would reconnect
     * in turn — the two trading it at socket speed, every round emitting presence events. So a superseded
     * close is the one ending the reconnect policy does not answer; every other one it does.
     */
    @Test
    void aSupersededCloseIsTerminalWhereAnyOtherCloseReconnects() throws Exception {
        FakeConnection first = new FakeConnection();
        FakeConnection second = new FakeConnection();

        try (Router router = new Router(() -> second)) {
            router.reconnect(1, 10, Integer.MAX_VALUE);
            router.connectTo(ClientInfo.create(), first).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

            first.onDisconnected().emit(CloseCodes.SUPERSEDED);
            Thread.sleep(100);

            assertTrue(second.sent().isEmpty(), "a superseded client must not reconnect");
        }

        FakeConnection dropped = new FakeConnection();
        FakeConnection replacement = new FakeConnection();

        try (Router router = new Router(() -> replacement)) {
            router.reconnect(1, 10, Integer.MAX_VALUE);
            router.connectTo(ClientInfo.create(), dropped).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

            // 1006: what a socket that died without a close frame reports.
            dropped.onDisconnected().emit(1006);
            Thread.sleep(100);

            assertTrue(replacement.sent().stream().anyMatch(sent -> Verbs.CLIENT_REGISTER.equals(sent.header().type())),
                    "an ordinary drop must still reconnect");
        }
    }

    @Test
    void onStateChangeReportsRegisteredNotMerelySocketOpen() throws Exception {
        FakeConnection connection = new FakeConnection();

        try (Router router = new Router(() -> connection)) {
            List<Boolean> states = new ArrayList<>();
            router.onStateChange().add(states::add);

            router.connectTo(ClientInfo.create(), connection).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

            assertEquals(List.of(true), states);
        }
    }

    @Test
    void discoveryFailureSurfacesRatherThanHanging() {
        assertNotNull(assertThrows(java.io.IOException.class, () ->
                io.minaret.discovery.BrokerDiscovery.discover("NO_SUCH_REALM_" + UUID.randomUUID())
        ).getMessage());
    }

}
