package io.minaret.messaging;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Layer 2's annotated surface — the half javascript/ could not port, because decorators there
 * need the transpile step that package exists to avoid.
 *
 * The planning assertions matter more than the round-trip ones: the whole design rests on an
 * ill-formed contract failing when the interface is introduced rather than on whichever later call
 * first reaches the bad method.
 */
class AnnotatedTest {

    private static final int TIMEOUT_MS = 5000;

    private static ChannelProvider provider(FakeConnection connection, Router router, String name) throws Exception {
        router.connectTo(ClientInfo.create(), connection).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        return router.routeId() == null ? null : ChannelProvider.create(router, name).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    @Test
    void providerPublishesWhenNoRecipientIsNamed() throws Exception {
        FakeConnection connection = new FakeConnection();

        try (Router router = new Router(() -> connection)) {
            ChannelProvider handle = provider(connection, router, "LAYER_1");
            connection.clearSent();

            Feed feed = handle.proxy(Feed.class);
            feed.tick("t1");

            FakeConnection.Sent sent = awaitSent(connection, Verbs.PUBLISH);
            assertEquals("\"t1\"", sent.body());
            assertEquals("LAYER_1", sent.header().to().channel());
            assertEquals("ticks", sent.header().to().topic());
        }
    }

    /**
     * A leading UUID on a provider method is addressing, not payload — so it must not appear in the
     * body, and it must ride in replyTo, which is what the notify verb frees up.
     */
    @Test
    void providerNotifiesOneClientWithTheRecipientInReplyTo() throws Exception {
        FakeConnection connection = new FakeConnection();

        try (Router router = new Router(() -> connection)) {
            ChannelProvider handle = provider(connection, router, "LAYER_1");
            connection.clearSent();

            UUID recipient = UUID.randomUUID();
            handle.proxy(Feed.class).slice(recipient, "block");

            FakeConnection.Sent sent = awaitSent(connection, Verbs.NOTIFY);
            assertEquals(recipient, sent.header().replyTo());
            assertEquals("\"block\"", sent.body(), "the recipient must not be packed into the body");
        }
    }

    @Test
    void multipleArgumentsPackPositionallyAndOneArgumentIsTheBodyItself() throws Exception {
        FakeConnection connection = new FakeConnection();

        try (Router router = new Router(() -> connection)) {
            ChannelProvider handle = provider(connection, router, "LAYER_1");
            connection.clearSent();

            handle.proxy(Feed.class).range("a", 2);

            assertEquals("[\"a\",2]", awaitSent(connection, Verbs.PUBLISH).body());
        }
    }

    @Test
    void boundMethodReceivesTheSenderRouteIdFromALeadingUuid() throws Exception {
        FakeConnection connection = new FakeConnection();

        try (Router router = new Router(() -> connection)) {
            ChannelProvider handle = provider(connection, router, "LAYER_1");

            Controller controller = new Controller();
            handle.bind(controller).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

            UUID caller = UUID.randomUUID();
            connection.raise(
                    Header.of(Verbs.DISPATCH, Destination.ofId(connection.topicId("viewport"))).withFrom(caller),
                    new Viewport(10, 40)
            );

            assertTrue(controller.received.await(TIMEOUT_MS, TimeUnit.MILLISECONDS));
            assertEquals(caller, controller.from.get());
            assertEquals(new Viewport(10, 40), controller.viewport.get());
        }
    }

    // --- planning: the shapes that must fail when the interface is introduced -------------------

    @Test
    void clientMayNotAddressARecipient() throws Exception {
        FakeConnection connection = new FakeConnection();

        try (Router router = new Router(() -> connection)) {
            router.connectTo(ClientInfo.create(), connection).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
            ChannelProvider handle = ChannelProvider.create(router, "LAYER_1").get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
            ChannelClient client = ChannelClient.create(router, "LAYER_1").get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

            assertNotNull(handle);
            IllegalArgumentException failure =
                    assertThrows(IllegalArgumentException.class, () -> client.proxy(Feed.class));

            assertTrue(failure.getMessage().contains("may only address its channel's provider"), failure.getMessage());
        }
    }

    /**
     * A provider-originated request has no wire representation: the Broker reads every frame a provider
     * sends on its own topic as a reply, so the call would hang forever.
     */
    @Test
    void providerMayNotExpectAnAnswer() throws Exception {
        FakeConnection connection = new FakeConnection();

        try (Router router = new Router(() -> connection)) {
            ChannelProvider handle = provider(connection, router, "LAYER_1");

            IllegalArgumentException failure =
                    assertThrows(IllegalArgumentException.class, () -> handle.proxy(Asking.class));

            assertTrue(failure.getMessage().contains("cannot ask on its own topic"), failure.getMessage());
        }
    }

    @Test
    void unannotatedMethodIsAGapInTheContractNotAnExtra() throws Exception {
        FakeConnection connection = new FakeConnection();

        try (Router router = new Router(() -> connection)) {
            ChannelProvider handle = provider(connection, router, "LAYER_1");

            assertTrue(
                    assertThrows(IllegalArgumentException.class, () -> handle.proxy(PartlyAnnotated.class))
                            .getMessage().contains("has no @Topic")
            );
        }
    }

    @Test
    void bindingToTheWrongChannelIsRejected() throws Exception {
        FakeConnection connection = new FakeConnection();

        try (Router router = new Router(() -> connection)) {
            ChannelProvider handle = provider(connection, router, "OTHER");

            CompletableFuture<Void> bound = handle.bind(new Controller());

            assertTrue(
                    assertThrows(java.util.concurrent.ExecutionException.class,
                            () -> bound.get(TIMEOUT_MS, TimeUnit.MILLISECONDS))
                            .getCause().getMessage().contains("declares channel 'LAYER_1'")
            );
        }
    }

    private static FakeConnection.Sent awaitSent(FakeConnection connection, String verb) throws InterruptedException {
        for (int attempt = 0; attempt < 100; attempt++) {
            List<FakeConnection.Sent> sent = connection.sent();
            for (FakeConnection.Sent candidate : sent)
                if (verb.equals(candidate.header().type())) return candidate;

            Thread.sleep(20);
        }

        throw new AssertionError("no '" + verb + "' frame was sent");
    }

    record Viewport(int start, int count) {
    }

    @Channel("LAYER_1")
    interface Feed {

        @Topic("ticks")
        void tick(String value);

        @Topic("slice")
        void slice(UUID to, String block);

        @Topic("range")
        void range(String from, int count);

    }

    @Channel("LAYER_1")
    interface Asking {

        @Topic("ask")
        CompletableFuture<String> ask(String question);

    }

    @Channel("LAYER_1")
    interface PartlyAnnotated {

        @Topic("ok")
        void ok(String value);

        void forgotten(String value);

    }

    @Channel("LAYER_1")
    static class Controller {

        final CountDownLatch received = new CountDownLatch(1);
        final AtomicReference<UUID> from = new AtomicReference<>();
        final AtomicReference<Viewport> viewport = new AtomicReference<>();

        @Topic("viewport")
        public void viewport(UUID sender, Viewport value) {
            from.set(sender);
            viewport.set(value);
            received.countDown();
        }

    }

}
