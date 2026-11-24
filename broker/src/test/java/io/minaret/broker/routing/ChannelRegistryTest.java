package io.minaret.broker.routing;

import io.minaret.broker.routing.ChannelRegistry.Channel;
import io.minaret.broker.routing.ChannelRegistry.SubscriptionNode;
import io.minaret.broker.wire.Uuid;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ChannelRegistry at the registry level; RoutingTest is the wire level. The walks here go node by node
 * through Topic.head and next, exactly as Session's fanout does, rather than through a snapshot.
 */
class ChannelRegistryTest {

    private static ChannelRegistry registry() {
        return new ChannelRegistry(new ConnectionRegistry(new BrokerMetrics(new SimpleMeterRegistry())));
    }

    private static Uuid newId() {
        return Uuid.of(UUID.randomUUID());
    }

    /** The live subscribers, walked as the fanout walks them. */
    private static List<Uuid> subscribersOf(ChannelRegistry channels, Uuid topicId) {
        List<Uuid> routes = new ArrayList<>();
        ChannelRegistry.Topic topic = channels.topic(topicId);
        if (topic == null) return routes;
        for (SubscriptionNode node = topic.head(); node != null; node = node.next())
            if (!node.unlinked()) routes.add(node.route.id);
        return routes;
    }

    private static void disconnect(ChannelRegistry channels, Uuid routeId, Channel provided) {
        channels.removeConnection(routeId);
        if (provided != null) channels.removeChannel(provided);
    }

    @Test
    void registerChannel_thenResolve_returnsRegisteredRouteId() {
        ChannelRegistry channels = registry();
        Uuid routeId = newId();
        assertNotNull(channels.registerChannel("LAYER_1", routeId));
        assertEquals(routeId, channels.channelProvider("LAYER_1"));
    }

    @Test
    void registerChannel_sameRouteIdTwice_isIdempotent() {
        ChannelRegistry channels = registry();
        Uuid routeId = newId();
        Channel first = channels.registerChannel("LAYER_1", routeId);
        assertEquals(first, channels.registerChannel("LAYER_1", routeId));
    }

    @Test
    void registerChannel_differentRouteIdForClaimedName_isRejected() {
        ChannelRegistry channels = registry();
        assertNotNull(channels.registerChannel("LAYER_1", newId()));
        assertNull(channels.registerChannel("LAYER_1", newId()));
    }

    @Test
    void resolve_beforeRegister_blocksThenCompletesOnceRegistered() throws Exception {
        ChannelRegistry channels = registry();
        CompletableFuture<Uuid> resolve = channels.resolve("LAYER_1");
        assertFalse(resolve.isDone());

        Uuid routeId = newId();
        channels.registerChannel("LAYER_1", routeId);
        assertEquals(routeId, resolve.get(5, TimeUnit.SECONDS));
    }

    @Test
    void resolve_cancelledBeforeRegister_isCancelled_andNotCompletedByALaterRegister() {
        ChannelRegistry channels = registry();
        CompletableFuture<Uuid> resolve = channels.resolve("NEVER");
        channels.cancelResolution("NEVER", resolve);
        channels.registerChannel("NEVER", newId());
        assertTrue(resolve.isCancelled());
    }

    @Test
    void registerTopic_thenListTopics_returnsRegisteredTopics() {
        ChannelRegistry channels = registry();
        Uuid routeId = newId();
        channels.registerChannel("LAYER_1", routeId);
        assertNotNull(channels.registerTopic(routeId, "LAYER_1", "echo"));
        assertNotNull(channels.registerTopic(routeId, "LAYER_1", "ping"));
        assertEquals(List.of("echo", "ping"), channels.listTopics(routeId, "LAYER_1").stream().sorted().toList());
    }

    @Test
    void registerTopic_withoutAChannel_fails() {
        assertNull(registry().registerTopic(newId(), "LAYER_1", "echo"));
    }

    @Test
    void listTopics_forUnknownChannel_returnsEmpty() {
        assertTrue(registry().listTopics(newId(), "LAYER_1").isEmpty());
    }

    @Test
    void topicId_isStable_acrossRegisterAndSubscribe_inEitherOrder() {
        ChannelRegistry registerFirst = registry();
        Uuid a = newId();
        registerFirst.registerChannel("A", a);
        Uuid registered = registerFirst.registerTopic(a, "A", "echo");
        assertEquals(registered, registerFirst.subscribe(a, "A", "echo", newId()));

        ChannelRegistry subscribeFirst = registry();
        Uuid b = newId();
        subscribeFirst.registerChannel("B", b);
        Uuid subscribed = subscribeFirst.subscribe(b, "B", "echo", newId());
        assertEquals(subscribed, subscribeFirst.registerTopic(b, "B", "echo"));
    }

    @Test
    void subscribe_thenSubscribersOf_returnsSubscriber() {
        ChannelRegistry channels = registry();
        Uuid provider = newId();
        Uuid subscriber = newId();
        channels.registerChannel("LAYER_1", provider);
        Uuid topicId = channels.subscribe(provider, "LAYER_1", "echo", subscriber);
        assertEquals(List.of(subscriber), subscribersOf(channels, topicId));
    }

    @Test
    void subscribe_twice_doesNotDuplicateTheSubscriber() {
        ChannelRegistry channels = registry();
        Uuid provider = newId();
        Uuid subscriber = newId();
        channels.registerChannel("LAYER_1", provider);
        Uuid first = channels.subscribe(provider, "LAYER_1", "echo", subscriber);
        Uuid second = channels.subscribe(provider, "LAYER_1", "echo", subscriber);
        assertEquals(first, second);
        assertEquals(List.of(subscriber), subscribersOf(channels, first));
    }

    @Test
    void removeConnection_dropsSubscriberFromFutureFanout() {
        ChannelRegistry channels = registry();
        Uuid provider = newId();
        Uuid subscriber = newId();
        channels.registerChannel("LAYER_1", provider);
        Uuid topicId = channels.subscribe(provider, "LAYER_1", "echo", subscriber);
        channels.removeConnection(subscriber);
        assertTrue(subscribersOf(channels, topicId).isEmpty());
    }

    @Test
    void removeConnection_unlinksMidListSubscriber_leavingNeighboursReachable() {
        ChannelRegistry channels = registry();
        Uuid provider = newId();
        channels.registerChannel("LAYER_1", provider);
        Uuid first = newId();
        Uuid middle = newId();
        Uuid last = newId();
        Uuid topicId = channels.subscribe(provider, "LAYER_1", "echo", first);
        channels.subscribe(provider, "LAYER_1", "echo", middle);
        channels.subscribe(provider, "LAYER_1", "echo", last);

        channels.removeConnection(middle);

        List<Uuid> remaining = subscribersOf(channels, topicId);
        assertEquals(Set.of(first, last), new HashSet<>(remaining));
        assertEquals(2, remaining.size());
    }

    @Test
    void removeConnection_unlinksSubscriberFromEveryTopicItHeld() {
        ChannelRegistry channels = registry();
        Uuid provider = newId();
        channels.registerChannel("LAYER_1", provider);
        Uuid subscriber = newId();
        Uuid survivor = newId();
        Uuid topicA = channels.subscribe(provider, "LAYER_1", "a", subscriber);
        Uuid topicB = channels.subscribe(provider, "LAYER_1", "b", subscriber);
        channels.subscribe(provider, "LAYER_1", "b", survivor);

        channels.removeConnection(subscriber);

        assertTrue(subscribersOf(channels, topicA).isEmpty());
        assertEquals(List.of(survivor), subscribersOf(channels, topicB));
    }

    @Test
    void aWalkStartedBeforeAnUnlink_stillTerminates() {
        assertWalkSurvives((channels, topicId, victim) -> channels.removeConnection(victim));
    }

    @Test
    void aWalkStartedBeforeAnUnsubscribe_stillTerminates() {
        assertWalkSurvives((channels, topicId, victim) -> channels.unsubscribe(topicId, victim));
    }

    private interface Unlinker {
        void unlink(ChannelRegistry channels, Uuid topicId, Uuid victim);
    }

    /** Parks a walk on the head, unlinks a node further down, and checks the walk still finishes without it. */
    private static void assertWalkSurvives(Unlinker unlinker) {
        ChannelRegistry channels = registry();
        Uuid provider = newId();
        channels.registerChannel("LAYER_1", provider);
        List<Uuid> subscribers = new ArrayList<>();
        Uuid topicId = null;
        for (int i = 0; i < 5; i++) {
            Uuid id = newId();
            subscribers.add(id);
            topicId = channels.subscribe(provider, "LAYER_1", "echo", id);
        }

        SubscriptionNode parked = channels.topic(topicId).head();
        Uuid victim = subscribers.get(1); // pushed at the head, so index 1 is second from the tail
        unlinker.unlink(channels, topicId, victim);

        List<Uuid> seen = new ArrayList<>();
        seen.add(parked.route.id);
        for (SubscriptionNode node = parked.next(); node != null; node = node.next())
            if (!node.unlinked()) seen.add(node.route.id);

        assertFalse(seen.contains(victim));
        assertEquals(4, seen.size());
    }

    @Test
    void unsubscribe_unlinksMidListSubscriber_leavingNeighboursReachable() {
        ChannelRegistry channels = registry();
        Uuid provider = newId();
        channels.registerChannel("LAYER_1", provider);
        Uuid first = newId();
        Uuid middle = newId();
        Uuid last = newId();
        Uuid topicId = channels.subscribe(provider, "LAYER_1", "echo", first);
        channels.subscribe(provider, "LAYER_1", "echo", middle);
        channels.subscribe(provider, "LAYER_1", "echo", last);

        assertTrue(channels.unsubscribe(topicId, middle));

        assertEquals(Set.of(first, last), new HashSet<>(subscribersOf(channels, topicId)));
    }

    @Test
    void unsubscribe_leavesOtherSubscriptionsIntact_andLaterDisconnectStillUnlinksThem() {
        ChannelRegistry channels = registry();
        Uuid provider = newId();
        channels.registerChannel("LAYER_1", provider);
        Uuid subscriber = newId();
        Uuid topicA = channels.subscribe(provider, "LAYER_1", "a", subscriber);
        Uuid topicB = channels.subscribe(provider, "LAYER_1", "b", subscriber);
        Uuid topicC = channels.subscribe(provider, "LAYER_1", "c", subscriber);

        assertTrue(channels.unsubscribe(topicB, subscriber));
        assertEquals(List.of(subscriber), subscribersOf(channels, topicA));
        assertTrue(subscribersOf(channels, topicB).isEmpty());
        assertEquals(List.of(subscriber), subscribersOf(channels, topicC));

        channels.removeConnection(subscriber);
        assertTrue(subscribersOf(channels, topicA).isEmpty());
        assertTrue(subscribersOf(channels, topicC).isEmpty());
    }

    @Test
    void unsubscribe_whenNotSubscribed_returnsFalse() {
        ChannelRegistry channels = registry();
        Uuid provider = newId();
        channels.registerChannel("LAYER_1", provider);
        Uuid topicId = channels.subscribe(provider, "LAYER_1", "echo", newId());
        assertFalse(channels.unsubscribe(topicId, newId()));
        assertFalse(channels.unsubscribe(newId(), newId()));
    }

    @Test
    void unsubscribe_isNotIdempotent_andResubscribeWorksAfterwards() {
        ChannelRegistry channels = registry();
        Uuid provider = newId();
        channels.registerChannel("LAYER_1", provider);
        Uuid subscriber = newId();
        Uuid topicId = channels.subscribe(provider, "LAYER_1", "echo", subscriber);

        assertTrue(channels.unsubscribe(topicId, subscriber));
        assertFalse(channels.unsubscribe(topicId, subscriber));
        assertTrue(subscribersOf(channels, topicId).isEmpty());

        assertEquals(topicId, channels.subscribe(provider, "LAYER_1", "echo", subscriber));
        assertEquals(List.of(subscriber), subscribersOf(channels, topicId));
    }

    /**
     * Lock-free walkers racing subscribe/unsubscribe churn. This is the test the volatile links
     * exist for. A non-volatile next could let a walker see a half-published node or a stale cycle.
     */
    @Test
    void walkedConcurrentlyWithChurn_neverLoopsDuplicatesOrLosesAStableSubscriber() throws Exception {
        ChannelRegistry channels = registry();
        Uuid provider = newId();
        channels.registerChannel("LAYER_1", provider);
        List<Uuid> stable = new ArrayList<>();
        Uuid topicId = null;
        for (int i = 0; i < 4; i++) {
            Uuid id = newId();
            stable.add(id);
            topicId = channels.subscribe(provider, "LAYER_1", "echo", id);
        }
        List<Uuid> churning = new ArrayList<>();
        for (int i = 0; i < 32; i++) churning.add(newId());
        Set<Uuid> known = new HashSet<>(stable);
        known.addAll(churning);

        Uuid topic = topicId;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        AtomicInteger walks = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(6);
        try {
            List<Future<?>> tasks = new ArrayList<>();
            for (int i = 0; i < 2; i++)
                tasks.add(pool.submit(() -> {
                    while (System.nanoTime() < deadline) {
                        Uuid id = churning.get(ThreadLocalRandom.current().nextInt(churning.size()));
                        channels.subscribe(provider, "LAYER_1", "echo", id);
                        channels.unsubscribe(topic, id);
                    }
                }));
            for (int i = 0; i < 4; i++)
                tasks.add(pool.submit(() -> {
                    while (System.nanoTime() < deadline) {
                        List<Uuid> seen = subscribersOf(channels, topic);
                        assertTrue(seen.size() <= known.size(), "the walk yielded more nodes than exist: the list is cyclic");
                        assertEquals(seen.size(), new HashSet<>(seen).size());
                        assertTrue(known.containsAll(seen));
                        assertTrue(seen.containsAll(stable));
                        walks.incrementAndGet();
                    }
                }));
            for (Future<?> task : tasks) task.get();
        } finally {
            pool.shutdownNow();
        }
        assertTrue(walks.get() > 0, "no fanout walk completed");
    }

    @Test
    void subscribe_afterProviderDisconnected_failsAndDoesNotResurrectTheTopic() {
        ChannelRegistry channels = registry();
        Uuid provider = newId();
        Channel channel = channels.registerChannel("LAYER_1", provider);
        Uuid topicId = channels.registerTopic(provider, "LAYER_1", "echo");

        disconnect(channels, provider, channel);

        assertNull(channels.subscribe(provider, "LAYER_1", "echo", newId()));
        assertNull(channels.topic(topicId));
        assertNull(channels.subscribe(provider, "LAYER_1", "unheard-of", newId()));
        assertNull(channels.registerTopic(provider, "LAYER_1", "unheard-of"));
    }

    /**
     * subscribe finds the channel lock-free, so a removeChannel can land before subscribe takes the
     * lock. The invariant is that no topic outlives its channel: once the provider is gone, no id
     * subscribe handed out still resolves. Without getOrAddTopic's reference re-check, a stale
     * Channel plants an orphan topic that nothing removes.
     */
    @Test
    void subscribe_racingProviderDisconnect_leavesNoTopicBehind() throws Exception {
        AtomicInteger orphans = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            for (int round = 0; round < 200; round++) {
                ChannelRegistry channels = registry();
                Uuid provider = newId();
                Channel channel = channels.registerChannel("LAYER_1", provider);
                CyclicBarrier start = new CyclicBarrier(2);
                List<Uuid> handedOut = new ArrayList<>();

                Future<?> subscribing = pool.submit(() -> {
                    start.await();
                    for (int i = 0; i < 50; i++) {
                        Uuid id = channels.subscribe(provider, "LAYER_1", "topic-" + i, newId());
                        if (id != null) handedOut.add(id);
                    }
                    return null;
                });
                Future<?> dropping = pool.submit(() -> {
                    start.await();
                    disconnect(channels, provider, channel);
                    return null;
                });
                subscribing.get();
                dropping.get();
                for (Uuid id : handedOut)
                    if (channels.topic(id) != null) orphans.incrementAndGet();
            }
        } finally {
            pool.shutdownNow();
        }
        assertEquals(0, orphans.get());
    }

    @Test
    void removeChannel_onProvider_prunesSubscribersItLeavesWithNoSubscriptions() {
        ChannelRegistry channels = registry();
        Uuid firstProvider = newId();
        Uuid subscriber = newId();
        Channel first = channels.registerChannel("LAYER_1", firstProvider);
        Uuid deadTopic = channels.subscribe(firstProvider, "LAYER_1", "echo", subscriber);
        disconnect(channels, firstProvider, first);
        assertTrue(subscribersOf(channels, deadTopic).isEmpty());

        Uuid secondProvider = newId();
        channels.registerChannel("OTHER", secondProvider);
        Uuid liveTopic = channels.subscribe(secondProvider, "OTHER", "echo", subscriber);
        assertEquals(List.of(subscriber), subscribersOf(channels, liveTopic));
        assertTrue(channels.unsubscribe(liveTopic, subscriber));
        assertTrue(subscribersOf(channels, liveTopic).isEmpty());

        liveTopic = channels.subscribe(secondProvider, "OTHER", "echo", subscriber);
        disconnect(channels, subscriber, null);
        assertTrue(subscribersOf(channels, liveTopic).isEmpty());
    }

    @Test
    void removeChannel_onProvider_freesChannelNameAndTopics() {
        ChannelRegistry channels = registry();
        Uuid provider = newId();
        Channel channel = channels.registerChannel("LAYER_1", provider);
        Uuid topicId = channels.registerTopic(provider, "LAYER_1", "echo");

        disconnect(channels, provider, channel);

        assertNull(channels.channelProvider("LAYER_1"));
        assertTrue(channels.listTopics(provider, "LAYER_1").isEmpty());
        assertNull(channels.topic(topicId));
        assertNotNull(channels.registerChannel("LAYER_1", newId()));
    }

    @Test
    void removeChannel_afterTheNameWasReclaimed_leavesTheNewRegistrationStanding() {
        ChannelRegistry channels = registry();
        Uuid oldProvider = newId();
        Uuid newProvider = newId();
        Channel stale = channels.registerChannel("LAYER_1", oldProvider);
        channels.removeChannel(stale);
        Channel reclaimed = channels.registerChannel("LAYER_1", newProvider);
        Uuid topicId = channels.registerTopic(newProvider, "LAYER_1", "echo");

        assertFalse(channels.removeChannel(stale));

        assertEquals(newProvider, channels.channelProvider("LAYER_1"));
        assertNotNull(channels.topic(topicId));
        assertNotSame(stale, reclaimed);
    }

    @Test
    void oneRouteId_canProvideMultipleChannels_bothIndependentlyUsable() {
        ChannelRegistry channels = registry();
        Uuid routeId = newId();
        channels.registerChannel("FIRST", routeId);
        channels.registerChannel("SECOND", routeId);
        Uuid first = channels.registerTopic(routeId, "FIRST", "echo");
        Uuid second = channels.registerTopic(routeId, "SECOND", "run");

        assertNotEquals(first, second);
        assertEquals(List.of("echo"), channels.listTopics(routeId, "FIRST"));
        assertEquals(List.of("run"), channels.listTopics(routeId, "SECOND"));
        assertTrue(channels.provides(routeId, "FIRST"));
        assertTrue(channels.provides(routeId, "SECOND"));
        assertFalse(channels.provides(routeId, "THIRD"));
    }

    @Test
    void removeChannel_onOneOfSeveralChannels_leavesTheOthersStanding() {
        ChannelRegistry channels = registry();
        Uuid routeId = newId();
        Channel first = channels.registerChannel("FIRST", routeId);
        Channel second = channels.registerChannel("SECOND", routeId);
        Uuid secondTopic = channels.registerTopic(routeId, "SECOND", "run");

        channels.removeChannel(first);

        assertNull(channels.channelProvider("FIRST"));
        assertFalse(channels.provides(routeId, "FIRST"));
        assertEquals(routeId, channels.channelProvider("SECOND"));
        assertTrue(channels.provides(routeId, "SECOND"));
        assertNotNull(channels.topic(secondTopic));

        assertNotNull(channels.registerChannel("FIRST", newId()));
        channels.removeChannel(second);
        assertNull(channels.channelProvider("SECOND"));
    }

    @Test
    void expectReply_forAnIdAnotherRouteAwaits_isRefused_andLeavesTheFirstClaimStanding() {
        ChannelRegistry channels = registry();
        Uuid correlation = newId();
        Uuid first = newId();
        assertTrue(channels.expectReply(correlation, first));
        assertFalse(channels.expectReply(correlation, newId()));
        assertEquals(first, channels.takeReplyRoute(correlation));
    }

    @Test
    void expectReply_byTheSameRouteTwice_isIdempotent() {
        ChannelRegistry channels = registry();
        Uuid correlation = newId();
        Uuid client = newId();
        assertTrue(channels.expectReply(correlation, client));
        assertTrue(channels.expectReply(correlation, client));
    }

    @Test
    void expectReply_storesACopy_soReusingTheProbeCannotRewriteTheClaim() {
        ChannelRegistry channels = registry();
        Uuid probe = newId();
        Uuid client = newId();
        Uuid original = probe.copy();
        assertTrue(channels.expectReply(probe, client));

        probe.set(newId()); // the next frame parsed into the same probe
        assertEquals(client, channels.takeReplyRoute(original));
    }

}
