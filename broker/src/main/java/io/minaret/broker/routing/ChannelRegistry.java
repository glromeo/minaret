package io.minaret.broker.routing;

import io.minaret.broker.routing.ConnectionRegistry.Route;
import io.minaret.broker.wire.Uuid;
import io.minaret.broker.wire.UuidTable;
import jakarta.inject.Singleton;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The channel and topic directory, the subscriber lists and the reply-correlation table: the port of
 * C#'s ChannelRegistry, with its invariants unchanged. CLAUDE.md "ChannelRegistry" is the
 * rationale.
 *
 * Every write takes one monitor, lock. A table is concurrent only if something reads it without the
 * lock: topics, providers (both levels), Channel.topics and pendingReplies. The rest are plain maps.
 *
 * The two tables on the per-frame path are keyed by raw longs, not Uuid objects. topics is a
 * copy-on-write UuidTable, replaced under lock when a topic is created or removed, read lock-free
 * by every id-addressed frame. pendingReplies is a ReplyTable, so a request/reply pair allocates
 * nothing.
 *
 * C#'s subscriber map (RouteId → that route's nodes) is gone: the list hangs off the subscriber's
 * ConnectionRegistry.Route, which each node also points at, so the fanout reads node.route.session
 * with no lookup.
 *
 * Unlike the C# original, the links the fanout walks lock-free are volatile: Topic.head,
 * SubscriptionNode.next and unlinked. .NET's memory model tolerates them being plain fields. Java's
 * does not, and without volatile a lock-free walker could observe a half-built node.
 */
@Singleton
public final class ChannelRegistry {

    private final Object lock = new Object();
    private final ConnectionRegistry connections;

    private final ConcurrentMap<Uuid, ConcurrentMap<String, Channel>> providers = new ConcurrentHashMap<>();
    private final Map<String, Channel> channels = new HashMap<>();
    private final ReplyTable pendingReplies = new ReplyTable();
    private final Map<String, List<CompletableFuture<Uuid>>> pendingResolutions = new HashMap<>();
    private volatile UuidTable<Topic> topics = new UuidTable<>();

    public ChannelRegistry(ConnectionRegistry connections) {
        this.connections = connections;
    }

    /**
     * Claims channelName for routeId and answers the Channel, or null when another route holds it.
     * Re-registering by the same route answers the existing channel. Either success completes every
     * resolve parked on the name, outside the lock.
     */
    Channel registerChannel(String channelName, Uuid routeId) {
        Channel channel;
        List<CompletableFuture<Uuid>> waiters;
        synchronized (lock) {
            Channel existing = channels.get(channelName);
            if (existing != null) {
                if (!existing.providerRouteId.equals(routeId)) return null;
                channel = existing;
            } else {
                channel = new Channel(channelName, routeId);
                channels.put(channelName, channel);
                providers.computeIfAbsent(routeId, ignored -> new ConcurrentHashMap<>()).put(channelName, channel);
            }
            waiters = pendingResolutions.remove(channelName);
        }

        if (waiters != null)
            for (CompletableFuture<Uuid> waiter : waiters) waiter.complete(routeId);

        return channel;
    }

    /**
     * Completes with the provider RouteId once channelName is registered. A name already registered
     * completes immediately.
     *
     * There is no Broker-side timeout. The caller bounds the wait: Session.handleChannelResolve
     * cancels its waiters through cancelResolution when its connection ends.
     */
    CompletableFuture<Uuid> resolve(String channelName) {
        synchronized (lock) {
            Channel channel = channels.get(channelName);
            if (channel != null) return CompletableFuture.completedFuture(channel.providerRouteId);

            CompletableFuture<Uuid> waiter = new CompletableFuture<>();
            pendingResolutions.computeIfAbsent(channelName, ignored -> new ArrayList<>()).add(waiter);
            return waiter;
        }
    }

    void cancelResolution(String channelName, CompletableFuture<Uuid> waiter) {
        synchronized (lock) {
            List<CompletableFuture<Uuid>> waiters = pendingResolutions.get(channelName);
            if (waiters != null) {
                waiters.remove(waiter);
                if (waiters.isEmpty()) pendingResolutions.remove(channelName);
            }
        }
        waiter.cancel(false);
    }

    /** The Broker-wide owner of a name. Tests only: Session never consults the global table (see resolveChannel). */
    Uuid channelProvider(String channelName) {
        synchronized (lock) {
            Channel channel = channels.get(channelName);
            return channel == null ? null : channel.providerRouteId;
        }
    }

    /** The topic's id, created on first registration; null when channelRouteId does not provide the channel. */
    Uuid registerTopic(Uuid channelRouteId, String channelName, String topicName) {
        Channel channel = providedChannel(channelRouteId, channelName);
        if (channel == null) return null;

        synchronized (lock) {
            Topic topic = getOrAddTopic(channel, topicName);
            return topic == null ? null : topic.id;
        }
    }

    boolean provides(Uuid routeId, String channelName) {
        return providedChannel(routeId, channelName) != null;
    }

    /** An existing topic by name, never creating one. */
    Topic topicByName(Uuid channelRouteId, String channelName, String topicName) {
        Channel channel = providedChannel(channelRouteId, channelName);
        return channel == null ? null : channel.topics.get(topicName);
    }

    /** Lock-free: the lookup every id-addressed dispatch, publish and notify starts with. */
    Topic topic(Uuid topicId) {
        return topics.get(topicId);
    }

    List<String> listTopics(Uuid channelRouteId, String channelName) {
        Channel channel = providedChannel(channelRouteId, channelName);
        return channel == null ? List.of() : new ArrayList<>(channel.topics.keySet());
    }

    /**
     * Adds subscriberRouteId to the topic (created on first subscribe) and answers its id. Idempotent.
     * Null when channelRouteId does not provide the channel.
     */
    Uuid subscribe(Uuid channelRouteId, String channelName, String topicName, Uuid subscriberRouteId) {
        Channel channel = providedChannel(channelRouteId, channelName);
        if (channel == null) return null;
        Route subscriber = connections.route(subscriberRouteId);

        synchronized (lock) {
            Topic topic = getOrAddTopic(channel, topicName);
            if (topic == null) return null;

            for (SubscriptionNode node = topic.head; node != null; node = node.next)
                if (node.route == subscriber && !node.unlinked) return topic.id;

            SubscriptionNode fresh = new SubscriptionNode(subscriber, topic);
            fresh.next = topic.head;
            if (topic.head != null) topic.head.prev = fresh;
            fresh.ownerNext = subscriber.subscriptions;
            subscriber.subscriptions = fresh;
            // Published last: a lock-free walker reading head sees a fully linked node.
            topic.head = fresh;

            return topic.id;
        }
    }

    boolean unsubscribe(Uuid topicId, Uuid subscriberRouteId) {
        Topic topic = topics.get(topicId);
        Route subscriber = connections.existingRoute(subscriberRouteId);
        if (topic == null || subscriber == null) return false;

        synchronized (lock) {
            for (SubscriptionNode node = topic.head; node != null; node = node.next)
                if (node.route == subscriber && !node.unlinked) {
                    unlink(node);
                    unlinkFromOwner(node);
                    return true;
                }
            return false;
        }
    }

    void removeConnection(Uuid routeId) {
        pendingReplies.removeRoute(routeId);

        Route route = connections.existingRoute(routeId);
        if (route == null) return;
        synchronized (lock) {
            for (SubscriptionNode node = route.subscriptions; node != null; node = node.ownerNext) unlink(node);
            route.subscriptions = null;
        }
    }

    /**
     * Frees channel's name claim and its topics, but only if channel is still the one registered
     * under its name, compared by reference. A reclaimed name belongs to someone else.
     */
    boolean removeChannel(Channel channel) {
        synchronized (lock) {
            if (channels.get(channel.name) != channel) return false;

            channels.remove(channel.name);
            Map<String, Channel> byName = providers.get(channel.providerRouteId);
            if (byName != null) {
                byName.remove(channel.name);
                if (byName.isEmpty()) providers.remove(channel.providerRouteId);
            }

            UuidTable<Topic> next = channel.topics.isEmpty() ? null : topics.copy();
            for (Topic topic : channel.topics.values()) {
                next.remove(topic.id);
                for (SubscriptionNode node = topic.head; node != null; ) {
                    SubscriptionNode following = node.next;
                    unlink(node);
                    unlinkFromOwner(node);
                    node = following;
                }
                topic.head = null;
            }
            if (next != null) topics = next;
            return true;
        }
    }

    /**
     * Every registered channel, taken atomically with subscribe. Session.handleEventSubscribe passes
     * the event subscription, so nothing can register between the snapshot and the subscribe.
     */
    List<Channel> snapshotChannelsAndSubscribe(Runnable subscribe) {
        synchronized (lock) {
            List<Channel> snapshot = new ArrayList<>(channels.values());
            subscribe.run();
            return snapshot;
        }
    }

    /**
     * Claims correlationId for routeId, and answers whether routeId now holds it. Re-claiming by the
     * same route is idempotent.
     *
     * correlationId may be a probe: ReplyTable stores its two longs, never the object. routeId is
     * stored, so it must be the session's own RouteId instance, never a probe.
     */
    boolean expectReply(Uuid correlationId, Uuid routeId) {
        return pendingReplies.claim(correlationId, routeId);
    }

    /** Consumes the claim: the route awaiting correlationId, or null. */
    Uuid takeReplyRoute(Uuid correlationId) {
        return pendingReplies.take(correlationId);
    }

    private Channel providedChannel(Uuid routeId, String channelName) {
        Map<String, Channel> byName = providers.get(routeId);
        return byName == null ? null : byName.get(channelName);
    }

    /**
     * The named topic on channel, created if absent. Null when channel is no longer the one registered
     * under its name. Caller holds lock.
     *
     * subscribe and registerTopic find channel lock-free and only then take the lock, so a
     * removeChannel can run in between. Without the reference re-check, the new topic would land in
     * topics on a channel nothing will ever clean up again: an id that resolves to a dead provider.
     * C#'s GetOrAddTopic makes the same check.
     * ChannelRegistryTest.subscribe_racingProviderDisconnect_leavesNoTopicBehind pins it.
     */
    private Topic getOrAddTopic(Channel channel, String topicName) {
        if (channels.get(channel.name) != channel) return null;
        Topic existing = channel.topics.get(topicName);
        if (existing != null) return existing;

        Topic topic = new Topic(UuidV7.next(), topicName, channel);
        channel.topics.put(topicName, topic);
        UuidTable<Topic> next = topics.copy();
        next.put(topic.id, topic);
        topics = next;
        return topic;
    }

    /**
     * Splices node out of its topic's list. It clears prev but keeps next, so a walker parked on
     * the node can still move forward.
     */
    private static void unlink(SubscriptionNode node) {
        if (node.unlinked) return;
        node.unlinked = true;

        if (node.prev != null) node.prev.next = node.next;
        else node.topic.head = node.next;
        if (node.next != null) node.next.prev = node.prev;
        node.prev = null;
    }

    /** Splices node out of its subscriber's list. A node removeConnection already dropped is not in it. */
    private static void unlinkFromOwner(SubscriptionNode node) {
        Route owner = node.route;
        if (owner.subscriptions == node) {
            owner.subscriptions = node.ownerNext;
        } else {
            for (SubscriptionNode previous = owner.subscriptions; previous != null; previous = previous.ownerNext)
                if (previous.ownerNext == node) {
                    previous.ownerNext = node.ownerNext;
                    break;
                }
        }
        node.ownerNext = null;
    }

    /**
     * A claimed channel name. Its topics map is read lock-free by topicByName and listTopics, and
     * iterated by Session.handleEventSubscribe's topic.registered replay; only the registry writes it.
     */
    static final class Channel {

        final String name;
        final Uuid providerRouteId;
        final ConcurrentMap<String, Topic> topics = new ConcurrentHashMap<>();

        Channel(String name, Uuid providerRouteId) {
            this.name = name;
            this.providerRouteId = providerRouteId;
        }

    }

    static final class Topic {

        final Uuid id;
        final String name;
        final Channel channel;

        /** The head of the subscriber list. Session's fanout walks it lock-free through next. */
        volatile SubscriptionNode head;

        Topic(Uuid id, String name, Channel channel) {
            this.id = id;
            this.name = name;
            this.channel = channel;
        }

        SubscriptionNode head() {
            return head;
        }

    }

    /**
     * One subscription, on two lists: its topic's (walked by fanout) and its subscriber Route's
     * (walked by disconnect). route is also how the fanout reaches the subscriber's session.
     */
    static final class SubscriptionNode {

        final Route route;
        final Topic topic;

        volatile SubscriptionNode next;
        volatile boolean unlinked;
        SubscriptionNode prev;
        SubscriptionNode ownerNext;

        SubscriptionNode(Route route, Topic topic) {
            this.route = route;
            this.topic = topic;
        }

        SubscriptionNode next() {
            return next;
        }

        boolean unlinked() {
            return unlinked;
        }

    }

}
