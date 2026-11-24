package io.minaret.broker.routing;

import io.minaret.broker.routing.TestBus.Socket;
import io.minaret.broker.wire.Uuid;
import io.minaret.messaging.ClientInfo;
import io.minaret.messaging.Destination;
import io.minaret.messaging.Header;
import io.minaret.messaging.wire.BodyEncoding;
import io.minaret.messaging.wire.Wire;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.concurrent.FastThreadLocalThread;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static io.minaret.broker.routing.TestBus.client;
import static io.minaret.broker.routing.TestBus.topicFrame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Turns "no allocation per relayed frame" into something a test checks. It counts the heap bytes
 * this thread allocates while Sessions relay a steady stream through EmbeddedChannels, and subtracts
 * a baseline loop that does everything except the Broker's own work: building the inbound frame,
 * driving the channel, draining and releasing the output.
 *
 * The budget is per frame, not zero. It absorbs the occasional chunk the MPSC queue links in and
 * JIT noise. A regression that allocates per frame, such as a String, a UUID or a lambda, costs
 * tens of bytes on every frame and lands well past it.
 */
class AllocationTest {

    private static final int WARMUP = 20_000;
    private static final int MEASURED = 20_000;
    private static final long BUDGET_BYTES_PER_FRAME = 16;

    private static final com.sun.management.ThreadMXBean THREADS = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();

    @Test
    void forwardToRoute_allocatesNothingPerFrame() {
        TestBus bus = new TestBus();
        UUID recipientRouteId = UUID.randomUUID();
        Socket recipient = bus.seeded(recipientRouteId);
        Socket sender = bus.registered(client());
        byte[] frame = Wire.serializeMessage(new Header(null, Destination.ofId(recipientRouteId), "greeting", null, 0, 30, BodyEncoding.RAW, null, ""), "hello").getBytes(StandardCharsets.UTF_8);

        assertWithinBudget("forward", List.of(new Step(sender.channel, frame)), List.of(recipient.channel));
    }

    /**
     * The RPC path: a request claims its ReplyTo, the provider's reply consumes it. One correlation id
     * is reused, so each round trip is one claim and one take on the same ReplyTable slot. A table
     * that copied the id into a key object, or boxed an entry, costs that on every round trip.
     */
    @Test
    void requestReply_allocatesNothingPerFrame() {
        TestBus bus = new TestBus();
        ClientInfo provider = client();
        Socket providerSocket = bus.registered(provider);
        bus.channels.registerChannel("LAYER_1", Uuid.of(provider.routeId()));
        Uuid topicId = bus.channels.registerTopic(Uuid.of(provider.routeId()), "LAYER_1", "echo");
        Socket requester = bus.registered(client());
        UUID correlation = UUID.randomUUID();
        Destination to = Destination.ofId(topicId.toUUID());
        byte[] request = topicFrame("dispatch", to, correlation, "ping").getBytes(StandardCharsets.UTF_8);
        byte[] reply = topicFrame("dispatch", to, correlation, "pong").getBytes(StandardCharsets.UTF_8);

        assertWithinBudget("request/reply", List.of(new Step(requester.channel, request), new Step(providerSocket.channel, reply)), List.of());
    }

    @Test
    void dispatchById_allocatesNothingPerFrame() {
        TestBus bus = new TestBus();
        ClientInfo provider = client();
        Socket providerSocket = bus.seeded(provider.routeId());
        bus.channels.registerChannel("LAYER_1", Uuid.of(provider.routeId()));
        Uuid topicId = bus.channels.registerTopic(Uuid.of(provider.routeId()), "LAYER_1", "echo");
        Socket sender = bus.registered(client());
        // No ReplyTo: one-way. requestReply_allocatesNothingPerFrame covers the correlated path.
        byte[] frame = topicFrame("dispatch", Destination.ofId(topicId.toUUID()), null, "hello").getBytes(StandardCharsets.UTF_8);

        assertWithinBudget("dispatch", List.of(new Step(sender.channel, frame)), List.of(providerSocket.channel));
    }

    @Test
    void publishFanout_allocatesNothingPerFrame() {
        TestBus bus = new TestBus();
        ClientInfo provider = client();
        bus.channels.registerChannel("LAYER_1", Uuid.of(provider.routeId()));
        List<EmbeddedChannel> subscribers = new ArrayList<>();
        Uuid topicId = null;
        for (int i = 0; i < 8; i++) {
            UUID routeId = UUID.randomUUID();
            subscribers.add(bus.seeded(routeId).channel);
            topicId = bus.channels.subscribe(Uuid.of(provider.routeId()), "LAYER_1", "ticks", Uuid.of(routeId));
        }
        Socket providerSocket = bus.registered(provider);
        // No ReplyTo, so no published ack: the ack is a Broker-originated control frame, not the relay.
        byte[] frame = topicFrame("publish", Destination.ofId(topicId.toUUID()), null, "tick").getBytes(StandardCharsets.UTF_8);

        assertWithinBudget("fanout x8", List.of(new Step(providerSocket.channel, frame)), subscribers);
    }

    /** One inbound frame of an iteration: the socket it arrives on and its bytes. */
    private record Step(EmbeddedChannel sender, byte[] frame) {
    }

    /**
     * Measures on a FastThreadLocalThread, the thread type of every Netty event loop. Netty 4.2
     * keeps Recycler pools and allocator thread caches only on those threads, so on the JUnit
     * thread each pooled buffer, derived buffer and outbound entry would be a fresh object.
     *
     * Leak detection is off while measuring: surefire runs paranoid, which records a stack trace on
     * every allocation. Production's default, SIMPLE, still samples 1 in 128.
     */
    private static void assertWithinBudget(String path, List<Step> steps, List<EmbeddedChannel> recipients) {
        long[] perFrame = new long[1];
        Throwable[] failure = new Throwable[1];
        Thread loop = new FastThreadLocalThread(() -> {
            try {
                perFrame[0] = measure(path, steps, recipients);
            } catch (Throwable e) {
                failure[0] = e;
            }
        }, "allocation-guard");
        loop.start();
        try {
            loop.join();
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
        if (failure[0] != null) throw new AssertionError(failure[0]);
        assertTrue(perFrame[0] <= BUDGET_BYTES_PER_FRAME, path + " allocates " + perFrame[0] + " bytes per frame, budget " + BUDGET_BYTES_PER_FRAME);
    }

    private static long measure(String path, List<Step> steps, List<EmbeddedChannel> recipients) {
        ResourceLeakDetector.Level level = ResourceLeakDetector.getLevel();
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.DISABLED);

        EmbeddedChannel[] senders = new EmbeddedChannel[steps.size()];
        ByteBuf[] sources = new ByteBuf[steps.size()];
        List<EmbeddedChannel> drained = new ArrayList<>(recipients);
        for (int s = 0; s < senders.length; s++) {
            Step step = steps.get(s);
            senders[s] = step.sender();
            sources[s] = PooledByteBufAllocator.DEFAULT.directBuffer(step.frame().length).writeBytes(step.frame());
            if (!drained.contains(step.sender())) drained.add(step.sender());
        }
        EmbeddedChannel[] channels = drained.toArray(new EmbeddedChannel[0]);

        try {
            run(senders, sources, channels, WARMUP, true);
            run(senders, sources, channels, WARMUP, false);

            long baseline = run(senders, sources, channels, MEASURED, false);
            long relaying = run(senders, sources, channels, MEASURED, true);

            long frames = (long) MEASURED * senders.length;
            long perFrame = Math.max(0, relaying - baseline) / frames;
            System.out.printf("%s: %d bytes/frame (relay %d, baseline %d over %d frames)%n", path, perFrame, relaying, baseline, frames);
            return perFrame;
        } finally {
            for (ByteBuf source : sources) source.release();
            ResourceLeakDetector.setLevel(level);
        }
    }

    /**
     * Bytes this thread allocated over count iterations, each sending every step's frame in order
     * and draining every channel after each. With relay off each frame is released instead of sent.
     */
    private static long run(EmbeddedChannel[] senders, ByteBuf[] sources, EmbeddedChannel[] channels, int count, boolean relay) {
        long threadId = Thread.currentThread().getId();
        long before = THREADS.getThreadAllocatedBytes(threadId);
        for (int i = 0; i < count; i++)
            for (int s = 0; s < senders.length; s++) {
                TextWebSocketFrame message = new TextWebSocketFrame(sources[s].retainedDuplicate());
                if (relay) senders[s].pipeline().fireChannelRead(message);
                else message.release();
                for (EmbeddedChannel channel : channels) {
                    channel.runPendingTasks();
                    Object out;
                    while ((out = channel.readOutbound()) != null) ReferenceCountUtil.release(out);
                }
            }
        return THREADS.getThreadAllocatedBytes(threadId) - before;
    }

}
