package io.minaret.broker.routing;

import io.minaret.broker.contract.CloseCodes;
import io.minaret.broker.routing.BrokerMetrics.Drop;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.util.internal.PlatformDependent;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * One connection's outbound half. Every frame for this socket goes
 * through it: the session's own acks, and every other session's forwards and fanouts.
 *
 * Any thread may enqueue, without locking: an MPSC queue plus one long-lived drain task. The first
 * enqueue after an idle period submits the task to the channel's event loop, so a burst costs one
 * execute and no per-frame lambda. The drain writes everything with the void promise and flushes
 * once.
 *
 * A slow consumer stays a local problem. A producer never touches the socket, and the
 * backlog is bounded at CAPACITY with drop-oldest, so a consumer that stops reading sheds its own
 * backlog. The bound counts frames still in this queue. Netty's outbound buffer, gated by channel
 * writability, holds the frames already handed to the socket.
 *
 * Ownership of each buffer passes to the ring on enqueue. The ring releases it by writing it
 * (Netty releases after the write), by dropping it, or by discarding it once the channel is gone.
 */
final class SendRing {

    static final int CAPACITY = 1024;

    /** Built once per ring and resubmitted for every burst, so scheduling allocates nothing. */
    private final Runnable drain = this::drain;

    private final Channel channel;
    private final BrokerMetrics metrics;
    /** Netty's JCTools MPSC queue, whose size() is two index reads, so it is the backlog count too. */
    private final Queue<ByteBuf> queue = PlatformDependent.newMpscQueue();
    private final AtomicBoolean scheduled = new AtomicBoolean();

    private volatile boolean closing;
    private volatile String closeReason;
    private boolean closeSent;

    SendRing(Channel channel, BrokerMetrics metrics) {
        this.channel = channel;
        this.metrics = metrics;
    }

    /** Takes ownership of frame, a finished socket buffer from FrameWriter. */
    void enqueue(ByteBuf frame) {
        if (closing) {
            frame.release();
            metrics.dropped(Drop.QUEUE_CLOSED);
            return;
        }

        queue.offer(frame);
        schedule();
    }

    /**
     * Writes what is queued, then closes the socket as superseded (4001) with reason. Further
     * enqueues are dropped: drain, then close.
     */
    void close(String reason) {
        closeReason = reason;
        closing = true;
        schedule();
    }

    /** Re-arms the drain. Also called from Session on writability changes and on teardown. */
    void schedule() {
        if (scheduled.compareAndSet(false, true)) channel.eventLoop().execute(drain);
    }

    /** Always runs on the channel's event loop, submitted only by schedule. */
    private void drain() {
        scheduled.set(false);

        if (!channel.isActive()) {
            discard(Drop.SEND_FAILED);
            return;
        }

        for (int excess = queue.size() - CAPACITY; excess > 0; excess--) {
            ByteBuf oldest = queue.poll();
            if (oldest == null) break;
            oldest.release();
            metrics.dropped(Drop.QUEUE_FULL);
        }

        boolean wrote = false;
        ByteBuf frame;
        while (channel.isWritable() && (frame = queue.poll()) != null) {
            channel.write(frame, channel.voidPromise());
            wrote = true;
        }
        if (wrote) channel.flush();

        if (closing && !closeSent && queue.isEmpty()) {
            closeSent = true;
            channel.writeAndFlush(new CloseWebSocketFrame(CloseCodes.SUPERSEDED, closeReason))
                    .addListener(ChannelFutureListener.CLOSE);
        }
    }

    /** Frames enqueued but not yet handed to the channel. Test seam, the counterpart of WaitForIdleAsync. */
    int pending() {
        return queue.size();
    }

    private void discard(Drop reason) {
        ByteBuf frame;
        while ((frame = queue.poll()) != null) {
            frame.release();
            metrics.dropped(reason);
        }
    }

}
