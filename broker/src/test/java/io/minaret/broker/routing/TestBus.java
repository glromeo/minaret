package io.minaret.broker.routing;

import io.minaret.broker.wire.FrameWriter;
import io.minaret.broker.wire.Json;
import io.minaret.broker.wire.Uuid;
import io.minaret.messaging.ClientInfo;
import io.minaret.messaging.Destination;
import io.minaret.messaging.Header;
import io.minaret.messaging.wire.BodyEncoding;
import io.minaret.messaging.wire.Wire;
import io.minaret.messaging.wire.WireFrame;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micronaut.serde.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.util.ReferenceCountUtil;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * A Broker with no server: real registries and real Sessions, each mounted on a Netty EmbeddedChannel.
 * It is what lets a test drive a teardown at a moment no real client can be made to produce, or watch
 * two sessions overlap.
 *
 * Frames are built and read back with the Java *client's* codec (io.minaret.messaging.wire), so
 * every assertion here also checks that the Broker's bytes parse under a second implementation of
 * the protocol.
 *
 * EmbeddedChannel runs tasks only when asked, so settle() drains every socket's pending work until
 * nothing moves. The SendRing drain and channel.resolve's hop back to the loop both run there, which
 * makes every test deterministic.
 */
final class TestBus {

    final BrokerMetrics metrics = new BrokerMetrics(new SimpleMeterRegistry());
    final ConnectionRegistry connections = new ConnectionRegistry(metrics);
    final ChannelRegistry channels = new ChannelRegistry(connections);
    final EventRegistry events = new EventRegistry();
    final IdentityRegistry identities;
    final Json json = new Json(ObjectMapper.getDefault());
    final Bus bus;

    private final List<Socket> sockets = new ArrayList<>();

    TestBus() {
        this(new IdentityRegistry());
    }

    TestBus(IdentityRegistry identities) {
        this.identities = identities;
        this.bus = new Bus(connections, channels, events, identities, json, metrics);
    }

    Socket open() {
        Session session = bus.newSession();
        Socket socket = new Socket(new EmbeddedChannel(session), session);
        sockets.add(socket);
        return socket;
    }

    /** A socket whose every write fails, as a peer that vanished mid-handshake. */
    Socket openFailingWrites() {
        Session session = bus.newSession();
        ChannelOutboundHandlerAdapter dead = new ChannelOutboundHandlerAdapter() {
            @Override
            public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
                ReferenceCountUtil.release(msg);
                promise.tryFailure(new java.io.IOException("Connection reset by peer"));
            }
        };
        Socket socket = new Socket(new EmbeddedChannel(dead, session), session);
        sockets.add(socket);
        return socket;
    }

    /** A connection entered straight into the registry under routeId, with no handshake: a pure recipient. */
    Socket seeded(UUID routeId) {
        Socket socket = open();
        connections.register(Uuid.of(routeId), socket.session());
        return socket;
    }

    /** Opens a socket and sends its client.register. */
    Socket registered(ClientInfo info) {
        Socket socket = open();
        socket.send(register(info));
        return socket;
    }

    void settle() {
        boolean moved;
        do {
            moved = false;
            for (Socket socket : sockets) {
                socket.channel.runPendingTasks();
                moved |= socket.collect();
            }
        } while (moved);
    }

    static ClientInfo client() {
        return new ClientInfo(UUID.randomUUID(), "test-" + UUID.randomUUID(), 0, Map.of());
    }

    static String register(ClientInfo info) {
        return Wire.serializeMessage(new Header(info.routeId(), Destination.EMPTY, "client.register", null, System.currentTimeMillis(), null, BodyEncoding.RAW, null, ""), info);
    }

    static String control(String verb, Object body) {
        return control(verb, body, UUID.randomUUID());
    }

    static String control(String verb, Object body, UUID replyTo) {
        return Wire.serializeMessage(new Header(null, Destination.EMPTY, verb, replyTo, System.currentTimeMillis(), null, BodyEncoding.RAW, null, ""), body);
    }

    static String topicFrame(String verb, Destination to, UUID replyTo, Object body) {
        return Wire.serializeMessage(new Header(null, to, verb, replyTo, System.currentTimeMillis(), null, BodyEncoding.RAW, null, ""), body);
    }

    static UUID uuid(Uuid id) {
        return id.toUUID();
    }

    final class Socket {

        final EmbeddedChannel channel;
        final List<String> sent = new ArrayList<>();
        Integer closedWith;
        private final Session session;

        Socket(EmbeddedChannel channel, Session session) {
            this.channel = channel;
            this.session = session;
        }

        Session session() {
            return session;
        }

        void send(String frame) {
            channel.writeInbound(new TextWebSocketFrame(Unpooled.copiedBuffer(frame, StandardCharsets.UTF_8)));
            settle();
        }

        void close() {
            channel.close();
            settle();
        }

        List<WireFrame> frames() {
            List<WireFrame> frames = new ArrayList<>();
            for (String raw : sent) {
                WireFrame.Parsed parsed = Wire.parseHeader(raw);
                if (parsed.isOk()) frames.add(parsed.frame());
            }
            return frames;
        }

        List<WireFrame> ofType(String type) {
            List<WireFrame> matching = new ArrayList<>();
            for (WireFrame frame : frames())
                if (frame.header().type().equals(type)) matching.add(frame);
            return matching;
        }

        WireFrame single(String type) {
            List<WireFrame> matching = ofType(type);
            if (matching.size() != 1) throw new AssertionError("Expected one " + type + " frame, got " + matching.size() + ": " + sent);
            return matching.get(0);
        }

        <T> T body(WireFrame frame, Class<T> type) {
            return Wire.parseBodyAs(frame, type);
        }

        UUID routeId() {
            Uuid id = session().routeId();
            return id == null ? null : id.toUUID();
        }

        /** Moves whatever the session wrote to the socket into sent; true when anything arrived. */
        private boolean collect() {
            boolean moved = false;
            Object out;
            while ((out = channel.readOutbound()) != null) {
                moved = true;
                try {
                    if (out instanceof CloseWebSocketFrame close) closedWith = close.statusCode();
                    else if (out instanceof ByteBuf buf) sent.add(FrameWriter.payload(buf).toString(StandardCharsets.UTF_8));
                } finally {
                    ReferenceCountUtil.release(out);
                }
            }
            return moved;
        }

    }

}
