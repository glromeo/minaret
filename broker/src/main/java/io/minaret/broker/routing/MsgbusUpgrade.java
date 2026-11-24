package io.minaret.broker.routing;

import io.minaret.broker.wire.Wire;
import io.micronaut.context.event.BeanCreatedEvent;
import io.micronaut.context.event.BeanCreatedEventListener;
import io.micronaut.http.netty.channel.ChannelPipelineCustomizer;
import io.micronaut.http.server.netty.NettyServerCustomizer;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.websocketx.WebSocketDecoderConfig;
import io.netty.handler.codec.http.websocketx.WebSocketFrameAggregator;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolConfig;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.ReferenceCountUtil;
import jakarta.inject.Singleton;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Takes /msgbus off Micronaut's request pipeline and hands the connection to a raw Netty Session.
 * This is Program.cs's `app.Map("/msgbus", ...)`.
 *
 * Micronaut's own WebSocket support surfaces messages as String or byte[]. That is a heap copy of
 * every frame, which is exactly what this Broker exists to avoid. So this handler sits directly
 * behind the HTTP codec on every HTTP/1.1 connection. It lets all other requests through untouched:
 * static files, /idl, keep-alive sequences of them. When a /msgbus upgrade arrives, it rewires the
 * pipeline:
 *
 *   ssl → http codec (swapped for the WebSocket codec by the handshake) → idle → protocol → aggregator → Session
 *
 * The Micronaut handlers behind it are removed, along with Micronaut's idle-state handler, whose
 * timeout is an HTTP request idle rather than a socket keepalive.
 *
 * Per-message compression stays off, as in C#. Bodies cross the Broker opaque, and compression
 * belongs to the M01 body.
 *
 * UTF-8 validation is off too. The Broker never decodes a relayed body, and Kestrel does not
 * validate either.
 */
final class MsgbusUpgrade extends ChannelInboundHandlerAdapter {

    static final String NAME = "messagebus-upgrade";
    static final String PATH = "/msgbus";

    /** Kestrel's KeepAliveInterval. Session pings after one silent period and closes after a second. */
    private static final long KEEPALIVE_SECONDS = 30;

    private final Bus bus;

    /** Set between a /msgbus request rejected with 400 and its last content, so its body is swallowed. */
    private boolean discarding;

    MsgbusUpgrade(Bus bus) {
        this.bus = bus;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (discarding) {
            if (msg instanceof LastHttpContent) discarding = false;
            ReferenceCountUtil.release(msg);
            return;
        }

        if (!(msg instanceof HttpRequest request) || !PATH.equals(path(request.uri()))) {
            ctx.fireChannelRead(msg);
            return;
        }

        if (!request.headers().containsValue(HttpHeaderNames.UPGRADE, HttpHeaderValues.WEBSOCKET, true)) {
            discarding = !(msg instanceof LastHttpContent);
            ReferenceCountUtil.release(msg);
            DefaultFullHttpResponse response = new DefaultFullHttpResponse(request.protocolVersion(), HttpResponseStatus.BAD_REQUEST);
            HttpUtil.setContentLength(response, 0);
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
            return;
        }

        ChannelPipeline pipeline = ctx.pipeline();
        List<String> names = pipeline.names();
        for (int i = names.indexOf(NAME) + 1; i < names.size(); i++)
            if (pipeline.get(names.get(i)) != null) pipeline.remove(names.get(i));
        if (pipeline.get(ChannelPipelineCustomizer.HANDLER_IDLE_STATE) != null) pipeline.remove(ChannelPipelineCustomizer.HANDLER_IDLE_STATE);

        WebSocketServerProtocolConfig config = WebSocketServerProtocolConfig.newBuilder()
                .websocketPath(PATH)
                .checkStartsWith(false)
                .handleCloseFrames(true)
                .dropPongFrames(true)
                .decoderConfig(WebSocketDecoderConfig.newBuilder()
                        .maxFramePayloadLength(Wire.MAX_MESSAGE_BYTES)
                        .allowExtensions(false)
                        .withUTF8Validator(false)
                        .build())
                .build();

        pipeline.addLast("msgbus-idle", new IdleStateHandler(KEEPALIVE_SECONDS, 0, 0, TimeUnit.SECONDS));
        pipeline.addLast("msgbus-protocol", new WebSocketServerProtocolHandler(config));
        pipeline.addLast("msgbus-aggregator", new WebSocketFrameAggregator(Wire.MAX_MESSAGE_BYTES));
        pipeline.addLast("msgbus-session", bus.newSession());

        ctx.fireChannelRead(msg);
        pipeline.remove(this);
        // Micronaut's HTTP/1 pipeline turns autoRead off and pulls each request with ctx.read() from
        // the handler just removed. Without this the handshake completes and nothing is read again.
        ctx.channel().config().setAutoRead(true);
    }

    private static String path(String uri) {
        int query = uri.indexOf('?');
        return query < 0 ? uri : uri.substring(0, query);
    }

    /**
     * Installs MsgbusUpgrade on every HTTP/1.1 connection, directly behind the server codec, once
     * Micronaut has built the connection's pipeline.
     */
    @Singleton
    static final class Installer implements BeanCreatedEventListener<NettyServerCustomizer.Registry> {

        private final Bus bus;

        Installer(Bus bus) {
            this.bus = bus;
        }

        @Override
        public NettyServerCustomizer.Registry onCreated(BeanCreatedEvent<NettyServerCustomizer.Registry> event) {
            event.getBean().register(new Customizer(bus, null));
            return event.getBean();
        }

    }

    private record Customizer(Bus bus, Channel channel) implements NettyServerCustomizer {

        @Override
        public NettyServerCustomizer specializeForChannel(Channel channel, ChannelRole role) {
            return role == ChannelRole.CONNECTION ? new Customizer(bus, channel) : this;
        }

        @Override
        public void onStreamPipelineBuilt() {
            if (channel == null) return;
            ChannelPipeline pipeline = channel.pipeline();
            if (pipeline.get(ChannelPipelineCustomizer.HANDLER_HTTP_SERVER_CODEC) != null)
                pipeline.addAfter(ChannelPipelineCustomizer.HANDLER_HTTP_SERVER_CODEC, NAME, new MsgbusUpgrade(bus));
        }

    }

}
