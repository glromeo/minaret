package io.minaret.messaging;

import io.minaret.discovery.BrokerConnectionInfo;
import io.minaret.discovery.BrokerDiscovery;
import io.minaret.discovery.Realm;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.concurrent.CompletableFuture;

/**
 * Convenience facade over {@link Router}: connect once, then create or connect to channels.
 *
 * There is no host here — this runtime has no DI container of its own, on purpose, because the one deployment that
 * wants one (the Spring demo) already has a better one and should own the wiring itself.
 */
public final class Channels implements AutoCloseable {

    private final Router router;

    private Channels(Router router) {
        this.router = router;
    }

    /**
     * The channel surface over a Router the caller built and connected itself, for when connect's
     * defaults don't fit.
     *
     * The benchmark's workers use it. They share a few HttpClients across thousands of connections
     * (each HttpClient runs its own selector thread), and they need a failed connect to surface rather
     * than retry forever under connect's reconnect policy.
     */
    public static Channels over(Router router) {
        return new Channels(router);
    }

    /**
     * The channel a type declares, or throws if it declares none. Read from the runtime type so a
     * subclass of an annotated class still resolves.
     */
    public static String channelNameOf(Class<?> type) {
        Channel channel = type.getAnnotation(Channel.class);
        if (channel == null)
            throw new IllegalArgumentException("Type '" + type.getSimpleName() + "' has no @Channel annotation.");

        return channel.value();
    }

    /**
     * Discovers the Broker in the resolved realm and connects, trusting a development certificate.
     * Retries indefinitely ({@link Router#reconnect()}) rather than failing on the first attempt, so a
     * client started before its Broker waits for it instead of erroring out.
     *
     * The certificate-trusting client is the default here rather than an opt-in because the Broker this
     * is discovering is by construction on the same machine, reached over the localhost development
     * certificate. A deployment with a real certificate should use {@link #connect(URI, ClientInfo)}
     * with a plain HttpClient.
     */
    public static CompletableFuture<Channels> connect(ClientInfo clientInfo) {
        String realm = Realm.resolve();
        Router router = new Router(() -> {
            try {
                BrokerConnectionInfo info = BrokerDiscovery.discover(realm);
                return new WebSocketConnection(URI.create("wss://localhost:" + info.port() + "/msgbus"), WebSocketConnection.trustingLocalhostClient());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        router.reconnect();
        return router.connect(clientInfo).thenApply(ignored -> new Channels(router));
    }

    /**
     * Connects to a Broker at a known URI, retrying indefinitely (see {@link #connect(ClientInfo)})
     * rather than failing while it isn't reachable yet.
     */
    public static CompletableFuture<Channels> connect(URI brokerUri, ClientInfo clientInfo) {
        Router router = new Router(() -> new WebSocketConnection(brokerUri, WebSocketConnection.trustingLocalhostClient()));

        router.reconnect();
        return router.connect(clientInfo).thenApply(ignored -> new Channels(router));
    }

    public Router router() {
        return router;
    }

    /**
     * Claims a channel name as this connection's provider identity. Fails if the name is already claimed
     * by a different, still-registered provider.
     */
    public CompletableFuture<ChannelProvider> createChannel(String channelName) {
        return ChannelProvider.create(router, channelName);
    }

    /**
     * Claims the channel the handlers object is annotated for and binds its topic methods to it, so a
     * provider is one call and the channel name is never written out twice.
     */
    public CompletableFuture<ChannelProvider> createChannel(Object handlers) {
        return createChannel(channelNameOf(handlers.getClass()))
                .thenCompose(provider -> provider.bind(handlers).thenApply(ignored -> provider));
    }

    /**
     * Resolves a channel name to its provider, blocking Broker-side until one registers.
     */
    public CompletableFuture<ChannelClient> connectChannel(String channelName) {
        return ChannelClient.create(router, channelName);
    }

    /**
     * Resolves the channel the contract is annotated for and answers a proxy over it — the client-side
     * mirror of {@link #createChannel(Object)}.
     */
    public <T> CompletableFuture<T> connectChannel(Class<T> contract) {
        return connectChannel(channelNameOf(contract)).thenApply(client -> client.proxy(contract));
    }

    @Override
    public void close() {
        router.close();
    }

}
