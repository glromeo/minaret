package io.minaret.benchmark;

import io.minaret.discovery.BrokerDiscovery;
import io.minaret.messaging.ClientInfo;
import io.minaret.messaging.Router;
import io.minaret.messaging.WebSocketConnection;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Connects every worker to one broker, discovered once.
 *
 * Discovery is a named pipe the broker serves one client at a time, so discovering per worker would
 * serialize connection setup across thousands of them. Main discovers once and every worker connects
 * to the resulting URI.
 *
 * Workers share a small pool of HttpClients round-robin. Each HttpClient runs one selector thread for
 * all its sockets: one per worker would mean thousands of selector threads, and one for all of them
 * would make the benchmark's own I/O the bottleneck. `--httpClients` sets the pool size (default: one
 * per core).
 */
final class Connector {

    private final URI broker;
    private final HttpClient[] httpClients;
    private final AtomicInteger next = new AtomicInteger();

    private Connector(URI broker, int httpClients) {
        this.broker = broker;
        this.httpClients = new HttpClient[Math.max(1, httpClients)];
        for (int i = 0; i < this.httpClients.length; i++) this.httpClients[i] = WebSocketConnection.trustingLocalhostClient();
    }

    static Connector discover(String realm, int httpClients) throws IOException {
        int port = BrokerDiscovery.discover(realm).port();
        return new Connector(URI.create("wss://localhost:" + port + "/msgbus"), httpClients);
    }

    URI broker() {
        return broker;
    }

    /**
     * A router connected under its own identity, with no reconnect policy, so a failed connect fails
     * the run instead of retrying in the background.
     *
     * Every worker's identity must be distinct. The default identity is the main class, shared by every
     * worker in this process, and the broker's "newer wins" rule would leave only the last one
     * connected.
     */
    CompletableFuture<Router> connect(String identity) {
        HttpClient httpClient = httpClients[Math.floorMod(next.getAndIncrement(), httpClients.length)];
        Router router = new Router(() -> new WebSocketConnection(broker, httpClient));
        return router.connect(ClientInfo.create(identity)).thenApply(registered -> router);
    }

}
