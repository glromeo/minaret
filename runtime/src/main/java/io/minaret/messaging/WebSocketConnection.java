package io.minaret.messaging;

import io.minaret.messaging.wire.Wire;
import io.minaret.messaging.wire.WireFrame;
import io.minaret.util.Emitter;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * The real {@link Connection}, over the JDK's own WebSocket client — no Netty, no dependency.
 *
 * Two things the JDK forces that the C# side does not:
 *
 * - Text arrives as CharSequence fragments with a last flag, so this stitches them itself. That is the
 *   one place C#'s ReceiveFrameAsync job reappears; there is still no relay path and no pooling, since
 *   a client is always its body's actual consumer.
 * - sendText must not be called again until the previous send's future completes. Sends are therefore
 *   chained through one future rather than issued concurrently — without it, two threads publishing at
 *   once is an IllegalStateException from the JDK rather than a race in this code.
 */
public final class WebSocketConnection implements Connection {

    private static final Logger LOG = System.getLogger(WebSocketConnection.class.getName());

    private final URI uri;
    private final HttpClient httpClient;
    private final Emitter<WireFrame> onFrame = new Emitter<>();
    private final Emitter<Integer> onDisconnected = new Emitter<>();
    private final Object sendLock = new Object();

    private volatile ConnectionState state = ConnectionState.CLOSED;
    private volatile WebSocket socket;

    /**
     * The tail of the send chain. Guarded by sendLock, which is held only long enough to append — never
     * across the send itself.
     */
    private CompletableFuture<Void> sendChain = CompletableFuture.completedFuture(null);

    public WebSocketConnection(URI uri, HttpClient httpClient) {
        this.uri = uri;
        this.httpClient = httpClient;
    }

    public WebSocketConnection(URI uri) {
        this(uri, HttpClient.newHttpClient());
    }

    /**
     * An HttpClient that trusts any certificate, for reaching a Broker on the ASP.NET development
     * certificate — which no Java truststore knows about.
     *
     * This disables certificate validation outright. It is a development affordance and is named like
     * one; a deployment reaching a Broker with a real certificate should pass a plain
     * HttpClient.newHttpClient() instead.
     */
    public static HttpClient trustingLocalhostClient() {
        TrustManager[] trustAll = {
                new X509TrustManager() {

                    @Override
                    public void checkClientTrusted(X509Certificate[] chain, String authType) {
                    }

                    @Override
                    public void checkServerTrusted(X509Certificate[] chain, String authType) {
                    }

                    @Override
                    public X509Certificate[] getAcceptedIssuers() {
                        return new X509Certificate[0];
                    }

                }
        };

        try {
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(null, trustAll, new java.security.SecureRandom());

            return HttpClient.newBuilder().sslContext(context).build();
        } catch (java.security.GeneralSecurityException e) {
            throw new IllegalStateException("Failed to build a certificate-trusting SSL context", e);
        }
    }

    @Override
    public ConnectionState state() {
        return state;
    }

    @Override
    public Emitter<WireFrame> onFrame() {
        return onFrame;
    }

    @Override
    public Emitter<Integer> onDisconnected() {
        return onDisconnected;
    }

    @Override
    public CompletableFuture<Void> connect() {
        state = ConnectionState.CONNECTING;

        return httpClient.newWebSocketBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .buildAsync(uri, new FrameListener())
                .thenAccept(connected -> {
                    socket = connected;
                    state = ConnectionState.OPEN;
                })
                .exceptionallyCompose(failure -> {
                    state = ConnectionState.CLOSED;
                    return CompletableFuture.failedFuture(failure);
                });
    }

    @Override
    public CompletableFuture<Void> send(Header header, String body) {
        WebSocket active = socket;
        if (active == null || state != ConnectionState.OPEN)
            return CompletableFuture.failedFuture(new IllegalStateException("Connection is not open."));

        String frame = Wire.writeHeaderAndBody(header, body);

        synchronized (sendLock) {
            // Appended to the chain whether or not the previous send failed, so one failure does not
            // wedge every later send behind it.
            CompletableFuture<Void> next = sendChain
                    .handle((ignored, failure) -> null)
                    .thenCompose(ignored -> active.sendText(frame, true))
                    .thenApply(ignored -> null);

            sendChain = next;
            return next;
        }
    }

    @Override
    public void close() {
        state = ConnectionState.CLOSED;

        WebSocket active = socket;
        socket = null;
        if (active != null) active.abort();
    }

    /**
     * Stitches text fragments into whole frames and parses each one.
     *
     * A frame that does not parse is logged and dropped, never fatal — the contract every parse entry
     * point in this codec keeps, and the reason a garbage frame cannot break the message loop.
     */
    private final class FrameListener implements WebSocket.Listener {

        private final StringBuilder pending = new StringBuilder();

        @Override
        public void onOpen(WebSocket webSocket) {
            webSocket.request(1);
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            pending.append(data);

            if (last) {
                String raw = pending.toString();
                pending.setLength(0);

                WireFrame.Parsed parsed = Wire.parseHeader(raw);
                if (parsed.isOk()) onFrame.emit(parsed.frame());
                else LOG.log(Level.WARNING, "Dropping unparseable frame: {0}", parsed.error());
            }

            webSocket.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            state = ConnectionState.CLOSED;
            // The code travels because it is the only thing separating a drop worth retrying from the
            // Broker saying this client has been replaced; see CloseCodes.
            onDisconnected.emit(statusCode);
            return null;
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            LOG.log(Level.WARNING, "WebSocket faulted", error);
            state = ConnectionState.CLOSED;
            // A fault carries no close frame, so there is no code to report — and a fault is exactly
            // the ending a reconnect policy exists for.
            onDisconnected.emit(null);
        }

    }

}
