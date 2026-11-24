package io.minaret.broker.e2e;

import io.minaret.messaging.ClientInfo;
import io.minaret.messaging.Destination;
import io.minaret.messaging.Header;
import io.minaret.messaging.wire.BodyEncoding;
import io.minaret.messaging.wire.Wire;
import io.minaret.messaging.wire.WireFrame;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A raw WebSocket to a live Broker, the Java counterpart of the ClientWebSocket MessageRoutingTests
 * drives: text frames out, whole messages in, with nothing of the client runtime in between. The
 * Java client's codec builds and reads the frames, so a mismatch shows up as a parse failure here.
 */
final class LiveSocket implements AutoCloseable {

    private static final HttpClient HTTP = HttpClient.newHttpClient();
    private static final long TIMEOUT_SECONDS = 10;

    private final BlockingQueue<String> received = new LinkedBlockingQueue<>();
    final CompletableFuture<Integer> closed = new CompletableFuture<>();
    private final WebSocket socket;

    LiveSocket(URI uri) throws Exception {
        socket = HTTP.newWebSocketBuilder().buildAsync(uri, new WebSocket.Listener() {
            private final StringBuilder partial = new StringBuilder();

            @Override
            public CompletionStage<?> onText(WebSocket ws, CharSequence data, boolean last) {
                partial.append(data);
                if (last) {
                    received.add(partial.toString());
                    partial.setLength(0);
                }
                ws.request(1);
                return null;
            }

            @Override
            public CompletionStage<?> onClose(WebSocket ws, int statusCode, String reason) {
                closed.complete(statusCode);
                return null;
            }

            @Override
            public void onError(WebSocket ws, Throwable error) {
                closed.completeExceptionally(error);
            }
        }).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    void send(String frame) throws Exception {
        socket.sendText(frame, true).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    String receive() throws Exception {
        String message = received.poll(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        if (message == null) throw new AssertionError("no message within " + TIMEOUT_SECONDS + "s");
        return message;
    }

    WireFrame receiveFrame() throws Exception {
        return parse(receive());
    }

    void assertNoMoreMessages() throws InterruptedException {
        String extra = received.poll(500, TimeUnit.MILLISECONDS);
        assertNull(extra, "expected no further messages");
    }

    /** Registers info and answers the ClientInfo the Broker acked, enriched with routeId and clock offset. */
    ClientInfo register(ClientInfo info) throws Exception {
        return register(info, System.currentTimeMillis());
    }

    ClientInfo register(ClientInfo info, long time) throws Exception {
        send(Wire.serializeMessage(header(info.routeId(), Destination.EMPTY, "client.register", time, null), info));
        WireFrame ack = receiveFrame();
        assertEquals("client.registered", ack.header().type());
        return Wire.parseBodyAs(ack, ClientInfo.class);
    }

    static WireFrame parse(String raw) {
        WireFrame.Parsed parsed = Wire.parseHeader(raw);
        assertTrue(parsed.isOk(), parsed.error());
        return parsed.frame();
    }

    static String bodyOf(String raw) {
        return Wire.parseBodyAs(parse(raw), String.class);
    }

    static Header header(java.util.UUID from, Destination to, String type, long time, Integer ttl) {
        return new Header(from, to, type, null, time, ttl, BodyEncoding.RAW, null, "");
    }

    @Override
    public void close() {
        socket.abort();
    }

    void closeNormally() throws Exception {
        socket.sendClose(WebSocket.NORMAL_CLOSURE, "").get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        closed.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

}
