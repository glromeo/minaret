package io.minaret.messaging;

import io.minaret.messaging.wire.WireFrame;
import io.minaret.util.Emitter;

import java.util.concurrent.CompletableFuture;

/**
 * The low-level transport abstraction {@link Router} depends on instead of a WebSocket directly, so
 * Router can be unit-tested against a fake.
 *
 * A received frame surfaces as a {@link WireFrame} — the already-parsed header plus the body exactly as
 * it arrived, still compressed when the header says so — rather than as one opaque string, so a
 * subscriber decodes only what it actually needs.
 */
public interface Connection extends AutoCloseable {

    ConnectionState state();

    /**
     * Raised for every inbound message that parses as a Wire header. Frames that do not parse are
     * dropped by the implementation and never reach here.
     */
    Emitter<WireFrame> onFrame();

    /**
     * Raised when the connection ends on its own — the receive loop exiting because the socket closed or
     * faulted — as opposed to a caller-driven {@link #close}. This is the one signal a reconnect policy
     * needs and the transport does not otherwise expose: {@link #state} is pull-only, so without this
     * nothing would ever notice a drop that happens between sends.
     *
     * Carries the close code the peer sent, or null when the connection ended without one — a fault, or a
     * socket that simply went away. {@link Router} reads it for exactly one decision: a
     * {@link CloseCodes#SUPERSEDED} close is terminal and must not be retried, where every other ending is
     * what the reconnect policy is for.
     */
    Emitter<Integer> onDisconnected();

    CompletableFuture<Void> connect();

    /**
     * header.encoding must already describe body as it is — the connection never re-encodes what it is
     * given, only renders the header around it.
     */
    CompletableFuture<Void> send(Header header, String body);

    @Override
    void close();

}
