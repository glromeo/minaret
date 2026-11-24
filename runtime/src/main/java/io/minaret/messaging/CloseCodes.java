package io.minaret.messaging;

/**
 * WebSocket close codes the Broker uses to say why it closed a connection. Mirrors broker's
 * contract CloseCodes.
 *
 * Values live in the 4000-4999 application range the WebSocket spec reserves for private use, so none
 * can collide with a protocol code an intermediary might send.
 */
public final class CloseCodes {

    /**
     * The connection was replaced by a newer one registering under the same {@code ClientInfo.identity},
     * and it is not coming back.
     *
     * **Terminal**: {@link Router} disarms its reconnect policy rather than retrying. An evicted client
     * that reconnected would claim the identity back, evicting the connection that replaced it, which
     * would reconnect in turn — the two trading it at socket speed, and since every registration and
     * teardown emits presence events, the visible symptom is an event storm rather than either client
     * failing.
     *
     * An explicit {@code connect} still works; what is disarmed is the automatic retry.
     */
    public static final int SUPERSEDED = 4001;

    private CloseCodes() {
    }

}
