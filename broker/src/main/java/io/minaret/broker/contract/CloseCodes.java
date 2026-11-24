package io.minaret.broker.contract;

/** Runtime/Messaging/CloseCodes.cs: the application WebSocket close codes. */
public final class CloseCodes {

    /** The prior holder of an identity was replaced; do not reconnect. SendRing.close sends it. */
    public static final int SUPERSEDED = 4001;

    private CloseCodes() {
    }

}
