package io.minaret.messaging;

/**
 * Deliberately coarser than the transport's own state enum: Router only ever needs to know whether it
 * may send, so a finer-grained mirror of the socket would be detail no caller acts on.
 */
public enum ConnectionState {

    CONNECTING,
    OPEN,
    CLOSED

}
