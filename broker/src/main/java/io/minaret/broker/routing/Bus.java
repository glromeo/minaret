package io.minaret.broker.routing;

import io.minaret.broker.contract.Contract.BrokerAck;
import io.minaret.broker.wire.Json;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.GlobalEventExecutor;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;

/**
 * Everything a Session shares with every other session. MsgbusUpgrade builds one Session per
 * connection from it, and its constructor stands in for ClientSession's C# primary constructor.
 *
 * It also holds every open /msgbus socket in sockets, for shutdown. A WebSocket connection never
 * completes as an HTTP request would, and its pipeline no longer contains the Micronaut handlers
 * graceful shutdown waits on. Closing the sockets here is what lets the Broker stop promptly. In C#,
 * linking each session's token to ApplicationStopping does the same job.
 */
@Singleton
public final class Bus {

    final ConnectionRegistry connections;
    final ChannelRegistry channels;
    final EventRegistry events;
    final IdentityRegistry identities;
    final Json json;
    final BrokerMetrics metrics;
    final ChannelGroup sockets = new DefaultChannelGroup("msgbus", GlobalEventExecutor.INSTANCE);

    /**
     * BrokerAck.OK, encoded once. Session.handlePublish acks every publish, as C# does, so encoding
     * it per frame would put a serde pass on the fanout path. AllocationTest's fanout case pins
     * that it stays off.
     */
    final Json.Encoded okAck;

    public Bus(ConnectionRegistry connections, ChannelRegistry channels, EventRegistry events, IdentityRegistry identities, Json json, BrokerMetrics metrics) {
        this.connections = connections;
        this.channels = channels;
        this.events = events;
        this.identities = identities;
        this.json = json;
        this.metrics = metrics;
        this.okAck = json.encode(BrokerAck.OK);
    }

    public Session newSession() {
        return new Session(this);
    }

    @PreDestroy
    void closeSockets() {
        sockets.close().awaitUninterruptibly();
    }

}
