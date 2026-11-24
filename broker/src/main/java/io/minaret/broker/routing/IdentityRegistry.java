package io.minaret.broker.routing;

import io.minaret.broker.wire.Uuid;
import jakarta.inject.Singleton;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * ClientInfo.identity → RouteId, for the lifetime of the process. Entries are never removed, so a
 * reconnect always has a claim to resolve against. The rationale is in CLAUDE.md "Identity".
 */
@Singleton
public final class IdentityRegistry {

    private final ConcurrentMap<String, Uuid> routeIdByIdentity = new ConcurrentHashMap<>();

    /**
     * Session.handleRegister resolves every registration through this. A known identity's RouteId
     * wins over proposed. An unknown identity claims proposed, or a fresh v7 id when proposed is
     * null.
     */
    Uuid resolveOrClaim(String identity, Uuid proposed) {
        return routeIdByIdentity.computeIfAbsent(identity, ignored -> proposed != null ? proposed : UuidV7.next());
    }

}
