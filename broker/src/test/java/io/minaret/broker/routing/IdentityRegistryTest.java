package io.minaret.broker.routing;

import io.minaret.broker.wire.Uuid;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/** The port of Broker.Tests' IdentityRegistryTests. */
class IdentityRegistryTest {

    private static Uuid newId() {
        return Uuid.of(UUID.randomUUID());
    }

    @Test
    void resolveOrClaim_firstCall_claimsTheProposedRouteId() {
        Uuid proposed = newId();
        assertEquals(proposed, new IdentityRegistry().resolveOrClaim("trading-app", proposed));
    }

    @Test
    void resolveOrClaim_knownIdentity_returnsTheRememberedRouteId_ignoringWhatWasProposed() {
        IdentityRegistry identities = new IdentityRegistry();
        Uuid claimed = identities.resolveOrClaim("trading-app", newId());
        assertEquals(claimed, identities.resolveOrClaim("trading-app", newId()));
    }

    @Test
    void resolveOrClaim_differentIdentities_claimIndependentRouteIds() {
        IdentityRegistry identities = new IdentityRegistry();
        assertNotEquals(identities.resolveOrClaim("app-a", newId()), identities.resolveOrClaim("app-b", newId()));
    }

    @Test
    void resolveOrClaim_noProposedRouteId_mintsAFreshOne() {
        Uuid minted = new IdentityRegistry().resolveOrClaim("trading-app", null);
        assertNotNull(minted);
        assertNotEquals(new Uuid(), minted);
    }

    @Test
    void resolveOrClaim_knownIdentity_returnsTheRememberedRouteId_evenWhenNothingIsProposedTheSecondTime() {
        IdentityRegistry identities = new IdentityRegistry();
        Uuid claimed = identities.resolveOrClaim("trading-app", newId());
        assertEquals(claimed, identities.resolveOrClaim("trading-app", null));
    }

}
