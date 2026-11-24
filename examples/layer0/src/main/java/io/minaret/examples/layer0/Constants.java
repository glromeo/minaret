package io.minaret.examples.layer0;

import java.util.UUID;

/**
 * What EchoProvider and EchoClient agree on out of band, in place of channel discovery.
 *
 * At Layer 0 the broker routes only by Header.to = RouteId, so the client can reach the provider only
 * by knowing its RouteId in advance. Layer 1's connectChannel("LAYER_1") replaces this whole type.
 */
final class Constants {

    /** Header.type of a client → provider echo request. */
    static final String REQUEST_TYPE = "echo";

    /** Header.type of the provider → client reply. */
    static final String REPLY_TYPE = "reply";

    /**
     * The RouteId EchoProvider proposes at registration and EchoClient sends to. Fixed, not
     * time-ordered, so both processes can hardcode it. The broker honours a proposed RouteId the first
     * time it sees an identity, hence the provider's own identity below.
     */
    static final UUID ROUTE_ID = UUID.fromString("00000000-0000-0000-0000-0000000ec000");

    /**
     * One identity per side. The default identity is "{main class}@{version}", which both processes
     * share. Under the broker's "newer wins" rule the second to register would supersede the first.
     */
    static final String PROVIDER_IDENTITY = "layer0-echo-provider";
    static final String CLIENT_IDENTITY = "layer0-echo-client";

    private Constants() {
    }

}
