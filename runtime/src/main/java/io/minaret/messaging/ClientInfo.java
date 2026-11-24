package io.minaret.messaging;

import java.util.Map;
import java.util.UUID;

/**
 * A client's identity as the Broker holds it.
 *
 * Records give the structural equality this relies on for free, including over properties, since Map
 * implements equals by content. That equality is what the enrichment round trip rests on: a client
 * compares the Broker's derived copy against what it sent.
 */
public record ClientInfo(UUID routeId, String identity, long clockOffsetMs, Map<String, String> properties) {

    public ClientInfo {
        if (identity == null || identity.isEmpty()) identity = defaultIdentity();
        if (properties == null) properties = Map.of();
    }

    /**
     * A fresh identity with no declared properties. routeId is left null — the Broker assigns one on
     * first contact and hands the same one back on every later reconnect under this identity (see
     * IdentityRegistry on the Broker side); nothing here needs to generate a UUID itself any more.
     */
    public static ClientInfo create() {
        return new ClientInfo(null, defaultIdentity(), 0, Map.of());
    }

    public static ClientInfo create(String identity) {
        return new ClientInfo(null, identity, 0, Map.of());
    }

    /**
     * A fresh identity declaring properties — the axis client.connected/client.disconnected
     * subscriptions are filtered on, and the reason a client event carries the whole ClientInfo rather
     * than just a RouteId.
     */
    public static ClientInfo create(Map<String, String> properties) {
        return new ClientInfo(null, defaultIdentity(), 0, Map.copyOf(properties));
    }

    /**
     * "{main class}@{implementation version}" as a best-effort stand-in for C#'s entry-assembly
     * name/version — the JDK has no single equivalent, so this reads the main class out of
     * sun.java.command and the version out of this package's manifest, falling back to the process id
     * if neither is available. Coarse on purpose: it identifies "this program", not "this running
     * instance of it" — a caller running more than one logical client in one process (a demo pairing a
     * provider with its client, a test simulating several) must override identity per instance, or they
     * collide under the Broker's "newer wins" rule and only the last one stays connected — each
     * registration evicts the one before it with {@link CloseCodes#SUPERSEDED}, which is terminal.
     */
    private static String defaultIdentity() {
        String command = System.getProperty("sun.java.command", "");
        String mainClass = command.isBlank() ? null : command.split("\\s+", 2)[0];
        String name = mainClass == null ? null : mainClass.substring(mainClass.lastIndexOf('.') + 1);

        if (name == null || name.isBlank()) return "pid-" + ProcessHandle.current().pid();

        String version = ClientInfo.class.getPackage().getImplementationVersion();
        return name + "@" + (version == null ? "0" : version);
    }

}
