package io.minaret.discovery;

/**
 * An isolated named partition for discovery, so more than one independent Broker plus its clients can
 * coexist on one machine. The realm name goes into the discovery pipe name, so a client only finds a
 * Broker running in the same realm it targets.
 *
 * Deliberately independent of any hosting-environment notion: a hosting environment answers "which
 * config profile is this process running under?", a realm answers "which Broker should this client
 * find?".
 */
public final class Realm {

    public static final String DEFAULT = "Default";

    private static final String ENVIRONMENT_VARIABLE = "MESSAGEBUS_REALM";

    private Realm() {
    }

    public static String resolve() {
        String value = System.getenv(ENVIRONMENT_VARIABLE);
        return value == null || value.isEmpty() ? DEFAULT : value;
    }

}
