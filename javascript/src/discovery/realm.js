/**
 * A realm is an isolated named partition for discovery, so more than one independent Broker plus its
 * clients can coexist on one machine. Mirrors Runtime/Discovery/Realm.cs.
 *
 * Deliberately independent of any hosting-environment concept: a hosting environment answers "which
 * config profile is this process running under?", a realm answers "which Broker should this client
 * find?".
 *
 * The environment variable is read through globalThis rather than a bare `process`, so this module
 * stays loadable in a browser — where there is no environment to read and the answer is always the
 * default. Nothing is normalized beyond the uppercasing the pipe name applies, so the value here and
 * the Broker's MessageBus:Realm must match exactly, case aside.
 */
export const DEFAULT_REALM = "Default";

export function resolveRealm() {
    return globalThis.process?.env?.MESSAGEBUS_REALM || DEFAULT_REALM;
}
