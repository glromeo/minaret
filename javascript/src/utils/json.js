/**
 * The one shared JSON convention, mirroring runtime's Json. Wire owns the *envelope*
 * encoding, this owns the *payload* encoding.
 *
 * There is nothing to configure here, and that is the point: wire bodies are camelCase on every
 * side, so every wire body in this package is a plain object with camelCase keys and no side
 * transforms names.
 *
 * Parse is total by design — an unparseable body is droppable input on a receive path, not an
 * exception to unwind a socket callback with — so it answers undefined rather than throwing. That
 * is unambiguous because `undefined` is not a JSON value: `null` round-trips as itself.
 */

export function stringify(value) {
    return JSON.stringify(value ?? null);
}

export function parse(text) {
    try {
        return JSON.parse(text);
    } catch {
        return undefined;
    }
}
