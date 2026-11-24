/**
 * The one shared JSON convention, mirroring Runtime/Utils/Json.cs. Wire owns the *envelope*
 * encoding, this owns the *payload* encoding.
 *
 * There is nothing to configure here, and that is the point: the C# side sets a camelCase naming
 * policy precisely so bodies read as ordinary JavaScript objects on this end. Every wire body in
 * this package is therefore a plain object with camelCase keys, and the two sides agree without
 * either one transforming names.
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
