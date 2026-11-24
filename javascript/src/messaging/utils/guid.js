import { decodeBase64Url, encodeBase64Url } from "./base64url.js";

/**
 * Guid <-> base64url of the 16 raw bytes, RFC-4122 big-endian, unpadded — exactly 22 chars on the
 * wire against 36 in canonical form. Stands in for System.Guid, which the C# side gets from the BCL.
 *
 * Big-endian is part of the wire contract, not an implementation detail: C# writes the bytes with
 * Guid.TryWriteBytes(span, bigEndian: true) rather than ToByteArray(), which is little-endian for
 * the first three fields. That form is self-consistent within C# but would not match this port.
 *
 * Pinned by the same literal WireTests pins the C# side with:
 * 00112233-4455-6677-8899-aabbccddeeff encodes to ABEiM0RVZneImaq7zN3u_w.
 */

const HEX = Array.from({ length: 256 }, (_, i) => i.toString(16).padStart(2, "0"));

/** The number of base64url chars 16 bytes always produce, unpadded. */
export const GUID_CHARS = 22;

const GUID_BYTES = 16;

function toHyphenated(bytes) {
    let hex = "";
    for (const byte of bytes) hex += HEX[byte];

    return `${hex.substring(0, 8)}-${hex.substring(8, 12)}-${hex.substring(12, 16)}-${hex.substring(16, 20)}-${hex.substring(20)}`;
}

/**
 * Encodes a canonical 8-4-4-4-12 Guid, as System.Text.Json writes one, into its wire form.
 */
export function encodeGuid(guid) {
    const hex = guid.replace(/-/g, "");
    if (hex.length !== 32) throw new Error(`not a guid: ${guid}`);

    const bytes = new Uint8Array(GUID_BYTES);
    for (let i = 0; i < GUID_BYTES; i++) bytes[i] = parseInt(hex.substring(i * 2, i * 2 + 2), 16);

    return encodeBase64Url(bytes);
}

/**
 * Reverses encodeGuid, throwing on anything that isn't exactly 22 chars decoding to 16 bytes — the
 * length check is what keeps a truncated segment from decoding to a plausible-looking shorter value.
 */
export function decodeGuid(encoded) {
    if (encoded.length !== GUID_CHARS) throw new Error(`not an encoded guid: ${encoded}`);

    const bytes = decodeBase64Url(encoded);
    if (bytes.length !== GUID_BYTES) throw new Error(`not an encoded guid: ${encoded}`);

    return toHyphenated(bytes);
}

/**
 * A fresh RouteId. Version 7 (time-ordered) to match Guid.CreateVersion7() on the C# side — nothing
 * on the wire parses the version out, but keeping the two the same means ids from either side sort
 * identically in logs.
 */
export function newGuid() {
    const bytes = crypto.getRandomValues(new Uint8Array(GUID_BYTES));
    const timestamp = Date.now();

    // 48-bit big-endian milliseconds, then version 7 and the RFC-4122 variant over the random tail.
    for (let i = 0; i < 6; i++) bytes[i] = (timestamp / 2 ** (8 * (5 - i))) & 0xff;
    bytes[6] = (bytes[6] & 0x0f) | 0x70;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;

    return toHyphenated(bytes);
}
