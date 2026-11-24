/**
 * base64url over raw bytes, unpadded — the encoding the wire format uses for Guids and for gzipped
 * bodies. Stands in for System.Buffers.Text.Base64Url, which the C# side gets from the BCL; there is
 * no counterpart file under Runtime/ because there is nothing there to write.
 *
 * The alphabet (A-Z a-z 0-9 - _) is disjoint from the frame's own `|` and `\0` delimiters and from
 * the `/` that marks a channel/topic path, which is why an encoded value never needs escaping and a
 * `/`-prefixed segment can never be mistaken for one.
 */

/**
 * btoa/atob take and return a latin-1 string, one char per byte, so the conversion either way is a
 * plain char-code walk. Chunked because String.fromCharCode(...bytes) blows the argument limit on a
 * body of any size, and a gzipped event.replay is exactly the case this exists for.
 */
const CHUNK = 8192;

export function encodeBase64Url(bytes) {
    let binary = "";
    for (let i = 0; i < bytes.length; i += CHUNK) binary += String.fromCharCode(...bytes.subarray(i, i + CHUNK));

    return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

/**
 * Throws on anything outside the alphabet — callers are parse paths that already treat a malformed
 * frame as droppable, so a throw here is caught there rather than propagated.
 */
export function decodeBase64Url(text) {
    const base64 = text.replace(/-/g, "+").replace(/_/g, "/");
    const binary = atob(base64.padEnd(Math.ceil(base64.length / 4) * 4, "="));

    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);

    return bytes;
}
