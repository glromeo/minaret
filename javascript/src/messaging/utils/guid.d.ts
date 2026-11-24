/**
 * Canonical 8-4-4-4-12 hyphenated form, as a JSON body carries a Guid. The wire form is the
 * 22-char base64url of the same 16 bytes and never appears as a value in this API — only inside a
 * rendered frame.
 */
export type Guid = string;

export declare const GUID_CHARS: 22;

export declare function encodeGuid(guid: Guid): string;

export declare function decodeGuid(encoded: string): Guid;

export declare function newGuid(): Guid;
