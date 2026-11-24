import type { Header } from "../message.js";

export declare const TAG: "M01";
export declare const FIELD_SEPARATOR: "|";
export declare const BODY_SEPARATOR: "\0";
export declare const COMPRESSION_THRESHOLD_BYTES: 1024;

export declare const BodyEncoding: {
    readonly Raw: "raw";
    readonly Gzip: "gzip";
};

export type BodyEncodingValue = (typeof BodyEncoding)[keyof typeof BodyEncoding];

/** A parsed header plus its body exactly as it arrived — still compressed if `encoding` says so. */
export interface WireFrame {
    readonly header: Header;
    readonly body: string;
}

/** Exactly one of the two is set: a parse either produced a frame or an error, never both. */
export type ParseResult =
    | { frame: WireFrame; error: null }
    | { frame: null; error: string };

export declare function tryParseHeader(raw: string): ParseResult;

/** The decoded body text, or "" when absent or undecodable — never a throw. */
export declare function parseBody(frame: WireFrame): Promise<string>;

/** The decoded body parsed as JSON, or undefined when absent or malformed. */
export declare function tryParseBody<T = unknown>(frame: WireFrame): Promise<T | undefined>;

export declare function prepareBody(
    header: Header,
    body: unknown
): Promise<{ header: Header; body: string }>;

export declare function writeHeaderAndBody(header: Header, body: string): string;

export declare function serializeText(header: Header, body: string): Promise<string>;

export declare function serializeMessage(header: Header, body: unknown): Promise<string>;
