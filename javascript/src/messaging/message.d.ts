import type { Guid } from "./utils/guid.js";
import type { BodyEncodingValue } from "./utils/wire.js";

/**
 * Either a resolved id, or an unresolved channel/topic path, or empty (Broker-addressed). Exactly
 * one of the two forms is populated; `Destination.fromId`/`fromPath` are the only constructors, so
 * the mutual exclusion is enforced rather than conventional.
 */
export interface DestinationValue {
    /** A TopicId for a topic verb, a recipient RouteId otherwise — the verb says which. */
    readonly id: Guid | null;
    readonly channel: string | null;
    readonly topic: string | null;
}

export declare const Destination: {
    readonly empty: DestinationValue;
    fromId(id: Guid): DestinationValue;
    fromPath(channel: string, topic: string): DestinationValue;
    isEmpty(destination: DestinationValue): boolean;
    isPath(destination: DestinationValue): boolean;
    toString(destination: DestinationValue): string;
};

export interface Header {
    readonly to: DestinationValue;
    /** The verb — what the recipient should do with this. Never which topic it is about. */
    readonly type: string;
    readonly from: Guid | null;
    readonly replyTo: Guid | null;
    readonly time: number;
    readonly ttl: number | null;
    readonly encoding: BodyEncodingValue;
    readonly bodyLength: number | null;
    readonly extras: string;
}

/** A Header paired with its already-decoded body — what Router.handleMessage carries. */
export interface Message<T = unknown> {
    readonly header: Header;
    readonly body: T;
}

export declare function header(
    type: string,
    to?: DestinationValue,
    overrides?: Partial<Omit<Header, "type">>
): Header;
