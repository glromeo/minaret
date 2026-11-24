import { signal } from "@preact/signals";
import type { ClientInfo, Guid } from "@messagebus/client";
import { clients } from "./store";

/**
 * Who has been seen on a channel, remembered across reloads.
 *
 * This is the one place in the monitor that outlives what the Broker knows, and it is deliberate:
 * the repository tree lists an API's *last known* provider and clients, which is a question the
 * event plane cannot answer — `store.ts` is a reflection of Broker state and drops a client the
 * moment it disconnects. So this map is written beside that one and persisted to localStorage.
 *
 * The cost is that a remembered entry can never be disproven: nothing on the bus says "that client
 * is not coming back", so a provider that was retired months ago still lists here until the operator
 * clears it. That is what `online` below is for — liveness is *never* stored, it is resolved against
 * `store.ts` on every read, so a row can be remembered and dim but never remembered and lit.
 */
export type ParticipantRole = "provider" | "client";

export interface Participant {
    routeId: Guid;
    role: ParticipantRole;
    /**
     * The `ClientInfo` fields as of the last time this participant was seen holding one. Null when
     * the channel event that introduced it arrived before the `client.connected` carrying its info —
     * the six subscriptions in `trackBus` replay independently, so neither order is guaranteed and
     * `refreshInfo` fills these in whenever the other half turns up.
     */
    identity: string | null;
    clockOffsetMs: number | null;
    properties: Record<string, string>;
    lastSeen: number;
}

/** A participant as the tree renders it: what was remembered, plus whether it is on the bus now. */
export interface ParticipantRow extends Participant {
    online: boolean;
}

const STORAGE_KEY = "messagebus.monitor.participants.v1";

/** Keyed by channel name. Replaced, never mutated — see `store.ts` on why a signal needs that. */
export const remembered = signal<ReadonlyMap<string, readonly Participant[]>>(load());

/**
 * Who is subscribed to each channel *right now*, from topic.subscribed/unsubscribed. Deliberately
 * not persisted and deliberately not merged into `remembered`: it is the live half, and writing it
 * to storage would produce exactly the "remembered as online" state the doc above rules out.
 *
 * Topics are held per subscriber rather than collapsed to a set of RouteIds, because the events are
 * per *topic* and a client subscribed to three topics on a channel would otherwise be dimmed by the
 * first unsubscribe while still holding the other two. A client is a live subscriber of the channel
 * while it holds at least one topic on it.
 *
 * A provider needs no equivalent — holding a channel is already on its `ClientNode` in `store.ts`.
 */
export const subscribers = signal<ReadonlyMap<string, ReadonlyMap<Guid, ReadonlySet<string>>>>(new Map());

export function rememberProvider(channel: string, routeId: Guid, info: ClientInfo | null): void {
    remember(channel, routeId, "provider", info);
}

export function rememberSubscriber(channel: string, topic: string, routeId: Guid, info: ClientInfo | null): void {
    remember(channel, routeId, "client", info);

    const byRoute = new Map(subscribers.value.get(channel) ?? []);
    byRoute.set(routeId, new Set([...(byRoute.get(routeId) ?? []), topic]));

    const next = new Map(subscribers.value);
    next.set(channel, byRoute);
    subscribers.value = next;
}

/**
 * Drops one topic from a subscriber's live set, and the subscriber itself once that was its last.
 * It stays remembered either way — the remembered list is what the tree renders, and this only
 * decides whether the row is lit.
 */
export function dropSubscriber(channel: string, topic: string, routeId: Guid): void {
    const held = subscribers.value.get(channel)?.get(routeId);
    if (!held?.has(topic)) return;

    const topics = new Set(held);
    topics.delete(topic);

    const byRoute = new Map(subscribers.value.get(channel) ?? []);
    if (topics.size === 0) byRoute.delete(routeId);
    else byRoute.set(routeId, topics);

    const next = new Map(subscribers.value);
    next.set(channel, byRoute);
    subscribers.value = next;
}

/**
 * Fills in (or refreshes) the `ClientInfo` half of every entry for a RouteId, on every
 * client.connected. Called unconditionally rather than only for entries missing it, so a client that
 * reconnected with a new identity or a re-measured clock offset updates rather than showing the
 * figures it had the first time it was seen.
 */
export function refreshInfo(routeId: Guid, info: ClientInfo | null): void {
    if (info === null) return;

    let touched = false;
    const next = new Map(remembered.value);

    for (const [channel, list] of next) {
        if (!list.some(entry => entry.routeId === routeId)) continue;
        touched = true;
        next.set(channel, list.map(entry => (entry.routeId === routeId ? { ...entry, ...snapshot(info), lastSeen: Date.now() } : entry)));
    }

    if (touched) commit(next);
}

/** Forgets everything remembered for one channel. The tree's only way to retire a stale entry. */
export function forgetChannel(channel: string): void {
    if (!remembered.value.has(channel)) return;

    const next = new Map(remembered.value);
    next.delete(channel);
    commit(next);
}

/**
 * The rows for one channel, ordered provider-first and then online-before-offline, each with its
 * liveness resolved against `store.ts` rather than read from storage.
 *
 * A provider is live when the client holding it still lists the channel, which covers both halves of
 * leaving — channel.removed strips the name, client.disconnected strips the whole node. A subscriber
 * is live when it is in `subscribers` *and* still connected, since unsubscribing and disconnecting
 * are separate departures and either one should dim the row.
 */
export function participantsOf(channel: string): ParticipantRow[] {
    const live = clients.value;
    const subscribed = subscribers.value.get(channel);
    const holds = (routeId: Guid) => (subscribed?.get(routeId)?.size ?? 0) > 0;

    const rows = (remembered.value.get(channel) ?? []).map(entry => ({
        ...entry,
        online:
            entry.role === "provider"
                ? live.get(entry.routeId)?.channels.some(held => held.name === channel) === true
                : holds(entry.routeId) && live.has(entry.routeId)
    }));

    return rows.sort(
        (a, b) =>
            rank(a) - rank(b) ||
            Number(b.online) - Number(a.online) ||
            b.lastSeen - a.lastSeen
    );
}

function rank(row: ParticipantRow): number {
    return row.role === "provider" ? 0 : 1;
}

function remember(channel: string, routeId: Guid, role: ParticipantRole, info: ClientInfo | null): void {
    const list = remembered.value.get(channel) ?? [];
    const existing = list.find(entry => entry.routeId === routeId && entry.role === role);

    // Keyed on RouteId *and* role: one connection can both provide a channel and subscribe to a
    // topic on another, and collapsing those into one row would make the pill claim a role the
    // client does not hold here.
    const entry: Participant = {
        routeId,
        role,
        identity: info?.identity ?? existing?.identity ?? null,
        clockOffsetMs: info?.clockOffsetMs ?? existing?.clockOffsetMs ?? null,
        properties: info?.properties ?? existing?.properties ?? {},
        lastSeen: Date.now()
    };

    const next = new Map(remembered.value);
    next.set(channel, existing ? list.map(item => (item === existing ? entry : item)) : [...list, entry]);
    commit(next);
}

function snapshot(info: ClientInfo): Pick<Participant, "identity" | "clockOffsetMs" | "properties"> {
    return { identity: info.identity, clockOffsetMs: info.clockOffsetMs, properties: info.properties };
}

function commit(next: ReadonlyMap<string, readonly Participant[]>): void {
    remembered.value = next;
    save(next);
}

/**
 * Storage is best-effort at both ends. A quota-exceeded write or a private-browsing block must not
 * take down the event handler that triggered it — the tree is still correct for this session with
 * nothing persisted — and a payload written by an older build is discarded rather than trusted,
 * since a half-understood entry would render as a row with no name and no way to remove it.
 */
function save(map: ReadonlyMap<string, readonly Participant[]>): void {
    try {
        localStorage.setItem(STORAGE_KEY, JSON.stringify([...map]));
    } catch {
        /* storage unavailable or full — this session keeps working, it just won't be remembered */
    }
}

function load(): ReadonlyMap<string, readonly Participant[]> {
    try {
        const stored = localStorage.getItem(STORAGE_KEY);
        if (stored === null) return new Map();

        const entries = JSON.parse(stored) as [string, Participant[]][];
        return new Map(
            entries
                .filter(([channel, list]) => typeof channel === "string" && Array.isArray(list))
                .map(([channel, list]) => [channel, list.filter(entry => typeof entry?.routeId === "string")])
        );
    } catch {
        return new Map();
    }
}
