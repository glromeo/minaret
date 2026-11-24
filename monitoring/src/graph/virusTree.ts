import type { BodySpec, LinkSpec } from "./simulation";
import type { ClientNode } from "./store";
import type { DynamicNode } from "./dynamicNodes";

/**
 * Builds the simulation's body-spec tree, the client/provider pairings across it, and a lookup from
 * body id back to what it represents — shared between `BusGraph`'s `sync()` call and its rendering,
 * so the two can never disagree on id shape, and so the dashed line the operator sees is drawn
 * between exactly the two bodies the attraction force acts on. Only a dynamic node (see
 * `dynamicNodes.ts`) grows channel/topic satellites; an ordinary Broker-observed client is a bare
 * leaf.
 *
 * **A dynamic node's body is keyed by its own id, not by its RouteId**, for its whole life — which
 * is what lets it be drawn before the Broker has ever heard of it and after the Broker has stopped
 * hearing from it. Keying it by RouteId instead would mean the body could only exist for exactly the
 * window `clients` holds an entry, and the id would *change* the moment registration acked, so the
 * simulation would discard the settled position and respawn the node somewhere else on its orbit.
 * The Broker-observed client with the same RouteId is therefore skipped in the loop below — it is
 * the same participant, drawn once, under the id that outlives its connection.
 *
 * `client === null` on a client node is the disconnected state the view draws transparent: a dynamic
 * node that has not registered yet, one whose socket dropped, or one closing and waiting for the
 * Broker to confirm it went.
 *
 * An ordinary Broker-observed client grows the same channel/topic satellites a dynamic node does,
 * off `ClientNode.channels` (mirrored from channel.registered/topic.registered in store.ts) rather
 * than a dynamic node's own accordion state — the two are just different sources for the same
 * shape. It only ever grows them in the provider role: nothing here tracks which topics of *other*
 * providers an arbitrary external client has subscribed to, so it has no client-role spikes.
 */

export type GraphNode =
    | { kind: "client"; client: ClientNode | null; dynamicNode: DynamicNode | null }
    | { kind: "channel"; dynamicNode: DynamicNode | null; channelName: string; role: "provider" | "client" }
    | { kind: "topic"; dynamicNode: DynamicNode | null; channelName: string; topic: string };

/**
 * A client-role spike and the provider-role spike of the same name — the relationship
 * `channel.resolve` (kind "channel") or `topic.subscribe` against a `topic.register` (kind "topic")
 * actually establishes. Paired across every dynamic node, not just within one.
 *
 * Two providers clashing on a name never both pair: the Broker rejects the second registration, so
 * only one of them is real, and the map below keeps whichever the walk met last. An unresolved
 * client spike — no matching provider name anywhere yet — simply has no partner and no entry here.
 *
 * `kind` is what the renderer picks its trim radius and CSS class off; the simulation ignores it and
 * reads the depth off the bodies themselves.
 */
export interface GraphLink extends LinkSpec {
    kind: "channel" | "topic";
}

/** `ownerId` is a dynamic node's own id or an ordinary client's RouteId — either way, whatever the
 * owning "client"-kind body is keyed by. */
export function channelBodyId(ownerId: string, channelName: string): string {
    return `channel:${ownerId}:${channelName}`;
}

export function topicBodyId(ownerId: string, channelName: string, topic: string): string {
    return `topic:${ownerId}:${channelName}:${topic}`;
}

export function buildVirusTree(
    clients: readonly ClientNode[],
    dynamicNodes: readonly DynamicNode[]
): { specs: BodySpec[]; links: GraphLink[]; nodes: Map<string, GraphNode> } {
    const specs: BodySpec[] = [];
    const nodes = new Map<string, GraphNode>();

    const byRouteId = new Map(clients.map(client => [client.routeId, client]));
    const claimed = new Set(dynamicNodes.map(node => node.routeId).filter((id): id is string => id !== null));

    // Keyed by what makes two spikes partners: a channel name, and a channel name plus topic. Both
    // sides are collected as the tree is walked and paired only afterwards, since a client spike is
    // as likely to be met before its provider as after. Shared across both loops below, so an
    // ordinary client's provider spike can link a dynamic node's client-role subscription (or vice
    // versa) exactly as two dynamic nodes already do.
    const providers = new Map<string, string>();
    const clientSpikes: { kind: GraphLink["kind"]; key: string; clientId: string }[] = [];

    for (const client of clients) {
        if (claimed.has(client.routeId)) continue;
        specs.push({ id: client.routeId, parentId: null });
        nodes.set(client.routeId, { kind: "client", client, dynamicNode: null });

        for (const channel of client.channels) {
            const channelId = channelBodyId(client.routeId, channel.name);
            specs.push({ id: channelId, parentId: client.routeId });
            nodes.set(channelId, { kind: "channel", dynamicNode: null, channelName: channel.name, role: "provider" });
            providers.set(channel.name, channelId);

            for (const topic of channel.topics) {
                const topicId = topicBodyId(client.routeId, channel.name, topic);
                specs.push({ id: topicId, parentId: channelId });
                nodes.set(topicId, { kind: "topic", dynamicNode: null, channelName: channel.name, topic });
                providers.set(`${channel.name}:${topic}`, topicId);
            }
        }
    }

    for (const dynamicNode of dynamicNodes) {
        const client = dynamicNode.routeId === null ? null : byRouteId.get(dynamicNode.routeId) ?? null;

        specs.push({ id: dynamicNode.id, parentId: null });
        nodes.set(dynamicNode.id, { kind: "client", client, dynamicNode });

        for (const channel of dynamicNode.channels) {
            const channelId = channelBodyId(dynamicNode.id, channel.name);
            specs.push({ id: channelId, parentId: dynamicNode.id });
            nodes.set(channelId, { kind: "channel", dynamicNode, channelName: channel.name, role: channel.role });

            if (channel.role === "provider") providers.set(channel.name, channelId);
            else clientSpikes.push({ kind: "channel", key: channel.name, clientId: channelId });

            for (const topic of channel.topics) {
                const topicId = topicBodyId(dynamicNode.id, channel.name, topic);
                specs.push({ id: topicId, parentId: channelId });
                nodes.set(topicId, { kind: "topic", dynamicNode, channelName: channel.name, topic });

                const key = `${channel.name}:${topic}`;
                if (channel.role === "provider") providers.set(key, topicId);
                else clientSpikes.push({ kind: "topic", key, clientId: topicId });
            }
        }
    }

    const links = clientSpikes
        .map(({ kind, key, clientId }) => {
            const providerId = providers.get(key);
            return providerId ? { kind, clientId, providerId } : null;
        })
        .filter((link): link is GraphLink => link !== null);

    return { specs, links, nodes };
}