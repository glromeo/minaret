import { signal } from "@preact/signals";
import type { ComponentChildren } from "preact";
import { dynamicNodes } from "../graph/dynamicNodes";
import { clients, label, selectedRouteId } from "../graph/store";
import { channelBodyId, topicBodyId } from "../graph/virusTree";
import { CopyButton } from "./CopyButton";
import { matches, PanelHeader } from "./PanelHeader";

/**
 * The selection-detail panel: shows what's actually known about the selected node from its
 * ClientInfo — no fabricated lag/throughput figures, since the wire protocol carries neither for
 * an arbitrary client.
 *
 * The name and subtitle of what is selected are never filtered — only the cards below them are.
 * A panel that could be made to show its detail cards for something it no longer named would be
 * answering a question about an unidentified node.
 */

/** Module-level, so a filter survives the panel being toggled away and back. */
const query = signal("");

export function Inspector() {
    return (
        <aside class="inspector">
            <PanelHeader
                title="Inspector"
                icon={
                    <>
                        <circle cx="7" cy="7" r="4.5" />
                        <path d="m10.4 10.4 3.1 3.1" />
                    </>
                }
                query={query}
                placeholder="filter details"
            />
            <div class="inspector-body">
                <Selection />
            </div>
        </aside>
    );
}

/**
 * A labelled detail, hidden when the filter excludes it. `text` is what the filter reads, passed
 * separately because `children` is markup — a card whose value is a channel tree or a copy button
 * has no single string the filter could recover from the VNode.
 */
function Card({ label: caption, text, children }: { label: string; text: string; children: ComponentChildren }) {
    if (!matches(query.value, caption, text)) return null;

    return (
        <div class="inspector-card">
            <div class="inspector-label">{caption}</div>
            {children}
        </div>
    );
}

function Heading({ name, sub }: { name: string; sub: string }) {
    return (
        <>
            <div class="inspector-name">{name}</div>
            <div class="inspector-sub">{sub}</div>
        </>
    );
}

function Selection() {
    const selected = selectedRouteId.value;

    if (selected === null) return <div class="inspector-empty">select a node to inspect it</div>;

    if (selected === "broker") return <Heading name="Broker" sub="message hub" />;

    // A selected id may be a channel or topic satellite (see graph/virusTree.ts) rather than a
    // client's RouteId — those don't exist in `clients` at all, so they're searched for by
    // reconstructing the same synthetic ids the graph builds bodies from. Checked against both
    // dynamic nodes (keyed by their own id) and ordinary Broker-observed clients (keyed by RouteId —
    // virusTree.ts grows the same satellites for both, see its own doc block).
    for (const node of dynamicNodes.value) {
        for (const channel of node.channels) {
            if (channelBodyId(node.id, channel.name) === selected)
                return (
                    <>
                        <Heading name={channel.name} sub={`channel · ${channel.role}`} />
                        <div class="inspector-divider" />
                        <Card label="Node" text={node.title}>
                            <div class="inspector-value">{node.title}</div>
                        </Card>
                        <Card label="Topics" text={channel.topics.join(", ")}>
                            <div class="inspector-value">{channel.topics.length === 0 ? "none yet" : channel.topics.join(", ")}</div>
                        </Card>
                    </>
                );

            for (const topic of channel.topics) {
                if (topicBodyId(node.id, channel.name, topic) === selected)
                    return (
                        <>
                            <Heading name={topic} sub="topic" />
                            <div class="inspector-divider" />
                            <Card label="Channel" text={channel.name}>
                                <div class="inspector-value">{channel.name}</div>
                            </Card>
                            <Card label="Node" text={node.title}>
                                <div class="inspector-value">{node.title}</div>
                            </Card>
                        </>
                    );
            }
        }
    }

    for (const owner of clients.value.values()) {
        for (const channel of owner.channels) {
            if (channelBodyId(owner.routeId, channel.name) === selected)
                return (
                    <>
                        <Heading name={channel.name} sub="channel · provider" />
                        <div class="inspector-divider" />
                        <Card label="Client" text={label(owner.routeId)}>
                            <div class="inspector-value">{label(owner.routeId)}</div>
                        </Card>
                        <Card label="Topics" text={channel.topics.join(", ")}>
                            <div class="inspector-value">{channel.topics.length === 0 ? "none yet" : channel.topics.join(", ")}</div>
                        </Card>
                    </>
                );

            for (const topic of channel.topics) {
                if (topicBodyId(owner.routeId, channel.name, topic) === selected)
                    return (
                        <>
                            <Heading name={topic} sub="topic" />
                            <div class="inspector-divider" />
                            <Card label="Channel" text={channel.name}>
                                <div class="inspector-value">{channel.name}</div>
                            </Card>
                            <Card label="Client" text={label(owner.routeId)}>
                                <div class="inspector-value">{label(owner.routeId)}</div>
                            </Card>
                        </>
                    );
            }
        }
    }

    // A dynamic node's graph body is keyed by its own id, not its RouteId (see graph/virusTree.ts),
    // so the selection has to be mapped back before it can name a client — and may name one that
    // isn't on the bus at all right now, which is the whole point of that keying.
    const dynamicNode = dynamicNodes.value.find(node => node.id === selected);
    const client = clients.value.get(dynamicNode?.routeId ?? selected) ?? null;

    if (!client)
        return dynamicNode ? (
            <Heading name={dynamicNode.title} sub={dynamicNode.closing ? "closing — waiting on the Broker" : "connecting…"} />
        ) : null;

    const properties = Object.entries(client.info?.properties ?? {});

    return (
        <>
            <Heading
                name={client.channels[0]?.name ?? client.info?.properties["role"] ?? "client"}
                sub={client.channels.length > 0 ? "provider" : "client"}
            />

            <div class="inspector-divider" />

            <Card label="Route Id" text={client.routeId}>
                <div class="inspector-value inspector-value--mono inspector-value--copyable">
                    <span class="route-id">{client.routeId}</span>
                    <CopyButton value={client.routeId} label="copy route id" />
                </div>
            </Card>

            {client.channels.length > 0 && (
                <Card
                    label={client.channels.length > 1 ? "Channels" : "Channel"}
                    text={client.channels.map(channel => `${channel.name} ${channel.topics.join(" ")}`).join(" ")}
                >
                    <ul class="channel-tree">
                        {client.channels.map(channel => (
                            <li key={channel.name} class="channel-tree-channel">
                                <button
                                    class="channel-tree-name"
                                    onClick={() => (selectedRouteId.value = channelBodyId(client.routeId, channel.name))}
                                >
                                    {channel.name}
                                </button>
                                {channel.topics.length > 0 && (
                                    <ul class="channel-tree-topics">
                                        {channel.topics.map(topic => (
                                            <li key={topic}>
                                                <button
                                                    class="channel-tree-name channel-tree-name--topic"
                                                    onClick={() => (selectedRouteId.value = topicBodyId(client.routeId, channel.name, topic))}
                                                >
                                                    {topic}
                                                </button>
                                            </li>
                                        ))}
                                    </ul>
                                )}
                            </li>
                        ))}
                    </ul>
                </Card>
            )}

            {client.info && (
                <Card label="Clock offset" text={`${client.info.clockOffsetMs} ms`}>
                    <div class="inspector-value">{client.info.clockOffsetMs} ms</div>
                </Card>
            )}

            {properties.length > 0 && (
                <Card label="Properties" text={properties.map(([key, value]) => `${key} ${value}`).join(" ")}>
                    {properties.map(([key, value]) => (
                        <div class="inspector-prop" key={key}>
                            <span class="inspector-prop-key">{key}</span>
                            <span class="inspector-prop-value">{value}</span>
                        </div>
                    ))}
                </Card>
            )}
        </>
    );
}
