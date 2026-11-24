import { useSignal } from "@preact/signals";
import { useEffect, useRef } from "preact/hooks";
import { Destination, header, MessageBus, newClientInfo } from "@messagebus/client";
import type { ChannelClient, ChannelProvider, ConnectionStateValue, Guid } from "@messagebus/client";
import { reportChannel, reportRouteId, reportTopic, type ChannelRole } from "../graph/dynamicNodes";
import { reportReceived, reportSent } from "../graph/traffic";
import { errorText, pushLog, type LogRow } from "../utils";
import { CopyButton } from "./CopyButton";

/**
 * The verb this panel's Layer-0 sends carry. Any verb the Broker doesn't recognise as control- or
 * data-plane falls through to a plain RouteId forward, so this is one arbitrary choice among many
 * rather than a reserved name — which is why it lives here and not in the client runtime's `Verbs`,
 * the shared vocabulary.
 */
const LAYER_0_VERB = "message";

/** Either handle: both expose `subscribe`, and `isProvider` narrows to the rest. */
type ChannelHandle = ChannelProvider | ChannelClient;

interface ChannelState {
    name: string;
    role: ChannelRole;
    /** True once channel.register/channel.resolve has acked — the row can add topics after this. */
    ready: boolean;
    error: string | null;
    expanded: boolean;
    topics: string[];
    topicInput: string;
    publishTopics: string[];
    publishBody: string;
    log: LogRow[];
}

function newChannel(name: string, role: ChannelRole): ChannelState {
    return {
        name,
        role,
        ready: false,
        error: null,
        expanded: true,
        topics: [],
        topicInput: "",
        publishTopics: [],
        publishBody: "",
        log: []
    };
}

/**
 * Mounted *inside* a dynamic node's iframe (see `DynamicNode.tsx`) — its own `MessageBus`
 * connection and a UI covering both layers it exposes:
 * - **Layer 0**: send a plain point-to-point message to an arbitrary RouteId.
 * - **Layer 1**: an accordion of channels this node has joined, each either one it provides or one
 *   it resolved as a client (a node may provide any number of channels), each expanding to its own
 *   accordion of topics created/joined on it.
 *
 * Runs in the iframe's own document, isolated from the host app's Preact tree and signals.
 */
export function NodeApp({ nodeId, title }: { nodeId: Guid; title: string }) {
    const state = useSignal<ConnectionStateValue>("connecting");
    const routeId = useSignal<Guid | null>(null);
    const error = useSignal<string | null>(null);

    const tab = useSignal<"direct" | "channels">("direct");

    // Layer 0
    const targetRouteId = useSignal("");
    const messageBody = useSignal("");
    const messageLog = useSignal<LogRow[]>([]);

    // Layer 1
    const channels = useSignal<ChannelState[]>([]);
    const newChannelName = useSignal("");

    const bus = useRef<MessageBus | null>(null);

    /** The handle per joined channel — a connection may provide and resolve any number of them. */
    const handles = useRef(new Map<string, ChannelHandle>());

    useEffect(() => {
        const controller = new AbortController();

        // Every dynamic node's `srcdoc` iframe resolves `location.pathname` the same way, so
        // `newClientInfo`'s default identity collides across nodes (same trap as `ExternalNodeApp`'s
        // doc block describes) — each new node would evict the previous one in the Broker's
        // IdentityRegistry and, since that close is terminal, leave every node but the newest dead.
        MessageBus.connect(newClientInfo({ identity: `monitor-node-${nodeId}`, role: "dynamic-node", node: title }), {
            persistent: true,
            signal: controller.signal
        })
            .then(connected => {
                bus.current = connected;
                state.value = "open";
                connected.router.onStateChange.add(next => (state.value = next));

                // Layer-0 traffic is matched on the verb this panel sends, not on the destination: a
                // control-plane ack is addressed to this RouteId too, and would otherwise read as an
                // inbound message.
                connected.router.handleMessage.add(({ header: head, body }) => {
                    if (head.type !== LAYER_0_VERB) return;
                    // No channel or topic: Layer-0 traffic concerns the node itself, so the dot
                    // travels the node's own edge to the Broker rather than a spike below it.
                    reportReceived(nodeId);
                    messageLog.value = pushLog(messageLog.value, `from ${head.from?.substring(0, 8) ?? "?"}: ${JSON.stringify(body)}`);
                });

                routeId.value = connected.router.routeId;
                if (connected.router.routeId !== null) reportRouteId(nodeId, connected.router.routeId);
            })
            .catch((failure: unknown) => {
                if (controller.signal.aborted) return;
                error.value = errorText(failure);
            });

        return () => {
            controller.abort();
            bus.current?.dispose();
        };
    }, []);

    function updateChannel(name: string, mutate: (channel: ChannelState) => ChannelState) {
        channels.value = channels.value.map(channel => (channel.name === name ? mutate(channel) : channel));
    }

    // No channel.unregister verb exists on the wire — this only drops the row from the UI's own
    // state. The Broker-side registration (provider) or resolve pin (client) outlives it until this
    // node's connection itself closes.
    function removeChannel(name: string): void {
        channels.value = channels.value.filter(channel => channel.name !== name);
        handles.current.delete(name);
    }

    function sendMessage() {
        const connected = bus.current;
        if (!connected || !targetRouteId.value.trim()) return;

        try {
            let body: unknown = messageBody.value;
            try {
                body = JSON.parse(messageBody.value);
            } catch {
                // Not JSON — send the raw string.
            }

            // Fire-and-forget: a plain RouteId forward is acked by nothing.
            void connected.router.sendMessage(header(LAYER_0_VERB, Destination.fromId(targetRouteId.value.trim())), body);
            reportSent(nodeId);
            messageLog.value = pushLog(messageLog.value, `to ${targetRouteId.value.substring(0, 8)}: ${messageBody.value}`);
        } catch (failure) {
            error.value = errorText(failure);
        }
    }

    async function addChannel(role: ChannelRole) {
        const name = newChannelName.value.trim();
        const connected = bus.current;
        if (!name || !connected || channels.value.some(c => c.name === name)) return;

        const channel = newChannel(name, role);
        channels.value = [...channels.value, channel];
        newChannelName.value = "";

        try {
            // createChannel claims the name; connectChannel blocks Broker-side until someone else
            // has (see ChannelClient) — which is why the row shows a wait rather than an error.
            const handle = role === "provider" ? await connected.createChannel(name) : await connected.connectChannel(name);
            handles.current.set(name, handle);
            reportChannel(nodeId, name, role);
            updateChannel(name, c => ({ ...c, ready: true }));
        } catch (failure) {
            updateChannel(name, c => ({ ...c, error: errorText(failure) }));
        }
    }

    async function addTopic(channelName: string) {
        const channel = channels.value.find(c => c.name === channelName);
        const handle = handles.current.get(channelName);
        if (!channel || !channel.topicInput.trim() || !handle) return;

        const topic = channel.topicInput.trim();

        try {
            // One call for both roles: `subscribe` is topic.register on a provider handle and
            // topic.subscribe on a client one — the same split the handles exist to make.
            await handle.subscribe(topic, body => {
                reportReceived(nodeId, channelName, topic);
                updateChannel(channelName, c => ({ ...c, log: pushLog(c.log, `${topic}: ${JSON.stringify(body)}`) }));
            });

            reportTopic(nodeId, channelName, topic);
            updateChannel(channelName, c => ({
                ...c,
                topics: [...c.topics, topic],
                topicInput: "",
                publishTopics: c.publishTopics.length > 0 ? c.publishTopics : [topic]
            }));
        } catch (failure) {
            updateChannel(channelName, c => ({ ...c, error: errorText(failure) }));
        }
    }

    async function sendOnTopic(channelName: string) {
        const channel = channels.value.find(c => c.name === channelName);
        const handle = handles.current.get(channelName);
        if (!channel || channel.publishTopics.length === 0 || !handle) return;

        try {
            let body: unknown = channel.publishBody;
            try {
                body = JSON.parse(channel.publishBody);
            } catch {
                // Not JSON — send the raw string.
            }

            for (const topic of channel.publishTopics) {
                // Reported before the await, not after: `publish` resolves on the Broker's ack, so
                // reporting the send once it completes would hold the dot back for a whole round
                // trip and show the departure only after the arrival. A send that then fails leaves
                // one dot that reached the Broker and none coming back, which is what happened.
                reportSent(nodeId, channelName, topic);

                // The handle's own direction decides the verb, not the UI's record of the role.
                if (handle.isProvider) await handle.publish(topic, body);
                else await handle.dispatch(topic, body);
            }
        } catch (failure) {
            updateChannel(channelName, c => ({ ...c, error: errorText(failure) }));
        }
    }

    return (
        <div class="node-app">
            <div class="node-app-header">
                <div class="node-app-status">
                    <span class={`node-app-dot node-app-dot--${state.value}`} />
                    {routeId.value ? <>
                        <span class="route-id">{routeId.value}</span>
                        <CopyButton value={routeId.value} label="copy route id" />
                    </> : state.value}
                </div>

                {error.value && <div class="node-app-error">{error.value}</div>}

                <div class="node-app-tabs">
                    <button
                        class={`node-app-tab${tab.value === "direct" ? " node-app-tab--active" : ""}`}
                        onClick={() => (tab.value = "direct")}
                    >
                        <svg class="node-app-tab-icon" viewBox="0 0 16 16">
                            <rect x="1.5" y="3.5" width="13" height="9" rx="1.2" />
                            <path d="M1.5 4.2 8 9.2l6.5-5" />
                        </svg>
                        Direct message
                    </button>
                    <button
                        class={`node-app-tab${tab.value === "channels" ? " node-app-tab--active" : ""}`}
                        onClick={() => (tab.value = "channels")}
                    >
                        <svg class="node-app-tab-icon" viewBox="0 0 16 16">
                            <path d="M2 8h4M10 8h4" />
                            <rect x="6" y="4.5" width="4" height="7" rx="1" />
                        </svg>
                        Channels
                    </button>
                </div>
            </div>

            <div class="node-app-content">
            {/* Layer 0 */}
            <div class="node-app-section" style={{ display: tab.value === "direct" ? undefined : "none" }}>
                <div class="node-app-row">
                    <textarea
                        class="node-app-input node-app-textarea"
                        placeholder="message body"
                        value={messageBody.value}
                        onInput={e => (messageBody.value = (e.target as HTMLTextAreaElement).value)}
                    />
                </div>
                <div class="node-app-row">
                    <input
                        class="node-app-input"
                        placeholder="route id"
                        value={targetRouteId.value}
                        onInput={e => (targetRouteId.value = (e.target as HTMLInputElement).value)}
                    />
                    <button class="node-app-button" onClick={sendMessage} disabled={!targetRouteId.value.trim()}>
                        send
                    </button>
                </div>
                <div class="node-app-log node-app-log--compact">
                    {(messageLog.value.length === 0 ? [{id:0, text:"no messages yet"}] : messageLog.value).map(row => (
                        <div class="node-app-log-row" key={row.id}>
                            <span class="node-app-log-body">{row.text}</span>
                        </div>
                    ))}
                </div>
            </div>

            {/* Layer 1: channels */}
            <div
                class="node-app-section node-app-section--grow"
                style={{ display: tab.value === "channels" ? undefined : "none" }}
            >
                <div class="node-app-accordion">
                    {channels.value.length === 0 && <div class="node-app-empty">no channels yet</div>}
                    {channels.value.map(channel => (
                        <div class="node-app-acc-item" key={channel.name}>
                            <div
                                class="node-app-acc-header"
                                onClick={() => updateChannel(channel.name, c => ({ ...c, expanded: !c.expanded }))}
                            >
                                <span class={`node-app-dot node-app-dot--${channel.ready ? "open" : "connecting"}`} />
                                <span class="node-app-acc-caret">{channel.expanded ? "▾" : "▸"}</span>
                                <span class="node-app-acc-title">{channel.name}</span>
                                <span class={`node-app-role-tag node-app-role-tag--${channel.role}`}>{channel.role}</span>
                                <button
                                    class={`node-app-icon-btn node-app-icon-btn--danger${!channel.ready && !channel.error ? " node-app-icon-btn--pending" : ""}`}
                                    onClick={e => {
                                        e.stopPropagation();
                                        removeChannel(channel.name);
                                    }}
                                    aria-label={`remove channel ${channel.name}`}
                                >
                                    <span class="node-app-spinner" />
                                    <span class="node-app-icon-trash" />
                                </button>
                            </div>

                            {channel.expanded && (
                                <div class="node-app-acc-body">
                                    {channel.error && <div class="node-app-error">{channel.error}</div>}

                                    {!channel.ready && !channel.error && (
                                        // channel.resolve blocks Broker-side with no timeout until a
                                        // provider registers this name (see Session.handleChannelResolve) —
                                        // this can be indefinite, so say so rather than leaving the
                                        // row looking stuck with no explanation. The topic form below
                                        // is withheld until then — a client can't create/subscribe
                                        // topics on a channel with no provider behind it yet.
                                        <div class="node-app-waiting">
                                            {channel.role === "provider" ? "registering…" : `waiting for a provider of "${channel.name}"…`}
                                        </div>
                                    )}

                                    {(channel.ready || channel.error) && (
                                        <>
                                            <div class="node-app-label">Topics</div>
                                            <div class="node-app-row">
                                                <input
                                                    class="node-app-input"
                                                    placeholder="topic name"
                                                    value={channel.topicInput}
                                                    onInput={e =>
                                                        updateChannel(channel.name, c => ({ ...c, topicInput: (e.target as HTMLInputElement).value }))
                                                    }
                                                    onKeyDown={e => e.key === "Enter" && channel.ready && addTopic(channel.name)}
                                                />
                                                <button class="node-app-button" onClick={() => addTopic(channel.name)} disabled={!channel.ready}>
                                                    add
                                                </button>
                                            </div>
                                            <div class="node-app-topics">
                                                {channel.topics.map(topic => (
                                                    <span
                                                        class={`node-app-chip${channel.publishTopics.includes(topic) ? " node-app-chip--selected" : ""}`}
                                                        key={topic}
                                                        onClick={e =>
                                                            updateChannel(channel.name, c => ({
                                                                ...c,
                                                                // plain click: radio-select this topic alone; ctrl/cmd+click: toggle it
                                                                // in/out of a multi-topic selection
                                                                publishTopics:
                                                                    e.ctrlKey || e.metaKey
                                                                        ? c.publishTopics.includes(topic)
                                                                            ? c.publishTopics.filter(t => t !== topic)
                                                                            : [...c.publishTopics, topic]
                                                                        : [topic]
                                                            }))
                                                        }
                                                    >
                                                        {topic}
                                                    </span>
                                                ))}
                                            </div>

                                            {channel.topics.length > 0 && (
                                                <>
                                                    <div class="node-app-row">
                                                        <input
                                                            class="node-app-input"
                                                            placeholder="message body"
                                                            value={channel.publishBody}
                                                            disabled={channel.publishTopics.length === 0}
                                                            onInput={e =>
                                                                updateChannel(channel.name, c => ({
                                                                    ...c,
                                                                    publishBody: (e.target as HTMLInputElement).value
                                                                }))
                                                            }
                                                            onKeyDown={e => e.key === "Enter" && sendOnTopic(channel.name)}
                                                        />
                                                        <button
                                                            class="node-app-button"
                                                            disabled={channel.publishTopics.length === 0}
                                                            onClick={() => sendOnTopic(channel.name)}
                                                        >
                                                            {channel.role === "provider" ? "publish" : "dispatch"}
                                                        </button>
                                                    </div>
                                                </>
                                            )}

                                            <div class="node-app-label">Received</div>
                                            <div class="node-app-log node-app-log--compact">
                                                {channel.log.length === 0 && <div class="node-app-empty">nothing yet</div>}
                                                {channel.log.map(row => (
                                                    <div class="node-app-log-row" key={row.id}>
                                                        <span class="node-app-log-body">{row.text}</span>
                                                    </div>
                                                ))}
                                            </div>
                                        </>
                                    )}
                                </div>
                            )}
                        </div>
                    ))}
                </div>
                <div class="node-app-row">
                    <input
                        class="node-app-input"
                        placeholder="channel name"
                        value={newChannelName.value}
                        onInput={e => (newChannelName.value = (e.target as HTMLInputElement).value)}
                        onKeyDown={e => e.key === "Enter" && addChannel("client")}
                    />
                    <button
                        class="node-app-button node-app-button--connect"
                        onClick={() => addChannel("client")}
                        disabled={!newChannelName.value.trim()}
                        title="join as client"
                    >
                        connect
                    </button>
                    <button
                        class="node-app-button node-app-button--create"
                        onClick={() => addChannel("provider")}
                        disabled={!newChannelName.value.trim()}
                        title="register as provider"
                    >
                        create
                    </button>
                </div>
            </div>
            </div>
        </div>
    );
}
