import { useSignal } from "@preact/signals";
import { useEffect, useRef } from "preact/hooks";
import { MessageBus, newClientInfo } from "@messagebus/client";
import type { ChannelClient, ConnectionStateValue, Guid } from "@messagebus/client";
import { clients } from "../graph/store";
import { reportReceived, reportSent } from "../graph/traffic";
import { errorText, pushLog, type LogRow } from "../utils";
import { CopyButton } from "./CopyButton";

interface ChannelState {
    ready: boolean;
    error: string | null;
    expanded: boolean;
    sendTopics: string[];
    body: string;
    log: LogRow[];
}

function newChannelState(): ChannelState {
    return { ready: false, error: null, expanded: true, sendTopics: [], body: "", log: [] };
}

/**
 * A read-only view onto one Broker-observed external client's channels and topics (`ClientNode.channels`
 * in `graph/store.ts`, mirrored off `channel.registered`/`topic.registered`), plus the ability to send
 * on them — the body of the floating window `BusGraph`'s hover popover opens via its eye button, one
 * per RouteId (`graph/externalNodes.ts`).
 *
 * Unlike `NodeApp` (which authors a dynamic node's own channels from scratch), this never registers
 * or declares anything: the channel/topic list is exactly what the store already knows the target
 * provides. This component's own `MessageBus` connection exists purely to resolve those channels as
 * a *client* and dispatch messages on them — a disposable inspector connection, unrelated to the
 * RouteId the window is named after, opened fresh on mount and torn down on unmount (window close).
 *
 * Every such window needs its own distinct `Identity`, or two opened at once collide in the Broker's
 * IdentityRegistry and the second evicts the first for good (newer wins, and the close is terminal —
 * see `broker/CLAUDE.md`) — hence the identity is derived from the target RouteId rather than left at
 * the default.
 */
export function ExternalNodeApp({ routeId }: { routeId: Guid }) {
    const state = useSignal<ConnectionStateValue>("connecting");
    const error = useSignal<string | null>(null);
    const channelStates = useSignal<Record<string, ChannelState>>({});

    const bus = useRef<MessageBus | null>(null);
    const handles = useRef(new Map<string, ChannelClient>());
    const subscribed = useRef(new Map<string, Set<string>>());
    const connecting = useRef(new Set<string>());

    function updateChannel(name: string, mutate: (c: ChannelState) => ChannelState) {
        channelStates.value = { ...channelStates.value, [name]: mutate(channelStates.value[name] ?? newChannelState()) };
    }

    /**
     * Ensures a handle — and a subscription for every topic the store currently knows about — exists
     * for every channel the target currently provides. Re-run whenever that picture changes (new
     * effect below), so a channel or topic declared after this window opened is picked up the same
     * way the graph's own satellites are (see `virusTree.ts`).
     */
    function sync() {
        const connected = bus.current;
        const client = clients.value.get(routeId);
        if (!connected || !client) return;

        for (const channel of client.channels) {
            const handle = handles.current.get(channel.name);
            if (!handle) {
                if (connecting.current.has(channel.name)) continue;
                connecting.current.add(channel.name);
                updateChannel(channel.name, c => ({ ...c, ready: false }));

                connected
                    .connectChannel(channel.name)
                    .then(h => {
                        handles.current.set(channel.name, h);
                        connecting.current.delete(channel.name);
                        updateChannel(channel.name, c => ({ ...c, ready: true }));
                        sync();
                    })
                    .catch((failure: unknown) => {
                        connecting.current.delete(channel.name);
                        updateChannel(channel.name, c => ({ ...c, error: errorText(failure) }));
                    });
                continue;
            }

            let subs = subscribed.current.get(channel.name);
            if (!subs) subscribed.current.set(channel.name, (subs = new Set()));

            for (const topic of channel.topics) {
                if (subs.has(topic)) continue;
                subs.add(topic);

                handle
                    .subscribe(topic, body => {
                        reportReceived(routeId, channel.name, topic);
                        updateChannel(channel.name, c => ({ ...c, log: pushLog(c.log, `${topic}: ${JSON.stringify(body)}`) }));
                    })
                    .then(() => updateChannel(channel.name, c => ({ ...c, sendTopics: c.sendTopics.length > 0 ? c.sendTopics : [topic] })))
                    .catch((failure: unknown) => updateChannel(channel.name, c => ({ ...c, error: errorText(failure) })));
            }
        }
    }

    useEffect(() => {
        const controller = new AbortController();

        MessageBus.connect(newClientInfo({ identity: `monitor-inspector-${routeId}`, role: "inspector", target: routeId }), {
            persistent: true,
            signal: controller.signal
        })
            .then(connected => {
                bus.current = connected;
                state.value = "open";
                connected.router.onStateChange.add(next => (state.value = next));
                sync();
            })
            .catch((failure: unknown) => {
                if (controller.signal.aborted) return;
                error.value = errorText(failure);
            });

        return () => {
            controller.abort();
            bus.current?.dispose();
        };
    }, [routeId]);

    // The dependency is the channels array itself (replaced, never mutated, on every store change —
    // see graph/store.ts), so this only re-runs when the target's picture of its own channels/topics
    // actually changed.
    useEffect(sync, [clients.value.get(routeId)?.channels]);

    async function send(channelName: string) {
        const channel = channelStates.value[channelName];
        const handle = handles.current.get(channelName);
        if (!channel || !handle || channel.sendTopics.length === 0) return;

        let body: unknown = channel.body;
        try {
            body = JSON.parse(channel.body);
        } catch {
            // Not JSON — send the raw string.
        }

        try {
            for (const topic of channel.sendTopics) {
                reportSent(routeId, channelName, topic);
                await handle.dispatch(topic, body);
            }
        } catch (failure) {
            updateChannel(channelName, c => ({ ...c, error: errorText(failure) }));
        }
    }

    const client = clients.value.get(routeId);

    return (
        <div class="ext-node">
            <div class="ext-node-status">
                <span class={`ext-node-dot ext-node-dot--${state.value}`} />
                <span class="route-id">{routeId}</span>
                <CopyButton value={routeId} label="copy route id" />
            </div>

            {error.value && <div class="ext-node-error">{error.value}</div>}

            {!client || client.channels.length === 0 ? (
                <div class="ext-node-empty">no channels yet</div>
            ) : (
                <div class="ext-node-accordion">
                    {client.channels.map(channel => {
                        const cs = channelStates.value[channel.name] ?? newChannelState();
                        return (
                            <div class="ext-node-acc-item" key={channel.name}>
                                <div class="ext-node-acc-header" onClick={() => updateChannel(channel.name, c => ({ ...c, expanded: !c.expanded }))}>
                                    <span class={`ext-node-dot ext-node-dot--${cs.ready ? "open" : "connecting"}`} />
                                    <span class="ext-node-acc-caret">{cs.expanded ? "▾" : "▸"}</span>
                                    <span class="ext-node-acc-title">{channel.name}</span>
                                </div>

                                {cs.expanded && (
                                    <div class="ext-node-acc-body">
                                        {cs.error && <div class="ext-node-error">{cs.error}</div>}

                                        {channel.topics.length === 0 ? (
                                            <div class="ext-node-empty">no topics yet</div>
                                        ) : (
                                            <>
                                                <div class="ext-node-topics">
                                                    {channel.topics.map(topic => (
                                                        <span
                                                            class={`ext-node-chip${cs.sendTopics.includes(topic) ? " ext-node-chip--selected" : ""}`}
                                                            key={topic}
                                                            onClick={e =>
                                                                updateChannel(channel.name, c => ({
                                                                    ...c,
                                                                    // plain click: radio-select this topic alone; ctrl/cmd+click: toggle it
                                                                    // in/out of a multi-topic selection
                                                                    sendTopics:
                                                                        (e as MouseEvent).ctrlKey || (e as MouseEvent).metaKey
                                                                            ? c.sendTopics.includes(topic)
                                                                                ? c.sendTopics.filter(t => t !== topic)
                                                                                : [...c.sendTopics, topic]
                                                                            : [topic]
                                                                }))
                                                            }
                                                        >
                                                            {topic}
                                                        </span>
                                                    ))}
                                                </div>

                                                <div class="ext-node-row">
                                                    <input
                                                        class="ext-node-input"
                                                        placeholder="message body"
                                                        value={cs.body}
                                                        disabled={cs.sendTopics.length === 0}
                                                        onInput={e => updateChannel(channel.name, c => ({ ...c, body: (e.target as HTMLInputElement).value }))}
                                                        onKeyDown={e => e.key === "Enter" && send(channel.name)}
                                                    />
                                                    <button class="ext-node-button" disabled={cs.sendTopics.length === 0} onClick={() => send(channel.name)}>
                                                        dispatch
                                                    </button>
                                                </div>

                                                <div class="ext-node-label">Received</div>
                                                <div class="ext-node-log">
                                                    {cs.log.length === 0 && <div class="ext-node-empty">nothing yet</div>}
                                                    {cs.log.map(row => (
                                                        <div class="ext-node-log-row" key={row.id}>
                                                            {row.text}
                                                        </div>
                                                    ))}
                                                </div>
                                            </>
                                        )}
                                    </div>
                                )}
                            </div>
                        );
                    })}
                </div>
            )}
        </div>
    );
}
