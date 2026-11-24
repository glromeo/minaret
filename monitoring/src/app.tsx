import {useEffect} from "preact/hooks";
import {useSignal} from "@preact/signals";
import {ApiWindow} from "./components/ApiWindow";
import {BusGraph} from "./components/BusGraph";
import {CanvasToolbar} from "./components/CanvasToolbar";
import {DynamicNode} from "./components/DynamicNode";
import {ExternalNodeWindow} from "./components/ExternalNodeWindow";
import {Inspector} from "./components/Inspector";
import {LogViewer} from "./components/LogViewer";
import {Splitter} from "./components/Splitter";
import {panes, TopNav} from "./components/TopNav";
import {RepositoryTree} from "./components/RepositoryTree";
import {apis, loadApis} from "./graph/apis";
import {dynamicNodes} from "./graph/dynamicNodes";
import {externalNodeWindows} from "./graph/externalNodes";
import {errorText} from "./utils";
import {brokerRouteId, clientList, connectionState, trackBus} from "./graph/store";
import {newClientInfo, Router, WebSocketConnection} from "@messagebus/client";
import {resolveBrokerUrl} from "@messagebus/client/discovery";

import pkg from "../package.json";

/**
 * The monitor names the *program*, not the instance, and that is deliberate: **two monitors are one
 * client, and the newer one wins.**
 *
 * Opening a second monitor therefore evicts the first, which stays evicted — the Broker closes it
 * with `CloseCodes.Superseded` and the client runtime treats that as terminal, so the displaced tab
 * reads OFFLINE and does not fight back. Making the identity unique per tab would avoid the eviction
 * instead, and was tried; it is the wrong answer here, because two tabs on one wall display is not
 * two clients and the Broker already has a rule for the case.
 *
 * `NodeApp` and `ExternalNodeApp` do the opposite (`monitor-node-${nodeId}`,
 * `monitor-inspector-${routeId}`) and are not inconsistent with this: those are many logical clients
 * inside *one* monitor, so each genuinely is a separate participant and must say so.
 */
const MONITOR_IDENTITY = `${pkg.name}@${pkg.version}`;

export function App() {
    const error = useSignal<string | null>(null);

    /** Sidebar width and the inspector's share of its height — plain px, dragged by Splitter. */
    const sidebarWidth = useSignal(280);
    const inspectorHeight = useSignal(280);

    /** The repository dock's width, on the opposite edge and dragged the same way. */
    const repositoryWidth = useSignal(296);

    useEffect(() => {
        // The monitor is a pure observer, so it needs Layer 0 only — a Router, not a MessageBus.
        // Building it here rather than through MessageBus.connect is also what lets onStateChange be
        // armed *before* the first attempt, so the pill shows the wait rather than starting at it.
        const controller = new AbortController();
        const router = new Router(async () => new WebSocketConnection(await resolveBrokerUrl()));

        router.onStateChange.add(state => (connectionState.value = state));
        router.reconnect();

        router.connect(
            newClientInfo({role: "monitor", identity: MONITOR_IDENTITY}),
            {signal: controller.signal}
        ).then(info => {
            // The monitor is a client like any other, so it appears in its own client.connected
            // replay; recording the RouteId is what lets the graph leave itself out.
            brokerRouteId.value = info.routeId;
            return trackBus(router);
        }).catch((failure: unknown) => {
            if (controller.signal.aborted) return;
            error.value = errorText(failure);
        });

        return () => {
            controller.abort();
            router.connection?.dispose();
        };
    }, []);

    // The IDL repository is REST beside the socket, not traffic on it, so it is fetched on its own
    // and a Broker that is up but has never stored a document is simply an empty answer. Its own
    // effect rather than a step in the one above: nothing about it waits on the bus, and a failure
    // here must not look like a failed connection.
    useEffect(() => {
        loadApis().catch((failure: unknown) => (error.value = `IDL repository: ${errorText(failure)}`));
    }, []);

    const liveLabel = connectionState.value === "open" ? "LIVE" : connectionState.value === "connecting" ? "CONNECTING" : "OFFLINE";

    // Read once: the sidebar's shape depends on how many of its two panes are showing, not just on
    // each one's own flag — with only one of them up there is nothing to split, so it takes the whole
    // height and the seam between them goes away rather than sitting against the sidebar's own edge.
    const view = panes.value;
    const sidebarShowing = view.inspector || view.log;

    return (
        <main>
            <header>
                <h1>Message Bus Studio</h1>
                <span class={`status status--${connectionState.value}`}>{connectionState.value}</span>
                <div class="nav-divider"/>
                <div class="header-spacer"/>
                <TopNav/>
                <div class="header-spacer"/>
                <div class="nav-divider"/>
                <div class="header-count">{clientList.value.length} connected</div>
                <div class={`live-pill live-pill--${connectionState.value}`}>
                    <span class="live-dot"/>
                    {liveLabel}
                </div>
            </header>

            {error.value && <p class="error">{error.value}</p>}

            <div class="canvas-row">
                {/* Docked ahead of the canvas rather than floated over it: the tree is a place to
                    stand, not a thing to consult and dismiss, and the graph's own toolbar already
                    occupies the canvas's top-left corner. */}
                {view.repository && (
                    <>
                        <div class="repository-dock" style={{width: `${repositoryWidth.value}px`}}>
                            <RepositoryTree/>
                        </div>

                        <Splitter axis="x" size={repositoryWidth} min={220} max={560}/>
                    </>
                )}

                <div class="canvas-stack">
                    {view.bus && <BusGraph/>}

                    {/* Outside the graph on purpose: both buttons create something that outlives it
                        (a node keeps its connection, an .idl is a stored document), so hiding the
                        graph must not take away the only way to add either. */}
                    <CanvasToolbar/>

                    {/* The windows below are *not* under the bus toggle, and the node ones cannot be:
                        unmounting a DynamicNode closes its iframe's bus connection (see below), so a
                        toggle that hid them would disconnect four live clients to tidy the view. The
                        toggle hides the graph and leaves what is floating over it alone. */}

                    {/* A closing node is deliberately dropped from this list while still present in
                        `dynamicNodes`: unmounting is what tears down its iframe and so closes its bus
                        connection, which is the very thing the Broker has to notice before the node
                        is removed for good. Rendering it hidden instead would hold the socket open
                        and the confirming `client.disconnected` would never come. */}
                    {dynamicNodes.value
                        .filter(node => !node.closing)
                        .map(node => (
                            <DynamicNode key={node.id} node={node}/>
                        ))}

                    {/* No such filter here: an API window has nothing behind it whose lifetime is
                        tied to being mounted, so a hidden one is simply hidden (see apis.ts).

                        Nor is this group under the repository toggle any more, now that the toggle
                        governs the dock: an editor is opened and closed from its own tree row, so
                        one thing has one owner and hiding the tree does not sweep away the document
                        the operator is in the middle of. Every row is mounted regardless of
                        windowOpen, which is what preserves each editor's CodeMirror document and
                        undo history across a close (see WindowState). */}
                    {apis.value.map(api => (
                        <ApiWindow key={api.id} api={api}/>
                    ))}

                    {/* Nor here: an external node window owns no bus participant the Broker needs to
                        hear the end of, so closing it just unmounts (see graph/externalNodes.ts). */}
                    {externalNodeWindows.value.map(node => (
                        <ExternalNodeWindow key={node.id} node={node}/>
                    ))}
                </div>

                {sidebarShowing && (
                    <>
                        <Splitter axis="x" size={sidebarWidth} min={220} max={560} reverse/>

                        <div class="sidebar" style={{width: `${sidebarWidth.value}px`}}>
                            {view.inspector && (
                                // The dragged height applies only while the log is up to take the
                                // rest; alone, the inspector flexes, so re-showing the log restores
                                // the split the operator last dragged rather than a default.
                                <div
                                    class="sidebar-top"
                                    style={view.log ? {height: `${inspectorHeight.value}px`} : {flex: 1}}
                                >
                                    <Inspector/>
                                </div>
                            )}

                            {view.inspector && view.log && <Splitter axis="y" size={inspectorHeight} min={80} max={600}/>}

                            {view.log && (
                                <div class="sidebar-bottom">
                                    <LogViewer/>
                                </div>
                            )}
                        </div>
                    </>
                )}
            </div>
        </main>
    );
}
