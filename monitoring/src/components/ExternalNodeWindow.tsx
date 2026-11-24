import type { ExternalNodeWindow as ExternalNodeWindowModel } from "../graph/externalNodes";
import {
    bringExternalNodeToFront,
    closeExternalNodeWindow,
    moveExternalNodeWindow,
    resizeExternalNodeWindow,
    toggleMaximizeExternalNodeWindow
} from "../graph/externalNodes";
import { clients, label } from "../graph/store";
import { ExternalNodeApp } from "./ExternalNodeApp";
import { FloatingWindow } from "./FloatingWindow";

/**
 * An external client's floating window: the same `FloatingWindow` chrome a dynamic node or an API
 * editor wears, around `ExternalNodeApp`. In this document, not an iframe — unlike a dynamic node
 * there is no independently-authored UI to isolate here, only a read-only view over Broker-observed
 * state plus a disposable connection `ExternalNodeApp` owns itself.
 *
 * Title falls back to the RouteId prefix for a target that has since disconnected — the window
 * stays open (closing it is the operator's call, not the Broker's, since nothing here is a bus
 * participant the Broker needs to hear the end of — see `graph/externalNodes.ts`).
 */
export function ExternalNodeWindow({ node }: { node: ExternalNodeWindowModel }) {
    const client = clients.value.get(node.id);
    const title = client ? label(node.id) : `${node.id.substring(0, 8)} (disconnected)`;

    return (
        <FloatingWindow
            window={node}
            title={title}
            class="ext-node-window"
            onFocus={() => bringExternalNodeToFront(node.id)}
            onMove={(x, y) => moveExternalNodeWindow(node.id, x, y)}
            onResize={(width, height) => resizeExternalNodeWindow(node.id, width, height)}
            onToggleMaximize={() => toggleMaximizeExternalNodeWindow(node.id)}
            onClose={() => closeExternalNodeWindow(node.id)}
        >
            <ExternalNodeApp routeId={node.id} />
        </FloatingWindow>
    );
}
