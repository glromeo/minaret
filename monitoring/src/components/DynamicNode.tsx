import { useSignal } from "@preact/signals";
import { render } from "preact";
import { useEffect, useRef } from "preact/hooks";
import type { DynamicNode as DynamicNodeModel } from "../graph/dynamicNodes";
import {
    bringToFront,
    closeDynamicNodeWindow,
    moveDynamicNode,
    resizeDynamicNode,
    toggleMaximizeDynamicNode
} from "../graph/dynamicNodes";
import { FloatingWindow } from "./FloatingWindow";
import { NodeApp } from "./NodeApp";

/**
 * Injected into the iframe's own document rather than shared with the host's styles.css — the
 * whole point of rendering into a separate document is that the two never bleed into each other.
 */
import NODE_APP_CSS from "./node-app.css?inline";

/**
 * A dynamic node's floating window: the shared `FloatingWindow` chrome (drag to move,
 * minimize/maximize/close) around an iframe whose document is a blank shell. Content is injected by Preact, not
 * loaded from a URL — a second `render()` call targets the iframe's own `contentDocument`, giving
 * the node's UI an isolated DOM/CSS scope from the host app (its own `<style>`, no bleed either
 * direction) while staying in the same JS realm and module graph as the host, so no separate
 * bundle/build step is needed to ship it.
 *
 * `srcdoc` rather than a same-origin route: the content has no independent URL to bookmark or
 * reload, it exists only as long as this node does, which matches a node the operator can add and
 * remove freely.
 *
 * Rendered by `App` for every entry in `dynamicNodes` regardless of `node.windowOpen` — the iframe
 * (and the bus connection `NodeApp` opens inside it) must stay mounted while the window is merely
 * hidden, or putting a window away would drop the node off the bus instead of just tucking its
 * window away. So it hides via CSS rather than unmounting, and double-clicking the node's graph
 * circle (`BusGraph.tsx`) sets `windowOpen` back on.
 *
 * **None of the three window controls ends the node's participation** — minimize and close are the
 * same action, hiding the window and leaving the node on the bus. Discarding a node is a
 * *graph* action, not a window one (`BusGraph`'s hover popover), because that is where the node
 * still exists to be acted on after its window is gone.
 */
export function DynamicNode({ node }: { node: DynamicNodeModel }) {
    const iframeRef = useRef<HTMLIFrameElement>(null);
    // srcdoc paints its own blank white document before `load` fires and this component gets a
    // chance to inject NODE_APP_CSS — visible for that gap, it's a white flash against the app's
    // dark chrome. Staying hidden (not unmounted — the iframe must still load and fire events)
    // until mount() has actually appended the stylesheet and content avoids it.
    const ready = useSignal(false);
    // The root NodeApp is rendered into, inside the iframe's own document — a second, independent
    // Preact root, not a child of this component's tree. Removing the <iframe> element only tears
    // down its document; it does not walk Preact's unmount lifecycle for a tree rooted separately
    // like this one, so NodeApp's own effect cleanup (the one that disposes the bus, see
    // NodeApp.tsx) would never run and the WebSocket would leak open indefinitely. This effect's own
    // cleanup calls render(null, root) explicitly to unmount that second tree when DynamicNode itself
    // unmounts (i.e. when the node is discarded from the graph, see discardDynamicNode), so the
    // connection closes deterministically instead of waiting on GC of a detached document.
    const appRoot = useRef<HTMLDivElement | null>(null);

    useEffect(() => {
        const iframe = iframeRef.current;
        if (!iframe) return;

        function mount() {
            const doc = iframe!.contentDocument;
            if (!doc) return;

            // Vite's dev-mode Fast Refresh can re-run this effect (cleanup + mount) without the
            // iframe's own document ever being torn down — the cleanup below only unmounts the
            // Preact tree, it never strips the DOM nodes mount() appended. Re-creating the style/root
            // on such a re-run would leave the previous, now-orphaned pair sitting in `doc.body`
            // alongside the new one, one more per refresh. Recycling whatever this document already
            // carries keeps a live document to exactly one style tag and one root, no matter how many
            // times mount() runs against it.
            let style = doc.head.querySelector<HTMLStyleElement>("style.node-app-style");
            if (!style) {
                style = doc.createElement("style");
                style.className = "node-app-style";
                doc.head.appendChild(style);
            }
            // Set on every mount(), not just on creation — NODE_APP_CSS is a fresh import binding
            // each time Fast Refresh re-runs this module (node-app.css changed), so a recycled style
            // element still needs its content refreshed to the latest sheet.
            style.textContent = NODE_APP_CSS;

            let root = doc.body.querySelector<HTMLDivElement>(".node-app-root");
            if (!root) {
                root = doc.createElement("div");
                // Unstyled otherwise, this div sits between `body` (definite height, from the CSS
                // below) and `.node-app` (`height: 100%`) — an unstyled div is auto-height, which
                // breaks the percentage chain and makes `.node-app` grow to fit its content instead of
                // clamping to the iframe viewport, so the iframe's own document scrolls instead of
                // `.node-app-content`.
                root.className = "node-app-root";
                doc.body.appendChild(root);

                // A click inside the iframe's own document is invisible to the host document's event
                // system entirely (a real cross-document boundary, not just a capture/bubble
                // question) — onPointerDownCapture on the outer .node-window only ever sees a click
                // that lands on the iframe element's own border/gutter. Listening here is the only
                // way an interaction with the node's actual content raises its window above an
                // overlapping sibling. Attached once, alongside the root itself, rather than on every
                // mount() — the listener would otherwise accumulate one instance per refresh too.
                doc.addEventListener("pointerdown", () => bringToFront(node.id));
            }
            appRoot.current = root;

            render(<NodeApp nodeId={node.id} title={node.title} />, root);
            ready.value = true;
        }

        // An iframe with a srcdoc goes through TWO documents: the initial empty one every iframe
        // starts with, then the srcdoc-parsed one that replaces it. Some browsers fire `load` for
        // the FIRST of those — so a single "readyState complete, or wait for one load event" check
        // (the previous approach here) can run mount() against the placeholder document moments
        // before the browser swaps in the real one, silently discarding everything just rendered
        // with no second trigger left to retry from (the load listener was one-shot and already
        // consumed). That is the actual cause of "the iframe doesn't render": not a missed event,
        // but a mount that fires and then gets thrown away underneath itself.
        //
        // Polling instead of trusting any single event sidesteps the ambiguity: it keeps checking
        // doc.title (set by the srcdoc markup below, absent on the placeholder document) until the
        // real document is the one in place, and only then mounts — once, verified against content
        // that could only come from our own srcdoc.
        let mounted = false;
        const poll = setInterval(() => {
            if (mounted) return;
            const doc = iframe.contentDocument;
            if (doc?.readyState === "complete" && doc.title === SRCDOC_MARKER) {
                mounted = true;
                clearInterval(poll);
                mount();
            }
        }, 30);

        return () => {
            clearInterval(poll);
            if (appRoot.current) render(null, appRoot.current);
        };
    }, [node.id]);

    return (
        <FloatingWindow
            window={node}
            title={node.title}
            onFocus={() => bringToFront(node.id)}
            onMove={(x, y) => moveDynamicNode(node.id, x, y)}
            onResize={(width, height) => resizeDynamicNode(node.id, width, height)}
            onToggleMaximize={() => toggleMaximizeDynamicNode(node.id)}
            onClose={() => closeDynamicNodeWindow(node.id)}
        >
            <iframe
                ref={iframeRef}
                class="node-window-frame"
                style={{ visibility: ready.value ? "visible" : "hidden" }}
                srcDoc={`<!doctype html><html><head><title>${SRCDOC_MARKER}</title></head><body></body></html>`}
            />
        </FloatingWindow>
    );
}

/**
 * A value no placeholder document could ever have — set as the srcdoc's `<title>` and checked
 * against `contentDocument.title` before mounting (see the mount-polling effect above), so a
 * `load` fired for the iframe's initial empty document is never mistaken for the real one.
 */
const SRCDOC_MARKER = "node-app-srcdoc";
