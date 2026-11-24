import type { ComponentChildren } from "preact";
import type { WindowState } from "../graph/windows";

interface FloatingWindowProps {
    /** The geometry to render at — owned by the caller's store, never by this component. */
    window: WindowState;
    title: ComponentChildren;
    /** Extra class on the outer element, for content-specific sizing/padding of the body. */
    class?: string;
    onFocus: () => void;
    onMove: (x: number, y: number) => void;
    onResize: (width: number, height: number) => void;
    onToggleMaximize: () => void;
    /**
     * What both the minimize and close buttons do. They are deliberately the same action: neither
     * control ends the thing behind the window (see `DynamicNode.tsx` for why that matters most for
     * a node — its bus connection must survive), so there is no second, harsher meaning for close
     * to carry.
     */
    onClose: () => void;
    children: ComponentChildren;
}

/**
 * The Windows-11-style chrome shared by every floating window on the canvas: a title bar that drags
 * and double-clicks to maximize, the three controls, and a corner resize handle.
 *
 * It renders geometry and raises events; it holds no state of its own. Every window is a row in
 * some store's signal-held list (`dynamicNodes`, `apis`), and a drag has to write *there* or the
 * position would live in two places and diverge the moment anything else touched the row — so the
 * pointer handlers here compute a target x/y and hand it straight to the caller.
 *
 * The body is `children`, not a fixed element, because the two kinds differ exactly there: a node's
 * body is an iframe with its own document and bus connection, an API's is a CodeMirror editor in
 * this document. Everything around the body is identical, and being identical is the point — the
 * two are the same kind of window to the operator.
 */
export function FloatingWindow({ window, title, class: className, onFocus, onMove, onResize, onToggleMaximize, onClose, children }: FloatingWindowProps) {
    function onDragTitleBar(event: PointerEvent) {
        // The window-control buttons live inside this same bar; capturing the pointer here would
        // swallow their click (a button never sees a matching pointerup once the bar owns the
        // pointer). A maximized window doesn't move either — same rationale as the resize handle.
        if ((event.target as HTMLElement).closest(".node-window-controls") || window.maximized) return;

        const handle = event.currentTarget as HTMLElement;
        handle.setPointerCapture(event.pointerId);

        const startX = event.clientX;
        const startY = event.clientY;
        const originX = window.x;
        const originY = window.y;

        function onPointerMove(moveEvent: PointerEvent) {
            onMove(originX + (moveEvent.clientX - startX), originY + (moveEvent.clientY - startY));
        }

        // lostpointercapture rather than pointerup — see onDragNode in BusGraph.tsx for why: it
        // fires whenever capture ends for any reason, where pointerup alone would leave a dangling
        // listener if the gesture was interrupted (pointercancel) instead of released normally.
        function onRelease() {
            handle.removeEventListener("pointermove", onPointerMove);
            handle.removeEventListener("lostpointercapture", onRelease);
        }

        handle.addEventListener("pointermove", onPointerMove);
        handle.addEventListener("lostpointercapture", onRelease);
    }

    function onDragResizeHandle(event: PointerEvent) {
        const handle = event.currentTarget as HTMLElement;
        handle.setPointerCapture(event.pointerId);

        const startX = event.clientX;
        const startY = event.clientY;
        const originWidth = window.width;
        const originHeight = window.height;

        function onPointerMove(moveEvent: PointerEvent) {
            onResize(originWidth + (moveEvent.clientX - startX), originHeight + (moveEvent.clientY - startY));
        }

        function onRelease() {
            handle.removeEventListener("pointermove", onPointerMove);
            handle.removeEventListener("lostpointercapture", onRelease);
        }

        handle.addEventListener("pointermove", onPointerMove);
        handle.addEventListener("lostpointercapture", onRelease);
    }

    return (
        <div
            class={`node-window${window.maximized ? " node-window--maximized" : ""}${className ? ` ${className}` : ""}`}
            style={
                window.maximized
                    ? { zIndex: window.zIndex, display: window.windowOpen ? "flex" : "none" }
                    : {
                          left: `${window.x}px`,
                          top: `${window.y}px`,
                          width: `${window.width}px`,
                          height: `${window.height}px`,
                          zIndex: window.zIndex,
                          display: window.windowOpen ? "flex" : "none"
                      }
            }
            onPointerDownCapture={onFocus}
        >
            <div class="node-window-bar" onPointerDown={onDragTitleBar} onDblClick={onToggleMaximize}>
                <span class="node-window-title">{title}</span>
                <div class="node-window-controls">
                    <button class="node-window-btn node-window-minimize" onClick={onClose} aria-label="minimize window">
                        &#x2013;
                    </button>
                    <button
                        class="node-window-btn node-window-maximize"
                        onClick={onToggleMaximize}
                        aria-label={window.maximized ? "restore window" : "maximize window"}
                    >
                        {window.maximized ? <span class="node-window-icon-restore" /> : <span class="node-window-icon-maximize" />}
                    </button>
                    <button class="node-window-btn node-window-close" onClick={onClose} aria-label="close window">
                        &#x2715;
                    </button>
                </div>
            </div>
            {children}
            {!window.maximized && <div class="node-window-resize" onPointerDown={onDragResizeHandle} />}
        </div>
    );
}
