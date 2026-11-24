import type { Signal } from "@preact/signals";

/**
 * A drag handle between two panes, resizing whichever pane is against `size` by writing its
 * pixel size straight into that signal — the caller owns clamping (min/max) since only it knows
 * the pane's sensible bounds.
 *
 * Pointer capture rather than a mousemove/mouseup pair on window: it keeps the drag alive even if
 * the pointer leaves the handle mid-drag (fast mouse movement, or dragging past the pane's own
 * edge), which a window listener would need extra bookkeeping to replicate.
 */
export function Splitter({
    axis,
    size,
    min = 160,
    max = 640,
    reverse = false
}: {
    axis: "x" | "y";
    size: Signal<number>;
    min?: number;
    max?: number;
    /** True when dragging toward decreasing coordinates should *grow* the pane (e.g. a right-hand pane whose left edge is being dragged). */
    reverse?: boolean;
}) {
    function onPointerDown(event: PointerEvent) {
        const handle = event.currentTarget as HTMLElement;
        handle.setPointerCapture(event.pointerId);

        const startCoord = axis === "x" ? event.clientX : event.clientY;
        const startSize = size.value;

        function onPointerMove(moveEvent: PointerEvent) {
            const coord = axis === "x" ? moveEvent.clientX : moveEvent.clientY;
            const delta = (coord - startCoord) * (reverse ? -1 : 1);
            size.value = Math.min(max, Math.max(min, startSize + delta));
        }

        function onPointerUp(upEvent: PointerEvent) {
            handle.releasePointerCapture(upEvent.pointerId);
            handle.removeEventListener("pointermove", onPointerMove);
            handle.removeEventListener("pointerup", onPointerUp);
        }

        handle.addEventListener("pointermove", onPointerMove);
        handle.addEventListener("pointerup", onPointerUp);
    }

    return <div class={`splitter splitter--${axis}`} onPointerDown={onPointerDown} />;
}
