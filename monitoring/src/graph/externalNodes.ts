import { signal } from "@preact/signals";
import type { Guid } from "@messagebus/client";
import { moveWindow, newWindowState, raiseWindow, resizeWindow, toggleWindowMaximized, type WindowState } from "./windows";

/**
 * A floating window inspecting one Broker-observed external client — opened from the graph's hover
 * popover (the eye button, see `BusGraph.tsx`), never spawned the way a dynamic node is. Keyed by
 * the client's own RouteId rather than a minted id, since there is exactly one of these per real
 * connection and re-opening it should raise the existing window instead of stacking a duplicate.
 *
 * Unlike a dynamic node, there is nothing on the bus for this window's lifetime to gate: the target
 * connection is a real external process that lives or dies on its own, entirely independent of
 * whether anyone is looking at it here. So closing (or minimizing — `FloatingWindow` treats both as
 * the same action) simply drops the row, tearing down the window body's own disposable inspector
 * connection (`ExternalNodeApp`) rather than merely hiding it the way a dynamic node's iframe stays
 * mounted.
 */
export interface ExternalNodeWindow extends WindowState {
    id: Guid;
}

export const externalNodeWindows = signal<ExternalNodeWindow[]>([]);

export const EXTERNAL_NODE_WIDTH = 340;
export const EXTERNAL_NODE_HEIGHT = 420;
export const MIN_EXTERNAL_NODE_WIDTH = 260;
export const MIN_EXTERNAL_NODE_HEIGHT = 220;

/** Opens routeId's window, or raises the one already open for it. */
export function openExternalNodeWindow(routeId: Guid): void {
    externalNodeWindows.value = externalNodeWindows.value.some(w => w.id === routeId)
        ? raiseWindow(externalNodeWindows.value, routeId)
        : [...externalNodeWindows.value, { id: routeId, ...newWindowState(EXTERNAL_NODE_WIDTH, EXTERNAL_NODE_HEIGHT) }];
}

export function closeExternalNodeWindow(id: Guid): void {
    externalNodeWindows.value = externalNodeWindows.value.filter(w => w.id !== id);
}

export function bringExternalNodeToFront(id: Guid): void {
    externalNodeWindows.value = raiseWindow(externalNodeWindows.value, id);
}

export function moveExternalNodeWindow(id: Guid, x: number, y: number): void {
    externalNodeWindows.value = moveWindow(externalNodeWindows.value, id, x, y);
}

export function resizeExternalNodeWindow(id: Guid, width: number, height: number): void {
    externalNodeWindows.value = resizeWindow(externalNodeWindows.value, id, width, height, MIN_EXTERNAL_NODE_WIDTH, MIN_EXTERNAL_NODE_HEIGHT);
}

export function toggleMaximizeExternalNodeWindow(id: Guid): void {
    externalNodeWindows.value = toggleWindowMaximized(externalNodeWindows.value, id);
}
