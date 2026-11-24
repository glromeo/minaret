import type { Guid } from "@messagebus/client";

/**
 * The floating-window geometry every window kind on the canvas carries — a dynamic node's window
 * (`dynamicNodes.ts`) and an API editor's (`apis.ts`) — together with the operations that move it.
 *
 * Extracted here for one reason that is not de-duplication: **the stacking order and the spawn
 * cascade are global to the canvas, not to a kind.** Both window kinds are absolutely positioned
 * siblings in the same stacking context, so a per-store z-counter would let a window just raised by
 * a click sit underneath an older window of the other kind, and a per-store spawn counter would
 * open the first API window exactly on top of the first node window. Both counters live in this
 * module and are handed out by `newWindowState`, so there is only ever one of each.
 *
 * The operations are pure list transforms rather than methods: each store owns its own signal and
 * writes the answer back to it, which keeps the "replace, never mutate" rule of a signal-held array
 * (see `store.ts`) in the store that owns the signal.
 */
export interface WindowState {
    /** Position in page px, dragged by the title bar. Ignored while `maximized`. */
    x: number;
    y: number;
    /** Size in px, dragged by the resize handle. Ignored while `maximized`. */
    width: number;
    height: number;
    /**
     * Whether the window is showing. A hidden window is still *mounted* — for a dynamic node that
     * is the whole point (the iframe's bus connection must survive a minimize, see
     * `DynamicNode.tsx`), and for an API editor it means the CodeMirror document and its undo
     * history survive too.
     */
    windowOpen: boolean;
    /**
     * Whether the window fills the graph area (Windows-11-style maximize). `restore` holds the
     * bounds to snap back to — captured once, on the transition *into* maximized, so repeated
     * maximize/restore cycles don't drift.
     */
    maximized: boolean;
    restore: { x: number; y: number; width: number; height: number } | null;
    /** Stacking order among floating windows — bumped to the current top on click/drag/reopen. */
    zIndex: number;
}

/** What the list transforms below need of an element: an identity, plus the geometry they act on. */
type Windowed = WindowState & { id: Guid };

let topZIndex = 0;
let spawnCount = 0;

/** The next free stacking level. Exported so a store can raise a window it is already rebuilding. */
export function nextZIndex(): number {
    return ++topZIndex;
}

/**
 * Opening geometry for a new window: the requested size, at the next step of a shared cascade so
 * successive windows are offset rather than stacked exactly, and on top.
 */
export function newWindowState(width: number, height: number): WindowState {
    const step = spawnCount++;
    return {
        x: 80 + (step % 5) * 48,
        y: 80 + (step % 5) * 40,
        width,
        height,
        windowOpen: true,
        maximized: false,
        restore: null,
        zIndex: nextZIndex()
    };
}

function patch<T extends Windowed>(list: T[], id: Guid, change: (item: T) => Partial<T> | null): T[] {
    return list.map(item => {
        if (item.id !== id) return item;
        const delta = change(item);
        return delta ? { ...item, ...delta } : item;
    });
}

/** Raises id above every other window — called on click/drag-start so overlapping windows stack sanely. */
export function raiseWindow<T extends Windowed>(list: T[], id: Guid): T[] {
    return patch(list, id, () => ({ zIndex: nextZIndex() }) as Partial<T>);
}

export function setWindowOpen<T extends Windowed>(list: T[], id: Guid, windowOpen: boolean): T[] {
    return patch(list, id, () => (windowOpen ? { windowOpen, zIndex: nextZIndex() } : { windowOpen }) as Partial<T>);
}

/** Drag is a no-op while maximized — the title bar still doubles as the restore trigger via double-click. */
export function moveWindow<T extends Windowed>(list: T[], id: Guid, x: number, y: number): T[] {
    return patch(list, id, item => (item.maximized ? null : ({ x, y } as Partial<T>)));
}

export function resizeWindow<T extends Windowed>(list: T[], id: Guid, width: number, height: number, minWidth: number, minHeight: number): T[] {
    return patch(list, id, item =>
        item.maximized ? null : ({ width: Math.max(minWidth, width), height: Math.max(minHeight, height) } as Partial<T>)
    );
}

/** Fills the graph area (100% of the window's positioned ancestor, see `styles.css`), or snaps back. */
export function toggleWindowMaximized<T extends Windowed>(list: T[], id: Guid): T[] {
    return patch(list, id, item =>
        item.maximized
            ? item.restore
                ? ({ maximized: false, ...item.restore, restore: null } as Partial<T>)
                : null
            : ({
                  maximized: true,
                  restore: { x: item.x, y: item.y, width: item.width, height: item.height },
                  zIndex: nextZIndex()
              } as Partial<T>)
    );
}
