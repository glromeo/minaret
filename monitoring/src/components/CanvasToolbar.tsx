import { addApi } from "../graph/apis";
import { addDynamicNode } from "../graph/dynamicNodes";

/**
 * The canvas's own two spawn buttons: a new dynamic node, a new `.idl`.
 *
 * Lifted out of `BusGraph` and rendered by `App` against `.canvas-stack` instead, because neither
 * button is about the graph — both create something that outlives it, and a node in particular is a
 * live bus participant whose windows stay on the canvas whether or not the graph is drawn. Left
 * inside `BusGraph` they vanished with it, which made hiding the graph also mean losing the only
 * way to add anything.
 *
 * Stroked SVG on currentColor rather than glyphs: the two icons have to read as one set at 16px,
 * which a "+" character and any document glyph never will — they come from different fonts' idea of
 * weight and baseline.
 */
export function CanvasToolbar() {
    return (
        <div class="graph-toolbar">
            <button class="graph-tool" onClick={() => addDynamicNode()} title="Add a dynamic node" aria-label="Add a dynamic node">
                <svg viewBox="0 0 16 16" width="16" height="16" aria-hidden="true">
                    <circle cx="8" cy="8" r="6.25" />
                    <path d="M8 5.25v5.5M5.25 8h5.5" />
                </svg>
            </button>
            <button class="graph-tool" onClick={() => addApi()} title="Add an API" aria-label="Add an API">
                <svg viewBox="0 0 16 16" width="16" height="16" aria-hidden="true">
                    <path d="M9.25 1.75H4.75a1 1 0 0 0-1 1v10.5a1 1 0 0 0 1 1h6.5a1 1 0 0 0 1-1V4.75z" />
                    <path d="M9.25 1.75v3h3" />
                    <path d="M6 8.25h4M6 10.75h2.75" />
                </svg>
            </button>
        </div>
    );
}
