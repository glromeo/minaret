/**
 * The small helpers the components share, held here rather than duplicated per component.
 *
 * Deliberately not in `graph/`: that directory is the DOM-free bus state, driveable headlessly, and
 * these are presentation-side conveniences with no bus meaning of their own.
 */

/**
 * A rejection rendered for display. Every async surface in this app lands in a `.catch` with an
 * `unknown`, and a bare `String(failure)` on an Error yields "Error: …" while `.message` on a
 * non-Error is `undefined` — so both halves are needed every time, which is why this exists at all.
 */
export function errorText(failure: unknown): string {
    return failure instanceof Error ? failure.message : String(failure);
}

/**
 * One row of a component's rolling text log — `NodeApp`'s per-channel log and `ExternalNodeApp`'s.
 *
 * Distinct from `graph/store.ts`'s `LogRow`, which is the Log Viewer's model of a real event-stream
 * occurrence (severity, the RouteId a click selects). These rows are local echoes of what one window
 * sent and received, with nothing behind them on the wire — merging the two would give every such
 * echo a severity and a routeId that mean nothing.
 */
export interface LogRow {
    id: string;
    text: string;
    time: number;
}

const LOG_LIMIT = 50;

let logSeq = 0;

/**
 * Prepends text to rows, newest first, capped at LOG_LIMIT. Returns a new array rather than mutating,
 * since these live in signals and a signal only notifies on assignment.
 *
 * The id is a monotonic sequence rather than the `Date.now()`-plus-random it replaces: two rows
 * pushed in the same millisecond were relying on the random half for their keys, which is a
 * collision waiting to reorder a keyed list.
 */
export function pushLog(rows: LogRow[], text: string): LogRow[] {
    return [{ id: `log-${logSeq++}`, text, time: Date.now() }, ...rows].slice(0, LOG_LIMIT);
}
