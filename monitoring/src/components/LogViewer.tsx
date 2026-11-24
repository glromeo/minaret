import { signal } from "@preact/signals";
import { useRef, useState } from "preact/hooks";
import { log, selectedRouteId } from "../graph/store";
import { matches, PanelHeader } from "./PanelHeader";

/** Module-level, so a filter survives the panel being toggled away and back. */
const query = signal("");

/**
 * The live event log: one row per real client/channel/topic occurrence (see store.ts's `pushLog`
 * call sites) rather than fabricated per-message traffic, since the wire protocol carries no
 * per-message telemetry to log. Rows are newest-first (log.value is unshifted), so "auto-scroll"
 * here means staying pinned to the top — pausing that pin is what a manual scroll down means.
 */
export function LogViewer() {
    // Filtered on text and severity, so "error" narrows to the severity column as readily as to a
    // message that happens to contain the word — both are what someone typing it is after.
    const rows = log.value.filter(row => matches(query.value, row.text, row.severity));
    const containerRef = useRef<HTMLDivElement>(null);
    const [pinned, setPinned] = useState(true);

    function onScroll() {
        const el = containerRef.current;
        if (!el) return;
        setPinned(el.scrollTop < 8);
    }

    return (
        <div class="log-viewer">
            <PanelHeader
                title="Event log"
                icon={<path d="M2.5 4.25h3M7.5 4.25h6M2.5 8h3M7.5 8h6M2.5 11.75h3M7.5 11.75h3.5" />}
                query={query}
                placeholder="filter events"
            >
                {/* Icon-only in the shared header: the words "resume auto-scroll" do not fit beside
                    a search field at this dock width, and the control only appears once the operator
                    has scrolled away, which is its own explanation. */}
                {!pinned && (
                    <button
                        class="log-resume"
                        title="Resume auto-scroll"
                        aria-label="Resume auto-scroll"
                        onClick={() => {
                            containerRef.current?.scrollTo({ top: 0, behavior: "smooth" });
                            setPinned(true);
                        }}
                    >
                        <svg viewBox="0 0 16 16" width="13" height="13" aria-hidden="true">
                            <path d="M8 12.75v-9M4.25 7.5 8 3.75l3.75 3.75" />
                        </svg>
                    </button>
                )}
            </PanelHeader>
            <div class="log-rows" ref={containerRef} onScroll={onScroll}>
                {rows.map(row => (
                    <div
                        key={row.id}
                        class={`log-row log-row--${row.severity}`}
                        onClick={() => row.routeId && (selectedRouteId.value = row.routeId)}
                    >
                        <div class="log-time">{formatTime(row.time)}</div>
                        <div class="log-text">{row.text}</div>
                        <div class="log-severity">{row.severity.toUpperCase()}</div>
                    </div>
                ))}
                {rows.length === 0 && (
                    <div class="log-empty">{query.value === "" ? "no events yet" : "nothing matches"}</div>
                )}
            </div>
        </div>
    );
}

function formatTime(ms: number): string {
    const date = new Date(ms);
    const pad = (n: number) => n.toString().padStart(2, "0");
    return `${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
}
