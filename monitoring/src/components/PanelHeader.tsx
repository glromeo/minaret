import type { Signal } from "@preact/signals";
import type { ComponentChildren } from "preact";

/**
 * The strip every docked panel wears: its name, a filter field, and whatever controls that panel
 * needs beside them.
 *
 * One component rather than three near-identical headers because the panels are read as a set — an
 * operator scanning for a name should not have to learn where each one keeps its search box, or
 * whether it has one. What each panel *does* with the query is its own business, and they differ:
 * `RepositoryTree` matches documents and channel names, `LogViewer` matches row text, `Inspector`
 * matches the detail cards of whatever is selected. So the query signal is owned by the caller and
 * this only writes to it.
 *
 * `children` is the trailing slot, currently used by `LogViewer` for its resume-auto-scroll button.
 * It sits after the field rather than before the title so a panel that grows a second control does
 * not shove the search box around.
 *
 * `icon` is the paths only, not a whole `<svg>` — the wrapper's `viewBox` and sizing belong to this
 * component, since the icon has to keep reading as one set with the other two headers' at whatever
 * size this decides on. Each panel passes the same glyph its `TopNav` toggle uses, so the button
 * that summons a panel and the panel itself name it identically — which is also what lets the label
 * beside it truncate in a narrow dock without the panel becoming anonymous.
 */
export function PanelHeader({
    title,
    icon,
    query,
    placeholder,
    children
}: {
    title: string;
    icon: ComponentChildren;
    query: Signal<string>;
    placeholder?: string;
    children?: ComponentChildren;
}) {
    return (
        <div class="panel-header">
            <span class="panel-title">
                <svg viewBox="0 0 16 16" width="20" height="20" aria-hidden="true">{icon}</svg>
                <span class="panel-title-label">{title}</span>
            </span>

            <div class="panel-search">
                <svg viewBox="0 0 16 16" width="16" height="16" aria-hidden="true">
                    <circle cx="7" cy="7" r="4.25" />
                    <path d="m10.25 10.25 3 3" />
                </svg>

                <input
                    type="search"
                    value={query.value}
                    placeholder={placeholder ?? "filter"}
                    spellcheck={false}
                    autocomplete="off"
                    aria-label={`Filter ${title}`}
                    onInput={event => (query.value = (event.currentTarget as HTMLInputElement).value)}
                />

                {/* Rendered only when there is something to clear: an always-present × on a field
                    this narrow reads as part of the border and costs the placeholder its room. */}
                {query.value !== "" && (
                    <button class="panel-search-clear" onClick={() => (query.value = "")} title="Clear filter" aria-label="Clear filter">
                        <svg viewBox="0 0 16 16" width="11" height="11" aria-hidden="true">
                            <path d="m4.5 4.5 7 7M11.5 4.5l-7 7" />
                        </svg>
                    </button>
                )}
            </div>

            {children}
        </div>
    );
}

/** Case-insensitive substring match over several fields at once, skipped entirely on an empty
 *  query — every panel's filter is the same question asked of different text. */
export function matches(query: string, ...fields: (string | null | undefined)[]): boolean {
    if (query === "") return true;

    const needle = query.toLowerCase();
    return fields.some(field => field != null && field.toLowerCase().includes(needle));
}
