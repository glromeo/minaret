import { signal } from "@preact/signals";

/**
 * Which of the four surfaces are on screen. They are independent flags, not a selection: every one
 * of them is a *pane* that shows or hides in place, so all four on is the default and all four off
 * is a legal (empty) canvas.
 *
 * `App` reads this to decide what to render, and the four toggles are the only writers. It is a
 * signal rather than component state for the ordinary reason (see `monitoring/CLAUDE.md`) plus one
 * local to it: the toggles live in the header and the panes they govern live in the canvas row, so a
 * hook would have to lift the state into their nearest common ancestor and thread a setter back down.
 */
export const panes = signal<Record<Pane, boolean>>({
    bus: true,
    inspector: true,
    log: true,
    repository: true
});

export type Pane = "bus" | "inspector" | "log" | "repository";

export function togglePane(pane: Pane): void {
    panes.value = { ...panes.value, [pane]: !panes.value[pane] };
}

/**
 * The header's view toggles: one segmented group, four panes.
 *
 * Grouped in a single inset well rather than sat as four loose buttons because the set has to read
 * as one control next to the wordmark — and because an active pane is filled with `--accent`, which
 * is the brightest thing in the header and would otherwise compete with the LIVE pill for the eye.
 *
 * Icons are stroked SVG on `currentColor`, the same choice `BusGraph`'s toolbar documents: the four
 * have to read as one set at 16px, which glyphs from a font never will, and it is what lets the
 * three states below colour icon and label together with one `color` rule.
 */
export function TopNav() {
    return (
        <nav class="nav-toggles" aria-label="Views">
            <Toggle pane="bus" label="Message bus">
                <circle cx="8" cy="8" r="1.9" />
                <circle cx="8" cy="2.4" r="1.5" />
                <circle cx="3.1" cy="11.4" r="1.5" />
                <circle cx="12.9" cy="11.4" r="1.5" />
                <path d="M8 3.9v2.2M6.6 9.2 4.3 10.6M9.4 9.2l2.3 1.4" />
            </Toggle>

            <Toggle pane="inspector" label="Inspector">
                <circle cx="7" cy="7" r="4.5" />
                <path d="m10.4 10.4 3.1 3.1" />
            </Toggle>

            <Toggle pane="log" label="Event log">
                <path d="M2.5 4.25h3M7.5 4.25h6M2.5 8h3M7.5 8h6M2.5 11.75h3M7.5 11.75h3.5" />
            </Toggle>

            <Toggle pane="repository" label="Repository">
                <path d="M2.5 2.75h11v2.75h-11z" />
                <path d="M3.5 5.5h9v7.75h-9z" />
                <path d="M6.5 8.25h3" />
            </Toggle>
        </nav>
    );
}

/**
 * One toggle. `aria-pressed` rather than `aria-selected` or a `role="tab"`, since these are four
 * independent switches — a tab list would promise the arrow-key navigation and the one-of-four
 * exclusivity that neither the markup nor `togglePane` has.
 *
 * The label is always in the DOM and dropped by a media query rather than by a prop, so a window
 * narrow enough to hide it still has the accessible name and the tooltip.
 */
function Toggle({ pane, label, children }: { pane: Pane; label: string; children: preact.ComponentChildren }) {
    const on = panes.value[pane];

    return (
        <button
            class={`nav-toggle${on ? " nav-toggle--on" : ""}`}
            aria-pressed={on}
            title={label}
            onClick={() => togglePane(pane)}
        >
            <svg viewBox="0 0 16 16" width="16" height="16" aria-hidden="true">{children}</svg>
            <span class="nav-toggle-label">{label}</span>
        </button>
    );
}
