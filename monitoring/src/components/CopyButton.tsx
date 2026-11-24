import { useSignal } from "@preact/signals";
import { useEffect } from "preact/hooks";

/**
 * A copy-to-clipboard button that swaps its icon for a check mark for a moment on success.
 *
 * Rendered into both the host document (`Inspector`) and a dynamic node's iframe (`NodeApp`), whose
 * stylesheets are deliberately separate (see `DynamicNode.tsx`). It therefore emits fixed class
 * names that each scope styles in its own palette — `styles.css` for the host, `NODE_APP_CSS` for
 * the iframe — and draws its icon as inline SVG rather than in CSS, so only the button chrome has
 * to be written twice instead of the glyph as well.
 *
 * The clipboard write goes through the *host* window's `navigator` in both cases: `NodeApp` is
 * host-realm code that merely renders into the iframe's document, so this never depends on the
 * iframe being granted the `clipboard-write` permission policy.
 */
export function CopyButton({ value, label = "copy" }: { value: string; label?: string }) {
    const copied = useSignal(false);

    // The check mark reverts on a timer, so a component torn down mid-flash — the node window
    // closed, the inspector's selection changed — has to cancel it rather than leave a callback
    // writing to the signal of an unmounted tree.
    useEffect(() => {
        if (!copied.value) return;
        const timer = setTimeout(() => (copied.value = false), 1200);
        return () => clearTimeout(timer);
    }, [copied.value]);

    async function copy(event: MouseEvent) {
        // Both call sites sit inside a parent that treats a click as select/expand.
        event.stopPropagation();
        try {
            await navigator.clipboard.writeText(value);
            copied.value = true;
        } catch {
            // Clipboard unavailable or denied — leave the icon unflipped rather than confirming a
            // copy that never happened.
        }
    }

    return (
        <button
            class={`copy-btn${copied.value ? " copy-btn--copied" : ""}`}
            onClick={copy}
            aria-label={copied.value ? "copied" : label}
            title={copied.value ? "copied" : label}
        >
            <svg viewBox="0 0 16 16" width="12" height="12" aria-hidden="true">
                {copied.value ? (
                    <path d="M3 8.5 6 11.5 13 4.5" />
                ) : (
                    <>
                        <rect x="5.75" y="5.75" width="8.5" height="8.5" rx="1.5" />
                        <path d="M10.25 4V3a1.25 1.25 0 0 0-1.25-1.25H3A1.25 1.25 0 0 0 1.75 3v6A1.25 1.25 0 0 0 3 10.25h1" />
                    </>
                )}
            </svg>
        </button>
    );
}