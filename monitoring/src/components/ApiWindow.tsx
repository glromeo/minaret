import { json, jsonParseLinter } from "@codemirror/lang-json";
import { linter } from "@codemirror/lint";
import { oneDark } from "@codemirror/theme-one-dark";
import { keymap, tooltips } from "@codemirror/view";
import { basicSetup, EditorView } from "codemirror";
import { useEffect, useRef, useState } from "preact/hooks";
import type { ApiDoc } from "../graph/apis";
import {
    bringApiToFront,
    closeApiWindow,
    deleteApi,
    isDirty,
    moveApi,
    renameApi,
    resizeApi,
    saveApi,
    toggleMaximizeApi,
    updateApiSource
} from "../graph/apis";
import { idlCompletions } from "../idl/completion";
import { jsonEditing } from "../idl/editing";
import { isValidName } from "../idl/repository";
import { FloatingWindow } from "./FloatingWindow";

/**
 * An API editor's floating window: the same chrome a dynamic node wears (`FloatingWindow`) around a
 * CodeMirror editor over a `.idl` document (`src/idl/`), plus the strip that names it and stores it.
 *
 * **In this document, not an iframe** — the one difference from `DynamicNode` worth stating, since
 * the two look alike. A node's iframe buys DOM/CSS isolation for a *bus participant* whose UI is
 * effectively a separate app; an API editor is a text buffer, and putting it behind a document
 * boundary would mean re-mounting CodeMirror's style modules and re-plumbing clipboard/focus into
 * that document for nothing in return.
 *
 * The editor is **uncontrolled**: CodeMirror owns the document, and `updateApiSource` mirrors edits
 * out to the store. The store's `source` is never pushed back in — doing so on every keystroke
 * would rewrite the document under the cursor and flatten the undo history. So the view is created
 * once, from `api.source` as it stood at mount, and lives until the window is discarded.
 *
 * The toolbar is where the *repository* shows through (`src/idl/repository.ts`), and its two
 * asymmetries are deliberate. Rename is local and immediate, since it only changes what the next
 * save is called. Delete is two-click, since it is the only control in the app that destroys
 * something the Broker is holding — the window controls above it all merely hide, by the same
 * reasoning `DynamicNode` records.
 */
export function ApiWindow({ api }: { api: ApiDoc }) {
    const hostRef = useRef<HTMLDivElement>(null);
    const viewRef = useRef<EditorView | null>(null);
    const [confirmingDelete, setConfirmingDelete] = useState(false);

    useEffect(() => {
        const parent = hostRef.current;
        if (!parent) return;

        const view = new EditorView({
            parent,
            doc: api.source,
            extensions: [
                basicSetup,
                json(),
                // Schema-driven, so the editor's vocabulary and the artifact's definition are the
                // same file (see `src/idl/`).
                idlCompletions(),
                // Tab/Shift-Tab motion between a property's two sides, and `"` inside a string. Both
                // outrank `basicSetup`'s own handling of those keys — see `jsonEditing`.
                jsonEditing(),
                // The one keystroke that leaves the editor: Mod-s stores the document. Bound here
                // rather than in `jsonEditing` because it is the only binding that needs to know
                // *which* document this view is showing.
                keymap.of([{ key: "Mod-s", preventDefault: true, run: () => (void saveApi(api.id), true) }]),
                // `.node-window` clips to its rounded corners (`overflow: hidden`), and CodeMirror
                // renders tooltips inside the editor by default — so the completion list would be
                // cut off at the window edge, worst exactly where it is most needed: a small window,
                // near the bottom. Hoisting them to the body escapes the clip; CodeMirror keeps
                // them positioned against the editor either way.
                tooltips({ parent: document.body }),
                // A malformed document is worth marking directly: the completion source walks the
                // syntax tree, so where the JSON doesn't parse it silently offers nothing, and
                // without this the editor would look like it had simply stopped knowing the schema.
                linter(jsonParseLinter()),
                oneDark,
                EditorView.updateListener.of(update => {
                    if (update.docChanged) updateApiSource(api.id, update.state.doc.toString());
                })
            ]
        });

        viewRef.current = view;
        return () => {
            view.destroy();
            viewRef.current = null;
        };
    }, [api.id]);

    // A hidden window is `display: none`, where every measurement CodeMirror can take is zero — so
    // the viewport it computed while hidden describes nothing, and a reopened window would paint an
    // empty or mis-scrolled gutter until the next keystroke forced a remeasure. Asking for one as
    // the window comes back (and after a resize drag, for the same reason at a smaller scale) is
    // what makes it correct on the first frame it is visible.
    useEffect(() => {
        if (api.windowOpen) viewRef.current?.requestMeasure();
    }, [api.windowOpen, api.width, api.height, api.maximized]);

    const dirty = isDirty(api);
    const nameValid = isValidName(api.name);

    // Armed rather than confirmed, and it disarms itself: a Delete left armed by a misclick would
    // sit there until the next click on it, which is exactly the click that would destroy the
    // document.
    function onDelete() {
        if (!confirmingDelete) {
            setConfirmingDelete(true);
            setTimeout(() => setConfirmingDelete(false), 3000);
            return;
        }

        setConfirmingDelete(false);
        void deleteApi(api.id);
    }

    return (
        <FloatingWindow
            window={api}
            title={`${api.name}.idl${dirty ? " •" : ""}`}
            class="api-window"
            onFocus={() => bringApiToFront(api.id)}
            onMove={(x, y) => moveApi(api.id, x, y)}
            onResize={(width, height) => resizeApi(api.id, width, height)}
            onToggleMaximize={() => toggleMaximizeApi(api.id)}
            onClose={() => closeApiWindow(api.id)}
        >
            <div class="api-window-toolbar">
                <input
                    class={`api-window-name${nameValid ? "" : " api-window-name--invalid"}`}
                    value={api.name}
                    spellcheck={false}
                    autocomplete="off"
                    aria-label="document name"
                    onInput={event => renameApi(api.id, (event.currentTarget as HTMLInputElement).value)}
                />
                <span class="api-window-ext">.idl</span>

                <button class="api-window-action" disabled={api.saving || !dirty || !nameValid} onClick={() => void saveApi(api.id)}>
                    {api.saving ? "Saving" : "Save"}
                </button>
                <button
                    class={`api-window-action${confirmingDelete ? " api-window-action--danger" : ""}`}
                    disabled={api.saving}
                    onClick={onDelete}
                >
                    {confirmingDelete ? "Confirm" : "Delete"}
                </button>

                <span class={`api-window-status${api.error ? " api-window-status--error" : ""}`} title={api.error ?? undefined}>
                    {api.error ?? (dirty ? "unsaved" : "stored")}
                </span>
            </div>

            <div class="api-window-editor" ref={hostRef} />
        </FloatingWindow>
    );
}
