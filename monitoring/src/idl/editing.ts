import { acceptCompletion } from "@codemirror/autocomplete";
import { syntaxTree } from "@codemirror/language";
import { type EditorState, type Extension, Prec } from "@codemirror/state";
import { EditorView, keymap } from "@codemirror/view";
import type { SyntaxNode, TreeCursor } from "@lezer/common";
import { VALUE_NODES } from "./document";

/**
 * Keyboard motion over a JSON document — the half of the editor that is about the shape of the text
 * rather than about the grammar behind it (`completion.ts` is the other).
 *
 * A **slot** is one side of a property, its name or its value, plus each element of an array, taken
 * in document order. That is the whole model, and both keys fall out of it: Tab is the next slot,
 * which from a name is its own value and from a value is the next property's name; Shift-Tab is the
 * previous one, which from a value is its name. So the two read as "the right-hand side" and "the
 * left-hand side" from wherever a property is entered, without either key having to know which side
 * it started on.
 *
 * Landing on a slot **selects what is editable about it** rather than parking a cursor at its edge:
 * the text between a string's quotes, the whole token of a number or a keyword, and — for an object
 * or an array, which has no text of its own — a cursor inside the brackets, ready for a first entry.
 * Typing then replaces the value, which is what a tabbed-to field is for.
 *
 * `"` is bound for the case that motion creates: with a string's text selected, a bracket-closer
 * reads the key as "quote this" and wraps the value in a second pair. The quotes are already there,
 * so the only thing left for the key to mean is the position they open.
 */
export function jsonEditing(): Extension {
    // Highest precedence for both halves: `closeBrackets` (from `basicSetup`) claims `"` through an
    // input handler of its own, and would have wrapped the selection before this one is asked.
    return Prec.highest([
        keymap.of([
            // A completion list open under the cursor is what Tab means in most editors, and moving
            // out from under one would be the surprising reading. `acceptCompletion` declines when no
            // list is open, which is what leaves Tab to the motion below the rest of the time.
            { key: "Tab", run: acceptCompletion },
            { key: "Tab", run: view => moveToSlot(view, true) },
            { key: "Shift-Tab", run: view => moveToSlot(view, false) }
        ]),
        EditorView.inputHandler.of(handleQuote)
    ]);
}

/** Everything that can stand as a value, so everything a slot can be besides a property's name. */
const SLOT_VALUES = new Set([...VALUE_NODES, "Object", "Array"]);

function moveToSlot(view: EditorView, forward: boolean): boolean {
    const main = view.state.selection.main;
    const slot = forward ? nextSlot(view.state, main.to) : previousSlot(view.state, main.from);

    // Nothing further to reach: Tab falls back to its browser meaning and moves focus out of the
    // editor, which is a keyboard user's only way out of it.
    if (!slot) return false;

    view.dispatch({ selection: contentOf(slot, view.state), scrollIntoView: true, userEvent: "select" });
    return true;
}

/**
 * The two directions, walked with the same pre-order `next` — which visits nodes in order of where
 * they begin, so it *is* document order, and a walk from `pos` reaches everything still ahead.
 *
 * The reverse is not symmetrical, and that is why it is a second function rather than a flag. A
 * cursor's `prev` is not the mirror of `next`: it takes siblings from last to first but still opens a
 * parent before its children, so the first slot it reaches going back from inside an object is that
 * object rather than the entry just before the cursor. The last match of a forward walk is the honest
 * answer, and it costs nothing to find: `from` only grows, so the walk stops where the cursor is.
 */
function nextSlot(state: EditorState, pos: number): SyntaxNode | null {
    const cursor = syntaxTree(state).cursorAt(pos, 1);

    do {
        const slot = cursor.from >= pos ? slotAt(cursor) : null;
        if (slot) return slot;
    } while (cursor.next());

    return null;
}

function previousSlot(state: EditorState, pos: number): SyntaxNode | null {
    const cursor = syntaxTree(state).cursor();
    let last: SyntaxNode | null = null;

    do {
        if (cursor.from >= pos) break;
        if (cursor.to <= pos) last = slotAt(cursor) ?? last;
    } while (cursor.next());

    return last;
}

/**
 * The slot a cursor is on, if it is on one. A value is told by its parent, not by its own kind — the
 * same `Object` node is a slot as a property's value and not as the document's root.
 */
function slotAt(cursor: TreeCursor): SyntaxNode | null {
    if (cursor.name === "PropertyName") return cursor.node;
    if (!SLOT_VALUES.has(cursor.name)) return null;

    const parent = cursor.node.parent;
    return parent?.name === "Property" || parent?.name === "Array" ? cursor.node : null;
}

/** What landing on a slot selects: a string's text, a token whole, or a cursor inside a container. */
function contentOf(node: SyntaxNode, state: EditorState): { anchor: number; head: number } {
    if (node.name === "Object" || node.name === "Array") return { anchor: node.from + 1, head: node.from + 1 };

    if (node.name === "String" || node.name === "PropertyName") {
        // A string still being typed may carry only its opening quote, its node ending at the text —
        // so the closer is stepped over only where there is one to step over.
        const closed = node.to - node.from > 1 && state.doc.sliceString(node.to - 1, node.to) === '"';
        return { anchor: node.from + 1, head: closed ? node.to - 1 : node.to };
    }

    return { anchor: node.from, head: node.to };
}

/** `"` inside a string is a request to get to the front of it, not to open another one. */
function handleQuote(view: EditorView, from: number, to: number, text: string): boolean {
    if (text !== '"') return false;

    const string = stringAround(view.state, from, to);
    if (!string) return false;

    view.dispatch({ selection: { anchor: string.from + 1 }, scrollIntoView: true, userEvent: "select" });
    return true;
}

/**
 * The string node containing a range, where "containing" excludes its own opening edge: at a position
 * where no string has been opened yet the key keeps its ordinary meaning and writes one.
 */
function stringAround(state: EditorState, from: number, to: number): SyntaxNode | null {
    let node: SyntaxNode | null = syntaxTree(state).resolveInner(from, 1);
    while (node && node.name !== "String" && node.name !== "PropertyName") node = node.parent;

    return node && node.from < from && to <= node.to ? node : null;
}
