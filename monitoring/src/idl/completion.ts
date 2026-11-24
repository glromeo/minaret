import { type Completion, type CompletionContext, type CompletionResult, startCompletion } from "@codemirror/autocomplete";
import { jsonLanguage } from "@codemirror/lang-json";
import type { Extension } from "@codemirror/state";
import type { EditorView } from "@codemirror/view";
import type { SyntaxNode } from "@lezer/common";
import { type Cursor, declarationsFor, deref, locate, recordOf, ruleAt, stringOnly, writtenKeys } from "./document";
import type { Field } from "./grammar";
import { contextAt as typeContextAt, TYPE_CANDIDATES, TYPE_LINK_SCOPES } from "./typeExpression";

/**
 * Autocomplete for `.idl` documents, driven by `idl.grammar`.
 *
 * The shape of it: find where the cursor is in the JSON tree, walk the grammar down that path, offer
 * what the rule there allows (`document.ts` does both). Position alone cannot answer it — `name` is
 * a field of both `channel` and `topic` and means something different in each — so the path is the
 * query, not the prefix.
 *
 * What the grammar buys over the JSON Schema this replaced is two things a shape language could not
 * express:
 *
 * - **Names that resolve.** A `link` rule completes to what the document actually declares, so
 *   `$extends` and every type reference offer the channel's own types.
 * - **Inside a string.** A type expression is a language of its own (`typeExpression.ts`), so the
 *   cursor in `array<Or` is at a *type position* and completes accordingly, where a pattern could
 *   only ever have matched the whole string or not.
 *
 * Free-form levels contribute no key completions, which is correct: the keys of `types`, `topics`
 * and `inbound` are the author's vocabulary. Their values still complete, because those are rules.
 */
export function idlCompletions(): Extension {
    return jsonLanguage.data.of({ autocomplete: completeIdl });
}

function completeIdl(context: CompletionContext): CompletionResult | null {
    const where = locate(context.state, context.pos);
    if (!where) return null;

    return where.kind === "key" ? completeKey(context, where) : completeValue(context, where);
}

function completeKey(context: CompletionContext, where: Cursor): CompletionResult | null {
    const record = recordOf(ruleAt(where.path));
    if (!record) return null;

    const token = context.matchBefore(/"[^"]*|[\w$]+/);
    if (!token && !context.explicit) return null;

    const from = token?.from ?? context.pos;
    // A completion triggered inside `"cha|"` has to swallow the closing quote too, or the applied
    // text lands next to an orphan one. Nothing else may be eaten: the range stops at that quote.
    const to = context.pos + (context.state.sliceDoc(context.pos, context.pos + 1) === '"' ? 1 : 0);

    const taken = writtenKeys(context.state, to);
    // `"name"` alone where a colon already follows, `"name": ` where the property is being written
    // from scratch — inserting a second colon into an existing property is the one way this could
    // corrupt the document rather than merely annoy.
    const trailing = /^\s*:/.test(context.state.sliceDoc(to, to + 8)) ? "" : ": ";

    const options: Completion[] = record.fields
        .filter(field => !taken.has(field.name))
        .map(field => ({
            label: field.name,
            ...applyKey(field, trailing),
            type: "property",
            boost: field.optional ? 0 : 1,
            ...(field.optional ? {} : { detail: "required" }),
            ...(field.doc ? { info: field.doc } : {})
        }));

    return options.length ? { from, to, options } : null;
}

/**
 * What choosing a key writes, and where it leaves the caret.
 *
 * A field whose every branch is a string has its quotes written too, with the caret between them: the
 * value cannot start any other way, so typing the quote yourself carries no decision — and being
 * *inside* a string is what makes the value's own completions apply, which is why the list is asked
 * to open again rather than stopping at the colon.
 */
function applyKey(field: Field, trailing: string): Pick<Completion, "apply"> {
    const head = `"${field.name}"${trailing}`;
    // No trailing colon means the property already has one, and so may already have a value.
    if (!trailing || !stringOnly(field.rule)) return { apply: head };

    return {
        apply: (view: EditorView, _completion: Completion, from: number, to: number) => {
            view.dispatch({ changes: { from, to, insert: `${head}""` }, selection: { anchor: from + head.length + 1 } });
            startCompletion(view);
        }
    };
}

function completeValue(context: CompletionContext, where: Cursor): CompletionResult | null {
    const rule = ruleAt(where.path);
    if (!rule) return null;

    const branches = rule.kind === "choice" ? rule.options.map(deref) : [rule];
    const string = where.node.name === "String" ? where.node : null;

    // Inside the quotes a completion inserts bare text; outside them it writes its own, since the
    // value is being started from a position where no string exists yet.
    const quote = string ? "" : '"';
    const options: Completion[] = [];

    for (const branch of branches) {
        if (!branch) continue;

        if (branch.kind === "null" && !string) {
            options.push({ label: "null", apply: "null", type: "keyword", detail: "fire-and-forget", boost: 2 });
        } else if (branch.kind === "link") {
            options.push(...linkOptions(context, where, branch.scope, quote));
        } else if (branch.kind === "lexical") {
            options.push(...typeOptions(context, where, string, quote));
        }
    }

    if (!options.length) return null;

    // Inside a string the replaced range is the partial word only — a nested position like
    // `array<Or|` must not have its `array<` prefix swallowed by the completion.
    const token = string ? context.matchBefore(/[\w$]+/) : context.matchBefore(/"[^"]*|[\w$]+/);
    if (!token && !context.explicit) return null;

    const from = token?.from ?? context.pos;
    const closing = !string && context.state.sliceDoc(context.pos, context.pos + 1) === '"' ? 1 : 0;

    return { from, to: context.pos + closing, options };
}

/** What a `link` resolves to, as completions: the names the document declares in that scope. */
function linkOptions(context: CompletionContext, where: Cursor, scope: string, quote: string): Completion[] {
    return declarationsFor(context.state, where.node, scope).map(name => ({
        label: name,
        apply: `${quote}${name}${quote}`,
        type: "class",
        detail: scope
    }));
}

/**
 * The type-expression completions, placed by where the cursor is *within* the string — so a bare
 * slot offers every form, a nested one offers them again inside the brackets, and an enum's member
 * list offers nothing, those names being the author's.
 */
function typeOptions(context: CompletionContext, where: Cursor, string: SyntaxNode | null, quote: string): Completion[] {
    // The String node spans its quotes; an unterminated one at end of line may have only the opener.
    const text = string ? context.state.sliceDoc(string.from + 1, string.to).replace(/"$/, "") : "";
    const offset = string ? context.pos - (string.from + 1) : 0;

    const inner = typeContextAt(text, offset);
    if (!inner || inner.kind === "member") return [];

    const forms: Completion[] = TYPE_CANDIDATES.map(candidate => ({
        label: candidate.label,
        // A function rather than a string so the caret can land in the form's first hole; a plain
        // `apply` string always leaves it at the end, which for `array<>` is outside the brackets.
        apply: (view: EditorView, _completion: Completion, from: number, to: number) =>
            view.dispatch({
                changes: { from, to, insert: `${quote}${candidate.insert}${quote}` },
                selection: { anchor: from + quote.length + candidate.caret }
            }),
        type: "type",
        detail: candidate.rule,
        ...(candidate.doc ? { info: candidate.doc } : {})
    }));

    // A type may also *be* a declared name — the `reference` alternative of the rule.
    return [...forms, ...TYPE_LINK_SCOPES.flatMap(scope => linkOptions(context, where, scope, quote))];
}
