import { syntaxTree } from "@codemirror/language";
import type { EditorState, Text } from "@codemirror/state";
import type { SyntaxNode } from "@lezer/common";
import { grammar, type Rule } from "./grammar";

/**
 * Walking a `.idl` document against `idl.grammar` — the half of completion that is about *this*
 * document rather than about CodeMirror.
 *
 * Kept apart from `completion.ts` because the two answer different questions and only one of them
 * needs an editor: this module turns a position in the JSON tree into a grammar rule and a set of
 * declared names, which is exactly what a validator will want too, and what a test can drive
 * without a view.
 */

/** Stands in for an array index in a path — a list's rule is the same for every element. */
export const ARRAY_ITEM = Symbol("array item");
export type PathSegment = string | typeof ARRAY_ITEM;

/** Lezer-json's leaf value nodes — everything scalar that can sit to the right of a colon. */
export const VALUE_NODES = new Set(["String", "Number", "True", "False", "Null"]);

export interface Cursor {
    kind: "key" | "value";
    path: PathSegment[];
    /** The innermost node, kept so a value completion can tell a string from a bare position. */
    node: SyntaxNode;
}

/**
 * Where the cursor sits, in grammar terms. Resolves with `-1` bias so a cursor at the end of a
 * half-typed name lands *inside* that name's node rather than after it.
 */
export function locate(state: EditorState, pos: number): Cursor | null {
    const doc = state.doc;
    let node: SyntaxNode = syntaxTree(state).resolveInner(pos, -1);

    // A bare word (no quotes yet) is a parse error, and the error node carries no path of its own —
    // whatever encloses it does. This is the common case while typing, not an edge case.
    if (node.type.isError && node.parent) node = node.parent;

    // A property name's own path ends with the name being typed, which is the thing being asked
    // about; the question is what may appear in the object holding it.
    if (node.name === "PropertyName") return { kind: "key", path: pathTo(node, doc).slice(0, -1), node };
    if (node.name === "Object") return { kind: "key", path: pathTo(node, doc), node };

    // Landing on the Property itself means the cursor is between the colon and any value.
    if (node.name === "Property") return { kind: "value", path: pathTo(node, doc), node };
    if (VALUE_NODES.has(node.name) && (node.parent?.name === "Property" || node.parent?.name === "Array")) {
        return { kind: "value", path: pathTo(node, doc), node };
    }

    return null;
}

/**
 * The path of property names from the document root down to `node`, with `ARRAY_ITEM` for each array
 * crossed — climbing, since a node knows its parent and not its address.
 */
export function pathTo(node: SyntaxNode, doc: Text): PathSegment[] {
    const path: PathSegment[] = [];

    for (let current: SyntaxNode | null = node; current; current = current.parent) {
        if (current.name === "Array") {
            path.unshift(ARRAY_ITEM);
        } else if (current.name === "Property") {
            const name = current.getChild("PropertyName");
            if (name) path.unshift(unquote(doc.sliceString(name.from, name.to)));
        }
    }

    return path;
}

export function unquote(text: string): string {
    return text.replace(/^"|"$/g, "");
}

/** Follows a document-rule reference to the rule it names. */
export function deref(rule: Rule | undefined): Rule | undefined {
    return rule?.kind === "ref" ? deref(grammar.rules.get(rule.name)?.rule) : rule;
}

/**
 * The rule governing the value at `path`, or undefined where the path leaves the grammar.
 *
 * A `choice` is discriminated by JSON kind, so descending through one means picking the branch that
 * *has* the shape the next step needs — an array segment takes the list branch, a key takes the
 * record or map branch. At the end of the path the whole choice is returned, since the caller may
 * legitimately want all of it (a `type | null` offers both).
 */
export function ruleAt(path: readonly PathSegment[]): Rule | undefined {
    let rule = deref(grammar.rules.get(grammar.start)?.rule);

    for (const segment of path) {
        if (rule?.kind === "choice") rule = deref(rule.options.find(option => accepts(option, segment)));
        if (!rule) return undefined;

        if (segment === ARRAY_ITEM) {
            rule = rule.kind === "list" ? deref(rule.of) : undefined;
        } else if (rule.kind === "map") {
            rule = deref(rule.entries.of);
        } else if (rule.kind === "record") {
            const field = rule.fields.find(candidate => candidate.name === segment);
            rule = deref(field ? field.rule : rule.rest?.of);
        } else {
            return undefined;
        }
    }

    return rule;
}

/** Whether a choice branch can take the given step — the JSON-kind discrimination, applied. */
function accepts(rule: Rule, segment: PathSegment): boolean {
    const resolved = deref(rule);
    if (!resolved) return false;

    return segment === ARRAY_ITEM ? resolved.kind === "list" : resolved.kind === "record" || resolved.kind === "map";
}

/**
 * Whether every value a rule admits is a JSON string, so a completion writing the key may write the
 * quotes too.
 *
 * A choice qualifies only if all of its branches do, which is what keeps `type | null` out: `null` is
 * a value the author may well mean, and pre-written quotes would have committed against it.
 */
export function stringOnly(rule: Rule | undefined): boolean {
    const resolved = deref(rule);
    if (!resolved) return false;
    if (resolved.kind === "choice") return resolved.options.every(stringOnly);

    return resolved.kind === "text" || resolved.kind === "lexical" || resolved.kind === "link";
}

/** The record among a rule's shapes, looking through a choice — a choice holds at most one. */
export function recordOf(rule: Rule | undefined): Extract<Rule, { kind: "record" }> | null {
    const resolved = rule?.kind === "choice" ? rule.options.map(deref).find(option => option?.kind === "record") : rule;

    return resolved?.kind === "record" ? resolved : null;
}

/**
 * The names a `link` resolves against: the keys of the map that `declares` the scope, found by
 * climbing to the object whose rule declares it.
 *
 * This is the answer the JSON Schema this replaced could not produce at all — it is not a fact about
 * the grammar but about the document, and only the two together know it. The climb is what gives a
 * scope its extent: the nearest enclosing declaration wins, so two channels each keep their own.
 */
export function declarationsFor(state: EditorState, from: SyntaxNode, scope: string): string[] {
    const doc = state.doc;

    for (let node: SyntaxNode | null = from; node; node = node.parent) {
        if (node.name !== "Object") continue;

        const record = recordOf(ruleAt(pathTo(node, doc)));
        if (!record) continue;

        const declaring = record.fields.find(field => {
            const target = deref(field.rule);
            return target?.kind === "map" && target.declares === scope;
        });
        if (!declaring) continue;

        const holder = propertyValue(node, declaring.name, doc);
        return holder ? keysOf(holder, doc) : [];
    }

    return [];
}

/**
 * The value node of `name` within an object, if it has one.
 *
 * Found by looking for a value *kind* rather than by stepping off the property name: lezer's tree
 * carries the punctuation too, so the node after a `PropertyName` is the `:`, and a property whose
 * value has not been typed yet has nothing beyond that.
 */
function propertyValue(object: SyntaxNode, name: string, doc: Text): SyntaxNode | null {
    for (let child = object.firstChild; child; child = child.nextSibling) {
        if (child.name !== "Property") continue;

        const key = child.getChild("PropertyName");
        if (!key || unquote(doc.sliceString(key.from, key.to)) !== name) continue;

        for (let part = key.nextSibling; part; part = part.nextSibling) {
            if (part.name === "Object" || part.name === "Array" || VALUE_NODES.has(part.name)) return part;
        }

        return null;
    }

    return null;
}

/** Every property name directly inside an object. */
export function keysOf(object: SyntaxNode, doc: Text): string[] {
    const names: string[] = [];

    for (let child = object.firstChild; child; child = child.nextSibling) {
        const name = child.name === "Property" ? child.getChild("PropertyName") : null;
        if (name) names.push(unquote(doc.sliceString(name.from, name.to)));
    }

    return names;
}

/**
 * The property names already written in the object being completed into, so they aren't offered
 * twice — except the one under the cursor, since completing a half-typed name into itself is the
 * point.
 */
export function writtenKeys(state: EditorState, at: number): Set<string> {
    let node: SyntaxNode | null = syntaxTree(state).resolveInner(at, -1);
    while (node && node.name !== "Object") node = node.parent;
    if (!node) return new Set();

    const names = new Set<string>();
    for (let child = node.firstChild; child; child = child.nextSibling) {
        const name = child.name === "Property" ? child.getChild("PropertyName") : null;
        if (name && !(name.from <= at && at <= name.to)) names.add(unquote(state.doc.sliceString(name.from, name.to)));
    }

    return names;
}
