import { grammar, type Peg } from "./grammar";

/**
 * The half of completion that happens *inside* a JSON string — a type expression.
 *
 * Two questions, and neither is answerable by the document grammar alone. Where in the expression is
 * the cursor (`contextAt`), and what may be written there (`candidates`). The answers are derived
 * from the lexicon rather than listed: the forms come out of each alternative of the `type` rule, so
 * adding one to `idl.grammar` adds it to the editor with nothing else touched.
 *
 * There is no PEG *runner* here. Completion needs to know what may come next, which is a question
 * about the grammar, not about the text so far — so the position is found by scanning back over the
 * few characters that can precede a type, and the offerings come from the rules. Validating a
 * finished expression is the other half, and belongs with the document validator.
 */

/** Where the cursor sits inside a type expression. */
export type TypeContext =
    /** A type may start here: at the beginning, or after `<`, `,` or the `[` of a tuple. */
    | { kind: "type" }
    /** An enum's member name, after `string[` / `number[` or a `|` — the author's word, not ours. */
    | { kind: "member" }
    | null;

/**
 * The keywords that turn a following `[` into an enum rather than a tuple, read off the lexicon:
 * every alternative of `enum` opens with a literal, and that literal is the discriminator.
 */
const ENUM_OPENERS: string[] = alternativesOf("enum").flatMap(expr => {
    const first = leadingLiteral(expr);
    return first ? [first] : [];
});

/**
 * Where `offset` falls in `text`, by looking at what precedes it.
 *
 * A scan rather than a parse because every position a type can begin at is preceded by exactly one
 * of four characters — nothing, `<`, `,` or `[` — which makes the lookbehind exact rather than
 * approximate. The one ambiguity is `[`: it opens a tuple, but also an enum's value list when a
 * scalar keyword sits in front of it, and that keyword is what tells them apart.
 */
export function contextAt(text: string, offset: number): TypeContext {
    const before = text.slice(0, offset).replace(/[A-Za-z0-9_-]*$/, "").trimEnd();

    if (before.length === 0) return { kind: "type" };

    const last = before[before.length - 1]!;
    if (last === "<" || last === ",") return { kind: "type" };
    if (last === "|") return { kind: "member" };

    if (last === "[") {
        const keyword = /([A-Za-z0-9_]*)$/.exec(before.slice(0, -1))?.[1] ?? "";
        return ENUM_OPENERS.includes(keyword) ? { kind: "member" } : { kind: "type" };
    }

    // After a closed form or a bare word the expression is complete; nothing may follow it here.
    return null;
}

/** One thing that may be written where a type is expected. */
export interface Candidate {
    /** The form with its holes named, e.g. `array<type>` — what the list shows. */
    label: string;
    /** The text to insert, holes empty: `array<>`. */
    insert: string;
    /** Where the caret lands within `insert` — the first hole, or the end. */
    caret: number;
    /** The lexicon rule it came from. */
    rule: string;
    doc?: string;
}

/**
 * Everything the lexicon says may start a type, in the order `type` lists its alternatives — which
 * is the order the parser tries them, so the list reads as the grammar does.
 *
 * A `link` alternative (`reference`) contributes no fixed form; it is the names declared in the
 * document, which only the caller can know. It is reported through `linkScopes` instead.
 */
export const TYPE_CANDIDATES: Candidate[] = alternativesOf("type").flatMap(expr => expandAlternative(expr));

/** The scopes a type may reference by name, so the caller can offer what the document declares. */
export const TYPE_LINK_SCOPES: string[] = alternativesOf("type").flatMap(expr => collectLinkScopes(expr));

/**
 * A rule reference among `type`'s alternatives is expanded one level — far enough that `scalar`
 * becomes six completions and `enum` becomes two, rather than one entry each that would insert the
 * rule's *name*. It stops there: `array`'s inner `type` is a hole to be filled, not a list to expand.
 */
function expandAlternative(expr: Peg, seen: Set<string> = new Set()): Candidate[] {
    if (expr.kind === "ref") {
        if (seen.has(expr.name)) return [];
        const def = grammar.lexicon.get(expr.name);
        if (!def) return [];

        return expandAlternative(def.expr, new Set([...seen, expr.name])).map(candidate => ({
            ...candidate,
            rule: candidate.rule || expr.name,
            ...(candidate.doc ?? def.doc ? { doc: candidate.doc ?? def.doc } : {})
        }));
    }

    if (expr.kind === "choice") return expr.options.flatMap(option => expandAlternative(option, seen));
    if (expr.kind === "link") return [];

    const skeleton = skeletonOf(expr);
    return skeleton.insert ? [{ ...skeleton, label: labelOf(expr), rule: "" }] : [];
}

function collectLinkScopes(expr: Peg, seen: Set<string> = new Set()): string[] {
    switch (expr.kind) {
        case "link":
            return [expr.scope];
        case "ref": {
            if (seen.has(expr.name)) return [];
            const def = grammar.lexicon.get(expr.name);
            return def ? collectLinkScopes(def.expr, new Set([...seen, expr.name])) : [];
        }
        case "choice":
            return expr.options.flatMap(option => collectLinkScopes(option, seen));
        default:
            return [];
    }
}

/**
 * The literal text of an expression with its holes left empty, and where the first hole is.
 *
 * A hole is any part that isn't a literal — a rule reference, a character class, a link. Repeats
 * contribute nothing at all: they are the optional tail of a form (`("|" identifier)*`), and a
 * completion that pre-wrote one would be inserting a second member nobody asked for.
 */
function skeletonOf(expr: Peg): { insert: string; caret: number } {
    let insert = "";
    let caret = -1;

    const walk = (node: Peg): void => {
        switch (node.kind) {
            case "literal":
                insert += node.text;
                break;
            case "sequence":
                node.items.forEach(walk);
                break;
            case "ref":
            case "class":
            case "link":
                if (caret < 0) caret = insert.length;
                break;
            case "choice":
                // Only reachable below a sequence, where a nested choice is a hole like any other.
                if (caret < 0) caret = insert.length;
                break;
            case "repeat":
                break;
        }
    };

    walk(expr);
    return { insert, caret: caret < 0 ? insert.length : caret };
}

/** The same walk, but naming each hole after the rule that fills it — for the completion's label. */
function labelOf(expr: Peg): string {
    switch (expr.kind) {
        case "literal":
            return expr.text;
        case "ref":
            return expr.name;
        case "class":
            return "…";
        case "link":
            return expr.scope;
        case "sequence":
            return expr.items.map(labelOf).join("");
        case "choice":
            return expr.options.map(labelOf).join("/");
        case "repeat":
            // The *minimum* form: one occurrence for `+`, none for `*` or `?`. A tuple reads
            // `[type,type]` because it needs a second member; an enum reads `string[identifier]`
            // because its `("|" identifier)*` tail needs none.
            return expr.min > 0 ? labelOf(expr.of) : "";
    }
}

function alternativesOf(rule: string): Peg[] {
    const def = grammar.lexicon.get(rule);
    if (!def) return [];

    return def.expr.kind === "choice" ? def.expr.options : [def.expr];
}

/** The first literal an expression must open with, if it opens with one. */
function leadingLiteral(expr: Peg): string | null {
    if (expr.kind === "literal") return expr.text;
    if (expr.kind === "sequence") return expr.items[0] ? leadingLiteral(expr.items[0]) : null;
    if (expr.kind === "ref") {
        const def = grammar.lexicon.get(expr.name);
        return def ? leadingLiteral(def.expr) : null;
    }

    return null;
}
