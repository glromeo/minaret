import source from "./idl.grammar?raw";

/**
 * The parser for `idl.grammar`, and the model it produces.
 *
 * The grammar file is the single description of a `.idl` document, and this turns it into something
 * walkable so the editor's vocabulary is *derived* rather than restated. That is the whole reason
 * the grammar is a file at all: a keyword list in TypeScript beside a grammar in prose is two copies
 * of a contract, and the copy nothing checks is the one that goes stale.
 *
 * Two languages in one file, split by the `lexicon` keyword. Above it, rules describe JSON — records
 * with named fields, maps with author-chosen keys, lists, choices discriminated by JSON kind. Below
 * it, rules are a PEG over the characters *inside* a string, which is where a type expression lives.
 * They are different enough that the split is a keyword rather than an inference: `[channel+]` and
 * `[A-Za-z_]` both open with a bracket, and only the section they sit in says which is meant.
 */

// ── The model ───────────────────────────────────────────────────────────────

/** A field of a record: its rule, whether it may be omitted, and the doc block above it. */
export interface Field {
    name: string;
    rule: Rule;
    optional: boolean;
    doc?: string;
}

/** Author-chosen keys and the rule their values follow — a `{[identifier]: X}` entry. */
export interface Entries {
    /** The lexicon rule every key must match. Never completed against: these keys are the author's. */
    keys: string;
    of: Rule;
    doc?: string;
}

export type Rule =
    /** `{a: X, b: Y}` — the listed fields. `rest` is an index signature alongside them. */
    | { kind: "record"; fields: Field[]; rest?: Entries }
    /** `{[identifier]: X}` — only an index signature, so every key is the author's. */
    | { kind: "map"; entries: Entries; min: number; declares?: string }
    | { kind: "list"; of: Rule; min: number }
    /** Any JSON string. */
    | { kind: "text" }
    /** A lexicon rule named in a value position: a string that must parse as it. */
    | { kind: "lexical"; rule: string }
    /** `X | Y` — discriminated by JSON kind, so at most one branch can apply to a value. */
    | { kind: "choice"; options: Rule[] }
    /** Another document rule by name. */
    | { kind: "ref"; name: string }
    /** A string that must name a key declared in a scope. */
    | { kind: "link"; scope: string }
    | { kind: "null" };

/** A PEG expression, over the characters of a string. */
export type Peg =
    | { kind: "literal"; text: string }
    | { kind: "class"; source: string }
    | { kind: "ref"; name: string }
    | { kind: "link"; scope: string }
    | { kind: "sequence"; items: Peg[] }
    /** Ordered: alternatives are tried left to right and the first success wins. */
    | { kind: "choice"; options: Peg[] }
    /** `*` is min 0, `+` is min 1, `?` is min 0 with `max` 1 — which is why `max` exists at all. */
    | { kind: "repeat"; of: Peg; min: number; max?: number };

export interface RuleDef {
    name: string;
    rule: Rule;
    doc?: string;
}

export interface LexDef {
    name: string;
    expr: Peg;
    doc?: string;
    /**
     * Whether the rule matches characters tightly, with no whitespace between its parts.
     *
     * Derived rather than declared: a rule containing a character class is scanning characters
     * (`identifier`, `integer`), and one built only of literals and references is scanning structure
     * (`array`, `map`), where `object<string, number>` has to tolerate the space after the comma.
     * Nothing in the grammar mixes the two, so the presence of a class settles it.
     */
    token: boolean;
}

export interface Grammar {
    name: string;
    version: number;
    /** The rule a document starts at — the first document rule declared. */
    start: string;
    rules: Map<string, RuleDef>;
    lexicon: Map<string, LexDef>;
}

// ── Scanner ─────────────────────────────────────────────────────────────────

const WORD = /[A-Za-z_$][A-Za-z0-9_$]*/y;
/** The grammar's own name may be hyphenated (`messagebus-idl`); nothing inside it may be. */
const DOTTED = /[A-Za-z_$][A-Za-z0-9_$.-]*/y;

/** A position to rewind to. Carries the pending doc block, or a lookahead would swallow it. */
interface Mark {
    at: number;
    pending: string | undefined;
}

class Scanner {
    private at = 0;
    /** The most recent `/** … *\/` block, held until a declaration claims it. */
    private pending: string | undefined;

    constructor(private readonly text: string) {}

    get done(): boolean {
        this.skip();
        return this.at >= this.text.length;
    }

    mark(): Mark {
        return { at: this.at, pending: this.pending };
    }

    reset(mark: Mark): void {
        this.at = mark.at;
        this.pending = mark.pending;
    }

    /** Consumes whitespace and comments, keeping the last doc block for whatever follows. */
    private skip(): void {
        for (;;) {
            while (this.at < this.text.length && /\s/.test(this.text[this.at]!)) this.at++;

            if (this.text.startsWith("/**", this.at)) {
                const end = this.text.indexOf("*/", this.at);
                if (end < 0) throw this.fail("unterminated doc comment");
                this.pending = cleanDoc(this.text.slice(this.at + 3, end));
                this.at = end + 2;
            } else if (this.text.startsWith("/*", this.at)) {
                const end = this.text.indexOf("*/", this.at);
                this.at = end < 0 ? this.text.length : end + 2;
            } else if (this.text.startsWith("//", this.at)) {
                const end = this.text.indexOf("\n", this.at);
                this.at = end < 0 ? this.text.length : end;
            } else {
                return;
            }
        }
    }

    /** Takes the doc block collected since the last call, if any. */
    takeDoc(): string | undefined {
        this.skip();
        const doc = this.pending;
        this.pending = undefined;
        return doc;
    }

    /** True (and consumed) if the next token is exactly `text`. */
    eat(text: string): boolean {
        this.skip();
        if (!this.text.startsWith(text, this.at)) return false;
        // A keyword must not be matched inside a longer word — `link` may not eat `links`.
        if (/[A-Za-z_]$/.test(text) && /[A-Za-z0-9_]/.test(this.text[this.at + text.length] ?? "")) return false;

        this.at += text.length;
        return true;
    }

    peek(text: string): boolean {
        const mark = this.mark();
        const hit = this.eat(text);
        this.reset(mark);
        return hit;
    }

    peekWord(): boolean {
        this.skip();
        WORD.lastIndex = this.at;
        return WORD.test(this.text) && WORD.lastIndex > this.at;
    }

    expect(text: string): void {
        if (!this.eat(text)) throw this.fail(`expected ${JSON.stringify(text)}`);
    }

    word(): string {
        return this.match(WORD, "a name");
    }

    /** A name that may carry dots or hyphens — the grammar's own identity line only. */
    dotted(): string {
        return this.match(DOTTED, "a name");
    }

    private match(pattern: RegExp, what: string): string {
        this.skip();
        pattern.lastIndex = this.at;
        const hit = pattern.exec(this.text);
        if (!hit || hit.index !== this.at) throw this.fail(`expected ${what}`);

        this.at = pattern.lastIndex;
        return hit[0];
    }

    /** A `"…"` literal, taken raw — the grammar has no escapes to interpret. */
    literal(): string {
        this.expect('"');
        const end = this.text.indexOf('"', this.at);
        if (end < 0) throw this.fail("unterminated literal");

        const value = this.text.slice(this.at, end);
        this.at = end + 1;
        return value;
    }

    /** A `[…]` character class, taken raw so it can become a RegExp unchanged. */
    charClass(): string {
        this.expect("[");
        const end = this.text.indexOf("]", this.at);
        if (end < 0) throw this.fail("unterminated character class");

        const value = this.text.slice(this.at, end);
        this.at = end + 1;
        return value;
    }

    fail(message: string): Error {
        const line = this.text.slice(0, this.at).split("\n").length;
        return new Error(`idl.grammar:${line}: ${message}`);
    }
}

/** Strips each continuation line's leading ` * ` and collapses the block to one paragraph. */
function cleanDoc(raw: string): string {
    return raw
        .split("\n")
        .map(line => line.replace(/^\s*\*/, "").trim())
        .join(" ")
        .replace(/\s+/g, " ")
        .trim();
}

// ── The parser ──────────────────────────────────────────────────────────────

export function parseGrammar(text: string): Grammar {
    const scanner = new Scanner(text);

    scanner.takeDoc();
    scanner.expect("grammar");
    const name = scanner.dotted();
    const version = Number(scanner.word().replace(/^v/, "")) || 1;

    const rules = new Map<string, RuleDef>();
    const lexicon = new Map<string, LexDef>();
    let start: string | undefined;
    let inLexicon = false;

    while (!scanner.done) {
        const doc = scanner.takeDoc();

        if (scanner.eat("lexicon")) {
            inLexicon = true;
            continue;
        }

        const ruleName = scanner.word();
        scanner.expect("=");

        if (inLexicon) {
            const expr = parsePeg(scanner);
            lexicon.set(ruleName, { name: ruleName, expr, token: hasCharClass(expr), ...(doc ? { doc } : {}) });
        } else {
            rules.set(ruleName, { name: ruleName, rule: parseRule(scanner), ...(doc ? { doc } : {}) });
            start ??= ruleName;
        }
    }

    if (!start) throw new Error("idl.grammar: no document rules");

    const grammar: Grammar = { name, version, start, rules, lexicon };
    for (const def of rules.values()) def.rule = bindLexicalRefs(def.rule, grammar);

    return grammar;
}

/**
 * Rewrites a bare name that turned out to name a *lexicon* rule into a `lexical` node.
 *
 * A pass rather than a decision at parse time because the lexicon is declared below every rule that
 * refers to it: `topic.outbound` mentions `type` some sixty lines before `type` exists. Which of the
 * two languages a name belongs to is knowable only once both halves are read.
 */
function bindLexicalRefs(rule: Rule, grammar: Grammar): Rule {
    switch (rule.kind) {
        case "ref":
            return grammar.rules.has(rule.name) ? rule : { kind: "lexical", rule: rule.name };
        case "record":
            return {
                kind: "record",
                fields: rule.fields.map(field => ({ ...field, rule: bindLexicalRefs(field.rule, grammar) })),
                ...(rule.rest ? { rest: { ...rule.rest, of: bindLexicalRefs(rule.rest.of, grammar) } } : {})
            };
        case "map":
            return { ...rule, entries: { ...rule.entries, of: bindLexicalRefs(rule.entries.of, grammar) } };
        case "list":
            return { ...rule, of: bindLexicalRefs(rule.of, grammar) };
        case "choice":
            return { kind: "choice", options: rule.options.map(option => bindLexicalRefs(option, grammar)) };
        default:
            return rule;
    }
}

/** A document rule: a `|` choice of primaries, discriminated by JSON kind. */
function parseRule(scanner: Scanner): Rule {
    const options = [parsePrimary(scanner)];
    while (scanner.eat("|")) options.push(parsePrimary(scanner));

    return options.length === 1 ? options[0]! : { kind: "choice", options };
}

function parsePrimary(scanner: Scanner): Rule {
    if (scanner.peek("{")) return parseObject(scanner);

    if (scanner.eat("[")) {
        const of = parseRule(scanner);
        const min = scanner.eat("+") ? 1 : 0;
        scanner.expect("]");

        return { kind: "list", of, min };
    }

    if (scanner.eat("link")) return { kind: "link", scope: scanner.word() };
    if (scanner.eat("text")) return { kind: "text" };
    if (scanner.eat("null")) return { kind: "null" };

    return { kind: "ref", name: scanner.word() };
}

/**
 * `{a: X}` is a record, `{[rule]: X}` a map, and a mix of the two a record with an index signature
 * (`fields`, whose `$extends` sits beside its free-form field names).
 */
function parseObject(scanner: Scanner): Rule {
    scanner.expect("{");

    const fields: Field[] = [];
    let rest: Entries | undefined;

    while (!scanner.eat("}")) {
        const doc = scanner.takeDoc();

        if (scanner.eat("[")) {
            const keys = scanner.word();
            scanner.expect("]");
            scanner.expect(":");
            rest = { keys, of: parseRule(scanner), ...(doc ? { doc } : {}) };
            continue;
        }

        const fieldName = scanner.word();
        const optional = scanner.eat("?");
        scanner.expect(":");
        fields.push({ name: fieldName, rule: parseRule(scanner), optional, ...(doc ? { doc } : {}) });
    }

    const min = scanner.eat("+") ? 1 : 0;
    const declares = scanner.eat("declares") ? scanner.word() : undefined;

    if (fields.length === 0) {
        if (!rest) throw scanner.fail("an object rule needs a field or an index signature");
        return { kind: "map", entries: rest, min, ...(declares ? { declares } : {}) };
    }

    return { kind: "record", fields, ...(rest ? { rest } : {}) };
}

// ── The lexicon's PEG ───────────────────────────────────────────────────────

function parsePeg(scanner: Scanner): Peg {
    const options = [parsePegSequence(scanner)];
    while (scanner.eat("/")) options.push(parsePegSequence(scanner));

    return options.length === 1 ? options[0]! : { kind: "choice", options };
}

/**
 * Juxtaposition, which is how a rule ends without a terminator: the sequence keeps taking terms
 * until one turns out to be the *next* rule's name, spotted by the `=` behind it and handed back.
 */
function parsePegSequence(scanner: Scanner): Peg {
    const items: Peg[] = [];

    for (;;) {
        const mark = scanner.mark();
        const term = parsePegTerm(scanner);
        if (!term) {
            scanner.reset(mark);
            break;
        }

        if (term.kind === "ref" && scanner.peek("=")) {
            scanner.reset(mark);
            break;
        }

        items.push(term);
    }

    if (items.length === 0) throw scanner.fail("expected an expression");

    return items.length === 1 ? items[0]! : { kind: "sequence", items };
}

function parsePegTerm(scanner: Scanner): Peg | null {
    const atom = parsePegAtom(scanner);
    if (!atom) return null;

    if (scanner.eat("*")) return { kind: "repeat", of: atom, min: 0 };
    if (scanner.eat("+")) return { kind: "repeat", of: atom, min: 1 };
    if (scanner.eat("?")) return { kind: "repeat", of: atom, min: 0, max: 1 };

    return atom;
}

function parsePegAtom(scanner: Scanner): Peg | null {
    if (scanner.done) return null;
    if (scanner.peek('"')) return { kind: "literal", text: scanner.literal() };
    if (scanner.peek("[")) return { kind: "class", source: scanner.charClass() };

    if (scanner.eat("(")) {
        const inner = parsePeg(scanner);
        scanner.expect(")");
        return inner;
    }

    if (scanner.eat("link")) return { kind: "link", scope: scanner.word() };
    if (scanner.peekWord()) return { kind: "ref", name: scanner.word() };

    return null;
}

/** A character class is what tells a token rule from a structural one, wherever it sits. */
function hasCharClass(expr: Peg): boolean {
    switch (expr.kind) {
        case "class":
            return true;
        case "sequence":
            return expr.items.some(hasCharClass);
        case "choice":
            return expr.options.some(hasCharClass);
        case "repeat":
            return hasCharClass(expr.of);
        default:
            return false;
    }
}

export const grammar: Grammar = parseGrammar(source);
