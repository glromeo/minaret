/**
 * The client for the Broker's IDL repository (`broker/.../idl/IdlController.java`), which is where an
 * `.idl` document lives once it is more than text in one operator's browser tab.
 *
 * Plain `fetch` against `/idl`, deliberately: an `.idl` is a design-time artifact, so the Broker
 * serves it over REST rather than over the bus, and this module has nothing to do with
 * `@messagebus/client`. Same-origin in a build (the Broker serves this app); in `npm run dev` Vite
 * proxies `/idl` for the same reason it proxies `/msgbus`.
 *
 * `graph/apis.ts` is the only caller. What it needs from every function here is that a failure
 * arrives as an `IdlRepositoryError` carrying the HTTP status, because the statuses are not
 * interchangeable to an editor: 412 means someone else saved and the operator must be told to
 * reconcile, 400 means their own document is malformed, and anything else is the Broker being
 * unreachable.
 */

/** One row of the listing, and also what a write answers with — see broker's IdlRepository.Entry. */
export interface IdlEntry {
    name: string;
    etag: string;
    size: number;
    updatedAt: string;
}

/** A fetched document: the author's own bytes, and the Etag a later write has to quote. */
export interface IdlDocument {
    source: string;
    etag: string;
}

export class IdlRepositoryError extends Error {
    readonly status: number;

    constructor(message: string, status: number) {
        super(message);
        this.name = "IdlRepositoryError";
        this.status = status;
    }
}

const BASE = "/idl";

/**
 * Mirrors `IdlRepository.IsValidName`, so a name the Broker would refuse is refused at the keystroke
 * rather than at the save. Deliberately *not* the whole rule — the Windows device names are left to
 * the server, since duplicating a list is how the two copies come to disagree, and the failure mode
 * here is only that a rare name takes a round trip to be rejected.
 */
export function isValidName(name: string): boolean {
    return /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/.test(name);
}

export async function listDocuments(): Promise<IdlEntry[]> {
    const response = await fetch(BASE, { headers: { accept: "application/json" } });
    if (!response.ok) throw await failureOf(response);
    return (await response.json()) as IdlEntry[];
}

export async function readDocument(name: string): Promise<IdlDocument> {
    const response = await fetch(`${BASE}/${encodeURIComponent(name)}`);
    if (!response.ok) throw await failureOf(response);
    return { source: await response.text(), etag: response.headers.get("etag") ?? "" };
}

/**
 * Writes a document, conditionally on what the caller believes it last saw.
 *
 * A null `etag` means "I have never saved this", which goes out as `If-None-Match: *` — create-only,
 * so a new editor window claiming a name can never silently overwrite a document someone else
 * already stored under it. An etag goes out as `If-Match`, so a save lands only on the revision it
 * was edited from. Either way a conflict is a 412 rather than a lost edit.
 */
export async function writeDocument(name: string, source: string, etag: string | null): Promise<IdlEntry> {
    const response = await fetch(`${BASE}/${encodeURIComponent(name)}`, {
        method: "PUT",
        headers: {
            "content-type": "application/json",
            ...(etag === null ? { "if-none-match": "*" } : { "if-match": etag })
        },
        body: source
    });

    if (!response.ok) throw await failureOf(response);
    return (await response.json()) as IdlEntry;
}

export async function deleteDocument(name: string, etag: string | null): Promise<void> {
    const response = await fetch(`${BASE}/${encodeURIComponent(name)}`, {
        method: "DELETE",
        headers: etag === null ? {} : { "if-match": etag }
    });

    if (!response.ok) throw await failureOf(response);
}

/**
 * Turns a failed response into the error the caller throws. Every endpoint answers a failure as an
 * RFC 9457 problem document, whose `detail` is written to be shown to whoever made the request — so
 * it is preferred over anything this module could compose from the status alone. A response that is
 * not one (a proxy's error page, most likely) falls back to the status text.
 */
async function failureOf(response: Response): Promise<IdlRepositoryError> {
    let message = response.statusText || `HTTP ${response.status}`;

    try {
        const problem = (await response.json()) as { title?: string; detail?: string };
        message = problem.detail ?? problem.title ?? message;
    } catch {
        // Not a problem document; the status text stands.
    }

    return new IdlRepositoryError(message, response.status);
}
