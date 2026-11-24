import { signal } from "@preact/signals";
import { newGuid, type Guid } from "@messagebus/client";
import { SAMPLE_IDL } from "../idl/sample";
import {
    deleteDocument,
    isValidName,
    listDocuments,
    readDocument,
    writeDocument,
    IdlRepositoryError
} from "../idl/repository";
import {
    moveWindow,
    newWindowState,
    raiseWindow,
    resizeWindow,
    setWindowOpen,
    toggleWindowMaximized,
    type WindowState
} from "./windows";

/**
 * An API the operator is authoring: a source document, the floating window editing it, and what the
 * Broker's IDL repository last said about it.
 *
 * Deliberately *not* a bus participant, which is what still makes it different from a dynamic node
 * (`dynamicNodes.ts`) despite wearing the same window chrome. A node is a live connection the Broker
 * routes to, and both ends of its life wait on a Broker event; an API is a document the Broker
 * merely *stores*, over REST (`src/idl/repository.ts`), so nothing about it is ever announced and
 * nothing waits.
 *
 * An API is the IDL half of Layer 2 (see the root `CLAUDE.md` and MESSAGING.md): a channel contract
 * as schema'd JSON, the durable artifact both sides would generate stubs from. It is now durable —
 * the repository is what makes an `.idl` outlive the tab that wrote it — but neither the extractor
 * that would write one from annotated types nor the generator that would read one exists yet.
 */
export interface ApiDoc extends WindowState {
    id: Guid;
    /** The name in the repository, without the `.idl` extension. Also what the window titles itself. */
    name: string;
    /** The `.idl` JSON shown in the editor, written back on every CodeMirror change. */
    source: string;
    /**
     * The source as the repository last confirmed it, or null for a document never saved. Two things
     * read it: the dirty marker (a document is clean when the two are identical) and nothing else —
     * it is never pushed back into the editor, which owns its own text (see `ApiWindow.tsx`).
     */
    stored: string | null;
    /**
     * The Etag of `stored`, and so the revision the next save is made against — null until the first
     * save, which is what tells `writeDocument` to claim the name rather than replace it.
     */
    etag: string | null;
    saving: boolean;
    /** The last save or delete failure, in the words the Broker used. Cleared by the next attempt. */
    error: string | null;
}

export const apis = signal<ApiDoc[]>([]);

export const DEFAULT_API_WIDTH = 520;
export const DEFAULT_API_HEIGHT = 420;
export const MIN_API_WIDTH = 320;
export const MIN_API_HEIGHT = 200;

let spawnCount = 0;

export function isDirty(api: ApiDoc): boolean {
    return api.source !== api.stored;
}

/**
 * Loads every document already in the repository, once, at startup (`app.tsx`).
 *
 * They arrive **closed**, which reverses what this function used to do. Opening a window each was
 * right while a window was the only way an API could be reached — a document that loaded invisibly
 * would have been indistinguishable from one that failed to load. `RepositoryTree` is now that
 * surface, so a loaded document is visible as a tree row, and opening twenty editors over the graph
 * to prove twenty documents exist is no longer a trade worth making.
 *
 * Documents are appended, so anything the operator added while the fetch was in flight keeps both
 * its place and its contents.
 */
export async function loadApis(): Promise<void> {
    const entries = await listDocuments();
    const loaded = await Promise.all(entries.map(async entry => {
        const document = await readDocument(entry.name);
        return { ...newApiDoc(entry.name, document.source, document.etag), windowOpen: false };
    }));

    // A fresh window must not be handed a name the repository already has, or its create-only first
    // save would be refused for a collision the operator never made.
    for (const entry of entries) {
        const numbered = /^api-(\d+)$/.exec(entry.name);
        if (numbered) spawnCount = Math.max(spawnCount, Number(numbered[1]));
    }

    apis.value = [...apis.value, ...loaded];
}

/** A new, unsaved document. It exists in the repository only once the operator saves it. */
export function addApi(): ApiDoc {
    const api = newApiDoc(`api-${++spawnCount}`, SAMPLE_IDL, null);

    apis.value = [...apis.value, api];
    return api;
}

/**
 * Stores the document under its current name.
 *
 * The etag it saves against is the one the last successful read or write returned, so a repository
 * that has moved on since — another window, another operator, a hand edit on disk — answers 412 and
 * the message says so rather than the edit being lost. Recovering from that is the operator's call
 * (rename, or reload and re-apply), which is why nothing here retries or force-writes.
 */
export async function saveApi(id: Guid): Promise<void> {
    const api = apis.value.find(candidate => candidate.id === id);
    if (!api || api.saving) return;

    if (!isValidName(api.name)) {
        patch(id, { error: `'${api.name}' is not a valid name: letters, digits, '.', '_' or '-', starting with a letter or digit.` });
        return;
    }

    patch(id, { saving: true, error: null });

    // The source is read again here rather than captured above: the editor is uncontrolled and the
    // operator keeps typing during the round trip, so what was sent is what must be recorded as
    // stored — recording the newer text would leave a dirty document marked clean.
    const sent = apis.value.find(candidate => candidate.id === id)?.source ?? api.source;

    try {
        const entry = await writeDocument(api.name, sent, api.etag);
        patch(id, { stored: sent, etag: entry.etag, saving: false, error: null });
    } catch (failure: unknown) {
        patch(id, { saving: false, error: messageOf(failure) });
    }
}

/**
 * Removes the document from the repository and closes its window.
 *
 * A never-saved document (`etag === null`) is only ever local, so it is dropped without a request —
 * the same immediacy `removeApi` has, and the reason the two are one function rather than a
 * conditional at the call site.
 */
export async function deleteApi(id: Guid): Promise<void> {
    const api = apis.value.find(candidate => candidate.id === id);
    if (!api || api.saving) return;

    if (api.etag === null) {
        removeApi(id);
        return;
    }

    patch(id, { saving: true, error: null });

    try {
        await deleteDocument(api.name, api.etag);
        removeApi(id);
    } catch (failure: unknown) {
        patch(id, { saving: false, error: messageOf(failure) });
    }
}

/**
 * Renames the document *locally*: the next save writes under the new name and claims it, and the
 * old stored document stays where it is. Renaming in the repository would be a copy and a delete
 * with no way to make the pair atomic, and an interrupted one loses a document — so the operation
 * this offers is the honest one, "save it as something else".
 */
export function renameApi(id: Guid, name: string): void {
    patch(id, { name, stored: null, etag: null, error: null });
}

/** Drops an API's window. The stored document, if there is one, is untouched — see `deleteApi`. */
export function removeApi(id: Guid): void {
    apis.value = apis.value.filter(api => api.id !== id);
}

/**
 * Records an edit. Called from CodeMirror's update listener, so it runs on every keystroke — the
 * signal write is cheap (one array rebuild) and only the dirty marker recomputes off `source`, but
 * note that the editor is *uncontrolled*: CodeMirror owns the document and this is a mirror of it,
 * never a value pushed back in. Feeding it back would fight the editor's own cursor and undo history.
 */
export function updateApiSource(id: Guid, source: string): void {
    patch(id, { source });
}

export function bringApiToFront(id: Guid): void {
    apis.value = raiseWindow(apis.value, id);
}

export function closeApiWindow(id: Guid): void {
    apis.value = setWindowOpen(apis.value, id, false);
}

export function openApiWindow(id: Guid): void {
    apis.value = setWindowOpen(apis.value, id, true);
}

/**
 * What a `RepositoryTree` row click does. Opening also raises (`setWindowOpen`), so a row is both
 * the way an editor is summoned and the way it is dismissed — which is why this is a toggle rather
 * than an open-and-raise: with the window already on top, a second click has no other meaning left.
 *
 * The row's own lit state is `windowOpen` itself, not a flag beside it, so the window's close button
 * un-toggles the row with nothing here involved.
 */
export function toggleApiWindow(id: Guid): void {
    const api = apis.value.find(candidate => candidate.id === id);
    if (api) apis.value = setWindowOpen(apis.value, id, !api.windowOpen);
}

/**
 * The channels an `.idl` declares, which is what joins a document to the bus traffic under it in
 * `RepositoryTree`.
 *
 * A list, not one name: a document is a top-level *array* of channel declarations (see `sample.ts`
 * and `idl.grammar`), so one file can carry several and the tree grows a level for them. Read out of
 * the document rather than off its filename because the two are free to differ, and normally do —
 * `sample.ts` is stored as `api-1` and declares `Trading.Orders`.
 *
 * **Falls back to the last stored revision when the working copy will not parse.** The editor is
 * uncontrolled and `source` mirrors every keystroke, so a document being typed into is unparseable
 * far more often than not, and re-deriving from nothing would make the subtree flicker away on the
 * first opening brace. `stored` is the last thing the repository confirmed, which is precisely the
 * last version whose channel names were real.
 */
export function channelsOf(api: ApiDoc): string[] {
    return declaredChannels(api.source) ?? declaredChannels(api.stored) ?? [];
}

function declaredChannels(source: string | null): string[] | null {
    if (source === null) return null;

    try {
        const parsed = JSON.parse(source) as unknown;
        if (!Array.isArray(parsed)) return null;

        return parsed
            .map(entry => (entry as { name?: unknown } | null)?.name)
            .filter((name): name is string => typeof name === "string" && name.length > 0);
    } catch {
        return null;
    }
}

export function moveApi(id: Guid, x: number, y: number): void {
    apis.value = moveWindow(apis.value, id, x, y);
}

export function resizeApi(id: Guid, width: number, height: number): void {
    apis.value = resizeWindow(apis.value, id, width, height, MIN_API_WIDTH, MIN_API_HEIGHT);
}

export function toggleMaximizeApi(id: Guid): void {
    apis.value = toggleWindowMaximized(apis.value, id);
}

function newApiDoc(name: string, source: string, etag: string | null): ApiDoc {
    return {
        id: newGuid(),
        name,
        source,
        stored: etag === null ? null : source,
        etag,
        saving: false,
        error: null,
        ...newWindowState(DEFAULT_API_WIDTH, DEFAULT_API_HEIGHT)
    };
}

function patch(id: Guid, changes: Partial<ApiDoc>): void {
    apis.value = apis.value.map(api => (api.id === id ? { ...api, ...changes } : api));
}

function messageOf(failure: unknown): string {
    return failure instanceof IdlRepositoryError ? failure.message : String(failure);
}
