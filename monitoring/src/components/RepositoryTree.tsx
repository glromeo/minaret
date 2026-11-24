import { signal } from "@preact/signals";
import type { Guid } from "@messagebus/client";
import { apis, channelsOf, isDirty, toggleApiWindow, type ApiDoc } from "../graph/apis";
import { forgetChannel, participantsOf, type ParticipantRow } from "../graph/participants";
import { selectedRouteId } from "../graph/store";
import { matches, PanelHeader } from "./PanelHeader";

/**
 * The repository dock: every `.idl` in the repository as a row, each expanding to the last known
 * provider and clients of the channel it describes.
 *
 * It is the list surface the app did not have, and its arrival is what let `loadApis` stop opening
 * a window per stored document. So the row is the editor's toggle — lit means that editor is on
 * screen — and it reads `api.windowOpen` directly rather than tracking its own flag, which is what
 * makes the window's own close button un-toggle the row.
 *
 * Expansion is held apart from that, in `expanded` below, because the two questions are independent:
 * an operator watching who is on a channel has no reason to have its document open, and one editing
 * a document has no reason to be shown its participants. Hence the separate chevron hit target.
 */
const expanded = signal<ReadonlySet<Guid>>(new Set());

/** Module-level, so a filter survives the panel being toggled away and back. */
const query = signal("");

function toggleExpanded(id: Guid): void {
    const next = new Set(expanded.value);
    if (!next.delete(id)) next.add(id);
    expanded.value = next;
}

export function RepositoryTree() {
    // Matched against the channels too, not just the filename: the two names routinely differ, and a
    // filter that only knew the file would fail exactly the operator who is looking for the channel
    // they saw on the graph.
    const documents = apis.value.filter(api => matches(query.value, api.name, ...channelsOf(api)));

    return (
        <aside class="repository">
            <PanelHeader
                title="Repository"
                icon={
                    <>
                        <path d="M2.5 2.75h11v2.75h-11z" />
                        <path d="M3.5 5.5h9v7.75h-9z" />
                        <path d="M6.5 8.25h3" />
                    </>
                }
                query={query}
                placeholder="filter documents"
            />

            <div class="repository-tree">
                {documents.map(api => (
                    <ApiNode key={api.id} api={api} />
                ))}
                {documents.length === 0 && (
                    <div class="repository-empty">{query.value === "" ? "no documents yet" : "nothing matches"}</div>
                )}
            </div>
        </aside>
    );
}

function ApiNode({ api }: { api: ApiDoc }) {
    const channels = channelsOf(api);
    const open = expanded.value.has(api.id);

    return (
        <div class="repository-node">
            <div class={`repository-row${api.windowOpen ? " repository-row--active" : ""}`}>
                {/* Its own button rather than a click zone on the row: the row is the editor toggle,
                    and a chevron that merely happened to sit inside it would open a window every
                    time the operator expanded a channel. */}
                <button
                    class={`repository-chevron${open ? " repository-chevron--open" : ""}`}
                    onClick={() => toggleExpanded(api.id)}
                    aria-expanded={open}
                    aria-label={`${open ? "Collapse" : "Expand"} ${api.name}`}
                >
                    <svg viewBox="0 0 16 16" width="14" height="14" aria-hidden="true">
                        <path d="M6 3.5 10.5 8 6 12.5" />
                    </svg>
                </button>

                <button
                    class="repository-name"
                    onClick={() => toggleApiWindow(api.id)}
                    aria-pressed={api.windowOpen}
                    title={`${api.name}.idl${channels.length > 0 ? ` — ${channels.join(", ")}` : ""}`}
                >
                    <svg class="repository-folder" viewBox="0 0 16 16" width="15" height="15" aria-hidden="true">
                        <path d="M1.75 12.75v-9a1 1 0 0 1 1-1h3l1.5 2h6a1 1 0 0 1 1 1v7a1 1 0 0 1-1 1h-10.5a1 1 0 0 1-1-1z" />
                    </svg>
                    <span class="repository-label">{api.name}</span>
                    {isDirty(api) && <span class="repository-dirty" title="unsaved changes" />}
                </button>
            </div>

            {open && (
                <div class="repository-children">
                    {channels.map(channel => (
                        <ChannelNode key={channel} channel={channel} />
                    ))}

                    {channels.length === 0 && <div class="repository-empty">declares no channel</div>}
                </div>
            )}
        </div>
    );
}

/**
 * One channel a document declares, and who has been on it. A level of its own rather than the
 * participants hanging straight off the API row, because a document is an array of declarations and
 * a two-channel file would otherwise pool two providers into one list with nothing saying which held
 * which. It also puts the channel's real name on screen — `sample.ts` is stored as `api-1` and
 * declares `Trading.Orders`, and only one of those two names is what a client registered against.
 */
function ChannelNode({ channel }: { channel: string }) {
    const rows = participantsOf(channel);

    return (
        <div class="repository-channel">
            <div class="repository-channel-head">
                <svg viewBox="0 0 16 16" width="13" height="13" aria-hidden="true">
                    <path d="M6.25 2.25 4.75 13.75M11.25 2.25 9.75 13.75M2.75 5.75h11M2.25 10.25h11" />
                </svg>
                <span class="repository-label">{channel}</span>
            </div>

            {rows.map(row => (
                <ParticipantEntry key={`${row.role}-${row.routeId}`} row={row} />
            ))}

            {rows.length === 0 && <div class="repository-empty">nobody has used it yet</div>}

            {/* The only way to retire a remembered row. Nothing on the bus says a client is never
                coming back (see participants.ts), so without this a stale entry would outlive every
                trace of what put it there. */}
            {rows.length > 0 && (
                <button class="repository-forget" onClick={() => forgetChannel(channel)}>
                    forget remembered
                </button>
            )}
        </div>
    );
}

function ParticipantEntry({ row }: { row: ParticipantRow }) {
    const name = row.properties["role"] ?? row.identity ?? row.routeId.substring(0, 8);

    return (
        <div class={`repository-participant${row.online ? "" : " repository-participant--offline"}`}>
            <button
                class="repository-participant-head"
                onClick={() => (selectedRouteId.value = row.routeId)}
                title={row.online ? "online — select in the graph" : "offline — last known"}
            >
                <span class={`repository-pill repository-pill--${row.role}${row.online ? " repository-pill--on" : ""}`}>
                    {row.role}
                </span>
                <span class="repository-participant-name">{name}</span>
            </button>

            {/* Offline rows drop to the timestamp alone: the rest of a ClientInfo describes a
                connection that no longer exists, and a route id nothing will answer on reads as
                current when it is not. */}
            <dl class="repository-details">
                {row.online ? (
                    <>
                        <Detail label="route id" value={row.routeId} mono />
                        {row.identity !== null && <Detail label="identity" value={row.identity} />}
                        {row.clockOffsetMs !== null && <Detail label="clock offset" value={`${row.clockOffsetMs} ms`} />}
                    </>
                ) : (
                    <Detail label="last seen" value={formatSeen(row.lastSeen)} />
                )}
            </dl>
        </div>
    );
}

function Detail({ label, value, mono }: { label: string; value: string; mono?: boolean }) {
    return (
        <div class="repository-detail">
            <dt>{label}</dt>
            <dd class={mono ? "repository-detail--mono" : undefined}>{value}</dd>
        </div>
    );
}

/** Clock time within the day it happened, the date once it is older — a bare 14:02 on a row last
 *  seen a week ago is the one reading of this field that is actively misleading. */
function formatSeen(at: number): string {
    const when = new Date(at);
    const sameDay = new Date().toDateString() === when.toDateString();
    const pad = (n: number) => n.toString().padStart(2, "0");

    return sameDay
        ? `${pad(when.getHours())}:${pad(when.getMinutes())}:${pad(when.getSeconds())}`
        : `${when.getFullYear()}-${pad(when.getMonth() + 1)}-${pad(when.getDate())} ${pad(when.getHours())}:${pad(when.getMinutes())}`;
}
