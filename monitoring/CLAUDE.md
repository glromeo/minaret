# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Preact + Vite + TSX app visualizing live Broker state. An npm project, not in the Maven reactor, so
`npm run build` is a separate step from `mvn verify` — see root `CLAUDE.md` for how it fits into the
layout. This file covers only what's local to `monitoring/`.

## Commands

```
npm ci
npm run dev        # Vite dev server; /msgbus proxied to wss://localhost:5443
npm run build      # tsc -b && vite build -> ../broker/wwwroot/monitor/ (gitignored)
npm run typecheck  # tsc -b --noEmit
```

Both `dev` and the built page need a live broker (`io.minaret.broker.Application` from the IDE). If
the socket fails with a TLS error, the dev certificate is not trusted — see root `CLAUDE.md` "Dev
certificate".

No test suite or lint script in this subproject.

## Architecture

- The wire protocol lives in `@messagebus/client` (`javascript/`, a `file:` dependency), not
  here — this app is a *consumer* of the client runtime, the same way an `examples/` module consumes
  `runtime`. Nothing under `src/` may reimplement any part of the protocol; a gap belongs in the
  package. `src/wire/` used to hold a partial copy and no longer exists.
- `src/graph/` — DOM-free bus state, so it's testable/driveable headlessly against a live Broker
  without a browser.
  - `store.ts` — `clients` signal (`Map<Guid, ClientNode>`, replaced not mutated on write, since a
    signal only notifies on assignment) built by subscribing to `client.connected/disconnected` and
    `channel.registered/removed` via `trackBus`. A channel is not its own node — it's a label on its
    provider's `ClientNode`, since a channel isn't independently addressable. `clientList` filters the
    monitor's own connection out of the client list using `brokerRouteId` recorded from the register
    ack.
  - `dynamicNodes.ts` — the operator-authored nodes the (+) button adds, held apart from `store.ts`
    because that map is a *reflection* of Broker state and would wipe them. **Neither end of a
    dynamic node's life is optimistic**: adding one draws it immediately but disconnected
    (`.client--pending`, transparent) and it only goes solid when `client.connected` for its RouteId
    reaches the store; discarding one marks it `closing`, which drops it from `App`'s render list —
    unmounting the iframe is what actually closes its socket — and it stays drawn, disconnected,
    until `client.disconnected` arrives and `reportDisconnected` retires it. The monitor shows what
    the Broker observes, so a node that appeared or vanished ahead of the Broker would be the one
    thing on the canvas that isn't true.

    **Discarding a node is a graph action, never a window one.** All three window controls
    (`DynamicNode.tsx`) only hide the window — minimize and close do the same thing — and the node
    stays on the bus, since the iframe stays mounted. The one thing that ends a node is the delete
    button on `BusGraph`'s hover popover, which is also the only surface a node still has once its
    window is away. Consequences worth knowing:
    - **A dynamic node's graph body is keyed by its own id, never its RouteId** (see `virusTree.ts`)
      — the body has to outlive the `clients` entry at both ends, and a mid-life id change would
      make the simulation discard its settled position.
    - A `routeId` here means *the Broker registered it*, which is why `reportRouteId` is called
      after the ack even though `newClientInfo` mints the id earlier. `discardDynamicNode` reads it
      that way: a node with no RouteId is removed outright, since no event will ever come for it.
    - A node that drops *without* being discarded is left alone — `connectPersistent` reconnects
      with the same `ClientInfo` and so the same RouteId, so it reads as disconnected until it's back.
    - `removeDynamicNode` is the force path (a second click on a closing node's delete button), for
      when the confirming event can't arrive because the monitor's own connection is down.
  - `windows.ts` — the floating-window geometry (`WindowState`) and the pure list transforms that
    move it, shared by `dynamicNodes.ts` and `apis.ts`. Shared **not for de-duplication**: the
    z-order counter and the spawn cascade are properties of the canvas, not of a window kind, and a
    per-store copy of either would let a just-clicked window sit under an older one of the other
    kind, or open the first API window exactly on top of the first node window.
  - `apis.ts` — operator-authored `.idl` documents, one per editor window, backed by the Broker's IDL
    repository (`src/idl/repository.ts`). Wears the same chrome as a dynamic node and is still the
    opposite of one in the way that matters: a node's life is gated on Broker *events* at both ends,
    an API is a document the Broker merely stores, so adding and discarding are immediate and nothing
    is ever announced. `source` is a *mirror* of CodeMirror's document (the editor is uncontrolled —
    see `ApiWindow.tsx`); `stored` is what the repository last confirmed, and the pair is the dirty
    flag. Three decisions worth knowing:
    - **`loadApis` loads stored documents closed.** It used to open a window each, because a window
      *was* how an API existed here — there was no list surface a document could otherwise be reached
      from. `RepositoryTree` is now that surface, so a loaded document is visible as a row, and
      opening twenty editors over the graph to prove twenty documents exist is no longer a trade
      worth making.
    - **`channelsOf` reads the channel names out of the document, falling back to `stored`.** A
      `.idl` is a top-level *array* of declarations, so one file can name several channels and the
      tree grows a level for them; those names normally differ from the file's own (`sample.ts` is
      stored as `api-1` and declares `Trading.Orders`), so the filename is not a substitute. The
      fallback matters because the editor is uncontrolled and `source` mirrors every keystroke — a
      document being typed into is unparseable far more often than not, and re-deriving from nothing
      would make the subtree flicker away on the first opening brace.
    - **Rename is local**: it clears the etag, so the next save claims the new name and leaves the old
      document where it was. Renaming in the repository would be a copy plus a delete with no way to
      make the pair atomic, and an interrupted one loses a document.
    - **Save records the source it actually sent**, re-read after the write rather than captured
      before it. The editor is uncontrolled and the operator keeps typing during the round trip, so
      recording the newer text would mark a dirty document clean.
  - `participants.ts` — who has been seen on each channel, keyed by channel name and **persisted to
    localStorage**. The one place in the monitor that deliberately outlives what the Broker knows:
    the repository tree lists an API's *last known* provider and clients, which the event plane
    cannot answer, since `store.ts` drops a client the moment it disconnects. Three things follow:
    - **Liveness is never stored.** `online` is resolved against `store.ts` on every read, so a row
      can be remembered-and-dim but never remembered-and-lit. Without that rule the persisted file
      would be claiming things about the bus that nothing had checked.
    - **A remembered entry can never be disproven** — nothing on the wire says a client is not coming
      back — which is why `forgetChannel` exists at all. It is the only way to retire a stale row.
    - **Live subscribers are held per topic, not as a set of RouteIds.** The events are per topic, so
      a client on three topics of a channel would otherwise be dimmed by the first unsubscribe while
      still holding the other two. Providers need no equivalent: holding a channel is already on the
      `ClientNode`. This is also why `trackBus` now subscribes to `topic.subscribed`, which nothing
      else in the app needs — the graph draws topics off their provider and never asks who listens.
  - `simulation.ts` — hand-rolled force layout (`Simulation`): pairwise repulsion + a spring pulling
    every body toward an orbit ring around its own parent + a weak attraction between linked
    client/provider spikes + damping. That last one exists for the *dashed line*, not the spikes:
    the spring holds a spike on its ring radially, so the pull spends itself rotating the spike
    around that ring until it faces its partner, which is what stops links crossing the canvas over
    unrelated nodes. It is therefore tuned against repulsion (the only force free in the tangential
    direction), not against the spring — see the `LEVELS` doc block. Hand-rolled instead of d3-force
    because the graph is a fixed-depth tree (Broker, clients, channels, topics) with a handful of
    cross-links, so the general-purpose center/collide machinery a library carries would be unused
    weight and its link solver would want the hierarchy expressed as links too. Positions live in a plain
    mutated array **outside the signal graph** — they change every frame at 60fps, and routing that
    through signals would re-run every computed/diff every VNode 60 times a second. `tick` is the one
    signal bumped per frame; components that read `tick.value` re-render, everything else reads
    `simulation.bodies` directly. The loop self-stops when kinetic energy drops below `REST` and
    `sync()` restarts it on membership change. `REST` is also the layout's *sensitivity floor* —
    a force too weak to beat it from a standing start is invisible, not slow — which is what made
    the link force (and, before it, inter-body repulsion) a no-op at its original value. Read its
    doc block before touching it. The corollary is that a level's repulsion and spring can be scaled
    together freely: the ratio fixes the geometry, the magnitude only fixes how fast it gets there,
    which is how the clients came to spread evenly around the ring rather than bunching.
  - `traffic.ts` — the yellow/purple dots that ride the edges when a message moves. **The source is
    local, and that is the whole constraint worth knowing**: the event plane is presence-only
    (the `wire-contract` skill's Events), so nothing on the wire says a message was routed, and the monitor's own
    connection sees only frames addressed to it. What is observable is the dynamic nodes — `NodeApp`
    shares the host's JS realm and module graph (see `DynamicNode.tsx`), so it reports its own sends
    and receives here exactly as it reports channels through `dynamicNodes.ts`. So **an external
    client's traffic animates nothing**; covering that needs a Broker-side per-message event, which
    is a wire-contract change and a per-message cost rather than something this module can infer.
    The two legs run **back to back, never at once** — an inbound dot is held until the last
    reported send reaches the Broker (`handoff`), since sender and receiver share this realm and
    report within a frame of each other, which without the delay reads as two messages rather than
    one hop. Three consequences of the design:
    - **A pulse names a body, never a position.** `BusGraph.pulsePath` resolves the polyline from
      live body positions every frame — origin, its parents, then the Broker's centre — so a dot
      rides the same edges `.edges` draws and follows a node that is still settling or being dragged.
      `pointAlong` walks that polyline by *arc length*, since a topic sits ~34px from its channel and
      a client ~190px from the Broker, and splitting the time per segment instead would visibly
      change the dot's speed mid-trip.
    - **A pulse's `start` can be in the future**, so expiry is a compacting walk rather than a splice
      of the array's front, and a not-yet-departed dot is skipped by the canvas instead of parked.
    - **It runs its own rAF loop, and `pulseTick` is a second signal beside `simulation.tick`.** The
      simulation self-stops at rest, which is precisely when someone is watching for traffic; sharing
      its loop would also mean running the physics to move a dot and jittering a layout the operator
      has just arranged.
- `src/idl/` — Layer 2's JSON-IDL, as far as this app needs it: the editor knows the shape of a
  channel contract, and the Broker now keeps what it writes, without anything yet *generating* from
  one. A `.idl` document is JSON; its grammar is not.
  - `repository.ts` — the `fetch` client for the Broker's `/idl` service (`broker/.../idl/`). Plain REST
    and nothing from `@messagebus/client`, because an `.idl` is a design-time artifact rather than
    traffic; `/idl` is proxied in dev for the same reason `/msgbus` is, so nothing under `src/` knows
    which mode it is in. Failures carry the HTTP status because the statuses are not interchangeable
    to an editor — 412 means someone else saved and the operator must reconcile, 400 means their own
    document is malformed. A first save goes out as `If-None-Match: *` (create-only, so a new window
    cannot adopt a name someone else stored) and every later one as `If-Match`.
  - `idl.grammar` — the one description of a `.idl`, in a DSL of its own. **It is not JSON Schema,
    and the reason is worth knowing before proposing a return to one:** the two constraints that
    matter most here are the two JSON Schema has no vocabulary for. A type expression nests
    (`array<object<string, Order>>`), which regex cannot match — a `pattern` accepting `array<.+>`
    accepted `array<@@@>` and `object<a,b,c>` too — and every type name has to *resolve* to a
    declaration elsewhere in the document, which no shape language can see. Both are first-class
    here: the lexicon recurses, and `link`/`declares` resolve names in a scope.
    Two halves split by the `lexicon` keyword — a keyword rather than an inference, since
    `[channel+]` and `[A-Za-z_]` both open with a bracket and only the section says which is meant.
    Above it, rules describe JSON; below it, a PEG describes the grammar *inside* a string.
    The lexicon's ordered choice is load-bearing three times over (`array` and `enum` before
    `scalar`, `scalar` before `reference`) and a wrong order fails quietly, since a PEG does not
    backtrack into an alternative that already succeeded — the file's own comment records it.
  - `grammar.ts` — parses that file at startup. Binding a bare name to the *lexicon* is a pass after
    parsing, not a decision during it: `topic.outbound` mentions `type` sixty lines before `type`
    exists, so which of the two languages a name belongs to is knowable only once both halves are
    read. Token rules (no implicit whitespace) are derived, not declared — a rule containing a
    character class is scanning characters, one built of literals and references is scanning
    structure, where `object<string, number>` has to tolerate the space.
  - `document.ts` — walks a document against the grammar: cursor → path → rule, plus `declarationsFor`,
    which is the payoff. It climbs to the object whose rule `declares` a scope and reads that map's
    keys, so a `link` completes to what the document actually contains. Held apart from
    `completion.ts` because it needs no editor and a validator will want exactly this.
  - `typeExpression.ts` — the language inside a string. `contextAt` scans back rather than parsing,
    because every position a type can begin at is preceded by exactly one of nothing, `<`, `,` or
    `[` — and the one ambiguity, `[` opening a tuple versus an enum's value list, is settled by the
    keyword in front of it, itself read off the lexicon. `TYPE_CANDIDATES` is **derived from the
    `type` rule**: each alternative is expanded one level (so `scalar` becomes six completions and
    `enum` two) and rendered to its literal skeleton with the caret at the first hole. Adding a form
    to `idl.grammar` adds it to the editor with nothing else touched.
  - `completion.ts` — the CodeMirror adapter over those two. A key whose value can only be a string
    is completed with its quotes already written and the caret between them, which is also what
    re-opens the list on the value: the grammar knows the value has no other shape, so the quote is a
    keystroke carrying no decision.
  - `editing.ts` — the keys that are about the *text* rather than the grammar, and the only place the
    editor overrides `basicSetup`. A **slot** is one side of a property (its name or its value) plus
    each array element; Tab is the next one in document order and Shift-Tab the previous, which is
    what makes them read as the right- and left-hand side from wherever a property is entered.
    Landing selects what is editable — a string's text between its quotes, a token whole, a cursor
    inside an empty container — so typing replaces the value. `"` inside a string moves to the front
    of it instead of opening a second one, which is the case Tab creates: with the text selected,
    `closeBrackets` would otherwise wrap the value in another pair.
  - `sample.ts` — what a new window opens with, exercising both kinds of `types` entry (a shape, and
    aliases for an enum and a tuple) and all three topic shapes.
- `src/components/BusGraph.tsx` — the SVG canvas: fixed `viewBox` (not viewport-sized) so window
  resizes never perturb the simulation's coordinate space. The spawn buttons that used to sit in its
  top-left corner are `CanvasToolbar` now — see above.
- `src/components/FloatingWindow.tsx` — the window chrome both kinds wear, stateless: it renders the
  geometry it is handed and calls back with a target, because the position belongs to the store's
  row and holding it in two places would diverge. The bodies are what differ — a node's is an iframe
  with its own document and bus connection, an API's is a CodeMirror editor **in this document**,
  since a text buffer gains nothing from a document boundary and would pay for it in re-mounted
  style modules and re-plumbed clipboard/focus. One consequence: `.node-window` clips to its rounded
  corners, so `ApiWindow` hoists CodeMirror's tooltips to `<body>` — otherwise the completion list is
  cut off at the window edge, worst in exactly the case that needs it (a small window, near the
  bottom). That is also why their styling sits in `styles.css` rather than under
  `.api-window-editor`.
- `src/components/PanelHeader.tsx` — the strip all three docked panels wear: name, filter field, and
  a trailing slot for panel-specific controls (`LogViewer`'s resume-auto-scroll, now icon-only
  because the words don't fit beside a field at dock width). One component rather than three
  near-identical headers because the panels are read as a set — an operator scanning for a name
  should not have to learn where each keeps its search box, or whether it has one. The query signal
  is owned by each panel, since what a filter *means* differs: documents and channel names in
  `RepositoryTree`, row text and severity in `LogViewer`, the detail cards in `Inspector`. Two
  consequences: each panel holds its query at module level, so a filter survives being toggled away;
  and `Inspector` never filters the name and subtitle of what is selected, only the cards under
  them — a panel showing detail cards for something it no longer named would be answering a question
  about an unidentified node.
- `src/components/CanvasToolbar.tsx` — the two spawn buttons (new dynamic node, new `.idl`), rendered
  by `App` against `.canvas-stack` rather than by `BusGraph` against the graph. Neither button is
  about the graph — both create something that outlives it — and inside `BusGraph` they vanished with
  it, so hiding the graph also removed the only way to add anything.
- `src/components/RepositoryTree.tsx` — the left dock: every `.idl` as a row, expanding to the
  channels it declares and, under each, the last known provider and clients from `participants.ts`.
  It is the list surface the app did not have, which is what let `loadApis` stop opening a window per
  document. Two decisions worth knowing:
  - **The row is the editor's toggle and reads `api.windowOpen` directly**, rather than tracking a
    flag beside it — which is what makes the window's own close button un-toggle the row with
    nothing in this component involved. Opening also raises, so a second click has no meaning left
    but to close.
  - **Expansion is a separate hit target** (the chevron), because the two questions are independent:
    watching who is on a channel is no reason to open its document, and editing a document is no
    reason to be shown its participants.
- `src/components/TopNav.tsx` — the header's four view toggles and the `panes` signal behind them.
  They are **independent switches, not a selection**: each shows or hides one surface in place, so
  all four on is the default and all four off is a legal empty canvas. Two consequences live in
  `app.tsx` rather than here, and both are about what a hidden pane must *not* cost:
  - **The bus toggle hides `BusGraph` and nothing else.** Dynamic-node windows float over the same
    canvas and unmounting one closes its iframe's bus connection, so folding them into the toggle
    would disconnect live clients to tidy the view.
  - **The repository toggle governs the dock, not the editors.** An editor is opened and closed from
    its own tree row, so one thing has one owner and hiding the tree does not sweep away the document
    the operator is in the middle of. Every `ApiWindow` stays mounted regardless of `windowOpen`,
    which is what preserves its CodeMirror document and undo history across a close.
  - The sidebar's shape follows how many of its two panes are up, not each flag alone: with one, it
    takes the whole height and the seam between them goes; with neither, the sidebar and its splitter
    go too. The dragged height is kept, not reset, so re-showing the log restores the operator's split.
- `src/app.tsx` / `main.tsx` — connects on mount, feeds `trackBus`, renders `BusGraph`.

## Signals over hooks

State is written from a *WebSocket callback*, not from a component. A hook-based store would have to
route that write back through a setter owned by some component's render cycle; a signal is written
directly from the event handler. This is also what lets the simulation run as a bare
`requestAnimationFrame` loop with no React/Preact lifecycle involved.

## How this app uses the client runtime

The protocol rationale is in the `wire-contract` skill and `javascript/CLAUDE.md`. What is local
to this app is only *which* surface it takes:

- **Identity is per logical client, and the monitor page is one of them.** `NodeApp`
  (`monitor-node-${nodeId}`) and `ExternalNodeApp` (`monitor-inspector-${routeId}`) each derive their
  own, because those are many participants inside *one* monitor. `app.tsx`'s `MONITOR_IDENTITY` is
  deliberately **not** unique per tab: two monitors are one client, so the Broker's newer-wins rule
  applies — the second tab evicts the first, which stays evicted (`CloseCodes.Superseded` is terminal,
  see the `wire-contract` skill) and reads OFFLINE. Making it per-tab avoids the eviction instead, and
  was tried and reverted: the Broker already has a rule for this case, and the rule is the answer.
- **`app.tsx` builds a bare `Router`, not a `MessageBus`.** The monitor is a pure observer, so it
  needs Layer 0 only — `router.on(...)` and nothing else. Building it directly is also what lets
  `onStateChange` be armed *before* the first connection attempt, so the status pill shows the wait
  rather than starting at it. Awaiting `on(...)` means "I have the full picture", not just
  "subscribed", which is why `trackBus` awaits all six subscriptions together.
- **`NodeApp.tsx` is a full participant**, so it takes `MessageBus` plus a handle per joined channel.
  Adding a topic is one call for both roles — `handle.subscribe` is `topic.register` on a provider
  handle and `topic.subscribe` on a client one — and sends pick their verb off `handle.isProvider`
  rather than off the UI's own record of the role.
- **Its Layer-0 panel picks its own verb** (`LAYER_0_VERB = "message"`). Any verb the Broker doesn't
  recognise falls through to a plain RouteId forward, so this is one arbitrary choice among many
  rather than a reserved name — which is why it is not in the package's `Verbs`, where the Java side
  has no such member either. Inbound Layer-0 traffic is matched on that verb rather than on the
  destination, since a control-plane ack is addressed to this RouteId too.
