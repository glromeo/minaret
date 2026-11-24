# TODO

What is genuinely open, and the arguments worth having in hand before re-opening each. Completed work
is not recorded here; that is what `git log` and the `CLAUDE.md` files are for.

## Structural

- [ ] **Distribute the subscriber lists: the remaining structural win, and the reason the single
      registry lock still exists.** A `SubscriptionNode` is spliced into *two* lists owned by *two
      different sessions*: its topic's list (the provider's session) and its owner's list (the
      subscriber's `Route`). Two sessions mutating one structure is what forces `ChannelRegistry`'s
      lock, so moving the roots onto sessions does **not** remove it. It needs either the same single
      lock (no gain) or per-topic locks with a fixed acquisition order to avoid deadlock.

      **Before attempting this, re-argue the `unlinked`/`next` invariant under the new ownership**: the
      lock-free fanout walk relies on `unlink` clearing `prev` but deliberately *not* `next`, and on the
      retention bound being one synchronous fanout. Neither is obviously preserved by per-topic locking.

      What stays global regardless: the topic id index (the id-addressed hot path), the channel and
      provider tables (how one session reaches another's channel), and `ReplyTable` (a rendezvous
      between two sessions, owned by neither).

- [ ] **`removeChannel`'s sweeping is irreducible; remember that before re-opening it.** Freeing the
      name, evicting topics from the id index and unlinking subscriber nodes all touch structures other
      connections read and write. A session cannot do that work by dropping its own reference. Session
      ownership bought the argument-instead-of-lookup and the identity guard, not a smaller teardown.

## Layer 1 — the last piece of the design

- [ ] **`DispatchMode.Any`/`All` (provider→client dispatch) is unbuilt.** Coupled to the item below;
      do them together.

- [ ] **`Session.handleDispatch` decides reply-vs-request by who sent it**, so a provider cannot send a
      *request* on its own topic. Correct for one-provider-per-channel; it must change for
      `DispatchMode.Any`/`All`, since direction would then come from the verb. It cannot be solved by
      correlation lookup (infinite echo loop, pinned by
      `RoutingTest.dispatch_providerReply_goesToTheAwaitingClient_notBackToTheProvider`).

## Found during the move off C#

- [ ] **The runtime's request/reply correlator is written three times**: `ChannelBase.awaitAck`,
      `ChannelBase.callOnTopic`, `Router.sendSubscribe`. Collapse them into one predicate-taking
      `Router.awaitReply`, as the C# runtime did with `AwaitReply`.

- [ ] **Doc prose still compares to C#.** About 140 lines across `runtime/src` and `broker/src` ("the port
      of C#'s X", "C# does Y"), and the JS module headers ("mirrors Runtime/X.cs"). The pointers that
      would dangle are fixed. What's left is wording that reads as history once the C# side is retired.
      Rewrite them when that happens, not before: while the C# Broker is a live peer, "same as C#" is
      still a statement about interop.

- [ ] **Quirks kept for parity with the C# Broker** (`broker/CLAUDE.md`): the
      `broker.connections.active` drift on supersede, the repeated `channel.registered` on a same-route
      re-register, a `dispatch` with no `replyTo` forwarded uncorrelated. Fix them once parity stops
      mattering.

- [ ] **Brand names on the wire.** The pipe `MessageBus.Broker.ConnectionInfo.<REALM>`,
      `MESSAGEBUS_REALM`, the `messagebus.*` config keys and `@messagebus/client` were kept so the C#
      and current JS clients interoperate. Renaming any of them is a coordinated change across every
      implementation.

## Cross-runtime parity

- [ ] **Layer 2's annotated surface is not ported to JS.** Decorators and metadata need the transpile
      step that package exists to avoid. A descriptor-object surface (`proxy(spec)` /
      `bind(handlers, spec)`) would express the same four call kinds without them, but that is a design
      decision rather than a transcription.

## Optional / deferred

- [ ] **Layer 2 (RPC / JSON-IDL) is still design-only where it counts**: nothing *generates* from an
      `.idl`. What exists is the authoring half: `monitoring/src/idl/` (grammar, completion engine,
      editor) and `broker/.../idl/` (the REST repository that stores what it writes). The two open
      pieces are the **extractor** (annotated `@Channel` types → `.idl`) and the
      **generator/validator** (`.idl` → stubs, and checking a live channel against its contract). The
      C# prototype's pipeline (`attic/`, `SALVAGE.md` in the old MessageBus repo) is the reference.

- [ ] **The broker validates only an `.idl`'s envelope** (`IdlValidator`: an array of channels each with
      a `name` and `topics`), because the grammar itself is written once, in TypeScript
      (`monitoring/src/idl/idl.grammar` + `grammar.ts`). A Java validator is wanted the moment anything
      generates from a stored document. Decide first whether the grammar is ported or the parser
      shared, since two copies of a language is the problem `idl.grammar` exists to avoid.

## Verification debt

- [ ] **`examples/layer1` is the only check of `runtime`'s `Router`/`Channels` against a real broker
      over a real socket**, and it is not part of `mvn verify`. The broker's e2e tests use real sockets
      but drive them raw, and runtime's own tests use `FakeConnection`. Run it after any wire-format or
      handshake change.
