import assert from "node:assert/strict";
import { test } from "node:test";
import { CloseCodes } from "../src/messaging/connection.js";
import { EventNames } from "../src/messaging/events.js";
import { Destination, header } from "../src/messaging/message.js";
import { newClientInfo, Router } from "../src/messaging/router.js";
import { newGuid } from "../src/messaging/utils/guid.js";
import { Verbs } from "../src/messaging/verbs.js";
import { silentLogger } from "../src/utils/logger.js";
import { FakeConnection, settle } from "./fake-connection.js";

function router(connection) {
    return new Router(() => connection, { log: silentLogger });
}

test("Connect completes the register round trip and adopts the Broker's enriched ClientInfo", async () => {
    const connection = new FakeConnection();
    connection.ackClockOffsetMs = 37;

    const info = newClientInfo({ role: "test" });
    const registered = await router(connection).connect(info);

    assert.notEqual(registered.routeId, null);
    assert.equal(registered.identity, info.identity);
    assert.equal(registered.clockOffsetMs, 37);
    assert.equal(connection.sent[0].header.type, Verbs.ClientRegister);
});

test("Connect is bounded by its signal when the Broker never acks", async () => {
    const connection = new FakeConnection();
    connection.suppressRegistrationAck = true;

    const controller = new AbortController();
    const connecting = router(connection).connect(newClientInfo(), { signal: controller.signal });
    controller.abort(new Error("gave up"));

    await assert.rejects(connecting, /gave up/);
});

test("Frames reach handlers in wire order even when a compressed one precedes a raw one", async () => {
    const connection = new FakeConnection();
    const bus = router(connection);
    await bus.connect(newClientInfo());

    const seen = [];
    bus.handleMessage.add(message => seen.push(message.body.tag));

    // The first body clears the compression threshold and so decodes asynchronously; the second
    // does not. Without the FIFO chain in Router the small one would overtake the large one, and
    // every ordering this protocol relies on — topic.resolved before the frame it resolves, replay
    // before its ack — would be a race.
    const filler = "x".repeat(2048);
    await connection.raise(header("first", Destination.fromId(bus.routeId)), { tag: "compressed", filler });
    await connection.raise(header("second", Destination.fromId(bus.routeId)), { tag: "raw" });
    await settle();

    assert.deepEqual(seen, ["compressed", "raw"]);
});

test("A replay reaches handlers before `on` resolves, and is marked as state rather than news", async () => {
    const connection = new FakeConnection();
    const providerRouteId = newGuid();

    connection.autoAck.set(Verbs.EventSubscribe, subscription => [
        {
            type: Verbs.EventReplay,
            // Not a reply to anything — it precedes the ack, which is what carries the correlation.
            replyTo: null,
            body: {
                name: subscription.name,
                channels: [{ name: subscription.name, channel: "LAYER_1", providerRouteId }]
            }
        },
        { type: Verbs.EventSubscribed, body: { ok: true } }
    ]);

    const bus = router(connection);
    await bus.connect(newClientInfo());

    const seen = [];
    await bus.on(EventNames.ChannelRegistered, (occurrence, origin) => seen.push([occurrence.channel, origin]));

    // Awaiting `on` means "I have the full picture", so the replay is already in hand here — not
    // merely scheduled to arrive.
    assert.deepEqual(seen, [["LAYER_1", "replay"]]);

    await connection.raise(
        header(Verbs.Event, Destination.fromId(bus.routeId), { from: null }),
        { name: EventNames.ChannelRegistered, channel: "OTHER", providerRouteId }
    );
    await settle();

    assert.deepEqual(seen, [["LAYER_1", "replay"], ["OTHER", "live"]]);
});

test("An event frame for a name nobody subscribed to is dropped, not surfaced as a message", async () => {
    const connection = new FakeConnection();
    const bus = router(connection);
    await bus.connect(newClientInfo());

    let messages = 0;
    bus.handleMessage.add(() => messages++);

    await connection.raise(
        header(Verbs.Event, Destination.fromId(bus.routeId), { from: null }),
        { name: EventNames.TopicSubscribed, channel: "LAYER_1", topic: "echo" }
    );
    await settle();

    assert.equal(messages, 0);
});

test("Reconnecting detaches the old connection, so its late ack can't complete the new registration", async () => {
    const stale = new FakeConnection();
    const fresh = new FakeConnection();

    const info = newClientInfo();
    const bus = new Router(() => stale, { log: silentLogger });
    await bus.connectTo(info, stale);

    fresh.ackClockOffsetMs = 99;
    const registered = await bus.connectTo(info, fresh);

    assert.equal(stale.disposed, true);

    // The stale socket answering after the fact must not be able to re-stamp the live view.
    stale.ackClockOffsetMs = -1;
    await stale.ackRegistration({ ...info, routeId: registered.routeId });
    await settle();

    assert.equal(bus.routeId, registered.routeId);
});

/**
 * The Broker evicted this client because another registered under its identity and took its
 * RouteId. Reconnecting would take the RouteId straight back and evict that connection, which would
 * reconnect in turn — the two trading it at socket speed, every round emitting presence events. So
 * a superseded close is the one ending the reconnect policy does not answer.
 */
test("A superseded close is terminal, and does not reconnect", async () => {
    const first = new FakeConnection();
    const second = new FakeConnection();

    let handed = 0;
    const bus = new Router(() => (handed++ === 0 ? first : second), { log: silentLogger });
    bus.reconnect(1, 10);
    await bus.connect(newClientInfo());

    first.onDisconnected.emit(CloseCodes.Superseded);
    await settle();

    assert.equal(handed, 1);
    assert.equal(second.sent.length, 0);
});

test("Every other close still reconnects", async () => {
    const first = new FakeConnection();
    const second = new FakeConnection();

    let handed = 0;
    const bus = new Router(() => (handed++ === 0 ? first : second), { log: silentLogger });
    bus.reconnect(1, 10);
    await bus.connect(newClientInfo());

    // 1006: the code a browser reports for a socket that died without a close frame.
    first.onDisconnected.emit(1006);
    await settle();

    assert.equal(handed, 2);
    assert.equal(second.sent[0].header.type, Verbs.ClientRegister);
});
