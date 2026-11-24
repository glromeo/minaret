import assert from "node:assert/strict";
import { test } from "node:test";
import { ChannelClient, ChannelProvider } from "../src/messaging/channels.js";
import { Destination, header } from "../src/messaging/message.js";
import { newClientInfo, Router } from "../src/messaging/router.js";
import { newGuid } from "../src/messaging/utils/guid.js";
import { Verbs } from "../src/messaging/verbs.js";
import { silentLogger } from "../src/utils/logger.js";
import { FakeConnection, settle } from "./fake-connection.js";

/**
 * The handles are what these drive, not MessageBus — MessageBus runs real discovery and builds a
 * real socket, whereas a handle is constructible against a fake. Same split the C# tests make.
 */
async function connected(topicId = newGuid(), providerRouteId = newGuid()) {
    const connection = new FakeConnection();

    connection.autoAck.set(Verbs.ChannelRegister, () => ({ type: Verbs.ChannelRegistered, body: { ok: true } }));
    connection.autoAck.set(Verbs.ChannelResolve, () => ({ type: Verbs.ChannelResolved, body: { ok: true, routeId: providerRouteId } }));
    connection.autoAck.set(Verbs.TopicRegister, () => ({ type: Verbs.TopicRegistered, body: { ok: true, topicId } }));
    connection.autoAck.set(Verbs.TopicSubscribe, () => ({ type: Verbs.TopicSubscribed, body: { ok: true, topicId } }));
    connection.autoAck.set(Verbs.Publish, () => ({ type: Verbs.Published, body: { ok: true } }));

    const router = new Router(() => connection, { log: silentLogger });
    await router.connect(newClientInfo());

    return { connection, router, topicId, providerRouteId };
}

/** The `to` segment as it went on the wire, which is the whole point of the path/id distinction. */
function sentDestinations(connection, verb) {
    return connection.sent.filter(f => f.header.type === verb).map(f => f.header.to);
}

test("Registering a topic learns its id, so a frame addressed by that id reaches the handler", async () => {
    const { connection, router, topicId } = await connected();
    const provider = await ChannelProvider.create(router, silentLogger, "LAYER_1");

    const seen = [];
    await provider.subscribe("echo", body => seen.push(body));

    await connection.raise(header(Verbs.Dispatch, Destination.fromId(topicId), { from: newGuid() }), "hello");
    await settle();

    assert.deepEqual(seen, ["hello"]);
});

test("A handler armed before its ack is promoted once the id lands, so nothing is missed in between", async () => {
    const { connection, router, topicId } = await connected();
    const provider = await ChannelProvider.create(router, silentLogger, "LAYER_1");

    // Not awaited: subscribe arms the handler *before* sending, precisely so a frame arriving while
    // the ack is in flight is still handled rather than dropped as an unknown TopicId.
    const subscribing = provider.subscribe("echo", () => {});
    const seen = [];
    provider.addTopicHandler("echo", body => seen.push(body));

    await subscribing;
    await connection.raise(header(Verbs.Dispatch, Destination.fromId(topicId), { from: newGuid() }), "late");
    await settle();

    assert.deepEqual(seen, ["late"]);
});

test("A first dispatch travels as /channel/topic, and later ones as the id the Broker acked back", async () => {
    const { connection, router, topicId } = await connected();
    const client = await ChannelClient.create(router, silentLogger, "LAYER_1");

    await client.dispatch("echo", "first");

    // The unsolicited ack that ends the path form — not a reply to anything, so no replyTo.
    await connection.raise(
        header(Verbs.TopicResolved, Destination.fromId(router.routeId), { from: null }),
        { ok: true, topicId, topics: ["echo"], channel: "LAYER_1" }
    );
    await settle();

    await client.dispatch("echo", "second");

    const destinations = sentDestinations(connection, Verbs.Dispatch);
    assert.deepEqual(destinations[0], { id: null, channel: "LAYER_1", topic: "echo" });
    assert.deepEqual(destinations[1], { id: topicId, channel: null, topic: null });
});

test("A topic.resolved for another channel's topic of the same name is not learned", async () => {
    const { connection, router, topicId } = await connected();
    const client = await ChannelClient.create(router, silentLogger, "LAYER_1");

    // Every handle on the connection sees the ack; this one names a different channel.
    await connection.raise(
        header(Verbs.TopicResolved, Destination.fromId(router.routeId), { from: null }),
        { ok: true, topicId, topics: ["echo"], channel: "OTHER" }
    );
    await settle();

    await client.dispatch("echo", "first");

    assert.deepEqual(sentDestinations(connection, Verbs.Dispatch)[0], { id: null, channel: "LAYER_1", topic: "echo" });
});

test("A request/reply call completes on the provider's answer, not on its own echoed frame", async () => {
    const { connection, router, topicId } = await connected();
    const client = await ChannelClient.create(router, silentLogger, "LAYER_1");
    await client.subscribe("echo", () => {});

    const calling = client.call("echo", "ping");
    await settle();

    const request = connection.sent.findLast(f => f.header.type === Verbs.Dispatch);
    assert.ok(request.header.replyTo, "a call must carry a correlation id");

    // The client's own request looped back — same verb, same correlation id, so only the origin
    // separates it from the answer. Completing on this would resolve the call with its argument.
    await connection.raise(
        header(Verbs.Dispatch, Destination.fromId(topicId), { from: router.routeId, replyTo: request.header.replyTo }),
        "ping"
    );
    await connection.raise(
        header(Verbs.Dispatch, Destination.fromId(topicId), { from: newGuid(), replyTo: request.header.replyTo }),
        "pong"
    );

    assert.equal(await calling, "pong");
});

test("Unsubscribing keeps handlers armed until the ack, then drops them", async () => {
    const { connection, router, topicId } = await connected();
    const client = await ChannelClient.create(router, silentLogger, "LAYER_1");

    const seen = [];
    await client.subscribe("ticks", body => seen.push(body));

    connection.autoAck.set(Verbs.TopicUnsubscribe, () => ({ type: Verbs.TopicUnsubscribed, body: { ok: true } }));
    await client.unsubscribe("ticks");

    await connection.raise(header(Verbs.Publish, Destination.fromId(topicId), { from: newGuid() }), "after");
    await settle();

    assert.deepEqual(seen, []);
});

test("A failed unsubscribe leaves the handlers in place — nothing stopped being delivered", async () => {
    const { connection, router, topicId } = await connected();
    const client = await ChannelClient.create(router, silentLogger, "LAYER_1");

    const seen = [];
    await client.subscribe("ticks", body => seen.push(body));

    connection.autoAck.set(Verbs.TopicUnsubscribe, () => ({ type: Verbs.TopicUnsubscribed, body: { ok: false, error: "not subscribed" } }));
    await assert.rejects(client.unsubscribe("ticks"), /not subscribed/);

    await connection.raise(header(Verbs.Publish, Destination.fromId(topicId), { from: newGuid() }), "still here");
    await settle();

    assert.deepEqual(seen, ["still here"]);
});

test("A provider's notify carries the recipient in replyTo, which a notification is free to use", async () => {
    const { connection, router } = await connected();
    const provider = await ChannelProvider.create(router, silentLogger, "LAYER_1");
    await provider.subscribe("echo", () => {});

    const recipient = newGuid();
    await provider.notify(recipient, "echo", "psst");

    const sent = connection.sent.findLast(f => f.header.type === Verbs.Notify);
    assert.equal(sent.header.replyTo, recipient);
    assert.equal(sent.header.to.id, provider.destinationFor("echo").id);
});
