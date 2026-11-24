import assert from "node:assert/strict";
import { test } from "node:test";
import { Destination, header } from "../src/messaging/message.js";
import { decodeGuid, encodeGuid, newGuid } from "../src/messaging/utils/guid.js";
import { prepareBody, serializeMessage, tryParseBody, tryParseHeader } from "../src/messaging/utils/wire.js";
import { Verbs } from "../src/messaging/verbs.js";

/**
 * The codec's cross-language obligations. What is pinned here is deliberately what the Java WireTests
 * pin — the same Guid literal, the same field order, the same treatment of an absent field — since
 * a JS-only assertion proves the port self-consistent without proving it agrees with anything.
 */

test("Guid encodes as base64url of its bytes in big-endian order, the literal the Java tests pin", () => {
    assert.equal(encodeGuid("00112233-4455-6677-8899-aabbccddeeff"), "ABEiM0RVZneImaq7zN3u_w");
    assert.equal(decodeGuid("ABEiM0RVZneImaq7zN3u_w"), "00112233-4455-6677-8899-aabbccddeeff");
});

test("Guid round-trips, and is always 22 chars on the wire", () => {
    const routeId = newGuid();

    assert.equal(encodeGuid(routeId).length, 22);
    assert.equal(decodeGuid(encodeGuid(routeId)), routeId);
});

test("A truncated Guid segment is rejected rather than decoded as a shorter value", () => {
    assert.equal(tryParseHeader("M01|ABEiM0RVZneImaq7zN3u|x|||||\0").error, "malformed guid");
});

test("Register handshake renders every field in the Broker's read order", async () => {
    const routeId = newGuid();
    const info = { routeId, clockOffsetMs: 0, properties: { role: "test" } };

    const raw = await serializeMessage(
        header(Verbs.ClientRegister, Destination.empty, { from: routeId, time: 1769472000000 }),
        info
    );

    // to | type | time | ttl | from | replyTo | enc | bodyLength
    assert.ok(raw.startsWith(`M01||client.register|1769472000000||${encodeGuid(routeId)}|||`), raw);

    const { frame, error } = tryParseHeader(raw);
    assert.equal(error, null);
    assert.equal(frame.header.type, Verbs.ClientRegister);
    assert.equal(frame.header.from, routeId);
    assert.equal(frame.header.time, 1769472000000);
    assert.equal(frame.header.ttl, null);
    assert.equal(frame.header.encoding, "raw");
    assert.equal(frame.header.bodyLength, Buffer.byteLength(frame.body));
    assert.deepEqual(await tryParseBody(frame), info);
});

test("A /channel/topic destination round-trips, and an explicit ttl of 0 stays 0", async () => {
    const raw = await serializeMessage(
        header(Verbs.Publish, Destination.fromPath("LAYER_1", "ticks"), { ttl: 0 }),
        "tick 1"
    );

    const { frame } = tryParseHeader(raw);
    assert.equal(frame.header.to.id, null);
    assert.equal(frame.header.to.channel, "LAYER_1");
    assert.equal(frame.header.to.topic, "ticks");
    // Not null: BrokerPolicy treats null and 0 alike today, and collapsing them here would be a
    // silent bug the moment it stops doing so.
    assert.equal(frame.header.ttl, 0);
    assert.equal(await tryParseBody(frame), "tick 1");
});

test("The path splits at the first slash, so a topic may contain one and a channel may not", async () => {
    const { frame } = tryParseHeader("M01|/LAYER_1/ticks/fast|publish|||||\0\"x\"");
    assert.equal(frame.header.to.channel, "LAYER_1");
    assert.equal(frame.header.to.topic, "ticks/fast");

    await assert.rejects(
        () => serializeMessage(header(Verbs.Publish, Destination.fromPath("LAYER/1", "ticks")), "x"),
        /Destination channel must not contain/
    );
});

test("An empty path half is a parse error rather than a name of \"\"", () => {
    assert.equal(tryParseHeader("M01|//ticks|publish|||||\0").error, "destination path must be /channel/topic");
    assert.equal(tryParseHeader("M01|/LAYER_1/|publish|||||\0").error, "destination path must be /channel/topic");
});

test("A body at or above the threshold is gzipped, and bodyLength counts the on-wire body", async () => {
    const big = { rows: Array.from({ length: 200 }, (_, i) => ({ i, name: `client-${i}` })) };

    const prepared = await prepareBody(header(Verbs.EventReplay), big);
    assert.equal(prepared.header.encoding, "gzip");
    assert.equal(prepared.header.bodyLength, prepared.body.length);
    assert.ok(prepared.body.length < JSON.stringify(big).length);

    const { frame } = tryParseHeader(await serializeMessage(header(Verbs.EventReplay), big));
    assert.equal(frame.header.encoding, "gzip");
    assert.deepEqual(await tryParseBody(frame), big);
});

test("A frame from a newer sender keeps its extras, and one from an older one parses without bodyLength", () => {
    assert.equal(tryParseHeader("M01||bulk|1|||||3|somefuturevalue\0\"x\"").frame.header.extras, "somefuturevalue");
    assert.equal(tryParseHeader("M01||bulk|1||||\0\"x\"").frame.header.bodyLength, null);
});

test("Malformed frames are reported, never thrown — a receive loop drops them and keeps going", () => {
    assert.equal(tryParseHeader("M02||x|||||\0").error, "unknown frame tag");
    assert.equal(tryParseHeader("M01||x|||||").error, "missing body separator");
    assert.equal(tryParseHeader("M01|||||||\0").error, "empty type");
    assert.equal(tryParseHeader("M01||x|nope||||\0").error, "malformed time");
    assert.equal(tryParseHeader("M01||x||||||-1\0").error, "malformed body length");
    assert.equal(tryParseHeader("M01||x|||||deflate|\0").error, "unknown body encoding");
});

test("An empty body is valid, and distinct from a missing separator", async () => {
    const { frame, error } = tryParseHeader("M01||x|||||\0");
    assert.equal(error, null);
    assert.equal(frame.body, "");
    assert.equal(await tryParseBody(frame), undefined);
});
