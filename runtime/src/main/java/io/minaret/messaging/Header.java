package io.minaret.messaging;

import io.minaret.messaging.wire.BodyEncoding;

import java.util.UUID;

/**
 * The wire envelope's header — what crosses the socket in front of every body, rendered by Wire's M01
 * framing.
 *
 * Two symmetric control-plane conventions run through it: a null from means the message originated from
 * the Broker itself (the registration ack) rather than from another client, and an empty to means the
 * message is addressed to the Broker itself (the register handshake) rather than to a topic or another
 * client.
 *
 * encoding, bodyLength and extras live here rather than as parameters threaded alongside every send and
 * parse call, because they describe a property of this message, not of the call handling it: a header
 * built to send a body already knows how that body is encoded and how long it is, and a received header
 * already carries both facts about how its body arrived.
 *
 * The withX methods stand in for C#'s `with` expression. They exist only for the fields something
 * actually re-stamps — a general-purpose builder would invite copies that silently drop a field added
 * later, which is exactly what `with` protects the C# side from.
 */
public record Header(
        UUID from,
        Destination to,
        String type,
        UUID replyTo,
        long time,
        Integer ttl,
        BodyEncoding encoding,
        Integer bodyLength,
        String extras
) {

    public Header {
        if (type == null || type.isEmpty()) throw new IllegalArgumentException("Header.type must not be empty.");
        if (to == null) to = Destination.EMPTY;
        if (encoding == null) encoding = BodyEncoding.RAW;
        if (extras == null) extras = "";
    }

    /**
     * A Broker-addressed header carrying only a verb — the control-plane shape.
     */
    public static Header of(String type) {
        return new Header(null, Destination.EMPTY, type, null, 0, null, BodyEncoding.RAW, null, "");
    }

    public static Header of(String type, Destination to) {
        return new Header(null, to, type, null, 0, null, BodyEncoding.RAW, null, "");
    }

    public static Header of(String type, Destination to, UUID replyTo) {
        return new Header(null, to, type, replyTo, 0, null, BodyEncoding.RAW, null, "");
    }

    public Header withFrom(UUID value) {
        return new Header(value, to, type, replyTo, time, ttl, encoding, bodyLength, extras);
    }

    public Header withTo(Destination value) {
        return new Header(from, value, type, replyTo, time, ttl, encoding, bodyLength, extras);
    }

    public Header withType(String value) {
        return new Header(from, to, value, replyTo, time, ttl, encoding, bodyLength, extras);
    }

    public Header withReplyTo(UUID value) {
        return new Header(from, to, type, value, time, ttl, encoding, bodyLength, extras);
    }

    public Header withTime(long value) {
        return new Header(from, to, type, replyTo, value, ttl, encoding, bodyLength, extras);
    }

    public Header withTtl(Integer value) {
        return new Header(from, to, type, replyTo, time, value, encoding, bodyLength, extras);
    }

    /**
     * Re-stamps how the accompanying body is encoded and how long it is. The two always change together
     * — a caller that compressed a body knows both facts at once — so they are set together rather than
     * by two calls that could disagree between them.
     */
    public Header withBody(BodyEncoding value, int length) {
        return new Header(from, to, type, replyTo, time, ttl, value, length, extras);
    }

    public Header withExtras(String value) {
        return new Header(from, to, type, replyTo, time, ttl, encoding, bodyLength, value);
    }

}
