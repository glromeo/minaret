package io.minaret.broker.contract;

import static io.minaret.broker.wire.Wire.ascii;

/**
 * Runtime/Messaging/Verbs.cs as pre-encoded bytes.
 *
 * Frame.typeIs compares against these in Session's dispatch, and FrameWriter writes them into the
 * frames the Broker originates, so a verb never needs a String on the hot path. The arrays are
 * shared and must never be written to.
 */
public final class Verbs {

    public static final byte[] CLIENT_REGISTER = ascii("client.register");
    public static final byte[] CLIENT_REGISTERED = ascii("client.registered");
    public static final byte[] CHANNEL_REGISTER = ascii("channel.register");
    public static final byte[] CHANNEL_REGISTERED = ascii("channel.registered");
    public static final byte[] CHANNEL_RESOLVE = ascii("channel.resolve");
    public static final byte[] CHANNEL_RESOLVED = ascii("channel.resolved");
    public static final byte[] TOPIC_REGISTER = ascii("topic.register");
    public static final byte[] TOPIC_REGISTERED = ascii("topic.registered");
    public static final byte[] TOPIC_LIST = ascii("topic.list");
    public static final byte[] TOPIC_LISTED = ascii("topic.listed");
    public static final byte[] TOPIC_SUBSCRIBE = ascii("topic.subscribe");
    public static final byte[] TOPIC_SUBSCRIBED = ascii("topic.subscribed");
    public static final byte[] TOPIC_UNSUBSCRIBE = ascii("topic.unsubscribe");
    public static final byte[] TOPIC_UNSUBSCRIBED = ascii("topic.unsubscribed");
    public static final byte[] TOPIC_RESOLVED = ascii("topic.resolved");
    public static final byte[] EVENT_SUBSCRIBE = ascii("event.subscribe");
    public static final byte[] EVENT_SUBSCRIBED = ascii("event.subscribed");
    public static final byte[] EVENT = ascii("event");
    public static final byte[] EVENT_REPLAY = ascii("event.replay");
    public static final byte[] DISPATCH = ascii("dispatch");
    public static final byte[] PUBLISH = ascii("publish");
    public static final byte[] PUBLISHED = ascii("published");
    public static final byte[] NOTIFY = ascii("notify");

    private Verbs() {
    }

}
