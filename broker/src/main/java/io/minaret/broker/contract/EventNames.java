package io.minaret.broker.contract;

/**
 * Runtime/Messaging/Events.cs's EventNames. Strings, unlike Verbs: they appear only inside JSON
 * bodies (an event's name, an event.subscribe's name), never as a frame's type.
 */
public final class EventNames {

    public static final String CLIENT_CONNECTED = "client.connected";
    public static final String CLIENT_DISCONNECTED = "client.disconnected";
    public static final String CHANNEL_REGISTERED = "channel.registered";
    public static final String CHANNEL_REMOVED = "channel.removed";
    public static final String TOPIC_REGISTERED = "topic.registered";
    public static final String TOPIC_SUBSCRIBED = "topic.subscribed";
    public static final String TOPIC_UNSUBSCRIBED = "topic.unsubscribed";

    private EventNames() {
    }

}
