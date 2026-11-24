package io.minaret.messaging;

/**
 * The Header.type vocabulary, shared with Runtime and the Broker so the three can't drift on a string
 * literal.
 *
 * type is the verb — what the recipient should do with the message — and nothing else. Topic identity
 * lives in Header.to, so "what is this?" and "which topic is this?" are answered by separate fields.
 *
 * Control-plane verbs are noun.verb and are Broker-addressed (an empty destination). Data-plane verbs
 * are bare and carry a topic in to.
 */
public final class Verbs {

    public static final String CLIENT_REGISTER = "client.register";
    public static final String CLIENT_REGISTERED = "client.registered";

    public static final String CHANNEL_REGISTER = "channel.register";
    public static final String CHANNEL_REGISTERED = "channel.registered";
    public static final String CHANNEL_RESOLVE = "channel.resolve";
    public static final String CHANNEL_RESOLVED = "channel.resolved";

    public static final String TOPIC_REGISTER = "topic.register";
    public static final String TOPIC_REGISTERED = "topic.registered";
    public static final String TOPIC_LIST = "topic.list";
    public static final String TOPIC_LISTED = "topic.listed";
    public static final String TOPIC_SUBSCRIBE = "topic.subscribe";
    public static final String TOPIC_SUBSCRIBED = "topic.subscribed";
    public static final String TOPIC_UNSUBSCRIBE = "topic.unsubscribe";
    public static final String TOPIC_UNSUBSCRIBED = "topic.unsubscribed";

    /**
     * The unsolicited /channel/topic resolution ack — not a reply to any request, so it carries no
     * replyTo.
     */
    public static final String TOPIC_RESOLVED = "topic.resolved";

    public static final String EVENT_SUBSCRIBE = "event.subscribe";
    public static final String EVENT_SUBSCRIBED = "event.subscribed";

    /**
     * The delivery verb for every live event. Which event it is lives in the body's name, not here — see
     * EventNames.
     */
    public static final String EVENT = "event";

    /**
     * The whole subscribe-time snapshot in one frame, as opposed to one EVENT frame per occurrence.
     */
    public static final String EVENT_REPLAY = "event.replay";

    public static final String DISPATCH = "dispatch";
    public static final String PUBLISH = "publish";
    public static final String PUBLISHED = "published";

    /**
     * The provider's unsolicited send to one client — publish reaches every subscriber, dispatch answers
     * someone who asked, and this is neither. It uses dispatch's header exactly, with replyTo carrying
     * the recipient RouteId: a notification correlates to nothing, so replyTo is free, and the verb is
     * what says so.
     */
    public static final String NOTIFY = "notify";

    private Verbs() {
    }

}
