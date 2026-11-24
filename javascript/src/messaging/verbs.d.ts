export declare const Verbs: {
    readonly ClientRegister: "client.register";
    readonly ClientRegistered: "client.registered";
    readonly ChannelRegister: "channel.register";
    readonly ChannelRegistered: "channel.registered";
    readonly ChannelResolve: "channel.resolve";
    readonly ChannelResolved: "channel.resolved";
    readonly TopicRegister: "topic.register";
    readonly TopicRegistered: "topic.registered";
    readonly TopicList: "topic.list";
    readonly TopicListed: "topic.listed";
    readonly TopicSubscribe: "topic.subscribe";
    readonly TopicSubscribed: "topic.subscribed";
    readonly TopicUnsubscribe: "topic.unsubscribe";
    readonly TopicUnsubscribed: "topic.unsubscribed";
    readonly TopicResolved: "topic.resolved";
    readonly EventSubscribe: "event.subscribe";
    readonly EventSubscribed: "event.subscribed";
    readonly Event: "event";
    readonly EventReplay: "event.replay";
    readonly Dispatch: "dispatch";
    readonly Publish: "publish";
    readonly Published: "published";
    readonly Notify: "notify";
};

export type Verb = (typeof Verbs)[keyof typeof Verbs];
