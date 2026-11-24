package io.minaret.messaging;

import com.fasterxml.jackson.databind.JsonNode;
import io.minaret.util.Json;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/**
 * The inbound mirror of {@link ChannelProxy}, turning each annotated method into a topic handler.
 *
 * A bound method's shape decides what it answers, symmetrically with the outbound rules: a non-void
 * return replies with its value, void answers nothing, and — new here, mirroring the outbound
 * leading-UUID-means-recipient rule — a leading UUID parameter receives the sender's RouteId. That last
 * one is what lets a handler key per-client state without reaching for the header.
 */
final class TopicBinding {

    private static final Logger LOG = System.getLogger(TopicBinding.class.getName());

    private TopicBinding() {
    }

    static CompletableFuture<Void> bindAll(ChannelBase channel, Object handlers) {
        Class<?> type = handlers.getClass();

        // Binding an instance to a handle on a different channel would register its topics under a name
        // the annotation never mentions — the right methods silently serving the wrong channel.
        String declared = Channels.channelNameOf(type);
        if (!declared.equals(channel.channelName()))
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                    "'" + type.getSimpleName() + "' declares channel '" + declared
                            + "', but this handle is on '" + channel.channelName() + "'."
            ));

        List<Method> bound = new ArrayList<>();
        for (Method method : type.getMethods())
            if (method.getAnnotation(Topic.class) != null) bound.add(method);

        if (bound.isEmpty())
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("'" + type.getSimpleName() + "' declares no @Topic methods to bind.")
            );

        CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
        for (Method method : bound) {
            String topic = method.getAnnotation(Topic.class).value();

            // Whether this registers the topic or subscribes to it is the handle's answer, not ours —
            // the same split subscribe already makes between a provider and a client.
            chain = chain.thenCompose(ignored -> channel.subscribe(topic, handlerFor(channel, handlers, method, topic)));
        }

        return chain;
    }

    private static BiConsumer<Header, JsonNode> handlerFor(ChannelBase channel, Object target, Method method, String topic) {
        Parameter[] parameters = method.getParameters();
        boolean directed = parameters.length > 0 && parameters[0].getType() == UUID.class;
        boolean answers = method.getReturnType() != void.class;

        method.setAccessible(true);

        return (header, body) -> {
            Object result;
            try {
                result = method.invoke(target, unpack(header, body, parameters, directed));
            } catch (IllegalAccessException | InvocationTargetException e) {
                Throwable cause = e instanceof InvocationTargetException invocation ? invocation.getCause() : e;
                LOG.log(Level.WARNING, "Handler for topic '" + topic + "' threw", cause);
                return;
            }

            if (!answers) return;

            // The correlation id is taken from the received header explicitly rather than from the
            // in-flight request, because the reply may be sent after an async hop — and resting every
            // reply's correctness on where a handler happens to yield is a poor bargain.
            if (result instanceof CompletableFuture<?> pending)
                pending.thenAccept(value -> channel.sendOnTopic(Verbs.DISPATCH, topic, value, header.replyTo()));
            else
                channel.sendOnTopic(Verbs.DISPATCH, topic, result, header.replyTo());
        };
    }

    /**
     * The exact inverse of TopicCall's packing — one parameter takes the whole body, several read
     * positionally off a JSON array. The two rules have to agree or a multi-argument topic is unusable
     * end to end.
     *
     * A leading UUID is filled from the sender's RouteId rather than from the body, and does not count
     * toward the payload parameters.
     */
    private static Object[] unpack(Header header, JsonNode body, Parameter[] parameters, boolean directed) {
        Object[] args = new Object[parameters.length];
        int start = 0;

        if (directed) {
            args[0] = header.from();
            start = 1;
        }

        int payload = parameters.length - start;
        if (payload == 0) return args;

        if (payload == 1) {
            args[start] = convert(body, parameters[start]);
            return args;
        }

        for (int i = 0; i < payload; i++) args[start + i] = convert(body.get(i), parameters[start + i]);

        return args;
    }

    private static Object convert(JsonNode node, Parameter parameter) {
        return Json.convert(node, Json.MAPPER.getTypeFactory().constructType(parameter.getParameterizedType()));
    }

}
