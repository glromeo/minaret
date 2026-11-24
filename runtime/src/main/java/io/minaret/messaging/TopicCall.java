package io.minaret.messaging;

import com.fasterxml.jackson.databind.JsonNode;
import io.minaret.util.Json;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * The precomputed plan for invoking one annotated method — topic, kind, and which arguments are payload
 * rather than addressing.
 *
 * Built once when the proxy is created, so a call costs a map lookup rather than re-reading annotations,
 * and so an ill-formed contract fails at the point the interface is introduced instead of on whichever
 * later call first reaches the bad method.
 *
 * None of the four kinds is spelled in an annotation. The handle the proxy was built on fixes the
 * direction, and the method's own signature then picks the delivery, so there is nothing to state twice
 * and nothing that can disagree.
 */
final class TopicCall {

    private final String topic;
    private final Kind kind;
    private final int payloadStart;
    private final int payloadEnd;

    /**
     * The declared answer type for a request, so the reply can be converted to it. Null for every other
     * kind.
     */
    private final Class<?> replyType;

    private TopicCall(String topic, Kind kind, int payloadStart, int payloadEnd, Class<?> replyType) {
        this.topic = topic;
        this.kind = kind;
        this.payloadStart = payloadStart;
        this.payloadEnd = payloadEnd;
        this.replyType = replyType;
    }

    /**
     * Plans every method on the contract, or throws describing the first one that cannot be planned.
     *
     * Every method must be annotated: a contract is the set of topics it names, so an unannotated member
     * is a gap in it rather than an extra.
     */
    static Map<Method, TopicCall> mapFor(Class<?> contract, boolean provider) {
        if (!contract.isInterface())
            throw new IllegalArgumentException("'" + contract.getSimpleName() + "' must be an interface — a proxy has no implementation to inherit.");

        Channels.channelNameOf(contract);

        Map<Method, TopicCall> map = new HashMap<>();
        for (Method method : contract.getMethods()) {
            if (method.isSynthetic() || method.isDefault()) continue;

            Topic topic = method.getAnnotation(Topic.class);
            if (topic == null)
                throw new IllegalArgumentException("'" + contract.getSimpleName() + "." + method.getName() + "' has no @Topic annotation.");

            map.put(method, plan(contract, method, topic.value(), provider));
        }

        if (map.isEmpty())
            throw new IllegalArgumentException("'" + contract.getSimpleName() + "' declares no @Topic methods.");

        return map;
    }

    private static TopicCall plan(Class<?> contract, Method method, String topic, boolean provider) {
        String where = "'" + contract.getSimpleName() + "." + method.getName() + "'";
        Class<?>[] parameters = method.getParameterTypes();
        Class<?> returns = method.getReturnType();

        boolean answered = returns == CompletableFuture.class;
        if (!answered && returns != void.class)
            throw new IllegalArgumentException(where + " must return void or CompletableFuture<T> — every topic call crosses the wire.");

        int end = parameters.length;

        // A leading UUID is addressing, not payload: on a provider it names the one client to reach.
        boolean directed = end > 0 && parameters[0] == UUID.class;

        if (directed && !provider)
            throw new IllegalArgumentException(where + " takes a recipient, but a client may only address its channel's provider.");

        if (answered && provider)
            throw new IllegalArgumentException(
                    where + " expects an answer, but a provider cannot ask on its own topic — "
                            + "the Broker reads every frame it sends there as a reply."
            );

        Kind kind = provider
                ? directed ? Kind.NOTIFY : Kind.PUBLISH
                : answered ? Kind.REQUEST : Kind.SEND;

        return new TopicCall(topic, kind, directed ? 1 : 0, end, answered ? replyTypeOf(method, where) : null);
    }

    /**
     * The T of a CompletableFuture&lt;T&gt; return. A raw CompletableFuture is rejected rather than
     * treated as Object: the reply has to be deserialized into something, and guessing is how a caller
     * ends up with a LinkedHashMap where it declared a record.
     */
    private static Class<?> replyTypeOf(Method method, String where) {
        Type generic = method.getGenericReturnType();
        if (!(generic instanceof ParameterizedType parameterized))
            throw new IllegalArgumentException(where + " must declare its answer type as CompletableFuture<T>, not a raw CompletableFuture.");

        Type argument = parameterized.getActualTypeArguments()[0];
        if (argument instanceof Class<?> type) return type;
        if (argument instanceof ParameterizedType nested && nested.getRawType() instanceof Class<?> raw) return raw;

        throw new IllegalArgumentException(where + " has an answer type this proxy cannot deserialize: " + argument);
    }

    /**
     * Answers the CompletableFuture the caller holds, or null for a void method.
     */
    Object invoke(ChannelBase channel, Object[] args) {
        Object[] arguments = args == null ? new Object[0] : args;
        Object body = pack(arguments);

        return switch (kind) {
            case REQUEST -> channel.callOnTopic(topic, body).thenApply(reply -> Json.convert((JsonNode) reply, replyType));
            case SEND -> voidResult(channel.sendOnTopic(Verbs.DISPATCH, topic, body, null));
            case PUBLISH -> voidResult(channel.publishOnTopic(topic, body));
            case NOTIFY -> voidResult(channel.sendOnTopic(Verbs.NOTIFY, topic, body, (UUID) arguments[0]));
        };
    }

    /**
     * A fire-and-forget call answers nothing, but its send must still be observed — a failure swallowed
     * here would be a message that silently never left.
     */
    private static Object voidResult(CompletableFuture<Void> sent) {
        sent.whenComplete((ignored, failure) -> {
            if (failure != null)
                System.getLogger(TopicCall.class.getName())
                        .log(System.Logger.Level.WARNING, "Topic send failed", failure);
        });

        return null;
    }

    /**
     * The payload arguments alone — a leading recipient is stripped, since it is not something the far
     * side reads.
     *
     * One argument is the body itself; several pack positionally as a JSON array. Wrapping the single
     * case in a one-element array would make every handler unwrap a shape that exists only for the rarer
     * one. {@link TopicBinding} is the exact inverse, and the two rules must agree or a multi-argument
     * topic is unusable end to end.
     */
    private Object pack(Object[] args) {
        int count = payloadEnd - payloadStart;
        if (count == 0) return null;
        if (count == 1) return args[payloadStart];

        Object[] packed = new Object[count];
        System.arraycopy(args, payloadStart, packed, 0, count);
        return packed;
    }

    /**
     * What one annotated method does when called. Four shapes, falling out of handle times signature.
     */
    private enum Kind {

        /**
         * Client to provider, expecting an answer.
         */
        REQUEST,

        /**
         * Client to provider, fire-and-forget. Sent with no correlation id, since one the Broker records
         * and nobody consumes is a leak in its pending-reply table.
         */
        SEND,

        /**
         * Provider to every subscriber.
         */
        PUBLISH,

        /**
         * Provider to one client, named by a leading UUID.
         */
        NOTIFY

    }

}
