package io.minaret.messaging;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Map;

/**
 * The implementation behind ChannelBase.proxy: every call becomes the send its {@link TopicCall} plans.
 *
 * This is the half of Layer 2 the JavaScript port could not carry across — decorators there need the
 * transpile step that package exists to avoid. Java has runtime annotations and a reflective proxy in
 * the JDK, so the C# DispatchProxy design ports directly rather than needing a redesign.
 */
final class ChannelProxy implements InvocationHandler {

    private final ChannelBase channel;
    private final Map<Method, TopicCall> calls;

    private ChannelProxy(ChannelBase channel, Map<Method, TopicCall> calls) {
        this.channel = channel;
        this.calls = calls;
    }

    static <T> T create(ChannelBase channel, Class<T> contract) {
        // Planned before the proxy exists, so an ill-formed contract throws from here rather than on
        // whichever later call first reaches the bad method.
        Map<Method, TopicCall> calls = TopicCall.mapFor(contract, channel.isProvider());

        Object proxy = Proxy.newProxyInstance(
                contract.getClassLoader(),
                new Class<?>[]{contract},
                new ChannelProxy(channel, calls)
        );

        return contract.cast(proxy);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) {
        // toString/hashCode/equals are the proxy's own, not topics.
        if (method.getDeclaringClass() == Object.class) {
            return switch (method.getName()) {
                case "toString" -> "ChannelProxy(" + channel.channelName() + ")";
                case "hashCode" -> System.identityHashCode(proxy);
                case "equals" -> proxy == args[0];
                default -> throw new UnsupportedOperationException(method.getName());
            };
        }

        TopicCall call = calls.get(method);
        if (call == null) throw new IllegalStateException("'" + method.getName() + "' was never planned as a topic call.");

        return call.invoke(channel, args);
    }

}
