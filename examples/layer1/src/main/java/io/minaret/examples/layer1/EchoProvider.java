package io.minaret.examples.layer1;

import io.minaret.messaging.ChannelProvider;
import io.minaret.messaging.Channels;
import io.minaret.messaging.ClientInfo;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.concurrent.CountDownLatch;

/**
 * The Layer-1 echo provider: claims the channel name LAYER_1 and answers every request on its echo
 * topic.
 *
 * The client resolves that name to this provider's RouteId when it connects. That is the discovery
 * step Layer 0 hardcoded as Constants.ROUTE_ID.
 */
final class EchoProvider {

    private static final Logger LOG = System.getLogger(EchoProvider.class.getName());

    private EchoProvider() {
    }

    static void run() throws InterruptedException {
        Channels bus = Channels.connect(ClientInfo.create("layer1-echo-provider")).join();
        ChannelProvider provider = bus.createChannel(Main.CHANNEL).join();

        provider.subscribe("echo", (header, body) -> {
            String payload = body.asText();
            LOG.log(Level.INFO, "Echo request from {0}: \"{1}\"", header.from(), payload);

            // No recipient: the reply goes to the topic, and the broker narrows it to the one client
            // awaiting this request's correlation id. dispatch picks that id up from the request being
            // handled.
            provider.dispatch("echo", "echo: " + payload);
        }).join();

        LOG.log(Level.INFO, "EchoProvider online on channel ''{0}'' as {1}. Waiting for requests... (Ctrl+C to quit)", Main.CHANNEL, provider.channelRouteId());

        new CountDownLatch(1).await();
    }

}
