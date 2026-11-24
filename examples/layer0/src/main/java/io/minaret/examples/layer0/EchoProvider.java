package io.minaret.examples.layer0;

import io.minaret.messaging.ClientInfo;
import io.minaret.messaging.Destination;
import io.minaret.messaging.Header;
import io.minaret.messaging.Router;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * The Layer-0 echo provider: registers under Constants.ROUTE_ID and answers every echo request to its
 * sender.
 *
 * Everything here is real: the WSS transport, broker discovery, the register handshake and
 * Header.to routing. The only stand-in is how the client finds the provider, which is what Layer 1's
 * channels provide.
 */
final class EchoProvider {

    private static final Logger LOG = System.getLogger(EchoProvider.class.getName());

    private EchoProvider() {
    }

    static void run(Router router) throws InterruptedException {
        router.handleMessage().add(message -> {
            Header header = message.header();
            if (!Constants.REQUEST_TYPE.equals(header.type())) return;

            // from is null on broker-originated messages: there is nobody to reply to.
            if (header.from() == null) {
                LOG.log(Level.WARNING, "Ignoring {0} with no sender to reply to", header.type());
                return;
            }

            String payload = message.body().asText();
            LOG.log(Level.INFO, "Echo request from {0}: \"{1}\"", header.from(), payload);
            router.sendMessage(Header.of(Constants.REPLY_TYPE, Destination.ofId(header.from())), "echo: " + payload);
        });

        router.connect(new ClientInfo(Constants.ROUTE_ID, Constants.PROVIDER_IDENTITY, 0, Map.of())).join();
        LOG.log(Level.INFO, "EchoProvider online as {0}. Waiting for requests... (Ctrl+C to quit)", Constants.ROUTE_ID);

        new CountDownLatch(1).await();
    }

}
