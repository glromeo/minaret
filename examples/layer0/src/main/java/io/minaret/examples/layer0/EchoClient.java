package io.minaret.examples.layer0;

import io.minaret.messaging.ClientInfo;
import io.minaret.messaging.Destination;
import io.minaret.messaging.Header;
import io.minaret.messaging.Router;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The Layer-0 echo client: sends echo requests to Constants.ROUTE_ID and prints the replies.
 *
 * Correlation is deliberately crude: a reply is recognised by Header.type alone, with one request in
 * flight at a time. Layer 1 replaces this with reply ids, so several requests can be in flight and each
 * is awaited on its own.
 */
final class EchoClient {

    private static final Logger LOG = System.getLogger(EchoClient.class.getName());

    /** The one in-flight request's reply, swapped in per request and completed by the handler. */
    private static volatile CompletableFuture<String> pendingReply = new CompletableFuture<>();

    private EchoClient() {
    }

    static void run(Router router) throws InterruptedException {
        router.handleMessage().add(message -> {
            if (Constants.REPLY_TYPE.equals(message.header().type())) pendingReply.complete(message.body().asText());
        });

        router.connect(ClientInfo.create(Constants.CLIENT_IDENTITY)).join();
        LOG.log(Level.INFO, "EchoClient connected. Sending echo requests to provider {0}.", Constants.ROUTE_ID);

        for (String text : List.of("hello", "world", "layer 0 works")) {
            pendingReply = new CompletableFuture<>();

            LOG.log(Level.INFO, "→ sending: \"{0}\"", text);
            router.sendMessage(Header.of(Constants.REQUEST_TYPE, Destination.ofId(Constants.ROUTE_ID)), text);

            try {
                LOG.log(Level.INFO, "← received: \"{0}\"", pendingReply.orTimeout(5, TimeUnit.SECONDS).join());
            } catch (CompletionException e) {
                if (!(e.getCause() instanceof TimeoutException)) throw e;
                LOG.log(Level.ERROR, "Timed out waiting for echo reply to \"{0}\". Is EchoProvider running?", text);
            }

            Thread.sleep(500);
        }

        LOG.log(Level.INFO, "Echo round-trips complete. Exiting.");
        router.close();
    }

}
