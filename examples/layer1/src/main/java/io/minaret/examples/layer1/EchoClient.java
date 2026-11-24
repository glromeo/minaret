package io.minaret.examples.layer1;

import io.minaret.messaging.ChannelClient;
import io.minaret.messaging.Channels;
import io.minaret.messaging.ClientInfo;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The Layer-1 echo client: resolves LAYER_1, fires every request, then checks that the replies arrived
 * in send order.
 *
 * Replies are kept in arrival order and nothing assigns an arriving reply to "the Nth request". Only
 * afterwards is that order compared with the send order, so the check really tests sequencing. Pairing
 * each arrival with the next pending request would relabel a reordered reply as correct instead of
 * catching it.
 */
final class EchoClient {

    private static final Logger LOG = System.getLogger(EchoClient.class.getName());

    private EchoClient() {
    }

    static void run() {
        try (Channels bus = Channels.connect(ClientInfo.create("layer1-echo-client")).join()) {
            ChannelClient client = bus.connectChannel(Main.CHANNEL).join();
            LOG.log(Level.INFO, "EchoClient connected. Sending echo requests to channel ''{0}'' ({1}).", Main.CHANNEL, client.channelRouteId());

            List<String> texts = List.of("hello", "world", "layer 1 works");
            List<String> expected = texts.stream().map(text -> "echo: " + text).toList();

            ConcurrentLinkedQueue<String> arrived = new ConcurrentLinkedQueue<>();
            CompletableFuture<Void> allArrived = new CompletableFuture<>();

            client.subscribe("echo", (header, body) -> {
                arrived.add(body.asText());
                if (arrived.size() >= expected.size()) allArrived.complete(null);
            }).join();

            for (String text : texts) {
                LOG.log(Level.INFO, "→ sending: \"{0}\"", text);
                client.dispatch("echo", text).join();
            }

            try {
                allArrived.orTimeout(5, TimeUnit.SECONDS).join();
            } catch (CompletionException e) {
                if (!(e.getCause() instanceof TimeoutException)) throw e;
                LOG.log(Level.ERROR, "Timed out waiting for all {0} echo replies. Is EchoProvider running?", expected.size());
            }

            List<String> actual = List.copyOf(arrived);
            actual.forEach(reply -> LOG.log(Level.INFO, "← received: \"{0}\"", reply));

            if (actual.equals(expected))
                LOG.log(Level.INFO, "Sequencing verified: {0} replies arrived in send order.", expected.size());
            else
                LOG.log(Level.ERROR, "Sequencing violated! Expected {0}, got {1}.", expected, actual);

            LOG.log(Level.INFO, "Echo round-trips complete. Exiting.");
        }
    }

}
