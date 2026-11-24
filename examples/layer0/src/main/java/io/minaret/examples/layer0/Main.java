package io.minaret.examples.layer0;

import io.minaret.discovery.BrokerDiscovery;
import io.minaret.discovery.Realm;
import io.minaret.messaging.Router;
import io.minaret.messaging.WebSocketConnection;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;

/**
 * The Layer-0 echo example's entry point: `--provider` or `--client`, each run as its own process.
 *
 * Both sides discover the broker in the realm MESSAGEBUS_REALM names (Default when unset), so run the
 * broker in the same realm first.
 */
public final class Main {

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        String mode = args.length == 1 ? args[0] : "";
        switch (mode) {
            case "--provider" -> EchoProvider.run(router());
            case "--client" -> EchoClient.run(router());
            default -> {
                System.err.println("Usage: layer0 --client | --provider");
                System.exit(1);
            }
        }
    }

    /**
     * A router over the discovered broker. Layer 0 has no Channels handle, so this is the same
     * connection factory Channels.connect builds, without the channel layer on top.
     */
    private static Router router() {
        String realm = Realm.resolve();
        return new Router(() -> {
            try {
                int port = BrokerDiscovery.discover(realm).port();
                return new WebSocketConnection(URI.create("wss://localhost:" + port + "/msgbus"), WebSocketConnection.trustingLocalhostClient());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

}
