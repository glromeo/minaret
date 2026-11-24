package io.minaret.examples.layer1;

/**
 * The Layer-1 echo example's entry point: `--provider` or `--client`, each run as its own process.
 *
 * Both sides discover the broker in the realm MESSAGEBUS_REALM names (Default when unset), so run the
 * broker in the same realm first. The client can start before the provider: connectChannel waits
 * broker-side until the channel is claimed.
 */
public final class Main {

    /** The channel the provider claims and the client resolves. */
    static final String CHANNEL = "LAYER_1";

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        String mode = args.length == 1 ? args[0] : "";
        switch (mode) {
            case "--provider" -> EchoProvider.run();
            case "--client" -> EchoClient.run();
            default -> {
                System.err.println("Usage: layer1 --client | --provider");
                System.exit(1);
            }
        }
    }

}
