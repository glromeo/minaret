package io.minaret.benchmark;

import java.time.Duration;
import java.util.Locale;

/**
 * One run's settings, parsed from `--name=value` arguments. Unknown names are ignored.
 *
 * Main reads it once and hands each worker only what that worker needs.
 */
record Options(
        Mode mode,
        int clients,
        int providers,
        int messagesPerClient,
        int perClientInFlight,
        int subscribers,
        int publishes,
        int payloadBytes,
        int connectConcurrency,
        int httpClients,
        Duration replyTimeout
) {

    /**
     * Which broker path a run exercises. They are separate benchmarks, not variations of one: a fanout
     * run's "messages" are deliveries, a dispatch run's are round trips, so the numbers do not compare.
     */
    enum Mode {

        /** Point-to-point request/reply: Session.forwardToRoute, no topic or subscriber list. */
        DISPATCH,

        /** One provider's publish fanned out to every subscriber of one topic: Session.handlePublish. */
        FANOUT
    }

    static Options parse(String[] args) {
        Mode mode = Mode.DISPATCH;
        int clients = 1000, providers = 1000, messagesPerClient = 1000, perClientInFlight = 10;
        int subscribers = 500, publishes = 2000, payloadBytes = 1024, connectConcurrency = 50;
        int httpClients = Runtime.getRuntime().availableProcessors();
        int replyTimeoutSeconds = 30;

        for (String arg : args) {
            int eq = arg.indexOf('=');
            if (!arg.startsWith("--") || eq < 0) continue;

            String value = arg.substring(eq + 1);
            switch (arg.substring(2, eq)) {
                case "mode" -> mode = Mode.valueOf(value.toUpperCase(Locale.ROOT));
                case "clients" -> clients = Integer.parseInt(value);
                case "providers" -> providers = Integer.parseInt(value);
                case "messagesPerClient" -> messagesPerClient = Integer.parseInt(value);
                case "perClientInFlight" -> perClientInFlight = Integer.parseInt(value);
                case "subscribers" -> subscribers = Integer.parseInt(value);
                case "publishes" -> publishes = Integer.parseInt(value);
                case "payloadBytes" -> payloadBytes = Integer.parseInt(value);
                case "connectConcurrency" -> connectConcurrency = Integer.parseInt(value);
                case "httpClients" -> httpClients = Integer.parseInt(value);
                case "replyTimeoutSeconds" -> replyTimeoutSeconds = Integer.parseInt(value);
                default -> {
                }
            }
        }

        return new Options(mode, clients, providers, messagesPerClient, perClientInFlight, subscribers, publishes,
                payloadBytes, connectConcurrency, httpClients, Duration.ofSeconds(replyTimeoutSeconds));
    }

}
