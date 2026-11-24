package io.minaret.benchmark;

import io.minaret.messaging.Destination;
import io.minaret.messaging.Header;
import io.minaret.messaging.Router;

/**
 * The dispatch benchmark's provider: answers every request to its sender with the request's own body.
 *
 * The body is echoed as the JsonNode it arrived as, never deserialized, so the provider's cost is one
 * parse and one render per request: as little client work as a reply can cost.
 */
final class DispatchProvider {

    static final String REQUEST = "benchmark-request";
    static final String REPLY = "benchmark-reply";

    private DispatchProvider() {
    }

    static void serve(Router router) {
        router.handleMessage().add(message -> {
            Header header = message.header();
            // from is null on broker-originated frames: nobody to reply to.
            if (!REQUEST.equals(header.type()) || header.from() == null) return;

            router.sendMessage(Header.of(REPLY, Destination.ofId(header.from())), message.body());
        });
    }

}
