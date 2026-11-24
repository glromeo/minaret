package io.minaret.benchmark;

import java.util.UUID;

/**
 * The body every benchmark message carries: a correlation id plus filler of the configured size.
 *
 * The id lives in the body because Header.replyTo means "route the reply to this", not "this is my
 * message id". DispatchClient and FanoutRecorder pair a send with its reply or delivery by it, and read
 * only that field back (body.path("id")), so measuring doesn't add a full deserialization per frame.
 */
record Payload(UUID id, String data) {

    static String filler(int bytes) {
        return "x".repeat(bytes);
    }

    static UUID idOf(com.fasterxml.jackson.databind.JsonNode body) {
        return UUID.fromString(body.path("id").asText());
    }

}
