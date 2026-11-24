package io.minaret.broker.idl;

import io.micronaut.json.tree.JsonNode;
import io.micronaut.serde.ObjectMapper;
import jakarta.inject.Singleton;

/**
 * Checks an IDL document's envelope, the port of C#'s IdlValidator: an array of channels, each with
 * a non-empty name and at least one topic. The grammar below that is the editor's
 * (Monitoring/src/idl/), deliberately not duplicated here.
 *
 * The messages are C#'s, word for word, except a parse failure's, which is the parser's own:
 * Jackson's here, System.Text.Json's there.
 */
@Singleton
public final class IdlValidator {

    private final ObjectMapper mapper;

    public IdlValidator(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    /** null when content is a valid IDL envelope, otherwise why not. */
    public String validate(byte[] content) {
        JsonNode root;
        try {
            root = mapper.readValue(content, JsonNode.class);
        } catch (Exception failure) {
            return failure.getMessage() == null ? "the document is not valid JSON." : failure.getMessage();
        }

        if (root == null || !root.isArray()) return "an IDL document is an array of channels.";
        if (root.size() == 0) return "an IDL document declares at least one channel.";

        for (int index = 0; index < root.size(); index++) {
            String error = validateChannel(root.get(index), index);
            if (error != null) return error;
        }
        return null;
    }

    private static String validateChannel(JsonNode channel, int index) {
        if (!channel.isObject()) return "channels[" + index + "] is not an object.";

        JsonNode name = channel.get("name");
        if (name == null || !name.isString() || name.getStringValue().isBlank())
            return "channels[" + index + "].name is required and must be a non-empty string.";

        JsonNode space = channel.get("namespace");
        if (space != null && !space.isString()) return "channels[" + index + "].namespace must be a string.";

        JsonNode types = channel.get("types");
        if (types != null && !types.isObject()) return "channels[" + index + "].types must be an object.";

        JsonNode topics = channel.get("topics");
        if (topics == null || !topics.isObject()) return "channels[" + index + "].topics is required and must be an object.";
        if (topics.size() == 0) return "channels[" + index + "].topics declares no topic.";

        return null;
    }

}
