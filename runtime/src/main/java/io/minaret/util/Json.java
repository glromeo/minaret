package io.minaret.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;

/**
 * The one shared JSON convention for this runtime, the counterpart of Runtime/Utils/Json.cs. It is the
 * body serializer (Wire goes through it) and the discovery-contract serializer; Wire owns the envelope
 * encoding, this owns the payload encoding.
 *
 * The two settings that matter are the ones that make Jackson agree with System.Text.Json as configured
 * on the C# side: property names are matched case-insensitively, and an unknown property is ignored
 * rather than fatal. The latter is not a convenience — it is what lets a newer Broker add a field to an
 * ack body without breaking every older client, which System.Text.Json gives by default and Jackson
 * does not.
 *
 * There is no camelCase naming strategy here even though the C# side sets one: C# property names are
 * PascalCase and need translating, Java field names are already camelCase and do not.
 */
public final class Json {

    public static final ObjectMapper MAPPER = JsonMapper.builder()
            .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    private Json() {
    }

    public static String stringify(Object value) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Failed to serialize " + value.getClass().getName(), e);
        }
    }

    public static byte[] stringifyToUtf8Bytes(Object value) {
        return stringify(value).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Parses json as T, or throws. The strict counterpart of {@link #tryParse} — used where a failure
     * means the caller cannot proceed at all, such as the discovery contract.
     */
    public static <T> T parse(String json, Class<T> type) {
        T value = tryParse(json, type);
        if (value == null)
            throw new IllegalStateException("Failed to deserialize payload as " + type.getSimpleName() + ": payload was empty or invalid.");

        return value;
    }

    /**
     * Parses json as T, answering null when it is empty or is not valid JSON for T. Total by design, the
     * same contract Wire.tryParseBody has: a garbage body must not throw into a receive loop.
     */
    public static <T> T tryParse(String json, Class<T> type) {
        if (json == null || json.isEmpty()) return null;
        try {
            return MAPPER.readValue(json, type);
        } catch (JsonProcessingException e) {
            return null;
        }
    }

    /**
     * Converts an already-parsed tree into T — how a handler reads a body it received as a JsonNode,
     * mirroring JsonElement.Deserialize on the C# side. Answers null rather than throwing, for the same
     * reason {@link #tryParse} does.
     */
    public static <T> T convert(JsonNode node, Class<T> type) {
        if (node == null || node.isNull()) return null;
        try {
            return MAPPER.treeToValue(node, type);
        } catch (JsonProcessingException e) {
            return null;
        }
    }

    /**
     * {@link #convert(JsonNode, Class)} for a generic or parameterized target, which a Class token
     * cannot express — needed by the annotated surface, where a bound method's parameter may be a
     * List or a Map.
     */
    public static Object convert(JsonNode node, JavaType type) {
        if (node == null || node.isNull()) return null;
        try {
            return MAPPER.readerFor(type).readValue(node);
        } catch (java.io.IOException e) {
            return null;
        }
    }

    public static JsonNode toTree(Object value) {
        return MAPPER.valueToTree(value);
    }

}
