package io.minaret.broker.e2e;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.runtime.server.EmbeddedServer;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.micronaut.test.support.TestPropertyProvider;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The /idl endpoints. One server serves the class and the directory is emptied before each test,
 * which gives each test a fresh repository because IdlRepository keeps no state outside the files.
 */
@MicronautTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class IdlEndpointsTest implements TestPropertyProvider {

    private static final String ONE_CHANNEL = "[{\"name\":\"Trading.Orders\",\"topics\":{\"submit\":{\"name\":\"Submit\",\"inbound\":{},\"outbound\":null}}}]";
    private static final HttpClient HTTP = HttpClient.newHttpClient();
    private static final ObjectMapper JSON = new ObjectMapper();

    @Inject
    EmbeddedServer server;

    private Path root;

    @Override
    public Map<String, String> getProperties() {
        Map<String, String> properties = new HashMap<>(MessageRoutingTest.brokerProperties());
        root = Path.of(properties.get("messagebus.idl.path"));
        return properties;
    }

    @BeforeEach
    void emptyTheRepository() throws IOException {
        try (Stream<Path> files = Files.list(root)) {
            for (Path file : (Iterable<Path>) files::iterator) Files.delete(file);
        }
    }

    private HttpRequest.Builder request(String path) {
        return HttpRequest.newBuilder(URI.create("http://localhost:" + server.getPort() + path));
    }

    private HttpResponse<String> send(HttpRequest.Builder request) throws Exception {
        return HTTP.send(request.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    }

    private HttpResponse<String> put(String name, String content) throws Exception {
        return put(name, content, null, null);
    }

    private HttpResponse<String> put(String name, String content, String ifMatch, String ifNoneMatch) throws Exception {
        HttpRequest.Builder request = request("/idl/" + name)
                .header("Content-Type", "application/json; charset=utf-8")
                .PUT(HttpRequest.BodyPublishers.ofString(content, StandardCharsets.UTF_8));
        if (ifMatch != null) request.header("If-Match", ifMatch);
        if (ifNoneMatch != null) request.header("If-None-Match", ifNoneMatch);
        return send(request);
    }

    private HttpResponse<String> get(String path) throws Exception {
        return send(request(path).GET());
    }

    private static String etag(HttpResponse<String> written) throws IOException {
        return JSON.readTree(written.body()).get("etag").asText();
    }

    private List<JsonNode> listing() throws Exception {
        HttpResponse<String> response = get("/idl");
        assertEquals(200, response.statusCode());
        List<JsonNode> entries = new ArrayList<>();
        JSON.readTree(response.body()).forEach(entries::add);
        return entries;
    }

    @Test
    void put_aNameNotYetStored_isCreated_withItsLocationAndEntry() throws Exception {
        HttpResponse<String> response = put("orders", ONE_CHANNEL);
        assertEquals(201, response.statusCode());
        assertEquals("/idl/orders", response.headers().firstValue("Location").orElse(null));
        JsonNode entry = JSON.readTree(response.body());
        assertEquals("orders", entry.get("name").asText());
        assertFalse(entry.get("etag").asText().isEmpty());
    }

    @Test
    void put_aNameAlreadyStored_isOk_ratherThanCreated() throws Exception {
        put("orders", ONE_CHANNEL);
        assertEquals(200, put("orders", ONE_CHANNEL.replace("Submit", "Send")).statusCode());
    }

    @Test
    void get_returnsTheStoredBytes_andTheEtagThatNamesThem() throws Exception {
        String formatted = "[\n  {\n    \"name\": \"Trading.Orders\",\n    \"topics\": { \"submit\": { \"name\": \"Submit\", \"inbound\": {}, \"outbound\": null } }\n  }\n]\n";
        HttpResponse<String> written = put("orders", formatted);

        HttpResponse<String> response = get("/idl/orders");
        assertEquals(200, response.statusCode());
        assertTrue(response.headers().firstValue("Content-Type").orElse("").startsWith("application/json"));
        assertEquals(formatted, response.body());
        assertEquals(etag(written), response.headers().firstValue("ETag").orElse(null));
    }

    @Test
    void get_aNameNotStored_isNotFound() throws Exception {
        HttpResponse<String> response = get("/idl/missing");
        assertEquals(404, response.statusCode());
        assertTrue(response.headers().firstValue("Content-Type").orElse("").startsWith("application/problem+json"));
    }

    @Test
    void get_withIfNoneMatchOnTheCurrentEtag_isNotModified() throws Exception {
        String stored = etag(put("orders", ONE_CHANNEL));
        assertEquals(304, send(request("/idl/orders").header("If-None-Match", stored).GET()).statusCode());
    }

    @Test
    void put_withIfMatchOnTheCurrentEtag_replaces() throws Exception {
        String stored = etag(put("orders", ONE_CHANNEL));
        assertEquals(200, put("orders", ONE_CHANNEL.replace("Submit", "Send"), stored, null).statusCode());
    }

    @Test
    void put_withIfMatchOnAStaleEtag_isPreconditionFailed_andKeepsTheStoredDocument() throws Exception {
        String stale = etag(put("orders", ONE_CHANNEL));
        put("orders", ONE_CHANNEL.replace("Submit", "Send"));

        assertEquals(412, put("orders", ONE_CHANNEL.replace("Submit", "Cancel"), stale, null).statusCode());
        assertTrue(get("/idl/orders").body().contains("Send"));
    }

    @Test
    void put_withIfNoneMatchStar_onANameAlreadyStored_isPreconditionFailed() throws Exception {
        put("orders", ONE_CHANNEL);
        assertEquals(412, put("orders", ONE_CHANNEL, null, "*").statusCode());
    }

    @Test
    void put_withIfNoneMatchStar_onAFreeName_isCreated() throws Exception {
        assertEquals(201, put("orders", ONE_CHANNEL, null, "*").statusCode());
    }

    @Test
    void put_textThatIsNotJson_isBadRequest() throws Exception {
        assertEquals(400, put("orders", "{ not json").statusCode());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "{\"name\":\"Trading.Orders\",\"topics\":{}}",
            "[]",
            "[{\"topics\":{\"submit\":{\"name\":\"Submit\",\"inbound\":{},\"outbound\":null}}}]",
            "[{\"name\":\"Trading.Orders\"}]",
            "[{\"name\":\"Trading.Orders\",\"topics\":{}}]",
            "[{\"name\":\"Trading.Orders\",\"types\":[],\"topics\":{\"submit\":{\"name\":\"Submit\",\"inbound\":{},\"outbound\":null}}}]",
    })
    void put_jsonThatIsNotAnIdlDocument_isBadRequest(String content) throws Exception {
        assertEquals(400, put("orders", content).statusCode());
    }

    @Test
    void put_aRejectedDocument_storesNothing() throws Exception {
        put("orders", ONE_CHANNEL);
        put("orders", "{ not json");
        assertTrue(get("/idl/orders").body().contains("Submit"));
    }

    /** Sent with no Content-Type at all, so nothing but the bytes says it is UTF-8. */
    @Test
    void put_aDocumentSavedWithAUtf8Bom_storesTheDocumentWithoutIt() throws Exception {
        byte[] json = ONE_CHANNEL.getBytes(StandardCharsets.UTF_8);
        byte[] withBom = new byte[json.length + 3];
        withBom[0] = (byte) 0xEF;
        withBom[1] = (byte) 0xBB;
        withBom[2] = (byte) 0xBF;
        System.arraycopy(json, 0, withBom, 3, json.length);

        HttpResponse<String> response = send(request("/idl/orders").PUT(HttpRequest.BodyPublishers.ofByteArray(withBom)));
        assertEquals(201, response.statusCode());
        assertEquals(ONE_CHANNEL, get("/idl/orders").body());
    }

    @ParameterizedTest
    @ValueSource(strings = {".hidden", "CON", "has%20space"})
    void put_aNameTheRepositoryWouldRefuse_isBadRequest(String name) throws Exception {
        assertEquals(400, put(name, ONE_CHANNEL).statusCode());
    }

    @Test
    void delete_aStoredName_removesItFromTheListing() throws Exception {
        put("orders", ONE_CHANNEL);
        put("fills", ONE_CHANNEL);

        assertEquals(204, send(request("/idl/orders").DELETE()).statusCode());
        assertEquals(List.of("fills"), listing().stream().map(entry -> entry.get("name").asText()).toList());
    }

    @Test
    void delete_aNameNotStored_isNotFound() throws Exception {
        assertEquals(404, send(request("/idl/missing").DELETE()).statusCode());
    }

    @Test
    void delete_withIfMatchOnAStaleEtag_isPreconditionFailed() throws Exception {
        String stale = etag(put("orders", ONE_CHANNEL));
        put("orders", ONE_CHANNEL.replace("Submit", "Send"));

        assertEquals(412, send(request("/idl/orders").header("If-Match", stale).DELETE()).statusCode());
        assertEquals(200, get("/idl/orders").statusCode());
    }

    @Test
    void get_theListing_isEmptyBeforeAnythingIsStored() throws Exception {
        assertTrue(listing().isEmpty());
    }

    @Test
    void get_theListing_reportsEveryStoredDocument_withTheEtagAGetWouldReturn() throws Exception {
        String orders = etag(put("orders", ONE_CHANNEL));
        put("fills", ONE_CHANNEL.replace("Submit", "Fill"));

        List<JsonNode> listing = listing();
        assertEquals(List.of("fills", "orders"), listing.stream().map(entry -> entry.get("name").asText()).toList());
        assertEquals(orders, listing.stream().filter(entry -> entry.get("name").asText().equals("orders")).findFirst().orElseThrow().get("etag").asText());
    }

}
