package io.minaret.broker.e2e;

import io.micronaut.runtime.server.EmbeddedServer;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.micronaut.test.support.TestPropertyProvider;
import jakarta.inject.Inject;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * broker/wwwroot is served with a directory answering its index.html. The monitor and the landing page
 * are both reached that way.
 */
@MicronautTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class StaticContentTest implements TestPropertyProvider {

    @Inject
    EmbeddedServer server;

    @Override
    public Map<String, String> getProperties() {
        return MessageRoutingTest.brokerProperties();
    }

    @ParameterizedTest
    @ValueSource(strings = {"/", "/monitor/"})
    void aDirectory_servesItsIndexHtml(String path) throws Exception {
        Path index = Path.of("wwwroot" + path + "index.html");
        assumeTrue(Files.exists(index), index + " not built");

        HttpResponse<String> response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + server.getPort() + path)).GET().build(),
                HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        assertEquals(Files.readString(index), response.body());
    }

}
