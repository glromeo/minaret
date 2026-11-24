package io.minaret.broker.e2e;

import io.minaret.broker.discovery.ConnectionInfoPublisher;
import io.minaret.broker.wire.Json;
import io.minaret.discovery.BrokerDiscovery;
import io.micronaut.runtime.server.EmbeddedServer;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.micronaut.test.support.TestPropertyProvider;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The port of BrokerConnectionInfoPublisherTests, end to end. The Java client's BrokerDiscovery reads
 * the pipe exactly as every real client does. C#'s BuildPayload tests have no counterpart: the port
 * comes from the running server, not from a Kestrel URL in configuration.
 */
@MicronautTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@EnabledOnOs(OS.WINDOWS)
class DiscoveryTest implements TestPropertyProvider {

    @Inject
    EmbeddedServer server;

    @Inject
    Json json;

    private String realm;

    @Override
    public Map<String, String> getProperties() {
        Map<String, String> properties = new HashMap<>(MessageRoutingTest.brokerProperties());
        realm = properties.get("messagebus.realm");
        return properties;
    }

    @Test
    void aClientDiscoveringTheRealm_readsTheRunningServersPort() throws Exception {
        assertEquals(server.getPort(), BrokerDiscovery.discover(realm).port());
        assertEquals(server.getPort(), BrokerDiscovery.discover(realm).port(), "the pipe serves one client after another");
    }

    @Test
    void aSecondBrokerInTheSameRealm_failsToStart() {
        assertThrows(IllegalStateException.class, () -> new ConnectionInfoPublisher(realm, json));
    }

}
