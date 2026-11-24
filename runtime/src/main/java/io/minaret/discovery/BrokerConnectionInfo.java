package io.minaret.discovery;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * How to reach the Broker, as it publishes itself over the discovery pipe. Currently just the port; the
 * host is always the local machine, since discovery is a same-machine mechanism by construction.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record BrokerConnectionInfo(int port) {
}
