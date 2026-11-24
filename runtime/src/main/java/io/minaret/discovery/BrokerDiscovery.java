package io.minaret.discovery;

import io.minaret.util.Json;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;

/**
 * Finds the Broker by reading the connection info it publishes on a realm-scoped named pipe.
 *
 * Windows only, and deliberately so rather than by oversight: a named pipe is what the Broker
 * publishes on, and the JDK can open one as an ordinary file. There is no JNA here and no Unix-socket
 * fallback — on any other platform this fails and the caller supplies an explicit Broker URI instead,
 * which is the same escape hatch a container deployment would need regardless.
 *
 * The pipe name is realm-scoped so several realms can coexist on one machine without colliding on a
 * single well-known name. Uppercasing matches the Broker's publisher exactly; no other normalization is
 * applied, so the two sides must agree on the realm value case-insensitively.
 */
public final class BrokerDiscovery {

    private static final String PIPE_NAME_PREFIX = "MessageBus.Broker.ConnectionInfo";

    private BrokerDiscovery() {
    }

    public static String pipeName(String realm) {
        return PIPE_NAME_PREFIX + "." + realm.toUpperCase(java.util.Locale.ROOT);
    }

    public static BrokerConnectionInfo discover() throws IOException {
        return discover(Realm.resolve());
    }

    /**
     * Reads the Broker's published connection info, or throws if no Broker is publishing in this realm.
     *
     * Opened read-only, and it must be: the Broker creates the pipe with PipeDirection.Out, so asking
     * for write access fails with "Access is denied" even though the pipe is right there — a failure
     * that reads like the Broker is missing when in fact it is running.
     */
    public static BrokerConnectionInfo discover(String realm) throws IOException {
        String path = "\\\\.\\pipe\\" + pipeName(realm);

        try (RandomAccessFile pipe = new RandomAccessFile(path, "r")) {
            byte[] buffer = new byte[512];
            int read = pipe.read(buffer);
            if (read <= 0) throw new IOException("Broker published no connection info on " + path);

            return Json.parse(new String(buffer, 0, read, StandardCharsets.UTF_8), BrokerConnectionInfo.class);
        } catch (java.io.FileNotFoundException | NoSuchFileException e) {
            throw new IOException(
                    "No Broker found in realm '" + realm + "' (pipe " + path + "). "
                            + "Start the Broker, or pass an explicit Broker URI.", e
            );
        }
    }

}
