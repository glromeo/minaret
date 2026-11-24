package io.minaret.broker.discovery;

import com.sun.jna.Platform;
import com.sun.jna.platform.win32.Kernel32;
import com.sun.jna.platform.win32.WinBase;
import com.sun.jna.platform.win32.WinError;
import com.sun.jna.platform.win32.WinNT;
import com.sun.jna.ptr.IntByReference;
import io.minaret.broker.contract.Contract.BrokerConnectionInfo;
import io.minaret.broker.wire.Json;
import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Value;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.runtime.server.event.ServerStartupEvent;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Locale;

/**
 * Serves this Broker's port on the realm's discovery pipe,
 * MessageBus.Broker.ConnectionInfo.{REALM upper}. The payload is exactly {"port":N} in UTF-8, with
 * no length prefix and no newline, which is what every client runtime reads.
 *
 * The pipe is created eagerly. This is a @Context bean, so it is built at startup, and with one
 * instance allowed a second Broker in the same realm fails to start instead of silently serving
 * nothing. It is outbound-only, which is why clients must open it "r".
 *
 * The port is the one the server actually bound, learned from ServerStartupEvent rather than parsed
 * back out of configuration, so a random test port publishes correctly too.
 *
 * Windows only. The JDK has no named-pipe server API, so this goes through JNA. Elsewhere it logs
 * and does nothing, and clients connect by explicit URI.
 */
@Context
public final class ConnectionInfoPublisher implements ApplicationEventListener<ServerStartupEvent> {

    private static final Logger log = LoggerFactory.getLogger(ConnectionInfoPublisher.class);

    private final String pipeName;
    private final Json json;
    private final WinNT.HANDLE pipe;
    private volatile boolean stopping;
    private Thread loop;

    public ConnectionInfoPublisher(@Value("${messagebus.realm:Default}") String realm, Json json) {
        this.pipeName = "MessageBus.Broker.ConnectionInfo." + realm.toUpperCase(Locale.ROOT);
        this.json = json;

        if (!Platform.isWindows()) {
            log.warn("Discovery pipe {} not published: named pipes are Windows-only. Clients must connect by URI.", pipeName);
            this.pipe = null;
            return;
        }

        this.pipe = Kernel32.INSTANCE.CreateNamedPipe(path(), WinBase.PIPE_ACCESS_OUTBOUND, WinBase.PIPE_TYPE_BYTE | WinBase.PIPE_WAIT,
                1, 512, 0, 0, null);
        if (WinBase.INVALID_HANDLE_VALUE.equals(pipe))
            throw new IllegalStateException("Cannot create discovery pipe " + pipeName + " (Win32 error " + Kernel32.INSTANCE.GetLastError()
                    + "): is another Broker already running in this realm?");
    }

    @Override
    public void onApplicationEvent(ServerStartupEvent event) {
        if (pipe == null || loop != null) return;

        byte[] payload = json.encode(new BrokerConnectionInfo(event.getSource().getPort())).bytes();
        loop = new Thread(() -> serve(payload), "messagebus-discovery");
        loop.setDaemon(true);
        loop.start();
        log.info("Publishing port {} on {}", event.getSource().getPort(), path());
    }

    /** One client per iteration: wait, write, flush (blocks until it is read), disconnect, repeat. */
    private void serve(byte[] payload) {
        Kernel32 kernel = Kernel32.INSTANCE;
        IntByReference written = new IntByReference();

        while (!stopping) {
            if (!kernel.ConnectNamedPipe(pipe, null) && kernel.GetLastError() != WinError.ERROR_PIPE_CONNECTED) {
                if (!stopping) log.warn("Discovery pipe {} failed to accept (Win32 error {})", pipeName, kernel.GetLastError());
                continue;
            }
            if (stopping) break;

            if (!kernel.WriteFile(pipe, payload, payload.length, written, null) || !kernel.FlushFileBuffers(pipe))
                log.warn("Discovery client on {} disconnected before reading (Win32 error {})", pipeName, kernel.GetLastError());

            kernel.DisconnectNamedPipe(pipe);
        }

        kernel.CloseHandle(pipe);
    }

    /**
     * Stops the loop. The loop is parked in ConnectNamedPipe, which nothing cancels from another
     * thread, so this connects to the pipe itself to wake it.
     */
    @PreDestroy
    void stop() {
        if (pipe == null) return;

        stopping = true;
        if (loop == null) {
            Kernel32.INSTANCE.CloseHandle(pipe);
            return;
        }

        try (RandomAccessFile wake = new RandomAccessFile(path(), "r")) {
            wake.read();
        } catch (IOException ignored) {
            // Already disconnected or closing: either way the loop sees stopping on its next turn.
        }
    }

    private String path() {
        return "\\\\.\\pipe\\" + pipeName;
    }

}
