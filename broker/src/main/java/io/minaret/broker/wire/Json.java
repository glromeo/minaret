package io.minaret.broker.wire;

import io.micronaut.core.type.Argument;
import io.micronaut.serde.ObjectMapper;
import io.netty.buffer.ByteBufInputStream;
import jakarta.inject.Singleton;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Base64;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Body (de)serialization for the frames the Broker itself reads or writes, over Micronaut's
 * compile-time serde. The envelope is FrameWriter's job; this class handles only what sits after the
 * \0.
 *
 * Control plane only, so it allocates freely. Session reads one body per Broker-addressed frame and
 * encodes one per ack or event. A relayed body never comes through here.
 */
@Singleton
public final class Json {

    private static final Base64.Decoder BASE64URL_DECODER = Base64.getUrlDecoder();
    private static final Base64.Encoder BASE64URL_ENCODER = Base64.getUrlEncoder().withoutPadding();

    private final ObjectMapper mapper;

    public Json(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    /** A body ready for FrameWriter.originate: the bytes, and whether they are gzip + base64url. */
    public record Encoded(boolean gzip, byte[] bytes) {
    }

    /**
     * frame's body deserialized as type, inflated first when it arrived gzipped, or null when it is
     * empty, undecodable or invalid for type. This is the null C#'s TryParseBody answers false for.
     * It also catches a record constructor's required-field check.
     */
    public <T> T read(Frame frame, Class<T> type) {
        int length = frame.bodyLengthActual();
        if (length == 0) return null;

        try (InputStream body = frame.gzip()
                ? inflate(frame)
                : new ByteBufInputStream(frame.buffer().slice(frame.bodyStart(), length))) {
            return mapper.readValue(body, Argument.of(type));
        } catch (Exception malformed) {
            return null;
        }
    }

    /**
     * value as wire-ready body bytes, gzipped then base64url-encoded once the JSON reaches
     * COMPRESSION_THRESHOLD_BYTES. The threshold is checked on the input size, as Wire.PrepareBody
     * checks it.
     */
    public Encoded encode(Object value) {
        byte[] plain;
        try {
            plain = mapper.writeValueAsBytes(value);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot serialize " + value.getClass().getSimpleName(), e);
        }

        if (plain.length < Wire.COMPRESSION_THRESHOLD_BYTES) return new Encoded(false, plain);

        ByteArrayOutputStream compressed = new ByteArrayOutputStream(plain.length / 2);
        try (GZIPOutputStream gzip = new GZIPOutputStream(compressed)) {
            gzip.write(plain);
        } catch (IOException impossible) {
            throw new IllegalStateException(impossible);
        }
        return new Encoded(true, BASE64URL_ENCODER.encode(compressed.toByteArray()));
    }

    private static InputStream inflate(Frame frame) throws IOException {
        byte[] encoded = new byte[frame.bodyLengthActual()];
        frame.buffer().getBytes(frame.bodyStart(), encoded);
        return new GZIPInputStream(new ByteArrayInputStream(BASE64URL_DECODER.decode(encoded)));
    }

}
