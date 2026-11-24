package io.minaret.messaging.wire;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.CRC32;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Pins Gzip to the JDK's own stream codec: what it writes is what GZIPOutputStream writes, and what
 * either one writes it reads back.
 */
class GzipTest {

    private static final String[] TEXTS = {"", "x", "x".repeat(Wire.COMPRESSION_THRESHOLD_BYTES), randomText(100_000, 1), "héllo wörld ✓ ".repeat(500)};

    @Test
    void compress_writesWhatGZIPOutputStreamWrites_byteForByte() throws IOException {
        for (String text : TEXTS) {
            byte[] plain = text.getBytes(StandardCharsets.UTF_8);
            assertArrayEquals(jdkGzip(plain), Gzip.compress(plain), () -> "for " + plain.length + " bytes");
        }
    }

    @Test
    void compress_isReadBackByGZIPInputStream() throws IOException {
        for (String text : TEXTS) {
            byte[] plain = text.getBytes(StandardCharsets.UTF_8);
            try (GZIPInputStream in = new GZIPInputStream(new ByteArrayInputStream(Gzip.compress(plain)))) {
                assertArrayEquals(plain, in.readAllBytes());
            }
        }
    }

    @Test
    void decompress_readsWhatGZIPOutputStreamWrites() throws IOException {
        for (String text : TEXTS)
            assertEquals(text, Gzip.decompressUtf8(jdkGzip(text.getBytes(StandardCharsets.UTF_8))));
    }

    /** Another sender's gzip may set every optional header field RFC 1952 allows; browsers set none, but nothing forbids it. */
    @Test
    void decompress_skipsEveryOptionalHeaderField() throws IOException {
        byte[] plain = "optional fields".getBytes(StandardCharsets.UTF_8);
        byte[] jdk = jdkGzip(plain);

        ByteArrayOutputStream header = new ByteArrayOutputStream();
        header.write(jdk, 0, 10);
        header.write(new byte[]{3, 0, 'a', 'b', 'c'});    // FEXTRA: length 3, then the field
        header.write("name.txt\0".getBytes(StandardCharsets.ISO_8859_1));
        header.write("a comment\0".getBytes(StandardCharsets.ISO_8859_1));
        byte[] head = header.toByteArray();
        head[3] = 2 | 4 | 8 | 16;

        CRC32 crc = new CRC32();
        crc.update(head);

        ByteArrayOutputStream gz = new ByteArrayOutputStream();
        gz.write(head);
        gz.write((int) crc.getValue());
        gz.write((int) crc.getValue() >>> 8);
        gz.write(jdk, 10, jdk.length - 10);

        assertEquals("optional fields", Gzip.decompressUtf8(gz.toByteArray()));
    }

    @Test
    void decompress_rejectsACorruptCrc() throws IOException {
        byte[] gz = jdkGzip("checked".getBytes(StandardCharsets.UTF_8));
        gz[gz.length - 8] ^= 1;

        assertThrows(ZipException.class, () -> Gzip.decompressUtf8(gz));
    }

    @Test
    void decompress_rejectsALengthThatDisagreesWithTheStream() throws IOException {
        byte[] gz = jdkGzip("measured".getBytes(StandardCharsets.UTF_8));
        gz[gz.length - 4]--;

        assertThrows(ZipException.class, () -> Gzip.decompressUtf8(gz));
    }

    @Test
    void decompress_rejectsATruncatedBody() throws IOException {
        byte[] gz = jdkGzip(randomText(5_000, 2).getBytes(StandardCharsets.UTF_8));

        assertThrows(ZipException.class, () -> Gzip.decompressUtf8(Arrays.copyOf(gz, gz.length - 20)));
    }

    @Test
    void decompress_rejectsBytesPastTheTrailer() throws IOException {
        byte[] gz = jdkGzip("one member".getBytes(StandardCharsets.UTF_8));
        byte[] twice = Arrays.copyOf(gz, 2 * gz.length);
        System.arraycopy(gz, 0, twice, gz.length, gz.length);

        assertThrows(ZipException.class, () -> Gzip.decompressUtf8(twice));
    }

    /** More threads than slots, so the pool's overflow path runs alongside its reuse path. */
    @Test
    void concurrentRoundTrips_neverCrossInstances() throws Exception {
        int threads = 4 * Runtime.getRuntime().availableProcessors();
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            List<Future<?>> runs = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                int seed = t;
                runs.add(pool.submit(() -> {
                    for (int i = 0; i < 200; i++) {
                        String text = randomText(1_000 + 37 * i, seed * 1000L + i);
                        assertEquals(text, Gzip.decompressUtf8(Gzip.compress(text.getBytes(StandardCharsets.UTF_8))));
                    }
                    return null;
                }));
            }
            for (Future<?> run : runs) run.get();
        } finally {
            pool.shutdownNow();
        }
    }

    private static byte[] jdkGzip(byte[] plain) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (GZIPOutputStream gz = new GZIPOutputStream(out)) {
            gz.write(plain);
        }
        return out.toByteArray();
    }

    /** Compressible but not trivially so: words drawn from a small vocabulary. */
    private static String randomText(int length, long seed) {
        String[] words = {"alpha ", "bravo ", "charlie ", "delta ", "echo ", "foxtrot ", "{\"id\":", "\"data\":"};
        Random random = new Random(seed);
        StringBuilder out = new StringBuilder(length + 16);
        while (out.length() < length) out.append(words[random.nextInt(words.length)]).append(random.nextInt(1000));
        return out.substring(0, length);
    }

}
