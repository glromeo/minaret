package io.minaret.messaging.wire;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.zip.CRC32;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;
import java.util.zip.ZipException;

/**
 * Gzip (RFC 1952) over pooled Deflater and Inflater instances, for the bodies Wire compresses.
 *
 * Wire.prepareBody and Wire.serializeText compress through it on every send at or above
 * COMPRESSION_THRESHOLD_BYTES. A GZIPOutputStream there would construct and end a Deflater per
 * message, and each one registers with, then removes itself from, the JDK's Cleaner: a list behind one
 * global lock. A benchmark process hosting 2000 Routers spent most of its run blocked on that lock. A
 * pooled instance is built once and never cleaned, which takes the lock off the per-message path. The
 * output is what GZIPOutputStream writes, byte for byte, header included, as GzipTest pins.
 *
 * Wire.parseBody inflates through it on every large receive, so it must read whatever any sender's
 * gzip writes: the optional header fields RFC 1952 allows, then the trailer's CRC32 and length, checked
 * as GZIPInputStream checks them. A body failing any check throws ZipException, which parseBody turns
 * into an empty body. Unlike GZIPInputStream it reads exactly one member: no sender writes more, and a
 * body with bytes past its trailer is rejected rather than half-read.
 */
final class Gzip {

    private static final int HEADER_BYTES = 10;
    private static final int TRAILER_BYTES = 8;

    /** Magic, CM = deflate, no flags, no mtime, no XFL, OS = unknown: GZIPOutputStream's header since JDK 16. */
    private static final byte[] HEADER = {0x1f, (byte) 0x8b, Deflater.DEFLATED, 0, 0, 0, 0, 0, 0, (byte) 0xff};

    private static final int FHCRC = 2, FEXTRA = 4, FNAME = 8, FCOMMENT = 16;

    /** Deflate's worst-case expansion ratio; a trailer claiming more than this is a lie, so nothing is allocated for it. */
    private static final int MAX_INFLATE_RATIO = 1032;

    private static final int SLOTS = 2 * Runtime.getRuntime().availableProcessors();

    private static final Pool<Deflater> DEFLATERS = new Pool<>(SLOTS, () -> new Deflater(Deflater.DEFAULT_COMPRESSION, true), Deflater::end);
    private static final Pool<Inflater> INFLATERS = new Pool<>(SLOTS, () -> new Inflater(true), Inflater::end);

    private Gzip() {
    }

    static byte[] compress(byte[] plain) {
        Deflater deflater = DEFLATERS.take();
        try {
            deflater.setInput(plain);
            deflater.finish();

            byte[] out = Arrays.copyOf(HEADER, HEADER_BYTES + plain.length / 2 + 64);
            int length = HEADER_BYTES;
            while (!deflater.finished()) {
                if (length == out.length) out = Arrays.copyOf(out, 2 * out.length);
                length += deflater.deflate(out, length, out.length - length);
            }

            CRC32 crc = new CRC32();
            crc.update(plain);

            out = Arrays.copyOf(out, length + TRAILER_BYTES);
            writeIntLE(out, length, (int) crc.getValue());
            writeIntLE(out, length + 4, plain.length);
            return out;
        } finally {
            deflater.reset();
            DEFLATERS.give(deflater);
        }
    }

    /**
     * Inflates into a buffer sized from the trailer's length, plus one byte so a stream longer than it
     * claims overflows into a detectable spare instead of being silently truncated.
     */
    static String decompressUtf8(byte[] gz) throws ZipException {
        int start = headerEnd(gz);
        int trailer = gz.length - TRAILER_BYTES;

        long size = readIntLE(gz, trailer + 4) & 0xffffffffL;
        if (size > (long) (trailer - start) * MAX_INFLATE_RATIO) throw new ZipException("gzip length exceeds what its body can hold");

        byte[] out = new byte[(int) size + 1];
        int length = 0;

        Inflater inflater = INFLATERS.take();
        try {
            inflater.setInput(gz, start, gz.length - start);
            while (!inflater.finished()) {
                if (length == out.length || inflater.needsInput() || inflater.needsDictionary())
                    throw new ZipException("corrupt gzip body");
                length += inflater.inflate(out, length, out.length - length);
            }
            if (inflater.getRemaining() != TRAILER_BYTES) throw new ZipException("gzip body is not one member");
        } catch (DataFormatException e) {
            throw new ZipException("corrupt gzip body: " + e.getMessage());
        } finally {
            inflater.reset();
            INFLATERS.give(inflater);
        }

        CRC32 crc = new CRC32();
        crc.update(out, 0, length);
        if (length != size || (int) crc.getValue() != readIntLE(gz, trailer)) throw new ZipException("gzip trailer mismatch");

        return new String(out, 0, length, StandardCharsets.UTF_8);
    }

    /** Where the deflate stream starts, past the fixed header and whichever optional fields its flags announce. */
    private static int headerEnd(byte[] gz) throws ZipException {
        if (gz.length < HEADER_BYTES + TRAILER_BYTES || gz[0] != HEADER[0] || gz[1] != HEADER[1] || gz[2] != Deflater.DEFLATED)
            throw new ZipException("not a gzip body");

        int flags = gz[3];
        int at = HEADER_BYTES;
        int limit = gz.length - TRAILER_BYTES;

        if ((flags & FEXTRA) != 0) {
            if (at + 2 > limit) throw new ZipException("truncated gzip header");
            at += 2 + ((gz[at] & 0xff) | (gz[at + 1] & 0xff) << 8);
        }
        if ((flags & FNAME) != 0) at = pastZero(gz, at, limit);
        if ((flags & FCOMMENT) != 0) at = pastZero(gz, at, limit);
        if ((flags & FHCRC) != 0) {
            if (at + 2 > limit) throw new ZipException("truncated gzip header");
            CRC32 crc = new CRC32();
            crc.update(gz, 0, at);
            if (((gz[at] & 0xff) | (gz[at + 1] & 0xff) << 8) != (int) (crc.getValue() & 0xffff)) throw new ZipException("gzip header crc mismatch");
            at += 2;
        }

        if (at > limit) throw new ZipException("truncated gzip header");
        return at;
    }

    private static int pastZero(byte[] gz, int at, int limit) throws ZipException {
        while (at < limit)
            if (gz[at++] == 0) return at;

        throw new ZipException("truncated gzip header");
    }

    private static int readIntLE(byte[] b, int at) {
        return (b[at] & 0xff) | (b[at + 1] & 0xff) << 8 | (b[at + 2] & 0xff) << 16 | (b[at + 3] & 0xff) << 24;
    }

    private static void writeIntLE(byte[] b, int at, int value) {
        b[at] = (byte) value;
        b[at + 1] = (byte) (value >>> 8);
        b[at + 2] = (byte) (value >>> 16);
        b[at + 3] = (byte) (value >>> 24);
    }

    /**
     * A fixed row of slots holding idle instances, each taken and returned with one compare-and-set.
     *
     * Gzip keeps one for Deflaters and one for Inflaters. take scans from a slot picked by the calling
     * thread's id, so concurrent threads mostly start on different slots, and builds a fresh instance
     * when every slot is empty. give parks the instance in the first empty slot and ends it when there is
     * none, so a burst of more threads than slots costs the old per-message construction for the
     * overflow but never grows what the pool keeps.
     */
    private static final class Pool<T> {

        private final AtomicReferenceArray<T> slots;
        private final Supplier<T> create;
        private final Consumer<T> end;

        Pool(int size, Supplier<T> create, Consumer<T> end) {
            this.slots = new AtomicReferenceArray<>(size);
            this.create = create;
            this.end = end;
        }

        @SuppressWarnings("deprecation") // Thread.threadId() is JDK 19+; getId() is the same value on 17.
        private int start() {
            return (int) Thread.currentThread().getId();
        }

        T take() {
            int n = slots.length(), from = start();
            for (int i = 0; i < n; i++) {
                int at = Math.floorMod(from + i, n);
                T idle = slots.get(at);
                if (idle != null && slots.compareAndSet(at, idle, null)) return idle;
            }
            return create.get();
        }

        void give(T instance) {
            int n = slots.length(), from = start();
            for (int i = 0; i < n; i++) {
                int at = Math.floorMod(from + i, n);
                if (slots.get(at) == null && slots.compareAndSet(at, null, instance)) return;
            }
            end.accept(instance);
        }

    }

}
