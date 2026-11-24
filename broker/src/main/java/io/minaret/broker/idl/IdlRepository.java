package io.minaret.broker.idl;

import io.micronaut.context.annotation.Value;
import io.micronaut.serde.annotation.Serdeable;
import jakarta.inject.Singleton;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * The .idl document store: one file per document, and an Etag that is a hash of the bytes.
 * CLAUDE.md "The IDL repository" gives the rationale.
 *
 * The store is the directory messagebus.idl.path names, resolved against the working directory
 * (broker/idl by default).
 *
 * One lock covers every write and delete, and the precondition is evaluated inside it. Reads take no
 * lock. That is sound because a write lands through a temp file and an atomic move, so a reader
 * sees the whole old document or the whole new one.
 */
@Singleton
public final class IdlRepository {

    private static final String EXTENSION = ".idl";
    private static final Pattern NAME = Pattern.compile("^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$");
    private static final Set<String> RESERVED = Set.of(
            "CON", "PRN", "AUX", "NUL",
            "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
            "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9");

    private final Path root;
    private final Object writeLock = new Object();

    public IdlRepository(@Value("${messagebus.idl.path:idl}") String path) throws IOException {
        root = Path.of(path).toAbsolutePath().normalize();
        Files.createDirectories(root);
    }

    @Serdeable
    public record Entry(String name, String etag, long size, Instant updatedAt) {
    }

    public record Document(byte[] content, String etag) {
    }

    public enum WriteOutcome { CREATED, UPDATED, PRECONDITION_FAILED }

    public enum DeleteOutcome { DELETED, NOT_FOUND, PRECONDITION_FAILED }

    public record WriteResult(WriteOutcome outcome, Entry entry) {
    }

    /**
     * If-Match / If-None-Match: *, evaluated against the current Etag (null when the document does
     * not exist). Weak tags match, since an Etag here is a content hash and has no weak form.
     */
    public record Precondition(String ifMatch, boolean ifNoneMatchAny) {

        /** An unconditional write or delete. Tests only: the controller always builds one from the request headers. */
        public static final Precondition NONE = new Precondition(null, false);

        boolean isSatisfiedBy(String currentEtag) {
            if (ifNoneMatchAny && currentEtag != null) return false;
            if (ifMatch == null) return true;
            if (ifMatch.trim().equals("*")) return currentEtag != null;
            if (currentEtag == null) return false;

            String current = normalize(currentEtag);
            for (String candidate : ifMatch.split(","))
                if (normalize(candidate).equals(current)) return true;
            return false;
        }

        static String normalize(String etag) {
            String value = etag.trim();
            if (value.startsWith("W/")) value = value.substring(2);
            int start = 0;
            int end = value.length();
            while (start < end && value.charAt(start) == '"') start++;
            while (end > start && value.charAt(end - 1) == '"') end--;
            return value.substring(start, end);
        }

    }

    public static boolean isValidName(String name) {
        return name != null && NAME.matcher(name).matches() && !RESERVED.contains(name.toUpperCase(Locale.ROOT));
    }

    public List<Entry> list() throws IOException {
        List<Entry> entries = new ArrayList<>();
        try (DirectoryStream<Path> files = Files.newDirectoryStream(root, "*" + EXTENSION)) {
            for (Path file : files) {
                String fileName = file.getFileName().toString();
                String name = fileName.substring(0, fileName.length() - EXTENSION.length());
                if (!isValidName(name)) continue;
                try {
                    byte[] content = Files.readAllBytes(file);
                    entries.add(new Entry(name, etagOf(content), content.length, Files.getLastModifiedTime(file).toInstant()));
                } catch (NoSuchFileException vanished) {
                    // Deleted between the listing and the read: not part of this listing.
                }
            }
        }
        entries.sort(Comparator.comparing(Entry::name));
        return entries;
    }

    public Document read(String name) throws IOException {
        try {
            byte[] content = Files.readAllBytes(pathOf(name));
            return new Document(content, etagOf(content));
        } catch (NoSuchFileException missing) {
            return null;
        }
    }

    public WriteResult write(String name, byte[] content, Precondition precondition) throws IOException {
        Path path = pathOf(name);
        synchronized (writeLock) {
            Document existing = read(name);
            if (!precondition.isSatisfiedBy(existing == null ? null : existing.etag())) return new WriteResult(WriteOutcome.PRECONDITION_FAILED, null);

            Path temporary = root.resolve(UUID.randomUUID() + ".tmp");
            Files.write(temporary, content);
            Files.move(temporary, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

            Entry entry = new Entry(name, etagOf(content), content.length, Files.getLastModifiedTime(path).toInstant());
            return new WriteResult(existing == null ? WriteOutcome.CREATED : WriteOutcome.UPDATED, entry);
        }
    }

    public DeleteOutcome delete(String name, Precondition precondition) throws IOException {
        synchronized (writeLock) {
            Document existing = read(name);
            if (existing == null) return DeleteOutcome.NOT_FOUND;
            if (!precondition.isSatisfiedBy(existing.etag())) return DeleteOutcome.PRECONDITION_FAILED;

            Files.delete(pathOf(name));
            return DeleteOutcome.DELETED;
        }
    }

    /** The first 16 bytes of the content's SHA-256, lowercase hex, quoted. */
    static String etagOf(byte[] content) {
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256").digest(content);
            return "\"" + HexFormat.of().formatHex(hash, 0, 16) + "\"";
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private Path pathOf(String name) {
        if (!isValidName(name)) throw new IllegalArgumentException("'" + name + "' is not a valid IDL document name.");
        return root.resolve(name + EXTENSION);
    }

}
