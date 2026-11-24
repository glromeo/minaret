package io.minaret.broker.idl;

import io.minaret.broker.idl.IdlRepository.DeleteOutcome;
import io.minaret.broker.idl.IdlRepository.Entry;
import io.minaret.broker.idl.IdlRepository.Precondition;
import io.minaret.broker.idl.IdlRepository.WriteOutcome;
import io.minaret.broker.idl.IdlRepository.WriteResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The port of Broker.Tests' IdlRepositoryTests. The repository is handed its directory directly, where C# derives it from a content root. */
class IdlRepositoryTest {

    private static final String ONE_CHANNEL = "[{\"name\":\"Trading.Orders\",\"topics\":{\"submit\":{\"name\":\"Submit\",\"inbound\":{},\"outbound\":null}}}]";

    @TempDir
    Path root;

    private IdlRepository repository;

    @BeforeEach
    void open() throws Exception {
        repository = new IdlRepository(root.toString());
    }

    private WriteResult write(String name, String content) throws Exception {
        return write(name, content, Precondition.NONE);
    }

    private WriteResult write(String name, String content, Precondition precondition) throws Exception {
        return repository.write(name, content.getBytes(StandardCharsets.UTF_8), precondition);
    }

    private String readText(String name) throws Exception {
        return new String(repository.read(name).content(), StandardCharsets.UTF_8);
    }

    @Test
    void write_aNameNotYetStored_createsTheDocument() throws Exception {
        WriteResult result = write("orders", ONE_CHANNEL);
        assertEquals(WriteOutcome.CREATED, result.outcome());
        assertEquals("orders", result.entry().name());
        assertEquals(ONE_CHANNEL.length(), result.entry().size());
    }

    @Test
    void write_aNameAlreadyStored_replacesIt() throws Exception {
        write("orders", ONE_CHANNEL);
        assertEquals(WriteOutcome.UPDATED, write("orders", ONE_CHANNEL.replace("Submit", "Send")).outcome());
        assertTrue(readText("orders").contains("Send"));
    }

    @Test
    void read_returnsTheAuthorsOwnBytes_formattingAndAll() throws Exception {
        String formatted = "[\r\n  {\r\n    \"name\": \"Trading.Orders\",\r\n    \"topics\": { \"submit\": { \"name\": \"Submit\", \"inbound\": {}, \"outbound\": null } }\r\n  }\r\n]\r\n";
        write("orders", formatted);
        assertEquals(formatted, readText("orders"));
    }

    @Test
    void read_aNameNotStored_isNull() throws Exception {
        assertNull(repository.read("missing"));
    }

    @Test
    void write_theSameContentTwice_keepsTheSameEtag() throws Exception {
        assertEquals(write("orders", ONE_CHANNEL).entry().etag(), write("orders", ONE_CHANNEL).entry().etag());
    }

    @Test
    void write_differentContent_changesTheEtag() throws Exception {
        String first = write("orders", ONE_CHANNEL).entry().etag();
        assertNotEquals(first, write("orders", ONE_CHANNEL.replace("Submit", "Send")).entry().etag());
    }

    @Test
    void write_ifMatchOnTheCurrentEtag_replaces() throws Exception {
        String stored = write("orders", ONE_CHANNEL).entry().etag();
        assertEquals(WriteOutcome.UPDATED, write("orders", ONE_CHANNEL.replace("Submit", "Send"), new Precondition(stored, false)).outcome());
    }

    @Test
    void write_ifMatchOnAStaleEtag_isRefused_andLeavesTheStoredDocumentAlone() throws Exception {
        String stale = write("orders", ONE_CHANNEL).entry().etag();
        write("orders", ONE_CHANNEL.replace("Submit", "Send"));

        WriteResult result = write("orders", ONE_CHANNEL.replace("Submit", "Cancel"), new Precondition(stale, false));

        assertEquals(WriteOutcome.PRECONDITION_FAILED, result.outcome());
        assertNull(result.entry());
        assertTrue(readText("orders").contains("Send"));
    }

    @Test
    void write_ifMatchStar_requiresTheDocumentToExist() throws Exception {
        assertEquals(WriteOutcome.PRECONDITION_FAILED, write("orders", ONE_CHANNEL, new Precondition("*", false)).outcome());
    }

    @Test
    void write_ifNoneMatchAny_refusesAnExistingName() throws Exception {
        write("orders", ONE_CHANNEL);
        assertEquals(WriteOutcome.PRECONDITION_FAILED, write("orders", ONE_CHANNEL, new Precondition(null, true)).outcome());
    }

    @Test
    void write_ifNoneMatchAny_onAFreeName_creates() throws Exception {
        assertEquals(WriteOutcome.CREATED, write("orders", ONE_CHANNEL, new Precondition(null, true)).outcome());
    }

    @ParameterizedTest
    @ValueSource(strings = {"%s", "W/%s", "\"0000\", %s"})
    void write_ifMatch_acceptsEveryFormTheHeaderAllows(String template) throws Exception {
        String stored = write("orders", ONE_CHANNEL).entry().etag();
        Precondition ifMatch = new Precondition(String.format(template, stored), false);
        assertEquals(WriteOutcome.UPDATED, write("orders", ONE_CHANNEL.replace("Submit", "Send"), ifMatch).outcome());
    }

    @Test
    void delete_aNameNotStored_isNotFound() throws Exception {
        assertEquals(DeleteOutcome.NOT_FOUND, repository.delete("missing", Precondition.NONE));
    }

    @Test
    void delete_aStoredName_removesIt() throws Exception {
        write("orders", ONE_CHANNEL);
        assertEquals(DeleteOutcome.DELETED, repository.delete("orders", Precondition.NONE));
        assertNull(repository.read("orders"));
    }

    @Test
    void delete_ifMatchOnAStaleEtag_isRefused_andKeepsTheDocument() throws Exception {
        String stale = write("orders", ONE_CHANNEL).entry().etag();
        write("orders", ONE_CHANNEL.replace("Submit", "Send"));
        assertEquals(DeleteOutcome.PRECONDITION_FAILED, repository.delete("orders", new Precondition(stale, false)));
        assertNotNull(repository.read("orders"));
    }

    @Test
    void list_returnsEveryStoredDocument_nameOrdered() throws Exception {
        write("orders", ONE_CHANNEL);
        write("fills", ONE_CHANNEL);
        write("quotes", ONE_CHANNEL);
        assertEquals(List.of("fills", "orders", "quotes"), repository.list().stream().map(Entry::name).toList());
    }

    @Test
    void list_anEmptyRepository_isEmpty() throws Exception {
        assertTrue(repository.list().isEmpty());
    }

    @Test
    void list_reportsTheSameEtagAReadWould() throws Exception {
        String written = write("orders", ONE_CHANNEL).entry().etag();
        assertEquals(written, repository.list().get(0).etag());
        assertEquals(written, repository.read("orders").etag());
    }

    @ParameterizedTest
    @ValueSource(strings = {"orders", "Trading.Orders", "api-1", "api_1", "0"})
    void isValidName_acceptsWhatAnAuthorWouldNameADocument(String name) {
        assertTrue(IdlRepository.isValidName(name));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "..", "../escape", "..\\escape", "nested/name", ".hidden", "has space", "CON", "nul", "LPT1"})
    void isValidName_rejectsWhatWouldNotBeAFileInTheRoot(String name) {
        assertFalse(IdlRepository.isValidName(name));
    }

    @Test
    void isValidName_rejectsANameLongerThan64Characters() {
        assertTrue(IdlRepository.isValidName("a".repeat(64)));
        assertFalse(IdlRepository.isValidName("a".repeat(65)));
    }

    @Test
    void read_anInvalidName_throws_ratherThanAnsweringNull() {
        assertThrows(IllegalArgumentException.class, () -> repository.read("../escape"));
    }

    @Test
    void list_ignoresAFileWhoseNameCouldNotHaveBeenWrittenThroughTheApi() throws Exception {
        write("orders", ONE_CHANNEL);
        Files.writeString(root.resolve(".hidden.idl"), ONE_CHANNEL);
        List<Entry> listed = repository.list();
        assertEquals(1, listed.size());
        assertEquals("orders", listed.get(0).name());
    }

}
