package io.minaret.broker.idl;

import io.minaret.broker.idl.IdlRepository.Document;
import io.minaret.broker.idl.IdlRepository.Entry;
import io.minaret.broker.idl.IdlRepository.Precondition;
import io.minaret.broker.idl.IdlRepository.WriteResult;
import io.micronaut.http.HttpHeaders;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Consumes;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Delete;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Put;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.serde.annotation.Serdeable;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;

/**
 * The /idl REST surface: list, get (with If-None-Match → 304),
 * conditional put and delete.
 *
 * Errors answer RFC 7807 problem bodies (title, detail, status, type), which the Monitoring API
 * editor reads.
 *
 * Runs on the blocking executor, because every handler touches the file system.
 */
@Controller("/idl")
@ExecuteOn(TaskExecutors.BLOCKING)
public final class IdlController {

    private static final int MAX_BODY_BYTES = 1024 * 1024;
    private static final MediaType PROBLEM = MediaType.of("application/problem+json");

    private final IdlRepository repository;
    private final IdlValidator validator;

    public IdlController(IdlRepository repository, IdlValidator validator) {
        this.repository = repository;
        this.validator = validator;
    }

    /** An RFC 7807 problem body. */
    @Serdeable
    public record Problem(String type, String title, int status, String detail) {
    }

    @Get
    public List<Entry> list() throws IOException {
        return repository.list();
    }

    @Get("/{name}")
    public HttpResponse<?> get(String name, HttpRequest<?> request) throws IOException {
        if (!IdlRepository.isValidName(name)) return invalidName(name);

        Document document = repository.read(name);
        if (document == null) return noSuchDocument(name);

        String ifNoneMatch = request.getHeaders().get(HttpHeaders.IF_NONE_MATCH);
        if (ifNoneMatch != null && new Precondition(ifNoneMatch, false).isSatisfiedBy(document.etag()))
            return HttpResponse.notModified().header(HttpHeaders.ETAG, document.etag());

        return HttpResponse.ok(document.content()).contentType(MediaType.APPLICATION_JSON_TYPE).header(HttpHeaders.ETAG, document.etag());
    }

    @Put("/{name}")
    @Consumes(MediaType.ALL)
    public HttpResponse<?> put(String name, HttpRequest<?> request, @Body byte[] body) throws IOException {
        if (!IdlRepository.isValidName(name)) return invalidName(name);

        byte[] content = body == null ? new byte[0] : body;
        if (content.length > MAX_BODY_BYTES)
            return problem(HttpStatus.REQUEST_ENTITY_TOO_LARGE, "IDL document too large", "An IDL document may be at most " + MAX_BODY_BYTES + " bytes.");

        if (content.length >= 3 && content[0] == (byte) 0xEF && content[1] == (byte) 0xBB && content[2] == (byte) 0xBF)
            content = Arrays.copyOfRange(content, 3, content.length);

        String error = validator.validate(content);
        if (error != null) return problem(HttpStatus.BAD_REQUEST, "Not a valid IDL document", error);

        WriteResult result = repository.write(name, content, preconditionOf(request));
        return switch (result.outcome()) {
            case CREATED -> HttpResponse.created(result.entry(), URI.create("/idl/" + name));
            case UPDATED -> HttpResponse.ok(result.entry());
            case PRECONDITION_FAILED -> preconditionFailed(name);
        };
    }

    @Delete("/{name}")
    public HttpResponse<?> delete(String name, HttpRequest<?> request) throws IOException {
        if (!IdlRepository.isValidName(name)) return invalidName(name);

        return switch (repository.delete(name, preconditionOf(request))) {
            case DELETED -> HttpResponse.noContent();
            case NOT_FOUND -> noSuchDocument(name);
            case PRECONDITION_FAILED -> preconditionFailed(name);
        };
    }

    private static Precondition preconditionOf(HttpRequest<?> request) {
        String ifMatch = request.getHeaders().get(HttpHeaders.IF_MATCH);
        String ifNoneMatch = request.getHeaders().get(HttpHeaders.IF_NONE_MATCH);
        return new Precondition(ifMatch == null || ifMatch.isBlank() ? null : ifMatch, ifNoneMatch != null && ifNoneMatch.trim().equals("*"));
    }

    private static HttpResponse<Problem> invalidName(String name) {
        return problem(HttpStatus.BAD_REQUEST, "Not a valid IDL document name",
                "'" + name + "' is not a valid name: 1 to 64 characters of letters, digits, '.', '_' or '-', starting with a letter or digit.");
    }

    private static HttpResponse<Problem> noSuchDocument(String name) {
        return problem(HttpStatus.NOT_FOUND, "No such IDL document", "No IDL document named '" + name + "' is stored.");
    }

    private static HttpResponse<Problem> preconditionFailed(String name) {
        return problem(HttpStatus.PRECONDITION_FAILED, "IDL document precondition failed",
                "'" + name + "' has changed since the version this request was made against, or already exists.");
    }

    private static HttpResponse<Problem> problem(HttpStatus status, String title, String detail) {
        return HttpResponse.<Problem>status(status)
                .contentType(PROBLEM)
                .body(new Problem(typeOf(status), title, status.getCode(), detail));
    }

    /** The RFC 9110 section each status links to. */
    private static String typeOf(HttpStatus status) {
        String section = switch (status) {
            case BAD_REQUEST -> "15.5.1";
            case NOT_FOUND -> "15.5.5";
            case PRECONDITION_FAILED -> "15.5.13";
            case REQUEST_ENTITY_TOO_LARGE -> "15.5.14";
            default -> "15";
        };
        return "https://tools.ietf.org/html/rfc9110#section-" + section;
    }

}
