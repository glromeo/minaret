package io.minaret.messaging;

/**
 * A {@link Header} paired with a decoded body — the client-facing shape, as opposed to the M01 framing
 * that carries it.
 *
 * Router hands subscribers a Message&lt;JsonNode&gt; so they need not know the payload's concrete type
 * up front: a subscriber reads the body as whichever type the header's verb implies.
 */
public record Message<T>(Header header, T body) {
}
