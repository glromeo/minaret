package io.minaret.messaging;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Names the channel a type talks on, so a caller passes the type instead of repeating a string literal
 * that has to match on both sides.
 *
 * Deliberately carries no role. Which direction an annotated type faces is decided by the handle it is
 * used on — a proxy built on a provider sends to clients, on a client sends to the provider — so a role
 * here would be a second, redundant answer to a question the call site has already answered, and one
 * that could disagree with it.
 *
 * Inherited, so a subclass of an annotated class still resolves.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
@Inherited
public @interface Channel {

    String value();

}
