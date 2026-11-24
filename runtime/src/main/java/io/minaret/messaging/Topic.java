package io.minaret.messaging;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Names the topic a method maps to. The topic name is the contract between the two sides, and the
 * method name is a local detail on each — so a provider's handler and a client's caller meet on the same
 * topic without either knowing what the other called it.
 *
 * Nothing else is expressed here. Whether a call is request/reply, fire-and-forget, a broadcast or a
 * directed notification follows from the method's own signature and the handle it is called on, so
 * there is no mode to state twice and no way to state it wrongly.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface Topic {

    String value();

}
