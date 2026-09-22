package com.legend;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This type use may be {@code null}, and every reader must handle it.
 *
 * <p>OUR OWN annotation (AUDIT_PROGRAM §3.1 decision): no JSpecify/JSR-305
 * dependency ever appears in production source. NullAway recognizes it via
 * {@code -XepOpt:NullAway:CustomNullableAnnotations} in {@code core/pom.xml}.
 *
 * <p>The rule it serves (§3.4): Java {@code null} may appear only inside a
 * method converting an external value into a domain type — a {@code @Nullable}
 * on a field/return/parameter is an HONEST declared sentinel, not a licence.
 */
@Retention(RetentionPolicy.CLASS)
@Target({ElementType.TYPE_USE, ElementType.FIELD, ElementType.METHOD,
        ElementType.PARAMETER, ElementType.RECORD_COMPONENT})
public @interface Nullable {
}
