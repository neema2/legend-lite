package com.legend;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This type use is never {@code null} — an explicit override inside an
 * otherwise-unannotated context (in NullAway-checked packages non-null is
 * already the default, so this appears rarely; see {@link Nullable}).
 */
@Retention(RetentionPolicy.CLASS)
@Target({ElementType.TYPE_USE, ElementType.FIELD, ElementType.METHOD,
        ElementType.PARAMETER, ElementType.RECORD_COMPONENT})
public @interface NonNull {
}
