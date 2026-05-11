package com.legend.parser.spec;

import java.util.Objects;

/**
 * StrictDate literal &mdash; a Pure date with no time component. Example:
 * {@code %2024-01-15}, {@code %2024-01}, or even {@code %2024} (year only).
 *
 * <p>{@code value} preserves the source text without the leading
 * {@code %}. See {@link CDateTime} for the form carrying a time
 * component and {@link CStrictTime} for time-only.
 */
public record CStrictDate(String value) implements ValueSpecification {
    public CStrictDate {
        Objects.requireNonNull(value, "value");
    }
}
