package com.legend.parser.spec;

import java.util.Objects;

/**
 * StrictTime literal &mdash; a Pure time of day with no date component.
 * Example: {@code %10:30:00} or {@code %10:30}.
 *
 * <p>{@code value} preserves the source text without the leading
 * {@code %}. Disambiguated from {@link CStrictDate} at parse time by
 * the presence of colons and absence of dashes in the lexed token.
 */
public record CStrictTime(String value) implements ValueSpecification {
    public CStrictTime {
        Objects.requireNonNull(value, "value");
    }
}
