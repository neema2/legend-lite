package com.legend.parser.spec;

import java.util.Objects;

/**
 * DateTime literal &mdash; a Pure {@code %YYYY-MM-DDThh:mm:ss[.fff][+ZZZZ]}.
 * Example: {@code %2024-01-15T10:30:00} or {@code %2024-01-15T10:30:00.123+0000}.
 *
 * <p>{@code value} is the source-level text <strong>without</strong> the
 * leading {@code %}. The parser does not interpret the date/time; it
 * preserves the literal exactly as written, leaving timezone
 * normalisation and calendar arithmetic to downstream layers. This
 * matches engine's record contract.
 *
 * <p>Distinguished from {@link CStrictDate} (no time component) at parse
 * time by the presence of {@code T} in the source. {@link CStrictTime}
 * covers the time-only form ({@code %hh:mm:ss}).
 */
public record CDateTime(String value) implements ValueSpecification {
    public CDateTime {
        Objects.requireNonNull(value, "value");
    }
}
