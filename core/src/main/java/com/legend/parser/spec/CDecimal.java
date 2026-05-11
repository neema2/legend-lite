package com.legend.parser.spec;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * Decimal literal &mdash; a Pure number with the {@code d}/{@code D}
 * suffix indicating arbitrary precision. Examples: {@code 42d},
 * {@code 3.14d}, {@code 3.14159265358979323846D}.
 *
 * <p>Stored as {@link BigDecimal} to preserve every digit of the source
 * literal; this is the field type the engine carries on its mirror
 * record, so cross-codebase corpora compare byte-for-byte after a
 * parser swap.
 */
public record CDecimal(BigDecimal value) implements ValueSpecification {
    public CDecimal {
        Objects.requireNonNull(value, "value");
    }
}
