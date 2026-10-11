package com.legend.protocol.spec;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * Decimal literal &mdash; a Pure number with the {@code d}/{@code D}
 * suffix indicating arbitrary precision. Examples: {@code 42d},
 * {@code 3.14d}, {@code 3.14159265358979323846D}.
 *
 * <p>Stored as {@link BigDecimal} to preserve every digit of the source
 * literal, its scale included ({@code 1.50d} is 1.50); this is the field
 * type the engine carries on its mirror record, so cross-codebase corpora
 * compare byte-for-byte after a parser swap. No source spelling: the JSON
 * carries the value alone, and the value is the digits (leg 8 step 2).
 */
public record CDecimal(BigDecimal value,
        @com.legend.base.Nullable com.legend.protocol.SourceInfo pos) implements ValueSpecification {

    /** Position-free convenience constructor. */
    public CDecimal(BigDecimal value) {
        this(value, null);
    }

    /** Position is excluded from equality. */
    @Override
    public boolean equals(Object o) {
        return o instanceof CDecimal other && value.equals(other.value());
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    public CDecimal {
        Objects.requireNonNull(value, "value");
    }
}
