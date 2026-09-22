package com.legend.protocol.spec;

import java.math.BigInteger;
import java.util.Objects;

/**
 * Integer literal. Example Pure source: {@code 42}.
 *
 * <p>Field type is {@link Number} to admit both {@link Long} (fits in 64
 * bits) and {@link BigInteger} (overflow), matching the engine record
 * shape. {@link com.legend.parser.SpecParser} picks the narrowest of the
 * two that holds the parsed value.
 */
public record CInteger(Number value, @com.legend.base.Nullable com.legend.protocol.SourceInfo pos)
        implements ValueSpecification {

    /** Position-free convenience constructor — keeps hand-built test expectations compiling. */
    public CInteger(Number value) {
        this(value, null);
    }

    /**
     * <b>Position is excluded from equality on purpose.</b> These records are compared
     * structurally by the compiler and by 111 hand-built test assertions of the form
     * {@code assertEquals(new CInteger(...), spec)}; including a span would break every one and
     * would make two structurally identical expressions unequal for no semantic reason.
     * {@code ValueSpecEqualityTest} guards this — do not "fix" it.
     */
    @Override
    public boolean equals(Object o) {
        return o instanceof CInteger other && java.util.Objects.equals(value, other.value());
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hashCode(value);
    }

    public CInteger {
        Objects.requireNonNull(value, "value");
    }

    public CInteger(long value) {
        this((Number) value);
    }

    public CInteger(BigInteger value) {
        this((Number) value);
    }
}
