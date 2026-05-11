package com.legend.parser.spec;

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
public record CInteger(Number value) implements ValueSpecification {
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
