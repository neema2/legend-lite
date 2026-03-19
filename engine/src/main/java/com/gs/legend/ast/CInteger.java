package com.gs.legend.ast;

import java.math.BigInteger;

/** Integer literal. Example: {@code 42} or {@code 9223372036854775898} */
public record CInteger(Number value) implements ValueSpecification {
    public CInteger(long value) {
        this((Number) value);
    }

    public CInteger(BigInteger value) {
        this((Number) value);
    }
}
