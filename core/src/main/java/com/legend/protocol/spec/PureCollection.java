package com.legend.protocol.spec;

import java.util.List;
import java.util.Objects;

/**
 * Collection literal &mdash; a Pure {@code [v1, v2, ...]} list.
 *
 * <p>Used for general lists of values ({@code [1, 2, 3]},
 * {@code ['a', 'b']}), arrays of lambdas
 * ({@code [{p|$p.name}, {p|$p.age}]}), and the source forms that
 * compile into Pure's variadic-collection arguments. C.1 only emits
 * collections of leaf {@link ValueSpecification}s; later phases admit
 * nested {@code AppliedFunction}, {@code LambdaFunction}, etc.
 *
 * <p>The empty literal {@code []} is a legal {@link PureCollection} with
 * {@link #values()} {@code .isEmpty() == true}.
 *
 * <p>Named {@code PureCollection} (not {@code Collection}) to avoid an
 * unhelpful clash with {@link java.util.Collection}; the engine record
 * is named the same way for the same reason.
 */
public record PureCollection(
        List<ValueSpecification> values,
        @com.legend.base.Nullable com.legend.protocol.SourceInfo pos) implements ValueSpecification {

    public PureCollection {
        Objects.requireNonNull(values, "values");
        values = List.copyOf(values);
    }

    /** Position-free form for synthesis and tests. For a literal {@code [...]} the parser
     *  sets the span of the whole bracketed form, brackets inclusive (engine convention). */
    public PureCollection(List<ValueSpecification> values) {
        this(values, null);
    }

    /** Position is excluded from equality — see {@code ValueSpecEqualityTest}. */
    @Override
    public boolean equals(Object o) {
        return o instanceof PureCollection other && values.equals(other.values());
    }

    @Override
    public int hashCode() {
        return values.hashCode();
    }
}
