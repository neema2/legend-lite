package com.legend.parser.spec;

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
        List<ValueSpecification> values) implements ValueSpecification {

    public PureCollection {
        Objects.requireNonNull(values, "values");
        values = List.copyOf(values);
    }
}
