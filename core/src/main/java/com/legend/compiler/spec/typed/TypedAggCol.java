package com.legend.compiler.spec.typed;

import java.util.Objects;

/**
 * One aggregate column of a {@code ~alias : x|map : y|reduce} specification: its
 * output name, the per-row map lambda, and the reduction over the grouped values.
 * The column's type is the <em>reduce</em> body's. A component of
 * {@link TypedAggColSpec} / {@link TypedAggColSpecArray} and of
 * {@link TypedGroupBy} / {@link TypedAggregate} &mdash; not a {@link TypedSpec}.
 *
 * @param name   the output column name (the colspec alias)
 * @param map    the checked per-row value extraction ({@code {T[1]->K[0..1]}})
 * @param reduce the checked reduction over the grouped values ({@code {K[*]->V[0..1]}})
 */
public record TypedAggCol(String name, TypedLambda map, TypedLambda reduce,
        TypedLambda orderKey, boolean orderAsc) {

    /** Un-ordered aggregation (the common case). */
    public TypedAggCol(String name, TypedLambda map, TypedLambda reduce) {
        this(name, map, reduce, null, true);
    }

    public TypedAggCol {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(map, "map");
        Objects.requireNonNull(reduce, "reduce");
    }
}
