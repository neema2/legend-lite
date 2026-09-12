package com.legend.compiler.spec.typed;

import java.util.List;
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
        List<AggOrder> order) {

    /** One ORDER BY key of an ordered aggregate ({@code string_agg(x, sep ORDER BY
     *  k [DESC] [NULLS FIRST|LAST])}): the key lowers in the map body's row scope. */
    public record AggOrder(TypedLambda key, boolean ascending,
            @com.legend.Nullable TypedSortInfo.NullOrder nullOrder) {
        public AggOrder {
            Objects.requireNonNull(key, "key");
        }
    }

    // NO short overload: a defaulted orderKey silently turned an ordered
    // aggregate into an unordered one at rebuild sites (remediation T2.2 —
    // the inliner's aggCol() was a live instance); every construction
    // names every field.

    public TypedAggCol {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(map, "map");
        Objects.requireNonNull(reduce, "reduce");
        order = List.copyOf(order);
    }
}
