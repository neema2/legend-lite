package com.legend.compiler.spec.typed;

import com.legend.compiler.spec.ExprType;

import java.util.ArrayList;
import java.util.List;

/**
 * A type-checked whole-relation {@code aggregate} (engine {@code AggregateChecker})
 * &mdash; {@code aggregate<T,K,V,R>(r, agg:AggColSpec(Array)<…,R>):Relation<R>[1]}:
 * a groupBy with no keys, collapsing the relation to one row of aggregates.
 *
 * @param source the relation being aggregated
 * @param aggs   the aggregate columns
 * @param info   the result &mdash; {@code Relation<R>} resolved
 */
public record TypedAggregate(TypedSpec source, List<TypedAggCol> aggs, ExprType info) implements TypedSpec {
    public TypedAggregate {
        aggs = List.copyOf(aggs);
    }

    @Override
    public List<TypedSpec> children() {
        List<TypedSpec> out = new ArrayList<>();
        out.add(source);
        aggs.forEach(a -> {
            out.add(a.map());
            out.add(a.reduce());
        });
        return out;
    }
}
