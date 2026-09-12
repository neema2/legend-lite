package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

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
            a.order().forEach(o -> out.add(o.key()));
        });
        return out;
    }

    @Override
    public TypedSpec withChildren(java.util.List<TypedSpec> kids) {
        int i = 1;
        java.util.List<TypedAggCol> as = new java.util.ArrayList<>(aggs.size());
        for (TypedAggCol a : aggs) {
            TypedLambda m = (TypedLambda) kids.get(i++);
            TypedLambda r = (TypedLambda) kids.get(i++);
            java.util.List<TypedAggCol.AggOrder> os = new java.util.ArrayList<>(a.order().size());
            for (TypedAggCol.AggOrder o : a.order()) {
                os.add(new TypedAggCol.AggOrder((TypedLambda) kids.get(i++), o.ascending(), o.nullOrder()));
            }
            as.add(new TypedAggCol(a.name(), m, r, os));
        }
        TypedSpec.expectChildren(kids, i, "TypedAggregate");
        return new TypedAggregate(kids.get(0), as, info);
    }
    @Override
    public TypedSpec withInfo(ExprType info) {
        return new TypedAggregate(source, aggs, info);
    }
}
