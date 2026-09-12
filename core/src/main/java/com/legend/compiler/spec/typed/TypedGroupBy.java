package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * A type-checked {@code groupBy} (engine {@code TypedGroupBy}) &mdash;
 * {@code groupBy<T,Z,K,V,R>(r, cols:ColSpec(Array)<Z⊆T>, agg:AggColSpec(Array)<…,R>)
 * :Relation<Z+R>[1]} and the class-source form (whose keys carry extraction
 * lambdas). Keys are validated by the {@code ⊆} constraint; each aggregate's type
 * comes from its reduce body; the output schema is {@code resolveOutput(Z+R)}
 * (keys first, aggregates after) &mdash; signature-driven throughout.
 *
 * @param source the relation or class collection being grouped
 * @param keys   the group keys (a column name, plus the extraction lambda for class sources)
 * @param aggs   the aggregate columns
 * @param info   the result &mdash; {@code Z+R} resolved
 */
public record TypedGroupBy(TypedSpec source, List<GroupKey> keys, List<TypedAggCol> aggs,
                           ExprType info) implements TypedSpec {

    public TypedGroupBy {
        keys = List.copyOf(keys);
        aggs = List.copyOf(aggs);
    }

    /** One group key: its output column name, and the extraction lambda for class-source groupBy. */
    public record GroupKey(String column, Optional<TypedLambda> fn) {
        public GroupKey {
            Objects.requireNonNull(column, "column");
            Objects.requireNonNull(fn, "fn");
        }
    }

    @Override
    public List<TypedSpec> children() {
        List<TypedSpec> out = new ArrayList<>();
        out.add(source);
        keys.forEach(k -> k.fn().ifPresent(out::add));
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
        java.util.List<GroupKey> ks = new java.util.ArrayList<>(keys.size());
        for (GroupKey k : keys) {
            ks.add(k.fn().isPresent()
                    ? new GroupKey(k.column(), java.util.Optional.of(
                            (TypedLambda) kids.get(i++)))
                    : k);
        }
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
        TypedSpec.expectChildren(kids, i, "TypedGroupBy");
        return new TypedGroupBy(kids.get(0), ks, as, info);
    }
    @Override
    public TypedSpec withInfo(ExprType info) {
        return new TypedGroupBy(source, keys, aggs, info);
    }
}
