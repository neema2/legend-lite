package com.legend.compiler.spec.typed;

import com.legend.compiler.spec.ExprType;

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
        });
        return out;
    }
}
