package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

import java.util.ArrayList;
import java.util.List;

/**
 * An aggregate {@code extend(~col:map:reduce)} (no explicit window) &mdash; a
 * whole-relation windowed aggregation: every row receives the aggregate over
 * the entire relation. Output is the signature's {@code T+R}.
 *
 * @param source the relation being extended
 * @param aggs   the aggregate columns
 * @param info   the result &mdash; {@code T+R} resolved
 */
public record TypedExtendAgg(TypedSpec source, List<TypedAggCol> aggs, ExprType info) implements TypedSpec {
    public TypedExtendAgg {
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
