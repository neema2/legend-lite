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
 * @param extentBoundary the aggregate window belongs to a CLASS EXTENT's
 *               pipeline &mdash; same contract as
 *               {@link TypedExtendWindow#extentBoundary()}: a whole-relation
 *               {@code SUM(x) OVER ()} over a mapped relation aggregates the
 *               extent's own rows, never the query-filtered subset.
 */
public record TypedExtendAgg(TypedSpec source, List<TypedAggCol> aggs, ExprType info,
                             boolean extentBoundary) implements TypedSpec {
    public TypedExtendAgg {
        aggs = List.copyOf(aggs);
    }

    /** The checker's constructor: an aggregate extend in query position. */
    public TypedExtendAgg(TypedSpec source, List<TypedAggCol> aggs, ExprType info) {
        this(source, aggs, info, false);
    }

    /** This extend as a class-extent boundary (see {@link #extentBoundary}). */
    public TypedExtendAgg withExtentBoundary() {
        return extentBoundary ? this : new TypedExtendAgg(source, aggs, info, true);
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

    @Override
    public TypedSpec withChildren(java.util.List<TypedSpec> kids) {
        TypedSpec.expectChildren(kids, 1 + 2 * aggs.size(), "TypedExtendAgg");
        java.util.List<TypedAggCol> as = new java.util.ArrayList<>(aggs.size());
        for (int i = 0; i < aggs.size(); i++) {
            TypedAggCol a = aggs.get(i);
            as.add(new TypedAggCol(a.name(), (TypedLambda) kids.get(1 + 2 * i),
                    (TypedLambda) kids.get(2 + 2 * i), a.order()));
        }
        return new TypedExtendAgg(kids.get(0), as, info, extentBoundary);
    }
    @Override
    public TypedSpec withInfo(ExprType info) {
        return new TypedExtendAgg(source, aggs, info, extentBoundary);
    }
}
