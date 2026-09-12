package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

import java.util.ArrayList;
import java.util.List;

/**
 * A windowed {@code extend(over(…), ~col:…)} (engine's window
 * {@code TypedWindowExtendCol} family) &mdash; adds columns computed per row
 * over a window: plain window functions ({@code columns}, three-parameter
 * {@code {p,w,r|…}} lambdas) or windowed aggregates ({@code aggs},
 * map:reduce colspecs). Exactly one of the two lists is non-empty. Output is
 * the signature's {@code T+Z} / {@code T+R}.
 *
 * @param source  the relation being extended
 * @param window  the checked window definition
 * @param columns window-function columns (empty for the aggregate form)
 * @param aggs    windowed-aggregate columns (empty for the function form)
 * @param info    the result &mdash; {@code T+Z}/{@code T+R} resolved
 * @param extentBoundary the window belongs to a CLASS EXTENT's pipeline
 *                (a Relation {@code ~func} set): the extent is an evaluation
 *                boundary &mdash; operators the query applies to the mapped
 *                class (filters above all) never fold INTO this window's
 *                select, so the window computes over the extent's own rows.
 *                In plain relation composition the engine folds an ordinary
 *                predicate to WHERE under a window (PCT
 *                testExtendFilterOutNull: the window sees the FILTERED
 *                rows); at the mapping seam it treats the mapped relation
 *                as a non-mergeable view (corpus testMappingWithWindowColumn:
 *                John ranks 2nd among ALL group members, the class filter
 *                only drops rows). Stamped by the resolver
 *                ({@code ClassSources}) when it extracts the extent pipeline;
 *                honored by the lowerer (isolates the window select).
 */
public record TypedExtendWindow(TypedSpec source, TypedOver window, List<TypedFuncCol> columns,
                                List<TypedAggCol> aggs, ExprType info,
                                boolean extentBoundary) implements TypedSpec {

    public TypedExtendWindow {
        columns = List.copyOf(columns);
        aggs = List.copyOf(aggs);
    }

    /** The checker's constructor: a window in query position (no boundary). */
    public TypedExtendWindow(TypedSpec source, TypedOver window, List<TypedFuncCol> columns,
                             List<TypedAggCol> aggs, ExprType info) {
        this(source, window, columns, aggs, info, false);
    }

    /** This window as a class-extent boundary (see {@link #extentBoundary}). */
    public TypedExtendWindow withExtentBoundary() {
        return extentBoundary ? this
                : new TypedExtendWindow(source, window, columns, aggs, info, true);
    }

    @Override
    public List<TypedSpec> children() {
        List<TypedSpec> out = new ArrayList<>();
        out.add(source);
        out.add(window);
        columns.forEach(c -> out.add(c.fn()));
        aggs.forEach(a -> {
            out.add(a.map());
            out.add(a.reduce());
        });
        return out;
    }

    @Override
    public TypedSpec withChildren(java.util.List<TypedSpec> kids) {
        TypedSpec.expectChildren(kids,
                2 + columns.size() + 2 * aggs.size(), "TypedExtendWindow");
        int i = 2;
        java.util.List<TypedFuncCol> cs = new java.util.ArrayList<>(columns.size());
        for (TypedFuncCol c : columns) {
            cs.add(new TypedFuncCol(c.name(), (TypedLambda) kids.get(i++), c.documentation()));
        }
        java.util.List<TypedAggCol> as = new java.util.ArrayList<>(aggs.size());
        for (TypedAggCol a : aggs) {
            TypedLambda m = (TypedLambda) kids.get(i++);
            TypedLambda r = (TypedLambda) kids.get(i++);
            as.add(new TypedAggCol(a.name(), m, r, a.order()));
        }
        return new TypedExtendWindow(kids.get(0), (TypedOver) kids.get(1), cs, as, info,
                extentBoundary);
    }
    @Override
    public TypedSpec withInfo(ExprType info) {
        return new TypedExtendWindow(source, window, columns, aggs, info, extentBoundary);
    }
}
