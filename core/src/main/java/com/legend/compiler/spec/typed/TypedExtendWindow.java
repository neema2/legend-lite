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
 */
public record TypedExtendWindow(TypedSpec source, TypedOver window, List<TypedFuncCol> columns,
                                List<TypedAggCol> aggs, ExprType info) implements TypedSpec {

    public TypedExtendWindow {
        columns = List.copyOf(columns);
        aggs = List.copyOf(aggs);
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
}
