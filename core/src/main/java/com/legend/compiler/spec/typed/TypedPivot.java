package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

import java.util.ArrayList;
import java.util.List;

/**
 * A relation {@code pivot(~cols, ~agg:map:reduce)} (engine {@code TypedPivot}).
 * The output schema is only <em>partially</em> static: {@link #info()} carries
 * the group-by columns (source &minus; pivot &minus; aggregate-value columns);
 * the pivoted columns are <strong>data-dependent</strong> (one per distinct pivot
 * value &times; aggregate) and are concretized by the standard
 * {@code ->cast(@Relation<(…)>)} idiom that follows a pivot. The aggregates ride
 * the node for lowering (engine keeps them as dynamic-column templates).
 *
 * @param source       the relation being pivoted
 * @param pivotColumns the columns whose values become output columns
 * @param values       STATIC pivot values ({@code pivot(~col, [v…], ~agg…)}) —
 *                     the output holds exactly one column set per listed value;
 *                     empty = dynamic (one per distinct data value)
 * @param aggs         the aggregate columns (the dynamic-column templates)
 * @param info         the static half of the schema &mdash; the group-by columns, {@code [1]}
 */
public record TypedPivot(TypedSpec source, List<String> pivotColumns, List<TypedSpec> values,
                         List<TypedAggCol> aggs, ExprType info) implements TypedSpec {

    public TypedPivot {
        pivotColumns = List.copyOf(pivotColumns);
        values = List.copyOf(values);
        aggs = List.copyOf(aggs);
    }

    @Override
    public List<TypedSpec> children() {
        List<TypedSpec> out = new ArrayList<>();
        out.add(source);
        out.addAll(values);
        aggs.forEach(a -> {
            out.add(a.map());
            out.add(a.reduce());
        });
        return out;
    }
}
