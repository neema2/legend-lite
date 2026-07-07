package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

import java.util.List;

/**
 * A type-checked relation {@code distinct} &mdash; {@code distinct<T>(r):Relation<T>[1]}
 * (whole-row dedup) or {@code distinct<X,T>(r, ~[cols]&sub;T):Relation<X>[1]}
 * (dedup on a column subset, which also narrows the schema to it). Checked
 * generically; this node is emission.
 *
 * @param source  the relation being deduplicated
 * @param columns the distinct-on column names (all columns for the whole-row form)
 * @param info    the result schema (source's, or the narrowed {@code X})
 */
public record TypedDistinct(TypedSpec source, List<String> columns, ExprType info) implements TypedSpec {
    public TypedDistinct {
        columns = List.copyOf(columns);
    }

    @Override
    public List<TypedSpec> children() {
        return List.of(source);
    }
}
