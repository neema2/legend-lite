package com.legend.compiler.spec.typed;

import com.legend.compiler.spec.ExprType;

import java.util.List;

/**
 * A type-checked relation {@code sort} (engine {@code TypedSort}) &mdash; the
 * relation overload {@code sort<X,T>(Relation<T>[1], SortInfo<X&sub;T>[*]):Relation<T>[1]}.
 * Sort only reorders rows, so its {@link #info()} is the <em>source</em> relation type
 * unchanged (schema preserved). The {@link #keys} carry each {@code asc(~col)}/
 * {@code desc(~col)} column + direction for lowering; each column is validated to exist
 * in the source schema at type-check time.
 *
 * @param source the relation being sorted
 * @param keys   the ordered sort columns (validated against {@code source}'s schema)
 * @param info   the result type &mdash; the source {@code RelationType[1]} (G-&alpha;), sourced
 *               from the sort signature's {@code Relation<T>[1]} return
 */
public record TypedSort(TypedSpec source, List<TypedSortKey> keys, ExprType info) implements TypedSpec {

    public TypedSort {
        keys = List.copyOf(keys);
    }

    /** One sort column: its name and direction. */
    public record TypedSortKey(String column, boolean ascending) {
    }

    @Override
    public List<TypedSpec> children() {
        return List.of(source);   // keys are column names, not expressions
    }
}
