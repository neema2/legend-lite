package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

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
 * @param pureNullOrder PROVENANCE (slice 10, the TypedJoin.userCondition
 *               precedent — no defaulting constructor): {@code true} for the
 *               modern colspec relation API ({@code sort(asc/desc(~col))}) and
 *               value-collection sorts, whose observable is PURE semantics —
 *               null is LARGEST, so lowering stamps explicit null placement
 *               (DESC NULLS FIRST; witness PCT testRange_..._WithOrderByDESC).
 *               {@code false} for the legacy TDS string-key shapes
 *               ({@code sort('COL', Dir)} / {@code sort(desc('COL'))} /
 *               {@code sort(['A','B'])}) — the engine-drop-in surface, where
 *               the engine emits NO null-order clause and the corpus expected
 *               values pin backend-default placement (witness
 *               tds::groupBy::simpleGroupBySum: null group LAST on desc)
 * @param info   the result type &mdash; the source {@code RelationType[1]} (G-&alpha;), sourced
 *               from the sort signature's {@code Relation<T>[1]} return
 */
public record TypedSort(TypedSpec source, List<TypedSortKey> keys,
        boolean pureNullOrder, ExprType info) implements TypedSpec {

    public TypedSort {
        keys = List.copyOf(keys);
    }

    /** One sort column: its name, direction and explicit null placement
     *  (null = the engine's canonical placement for a bare key). */
    public record TypedSortKey(String column, boolean ascending,
            @com.legend.Nullable TypedSortInfo.NullOrder nullOrder) {
    }

    @Override
    public List<TypedSpec> children() {
        return List.of(source);   // keys are column names, not expressions
    }

    @Override
    public TypedSpec withChildren(java.util.List<TypedSpec> kids) {
        TypedSpec.expectChildren(kids, 1, "TypedSort");
        return new TypedSort(kids.get(0), keys, pureNullOrder, info);
    }
    @Override
    public TypedSpec withInfo(ExprType info) {
        return new TypedSort(source, keys, pureNullOrder, info);
    }
}
