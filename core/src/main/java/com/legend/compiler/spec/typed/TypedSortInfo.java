package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

import java.util.List;

/**
 * A type-checked sort key {@code asc(~col)} / {@code desc(~col)} &mdash; a value
 * of type {@code SortInfo<(col:…)>[1]}, produced by the generic check of the
 * {@code asc<T>(ColSpec<T>[1]):SortInfo<T>[1]} family and consumed by the
 * relation {@code sort} emission (which flattens it into
 * {@link TypedSort.TypedSortKey}).
 *
 * @param column    the sort column's name
 * @param ascending {@code true} for {@code asc}/{@code ascending}
 * @param info      {@code SortInfo<(column:…)>[1]}, from the signature
 */
public record TypedSortInfo(String column, boolean ascending, ExprType info) implements TypedSpec {
    @Override
    public List<TypedSpec> children() {
        return List.of();
    }
}
