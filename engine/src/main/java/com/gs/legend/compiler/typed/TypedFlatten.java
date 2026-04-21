package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;

/**
 * Relational UNNEST: {@code source->flatten(~column)}.
 *
 * <p>The named column must exist in {@code source}'s schema; its type in the
 * output schema is widened to {@code Variant} (JSON), and every other column
 * is carried through unchanged.
 *
 * @param source The upstream relation being flattened.
 * @param column The column name whose nested values are expanded. A
 *               {@code null} / empty column here would be a compile-time bug
 *               caught by {@code FlattenChecker}, so this field is always
 *               non-null once constructed.
 * @param info   Output {@link ExpressionType} — always a {@code Relation}.
 */
public record TypedFlatten(
        TypedSpec source,
        String column,
        ExpressionType info
) implements TypedSpec {
    public TypedFlatten {
        if (column == null || column.isEmpty()) {
            throw new IllegalArgumentException(
                    "TypedFlatten.column must be non-empty");
        }
    }
}
