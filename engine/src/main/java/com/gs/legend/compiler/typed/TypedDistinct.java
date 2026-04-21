package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;

import java.util.List;

/**
 * {@code distinct()} on a relation — drop duplicate rows.
 *
 * <p>Two overloads collapse into one variant:
 * <ul>
 *   <li>{@code distinct<T>(rel)} → {@code columns} is empty (all source columns
 *       participate in de-duplication and appear in the output).</li>
 *   <li>{@code distinct<X,T>(rel, ~[c1,c2,...])} → {@code columns} carries the
 *       projected subset used for both the de-duplication key and the output
 *       schema.</li>
 * </ul>
 *
 * @param source  Upstream relational expression.
 * @param columns Empty list = all source columns; otherwise the projected /
 *                dedup-key subset (in user-supplied order).
 * @param info    Output {@link ExpressionType} — always a {@code Relation}.
 */
public record TypedDistinct(
        TypedSpec source,
        List<String> columns,
        ExpressionType info
) implements TypedSpec {
    public TypedDistinct {
        columns = List.copyOf(columns);
    }
}
