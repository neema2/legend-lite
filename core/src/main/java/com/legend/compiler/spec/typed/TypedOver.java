package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

import java.util.List;
import java.util.Optional;

/**
 * A window definition {@code over(~partition [, asc(~key)…] [, rows(a,b)])}
 * (engine {@code TypedOver}) &mdash; a first-class {@code _Window<(cols:?)>[1]}
 * value whose partition/sort columns validate against the enclosing
 * {@code extend}'s source when the window unifies with its {@code _Window<T>}
 * parameter (the unsolved-fragment rebind rule). The optional frame is a checked
 * {@code Rows}/{@code _Range} value.
 *
 * @param partitions the partition column names, possibly empty
 * @param sortKeys   the ordering keys, possibly empty
 * @param frame      the frame clause ({@code rows(a,b)}), if present
 * @param info       {@code _Window<(cols:?)>[1]}
 */
public record TypedOver(List<String> partitions, List<TypedSort.TypedSortKey> sortKeys,
                        Optional<TypedSpec> frame, ExprType info) implements TypedSpec {
    public TypedOver {
        partitions = List.copyOf(partitions);
        sortKeys = List.copyOf(sortKeys);
    }

    @Override
    public List<TypedSpec> children() {
        return frame.map(List::of).orElseGet(List::of);
    }
}
