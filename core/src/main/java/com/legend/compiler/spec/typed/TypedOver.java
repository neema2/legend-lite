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
                        Optional<WindowFrame> frame, ExprType info) implements TypedSpec {
    public TypedOver {
        partitions = List.copyOf(partitions);
        sortKeys = List.copyOf(sortKeys);
    }

    // frame is a CLASSIFIED value (WindowFrame): its literal bounds were
    // decided and validated at the checker. Its children are the bounds that
    // were no literal there (WindowFrame.Bound.Offset: a function's parameter,
    // arithmetic over one), for the inliner to substitute the call's arguments
    // into; each is classified again as it is rebuilt (WindowFrame.withOffsets).
    @Override
    public List<TypedSpec> children() {
        return frame.map(WindowFrame::offsets).orElse(List.of());
    }

    @Override
    public TypedSpec withChildren(java.util.List<TypedSpec> kids) {
        TypedSpec.expectChildren(kids, children().size(), "TypedOver");
        return kids.isEmpty() ? this
                : new TypedOver(partitions, sortKeys, frame.map(f -> f.withOffsets(kids)), info);
    }
    @Override
    public TypedSpec withInfo(ExprType info) {
        return new TypedOver(partitions, sortKeys, frame, info);
    }
}
