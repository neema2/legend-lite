package com.legend.compiler.spec.typed;

import com.legend.compiler.spec.ExprType;

import java.util.List;

/**
 * A type-checked relation {@code select} &mdash; column projection by name:
 * {@code select<T>(r)[1]} (all columns) or {@code select<T,Z>(r, ~col / ~[cols]
 * &sub;T):Relation<Z>[1]} (a narrowing). Checked generically (the {@code ⊆}
 * constraint validates the names and binds {@code Z}); this node is emission.
 *
 * @param source  the relation being projected
 * @param columns the selected column names, in output order (all of them for the no-arg form)
 * @param info    the result &mdash; {@code Relation<Z>} resolved to the concrete narrowed row
 */
public record TypedSelect(TypedSpec source, List<String> columns, ExprType info) implements TypedSpec {
    public TypedSelect {
        columns = List.copyOf(columns);
    }

    @Override
    public List<TypedSpec> children() {
        return List.of(source);
    }
}
