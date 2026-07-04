package com.legend.compiler.spec.typed;

import com.legend.compiler.spec.ExprType;

import java.util.List;

/**
 * A type-checked relation {@code rename} (engine {@code TypedRename}) &mdash; the relation
 * overload {@code rename<T,Z,V>(Relation<T>[1], old:ColSpec[1], new:ColSpec[1]):Relation<T-Z+V>[1]}.
 * Unlike {@code sort}, rename <em>changes</em> the schema: each {@code old} column (validated to
 * exist) is renamed to {@code new}, keeping its type/multiplicity. So {@link #info()} is a fresh
 * {@link com.legend.compiler.element.type.Type.RelationType} (the {@code T-Z+V} of the signature,
 * computed here because the schema-algebra needs the colspec names), at the signature's {@code [1]}.
 *
 * @param source  the relation being renamed
 * @param renames the ordered {@code old -> new} column renames
 * @param info    the result type &mdash; the renamed {@code RelationType[1]} (G-&alpha;)
 */
public record TypedRename(TypedSpec source, List<ColRename> renames, ExprType info) implements TypedSpec {

    public TypedRename {
        renames = List.copyOf(renames);
    }

    /** One column rename: {@code from -> to}. */
    public record ColRename(String from, String to) {
    }

    @Override
    public List<TypedSpec> children() {
        return List.of(source);   // renames are column names, not expressions
    }
}
