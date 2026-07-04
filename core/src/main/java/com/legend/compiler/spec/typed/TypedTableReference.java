package com.legend.compiler.spec.typed;

import com.legend.compiler.spec.ExprType;

import java.util.List;

/**
 * A type-checked relational table reference (engine {@code TypedTableReference})
 * &mdash; the {@code #>{db.TABLE}#} source. Its {@link #info()} is a bare
 * {@link com.legend.compiler.element.type.Type.RelationType} carrying the table's
 * column schema at multiplicity {@code [1]} (one relation value), per the G-&alpha;
 * relation-representation discipline (PHASE_G_SPEC_COMPILER.md &sect;G-&alpha;): a
 * computed relation value is a row-struct, so downstream relation operators
 * ({@code filter}, {@code sort}, {@code rename}, &hellip;) read columns directly
 * off the type.
 *
 * @param store the resolved database FQN (e.g. {@code "store::PersonDatabase"})
 * @param table the physical table name (e.g. {@code "T_PERSON"})
 * @param info  the relation type ({@link com.legend.compiler.element.type.Type.RelationType}) at {@code [1]}
 */
public record TypedTableReference(String store, String table, ExprType info) implements TypedSpec {
    @Override
    public List<TypedSpec> children() {
        return List.of();
    }
}
