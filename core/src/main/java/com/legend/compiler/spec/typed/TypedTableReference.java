package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

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
public record TypedTableReference(String store, String table, ExprType info,
                                  boolean accessor,
                                  boolean tabularFunction) implements TypedSpec {

    /** The arity before TabularFunction was executable. */
    public TypedTableReference(String store, String table, ExprType info,
            boolean accessor) {
        this(store, table, info, accessor, false);
    }
    /** {@code accessor}: the {@code #>{db.TABLE}#} relation-accessor
     * spelling (engine: columns typed as precisePrimitives from the
     * DDL) versus {@code tableReference(db, schema, table)} (the Table
     * value tableToTDS wraps — base pure types). */
    public TypedTableReference(String store, String table, ExprType info) {
        this(store, table, info, false);
    }

    @Override
    public List<TypedSpec> children() {
        return List.of();
    }

    @Override
    public TypedSpec withChildren(java.util.List<TypedSpec> kids) {
        TypedSpec.expectChildren(kids, 0, "TypedTableReference");
        return this;
    }
    @Override
    public TypedSpec withInfo(ExprType info) {
        // tabularFunction carried explicitly. The convenience
        // constructor defaults it to false, so any rebuild that forgets
        // it silently turns a function back into a table -- which is
        // what happened here, and the query planned as `FROM FN`
        // instead of `FROM FN()`.
        return new TypedTableReference(store, table, info, accessor,
                tabularFunction);
    }
}
