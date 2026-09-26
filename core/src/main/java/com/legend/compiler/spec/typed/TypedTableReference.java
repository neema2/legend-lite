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
 * @param quotedColumns the columns the table's DDL declared QUOTED, by their
 *              (bare) names: the relation type names every column bare, and the
 *              lowering spells these delimited (a rendering fact, not identity)
 * @param call  the named relation is a TABULAR FUNCTION: the lowering reads it
 *              as a call ({@code FROM fn()}), not a table reference
 */
public record TypedTableReference(String store, String table, ExprType info,
                                  boolean accessor, @com.legend.base.Nullable String frame,
                                  java.util.Set<String> quotedColumns, boolean call)
        implements TypedSpec {
    public TypedTableReference {
        quotedColumns = java.util.Set.copyOf(quotedColumns);
    }

    public TypedTableReference(String store, String table, ExprType info, boolean accessor,
            @com.legend.base.Nullable String frame) {
        this(store, table, info, accessor, frame, java.util.Set.of(), false);
    }

    /** {@code frame}: the rows of this table identity are a PLANNED
     * frame's (a class-rooted let executed once as a CTE — lean ladder rung
     * 12): the lowering reads the CTE by name where the store table stood;
     * every mapping fact of the class (columns, filters, joins) applies to
     * those rows exactly as to the table's. */
    public TypedTableReference(String store, String table, ExprType info, boolean accessor) {
        this(store, table, info, accessor, null);
    }

    public TypedTableReference withFrame(String frame) {
        return new TypedTableReference(store, table, info, accessor, frame, quotedColumns, call);
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
        return new TypedTableReference(store, table, info, accessor, frame, quotedColumns, call);
    }
}
