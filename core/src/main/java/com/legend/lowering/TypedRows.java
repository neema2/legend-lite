// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;

import java.util.List;

/**
 * A query's TYPED ROWS (docs/RESULT_BOUNDARY_PROGRAM_2026_10_26.md, the end state of 2026-10-27): the statement whose
 * rows a session reads from the driver by the plan's columns, and the typed relation those columns are -- no output
 * format in either; a writer at the edge spells each cell. (The database-written wires this replaces,
 * {@code WireRender}, were deleted with the result boundary program's phase 3.)
 */
public final class TypedRows {

    private TypedRows() {
    }

    /** The answer's typed relation: a tabular root's own schema, its row-struct columns flattened to the columns the
     *  statement answers ({@link #flat}); a scalar or a collection root is the one-column {@code value} relation. */
    public static Type.RelationType schema(ExprType info) {
        Type.RelationType rt = Type.schemaView(info.type());
        return rt != null ? flat(rt)
                : new Type.RelationType(List.of(new Type.Column("value", info.type(), info.multiplicity())));
    }

    /** A row-struct column (a user navigate's slot, typed nesting over a FLAT physical reality) expanded to the
     *  prefixed columns the join emitted -- {@code slot_COL}, each nullable (LEFT semantics) -- exactly as the
     *  lowering spells the statement's outputs ({@code Lowerer.outputsOf}); any other column as it is. The one rule,
     *  for the plan's columns and the judges' alike (2026-10-29, track T: a plan declared the struct column as one
     *  column, and the statement answered its parts). */
    public static Type.RelationType flat(Type.RelationType schema) {
        if (schema.columns().stream().noneMatch(c -> c.type() instanceof Type.RelationType)) {
            return schema;
        }
        List<Type.Column> out = new java.util.ArrayList<>(schema.columns().size());
        for (Type.Column c : schema.columns()) {
            if (c.type() instanceof Type.RelationType sub) {
                for (Type.Column sc : sub.columns()) {
                    // the LEFT join answers NULL where no row matches: a slot column is optional whatever the
                    // target column's own bound, as the lowering marks every slot output nullable
                    Multiplicity m = sc.multiplicity() instanceof Multiplicity.Bounded b
                            ? new Multiplicity.Bounded(0, b.upper()) : sc.multiplicity();
                    out.add(new Type.Column(c.name() + "_" + sc.name(), sc.type(), m));
                }
            } else {
                out.add(c);
            }
        }
        // the schema's dynamic (pivot) columns ride unchanged
        return new Type.RelationType(out, schema.dynamicColumns());
    }

    /** The typed rows' statement (the judges', the plan path program's step 7, 2026-10-18): the statement itself, its
     *  outputs unchanged, but for a sort key that is no output, carried as a hidden column of the inner select and
     *  ordered by outside ({@link Render#typedRows}); a WITH keeps its CTEs at the head. */
    public static SqlQuery statement(SqlQuery plan) {
        if (plan instanceof com.legend.sql.SqlWith w) {
            return w.with(w.ctes(), statement(w.body()));
        }
        if (plan instanceof SqlSelect s && !s.orderBy().isEmpty()) {
            return Render.typedRows(s, "_rows1");
        }
        return plan;
    }

    /** Does {@code plan} answer a dynamic pivot's columns UNNAMED: a pivot whose columns the database names (no
     *  {@code IN} list), and a typed schema that still carries the pivot's templates (its dynamic columns)? A cast
     *  over such a pivot names its columns statically and the lowering projects them (the plan path program's step
     *  7, 2026-10-18): the schema then has no templates, and the plan is an ordinary relation. */
    public static boolean dynamicPivotUnnamed(SqlQuery plan, Type.RelationType schema) {
        return !schema.dynamicColumns().isEmpty() && hasDynamicPivot(plan);
    }

    /** Does {@code plan} hold a pivot whose columns the database names (no {@code IN} list)? */
    public static boolean hasDynamicPivot(SqlQuery plan) {
        return KeyedPivot.pivots(plan).stream().anyMatch(p -> p.in().isEmpty());
    }
}
