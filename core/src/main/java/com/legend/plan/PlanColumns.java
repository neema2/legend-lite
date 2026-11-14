// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.plan;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.executionplan.ColumnFacts;
import com.legend.executionplan.ExecutionPlan;
import com.legend.sql.SqlQuery;

import java.util.List;

/**
 * A query's result columns with their three type facts (docs/PLAN_PATH_PROGRAM_2026_10_17.md §3): the answer's typed
 * relation ({@code TypedRows.schema}: a relation's columns with a row-struct column flattened to the columns the
 * statement answers, or a value's one column {@code value}) joined with the lowered query's slots and the store's
 * declared types ({@link ColumnFacts}). A graph result has no columns: its objects are the wire's. (Until 2026-10-29
 * a second derivation here took the root's columns unflattened, and a plan over a navigate's slot declared one column
 * where the statement answered its parts: one derivation now, the lowering's.)
 */
public final class PlanColumns {
    private PlanColumns() {
    }

    /** The typed result's shape of a root: a relation's rows, a graph fetch's objects, else a value's elements
     *  (the plan's {@code TdsResult.Shape} for the compiler's {@link ResultShape}). */
    public static ExecutionPlan.TdsResult.Shape shapeOf(ExprType root) {
        return switch (ResultShape.of(root)) {
            case TABULAR -> ExecutionPlan.TdsResult.Shape.RELATION;
            case GRAPH -> ExecutionPlan.TdsResult.Shape.OBJECTS;
            case SCALAR, COLLECTION -> ExecutionPlan.TdsResult.Shape.VALUE;
        };
    }

    /** A column's multiplicity in the plan's terms (a column's is always bounded). */
    public static ExecutionPlan.Multiplicity multiplicity(Multiplicity m) {
        Multiplicity.Bounded b = m.requireBounded("a result column's multiplicity");
        return new ExecutionPlan.Multiplicity(b.lower(), b.upper());
    }

    /** {@code schema}'s columns (the answer's typed relation, {@code TypedRows.schema}) over {@code lowered}, in a
     *  result of {@code shape}. */
    public static List<ExecutionPlan.Column> of(Type.RelationType schema, SqlQuery lowered,
            ExecutionPlan.TdsResult.Shape shape) {
        return ColumnFacts.columns(schema.columns().stream()
                .map(c -> new ColumnFacts.Pure(c.name(), c.type().typeName(), multiplicity(c.multiplicity())))
                .toList(), lowered, shape);
    }
}
