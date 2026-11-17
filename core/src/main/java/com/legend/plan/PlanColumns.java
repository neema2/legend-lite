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

    /** A precise decimal type's digits as the plan's data ({@code Decimal(38,18)}: precision 38, scale 18), or null for
     *  any other type -- a plain {@code Decimal} carries none (v57, 2026-11-17, the orchestrator's ruling). */
    public static ExecutionPlan.@com.legend.base.Nullable DecimalDigits decimal(
            com.legend.compiler.element.type.Type t) {
        return t instanceof com.legend.compiler.element.type.Type.PrecisionDecimal d
                ? new ExecutionPlan.DecimalDigits(d.precision(), d.scale()) : null;
    }

    /** {@code schema}'s columns (the answer's typed relation, {@code TypedRows.schema}) over {@code lowered}, in a
     *  result of {@code shape}. Each column's type is its plain path ({@link PureSpelling#typePath}: {@code Decimal},
     *  never {@code Decimal(10,2)}): a precise decimal's precision and scale are data -- its {@code store} where it is
     *  a store column, its {@code slot} always -- never spelled into the name a reader would parse (the orchestrator's
     *  ruling of 2026-11-17, Python's finding; until then {@code Type.typeName()}). */
    public static List<ExecutionPlan.Column> of(Type.RelationType schema, SqlQuery lowered,
            ExecutionPlan.TdsResult.Shape shape) {
        return ColumnFacts.columns(schema.columns().stream()
                .map(c -> new ColumnFacts.Pure(c.name(), PureSpelling.typePath(c.type()), multiplicity(c.multiplicity()),
                        decimal(c.type())))
                .toList(), lowered, shape);
    }
}
