// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;
import com.legend.sql.SqlQuery;

import java.util.List;

/**
 * A VALUE A PLANNED STATEMENT ANSWERS in its one cell (B2d.2 of the total-wall program, 2026-11-11): a statement the
 * planner wrote itself, read where the value stands -- a scalar subquery in whatever statement the reader's expression
 * lowers to. The test-data generator's text ({@code TestDataGenResult.dataCsvString}, {@code TestDataPlanner}'s one
 * read-only statement) and the database's comparisons over a generated slice are such values: their statements are
 * planning's, known before anything runs, and the database computes each. The {@link TypedFrameRef} precedent: a typed
 * leaf carrying a statement planned already, never a Pure node in the IR.
 *
 * @param statement the planned statement: one row, one column, its cell the value
 * @param info      the value's type (a String or a Boolean, to one)
 */
public record TypedPlannedValue(SqlQuery statement, ExprType info) implements TypedSpec {

    @Override
    public List<TypedSpec> children() {
        return List.of();
    }

    @Override
    public TypedSpec withChildren(List<TypedSpec> kids) {
        TypedSpec.expectChildren(kids, 0, "TypedPlannedValue");
        return this;
    }

    @Override
    public TypedSpec withInfo(ExprType info) {
        return new TypedPlannedValue(statement, info);
    }
}
