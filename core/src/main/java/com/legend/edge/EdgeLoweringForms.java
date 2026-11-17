// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.sql.RaiseKind;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;

import java.util.Set;
import java.util.function.Supplier;

/**
 * THE EDGE'S FORMS OF THE LOWERING'S TWO-FORM CHOICES ({@link com.legend.lowering.LoweringForms}, the audit's K5,
 * 2026-11-17): legend-engine's choices, held here so the product branches on none of them.
 *
 * @param plain         the raises written as the plain operation, the database's own error passing through: none for
 *                      PCT and the corpus (legend-pure's guarded words), legend-engine's census class A for the pure/v1
 *                      route (B7, the user's ruling of 2026-11-17: "pure/v1 matches legend-engine")
 * @param engineSqlShape legend-engine's statement shape (the engine SQL's lowering, {@code EnginePlans}): a view an
 *                      aliased derived table ({@code ViewSelectSQLQuery}), a wire coercion left to the reader
 */
record EdgeLoweringForms(Set<RaiseKind> plain, boolean engineSqlShape) implements com.legend.lowering.LoweringForms {

    /** PCT's and the corpus's style ({@link PureText#UPSTREAM}): every check written, views flat, wire casts written. */
    static final EdgeLoweringForms PCT = new EdgeLoweringForms(Set.of(), false);

    /** The pure/v1 route's style ({@link PureText#PURE_V1}): legend-engine's unchecked raises plain. */
    static final EdgeLoweringForms PURE_V1 = new EdgeLoweringForms(PureText.ENGINE_UNCHECKED, false);

    /** legend-engine's statement shape, the engine SQL's ({@code EnginePlans.UPSTREAM_SHAPE}): asked for its views and
     *  wire casts; its raises are the plan style's, never this shape's. */
    static final EdgeLoweringForms ENGINE_SQL_SHAPE = new EdgeLoweringForms(Set.of(), true);

    EdgeLoweringForms {
        plain = plain.isEmpty() ? Set.of() : java.util.Collections.unmodifiableSet(java.util.EnumSet.copyOf(plain));
    }

    @Override
    public SqlExpr raise(RaiseKind kind, Supplier<SqlExpr> checked, SqlExpr plainOperation) {
        if (engineSqlShape) {
            throw new IllegalStateException("a raise asked of legend-engine's statement shape: the raises are the plan"
                    + " style's (Styled.forms), the shape's forms its views and wire casts");
        }
        return plain.contains(kind) ? plainOperation : checked.get();
    }

    @Override
    public SqlSelect view(SqlSelect body, Supplier<String> alias, String view) {
        return engineSqlShape ? SqlSelect.starOf(new SqlSource.Subselect(body, alias.get(), view)) : body;
    }

    @Override
    public SqlExpr wireCast(SqlExpr value, Supplier<SqlExpr> written) {
        return engineSqlShape ? value : written.get();
    }
}
