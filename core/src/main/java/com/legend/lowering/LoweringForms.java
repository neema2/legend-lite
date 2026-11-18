// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.sql.RaiseKind;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlSelect;

import java.util.function.Supplier;

/**
 * THE LOWERING'S TWO-FORM CHOICES (the audit's K5, 2026-11-17, the orchestrator's ruling: one two-form kind,
 * {@code PlanStyle.twoForm}): what the product writes in its own canonical form and the compatibility edge in
 * upstream's. The product never branches on an upstream flag: it asks its form, and {@link #PRODUCT} answers the
 * product's way; the edge's forms (in {@code com.legend.edge}) answer legend-engine's.
 *
 * <ul>
 * <li>{@link #raise}: a check the plan writes -- the product writes every one ({@code checked}); legend-engine's pure/v1
 * route writes none for its census class A, the plain operation, the database's own error passing through.</li>
 * <li>{@link #view}: a view at relation position -- the product lowers its body flat (a name is not a nesting);
 * legend-engine's SQL nests it as an aliased derived table ({@code ViewSelectSQLQuery}).</li>
 * <li>{@link #wireCast}: a mapping's wire coercion -- the product writes the SQL cast (DuckDB does not wire-convert,
 * audit 19 F7); legend-engine's SQL leaves it to its reader, which converts on the wire.</li>
 * <li>{@link #isNumeric}: the relational isNumeric -- the product's a real numeric check, the same on every database
 * ({@code SqlFn.IS_NUMERIC}; the user's ruling of 2026-11-17 23:05, census J20); legend-engine's H2 compares
 * {@code lower(s) = upper(s)}, the edge's.</li>
 * </ul>
 */
public interface LoweringForms {

    /** A raise of {@code kind}: the guarded operation ({@code checked}) or the plain one ({@code plain}). */
    SqlExpr raise(RaiseKind kind, Supplier<SqlExpr> checked, SqlExpr plain);

    /** A view's relation: its body lowered ({@code body}), named {@code view}; {@code alias} mints an alias if one is
     *  written. */
    SqlSelect view(SqlSelect body, Supplier<String> alias, String view);

    /** A mapping's wire coercion of {@code value}: the cast written ({@code written}) or the value as read. */
    SqlExpr wireCast(SqlExpr value, Supplier<SqlExpr> written);

    /** The relational isNumeric of {@code s}. */
    SqlExpr isNumeric(SqlExpr s);

    /** The product's forms: every check written, a view flat, a wire coercion's cast written, isNumeric a real
     *  numeric check. */
    LoweringForms PRODUCT = new LoweringForms() {
        @Override
        public SqlExpr raise(RaiseKind kind, Supplier<SqlExpr> checked, SqlExpr plain) {
            return checked.get();
        }

        @Override
        public SqlSelect view(SqlSelect body, Supplier<String> alias, String view) {
            return body;
        }

        @Override
        public SqlExpr wireCast(SqlExpr value, Supplier<SqlExpr> written) {
            return written.get();
        }

        @Override
        public SqlExpr isNumeric(SqlExpr s) {
            return SqlExpr.Call.of(com.legend.sql.SqlFn.IS_NUMERIC, s);
        }
    };
}
