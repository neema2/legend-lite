// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlSelect;


/**
 * THE PRODUCT'S FORM OF THE LOWERING'S TWO-FORM CHOICES ({@link LoweringForms}; a product class of its own since
 * 2026-11-17, the orchestrator's ruling: a two-form kind holds types only, criterion (a)): every check written, a view
 * lowered flat, a wire coercion's cast written, an execute's values lite's array; lean SQL (a shared relation written once,
 * a keyed count folded, an unread computed projection dropped; the edge audit's J23).
 */
public final class ProductLoweringForms implements LoweringForms {

    public static final ProductLoweringForms INSTANCE = new ProductLoweringForms();

    private ProductLoweringForms() {
    }



    @Override
    public SqlSelect executeValues(SqlSelect own) {
        return own;
    }

    /** No connection: the reference names none it was not made from. */
    @Override
    public String objectReferenceConnection() {
        return "";
    }




    /** An exact decimal at the product's Decimal precision ({@code PureSql.type(DECIMAL)}). */
    @Override
    public SqlExpr parsedDecimal(SqlExpr text) {
        return new SqlExpr.Cast(text, PureSql.type(com.legend.compiler.element.type.Type.Primitive.DECIMAL),
                SqlExpr.Reason.USER);
    }

    /** The text as written, read as JSON. */
    @Override
    public SqlExpr parsedJson(SqlExpr text) {
        return new SqlExpr.Cast(text, com.legend.sql.SqlType.Scalar.JSON, SqlExpr.Reason.CARRIER);
    }

    /** The correlated EXISTS as lowered, the leanest SQL with the same answer (the edge audit's J19 (a), 2026-11-18,
     *  register S367): a dialect that cannot correlate one decomposes it at render (ClickHouse). */
    @Override
    public SqlSelect existsJoin(SqlSelect filtered, ExistsSites sites) {
        return filtered;
    }

    /** The product's isNumeric: a real numeric check, the same on every database (census J20). */
    @Override
    public SqlExpr isNumeric(SqlExpr s) {
        return SqlExpr.Call.of(com.legend.sql.SqlFn.IS_NUMERIC, s);
    }

    /** THE PRODUCT'S REFUSAL of legend-engine's result envelope (Phase 6's B5, 2026-11-17, the orchestrator's ruling):
     *  by name, pointing to the product's result writers -- at lowering for a relation or class root, at planning for
     *  a serialized root (train 17 item 2, 2026-11-18: the planner's ExecutedValues kind, the product's form). */
    public static com.legend.error.ModelException envelopeRefused() {
        return new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.LOWER,
                "executeLegendQuery's result envelope is legend-engine's JSON, the compatibility edge's: the product"
                        + " writes a query's rows by its result writers (JSON, CSV, Arrow)");
    }



    /** Each predicate appended, no zones kept (L10): conjunct order is no answer's, and the product's EXISTS is lowered
     *  as it is ({@link #existsJoin}). */
    @Override
    public WhereZones whereZones() {
        return APPENDED;
    }

    /** The product's WHERE merges: stateless, so one serves every statement. */
    private static final WhereZones APPENDED = new WhereZones() {
        @Override
        public SqlExpr merge(@com.legend.base.Nullable SqlExpr existing, SqlExpr predicate,
                com.legend.compiler.spec.typed.TypedFilter.Stamp stamp) {
            return existing == null ? predicate : Fold.mergeAnd(existing, predicate);
        }

        @Override
        public @com.legend.base.Nullable SqlExpr keysWhere(java.util.List<SqlExpr> local,
                @com.legend.base.Nullable SqlExpr where) {
            return com.legend.sql.ExistsJoins.encounterOrder(local, where);
        }
    };
}
