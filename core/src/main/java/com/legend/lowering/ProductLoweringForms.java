// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.sql.RaiseKind;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlSelect;

import java.util.function.Supplier;

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
    public SqlSelect executeValues(Supplier<SqlSelect> array, Supplier<SqlSelect> oneBare) {
        return array.get();
    }

    /** No connection: the reference names none it was not made from. */
    @Override
    public String objectReferenceConnection() {
        return "";
    }

    @Override
    public com.legend.sql.SqlQuery relations(Supplier<com.legend.sql.SqlQuery> shared,
            Supplier<com.legend.sql.SqlQuery> unshared) {
        return shared.get();
    }

    @Override
    public com.legend.sql.SqlQuery keyedCounts(com.legend.sql.SqlQuery q,
            java.util.function.UnaryOperator<com.legend.sql.SqlQuery> fold) {
        return fold.apply(q);
    }

    @Override
    public com.legend.sql.SqlQuery computedPruning(Supplier<com.legend.sql.SqlQuery> pruned,
            Supplier<com.legend.sql.SqlQuery> kept) {
        return pruned.get();
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
    public SqlSelect existsJoin(SqlSelect filtered, java.util.function.UnaryOperator<SqlSelect> joinForm) {
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
}
