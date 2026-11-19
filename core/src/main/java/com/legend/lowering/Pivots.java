// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedAggCol;
import com.legend.compiler.spec.typed.TypedPivot;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * PIVOT lowering (extracted from the Lowerer at the 3,500-line shape
 * guard): single- and composite-key pivots, the static-value IN
 * pre-filter (engine row-restriction semantics), and the dynamic-column
 * outputs the two-phase boundary later staticizes.
 */
final class Pivots {

    private Pivots() {
    }

    static SqlSelect lower(Lowerer lw, TypedPivot pv) {
        SqlSelect src = lw.relation(pv.source());
        SqlSource inner = lw.asRightSide(src);
        List<SqlExpr> on;
        if (pv.pivotColumns().size() == 1) {
            on = List.of(Fold.sourceColumn(inner, pv.pivotColumns().get(0)));
        } else {
            // MULTI-column pivot: synthesize the COMPOSITE KEY — the pivot
            // columns concatenated with the '__|__' separator (the same
            // separator the dynamic-column templates carry), the originals
            // EXCLUDE'd — then pivot the single synthetic key.
            String keyName = lw.nextAlias();
            SqlExpr key = null;
            for (String c : pv.pivotColumns()) {
                SqlExpr col = new SqlExpr.Cast(
                        Objects.requireNonNull(Fold.sourceColumn(inner, c), c),
                        SqlType.Scalar.VARCHAR, SqlExpr.Reason.OPERATOR);
                key = key == null ? col
                        : SqlExpr.Call.of(SqlFn.CONCAT,
                                SqlExpr.Call.of(SqlFn.CONCAT, key,
                                        new SqlExpr.StringLit(com.legend.compiler.element.type
                                                .Type.RelationType.PIVOT_SEPARATOR)),
                                col);
            }
            // outputs-from-projections: the StarExcept expands inner's
            // outputs minus the pivot columns; the key slot rides its
            // projection — the old hand-built keyedOutputs list is gone
            SqlSelect keyed = SqlSelect.starOf(inner).withProjections(
                    List.of(new SqlSelect.Projection(
                                    new SqlExpr.StarExcept(inner.alias(), pv.pivotColumns()), null, null),
                            new SqlSelect.Projection(
                                    Objects.requireNonNull(key,
                                            "pivot requires a key column"),
                                    keyName,
                                    new OutputCol(keyName,
                                            SqlType.Scalar.VARCHAR, false))));
            inner = new SqlSource.Subselect(keyed, lw.nextAlias(), null);
            on = List.of(Fold.sourceColumn(inner, keyName));
        }
        List<SqlSource.Pivot.Using> usings = new ArrayList<>();
        SqlSelect forAgg = SqlSelect.starOf(inner);
        for (TypedAggCol a : pv.aggs()) {
            // the using carries its LOWERING-typed result slot — the
            // typed fact pivot-generated columns inherit (E1)
            Type aggT = a.reduce().functionType().result().type();
            usings.add(new SqlSource.Pivot.Using(absentIsEmpty(lw.aggExpr(forAgg, a)),
                    a.name(), PureSql.type(aggT)));
        }
        // Static pivot values pin the output columns via PIVOT ... IN (v…).
        List<SqlExpr> in = pv.values().stream()
                .map(v -> lw.scalar(v, (var, name) -> {
                    throw new IllegalStateException(
                            "pivot values must be literal, referenced column: " + name);
                }))
                .toList();
        // ENGINE static-pivot semantics: rows whose key is OUTSIDE the
        // explicit value list are FILTERED before pivoting — DuckDB's
        // PIVOT ... IN keeps them as extra groups (witness
        // testStaticPivot_SingleSingle_StringPivotValue: 9 groups where
        // the engine produces 3). Pre-filter the source when values are
        // pinned and the key is a single column.
        if (!in.isEmpty() && on.size() == 1) {
            // the filter references the key UNQUALIFIED — the wrapper has
            // exactly one source, and the original alias does not survive
            // H2's staticized-pivot restructuring (G7 caught the stale
            // qualifier: Column "_tds0.year" not found)
            SqlExpr key = on.get(0) instanceof SqlExpr.Column kc
                    ? SqlExpr.Column.derived(null, kc.name())
                    : on.get(0);
            SqlSelect filtered = SqlSelect.starOf(inner)
                    .withWhere(new SqlExpr.Membership(key,
                            new SqlExpr.ArrayLit(in)));
            // the wrapper KEEPS the inner source's alias — downstream
            // pivot emulation (H2 staticize) references columns through
            // the original name (G7: Column "_tds0.year" not found under
            // a fresh alias)
            inner = new SqlSource.Subselect(filtered, inner.alias(), null);
        }
        // Fully-qualified refs; a dialect whose PIVOT forbids qualifiers in
        // USING (DuckDB) strips them AT RENDER TIME.
        // a cast over the pivot names its values later (Lowerer.castOverDynamicPivot)
        return SqlSelect.starOf(new SqlSource.Pivot(inner, on, in, List.of(), usings, lw.nextAlias(),
                lw.outputsOf(pv.info())));
    }

    /**
     * A pivot's cell for a (group, key) pair with no rows is empty for every aggregate, a count's too: Pure's pivot is
     * TestTDS.applyPivot (both runtimes), which writes no value where the pair has no rows (register S379, proposed
     * 2026-11-19 by the orchestrator under the user's goal of ~13:25 UTC, "lite follows Pure", the user's ruling of this
     * case pending; legend-engine's database PIVOT answers 0). Every other
     * aggregate over no rows is already null in SQL; a count is 0, so a count is written as the sum of each row's 1 where
     * its value is present and 0 where it is empty -- a sum over no rows is null on every database, whatever the pivot's
     * form (a native PIVOT's filter, the CASE emulation, the dynamic pivot's keyed run), and a present pair whose values
     * are all empty still counts 0. A value proven never null counts 1 each. A distinct count has no such sum, and is
     * refused by name rather than answered 0.
     */
    private static SqlAgg.Reducer absentIsEmpty(SqlAgg.Reducer agg) {
        if (agg.fn() != SqlAgg.Fn.COUNT) {
            return agg;
        }
        if (agg.distinct()) {
            throw new com.legend.error.NotImplementedException("a pivot's distinct count: its cell for a group with no"
                    + " rows of a key is empty in Pure, and a distinct count over no rows is 0 in SQL (register S379)");
        }
        SqlExpr each;
        if (agg.args().isEmpty() || agg.args().get(0).type() instanceof com.legend.sql.TypeFact.Typed t && !t.nullable()) {
            each = new SqlExpr.IntLit(1);
        } else {
            each = new SqlExpr.Case(List.of(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.IS_NULL, agg.args().get(0)),
                    new SqlExpr.IntLit(0))), new SqlExpr.IntLit(1), SqlExpr.Reason.OPERATOR);
        }
        return new SqlAgg.Reducer(SqlAgg.Fn.SUM, List.of(each), false, agg.orderBy());
    }
}
