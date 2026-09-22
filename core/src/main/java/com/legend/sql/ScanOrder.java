// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql;

import java.util.List;

/**
 * Deterministic BASE-SCAN ordering for a query that asked for none —
 * the ONE owner of the "driving table's physical row order" key (user
 * design, 2026-08-29). Two consumers, two scopes:
 * <ul>
 *   <li>the ASSERT/VERDICT boundary ({@code CanonicalRenderSql}):
 *       results under assertion are deterministically ordered even
 *       when the test forgot to sort — a FEATURE of the assert
 *       surface, always on (positional reads over an unordered
 *       relation are otherwise semantically undefined);</li>
 *   <li>ENGINE-CORPUS-COMPAT ({@code StableScanOrder}, flag-gated):
 *       the engine's own tests rely on H2's implicit scan order —
 *       replaying them on DuckDB opts the HOST-channel statements in.
 *       The platform default stays order-honest.</li>
 * </ul>
 * Scope of the key (measured, not asserted): join trees — with or
 * without a subselect frame (batch 63: DuckDB's hash join reorders the
 * joined table's rows within a driving row; H2's nested loop does not)
 * — rooted at a bare table scan; never over DISTINCT/GROUP BY/aggregate
 * roots (one-row or set semantics), never over a user ORDER BY. A
 * root select carrying LIMIT/OFFSET orders at the same level (order
 * applies before the cap); a bare cap WRAPPER over an orderable inner
 * collapses into it (the only way an OFFSET reads a defined row).
 */
public final class ScanOrder {

    private ScanOrder() {
    }

    /** The query with the deterministic scan-order key applied where
     * the shape needs and permits it; identity otherwise. */
    public static SqlQuery stabilize(SqlQuery q) {
        if (!(q instanceof SqlSelect s)) {
            return q;
        }
        SqlSource.Table base = orderableBase(s);
        if (base != null) {
            return ordered(s, base);
        }
        // a rows->at(N)-style cap whose inner carries the relation
        if (s.from() instanceof SqlSource.Subselect sub
                && sub.inner() instanceof SqlSelect inner
                && (inner.limit() != null || inner.offset() != null)) {
            SqlSource.Table innerBase = orderableBase(inner);
            if (innerBase != null) {
                return s.withFrom(new SqlSource.Subselect(
                        ordered(inner, innerBase), sub.alias(),
                        sub.frameName()));
            }
        }
        return q;
    }

    /** The key is LEXICOGRAPHIC over the join tree's base-table scans in
     * join order — the driving table first, then each joined table: H2's
     * nested-loop join emits the driving scan's order and, within one
     * driving row, the joined table's scan order (Product ⋈
     * Product_Synonym with synonyms 11→P1, 12→P2, 13→P1 reads (P1,11),
     * (P1,13), (P2,12) — the enum projection tests' rows->at(i)). Frames
     * (subselects) contribute no key. */
    private static SqlSelect ordered(SqlSelect s, SqlSource.Table base) {
        List<SqlSelect.SortKey> keys = new java.util.ArrayList<>();
        for (SqlSource.Table t : scanTables(s.from())) {
            keys.add(new SqlSelect.SortKey(
                    new SqlExpr.RowOrder(t.alias()), true, null, null));
        }
        if (keys.isEmpty()) {
            keys.add(new SqlSelect.SortKey(
                    new SqlExpr.RowOrder(base.alias()), true, null, null));
        }
        return s.withOrderBy(keys);
    }

    /** Base-table scans of a join tree in join order (leftmost first). */
    private static List<SqlSource.Table> scanTables(SqlSource from) {
        List<SqlSource.Table> out = new java.util.ArrayList<>();
        if (from instanceof SqlSource.Join j) {
            out.addAll(scanTables(j.left()));
            if (j.right() instanceof SqlSource.Table t) {
                out.add(t);
            }
        } else if (from instanceof SqlSource.Table t) {
            out.add(t);
        }
        return out;
    }


    /** The driving base-table scan IF the select is order-stabilizable
     * (see class doc); null otherwise. */
    private static SqlSource.@com.legend.base.Nullable Table orderableBase(SqlSelect s) {
        if (!s.orderBy().isEmpty()
                || s.distinct()
                || !s.groupBy().isEmpty()
                || s.qualify() != null
                || !(s.from() instanceof SqlSource.Join)
                || s.projections().stream().anyMatch(
                        p -> containsReducer(p.expr()))) {
            return null;
        }
        // plain-table joins are in scope too (batch 63): DuckDB's hash
        // join does not preserve the joined table's scan order within a
        // driving row, which H2's nested loop does
        SqlSource leftmost = s.from();
        while (leftmost instanceof SqlSource.Join j) {
            leftmost = j.left();
        }
        return leftmost instanceof SqlSource.Table t ? t : null;
    }

    /** A top-level aggregate anywhere in the expression — Reducer,
     * JsonArrayAgg (graph serialization), OrderedListAgg — collapses the
     * select to one row (subqueries are their own scope, do not count). */
    private static boolean containsReducer(SqlExpr e) {
        if (e instanceof SqlAgg.Reducer
                || e instanceof SqlExpr.JsonArrayAgg
                || e instanceof SqlExpr.OrderedListAgg) {
            return true;
        }
        if (e instanceof SqlExpr.ScalarSubquery
                || e instanceof SqlExpr.Exists) {
            return false;
        }
        for (SqlExpr c : e.children()) {
            if (containsReducer(c)) {
                return true;
            }
        }
        return false;
    }
}
