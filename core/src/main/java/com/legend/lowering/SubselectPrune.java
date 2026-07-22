// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.sql.OutputCol;
import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlUnion;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * DEMAND-DRIVEN SUBSELECT COLUMN PRUNING (engine parity): the engine's
 * isolated subselects enumerate only the columns the outer query consumes
 * (e.g. golden {@code testMappingAssociationToAdvancedJoin.pure:31} —
 * {@code (select FIRMID, LASTNAME from ...)}); ours enumerated the whole
 * schema, which bloats every composite and BREAKS on corpus stores whose
 * model declares columns the physical table never carries
 * ({@code personExtensionTable.NUMBER}, advancedRelationalSetUp.pure:19).
 *
 * <p>CONSERVATIVE by construction: only plain-column projections of a
 * {@link SqlSource.Subselect}'s inner select are dropped, only when the
 * output name is unreferenced under the subselect's alias anywhere in the
 * tree (aliases are globally unique — t0..tN, one counter), and never when
 * the inner select is {@code distinct} / grouped / having / qualified
 * (column sets are semantics there), never when a star reads the alias,
 * never in set-operation branches (positional columns), and never the
 * ROOT query (executors read root columns by index). Star projections
 * inside a select always survive. At least one projection is kept —
 * an empty SELECT list is not SQL.
 */
final class SubselectPrune {

    private SubselectPrune() {
    }

    /** Per-alias column reads + star'd aliases + unqualified name reads. */
    private record Refs(Map<String, Set<String>> cols, Set<String> starred,
                        Set<String> unqualified) {
        Refs() {
            this(new HashMap<>(), new HashSet<>(), new HashSet<>());
        }

        void col(String table, String name) {
            if (table == null) {
                unqualified.add(name);
            } else {
                cols.computeIfAbsent(table, k -> new HashSet<>()).add(name);
            }
        }
    }

    static SqlQuery prune(SqlQuery q) {
        Refs refs = new Refs();
        collectQuery(q, refs);
        return rewriteQuery(q, refs);
    }

    // ===== reference collection (exhaustive — a new variant must be
    // classified here, same stance as the Lowerer's windowize) =====

    private static void collectQuery(SqlQuery q, Refs r) {
        switch (q) {
            case SqlSelect s -> collectSelect(s, r);
            case SqlUnion u -> u.branches().forEach(b -> collectQuery(b, r));
        }
    }

    private static void collectSelect(SqlSelect s, Refs r) {
        s.projections().forEach(p -> collectExpr(p.expr(), r));
        if (s.where() != null) {
            collectExpr(s.where(), r);
        }
        s.groupBy().forEach(g -> collectExpr(g, r));
        if (s.having() != null) {
            collectExpr(s.having(), r);
        }
        if (s.qualify() != null) {
            collectExpr(s.qualify(), r);
        }
        s.orderBy().forEach(k -> collectExpr(k.expr(), r));
        if (s.from() != null) {   // scalar SELECT without FROM
            if (s.projections().isEmpty()) {
                // EMPTY projections render as SELECT * — the whole FROM is
                // implicitly consumed (gate catch: a starOf root over a
                // pruned subselect narrowed the typed schema)
                starFromAliases(s.from(), r);
            }
            collectSource(s.from(), r);
        }
    }

    /** Every alias in the FROM tree marked star-consumed. */
    private static void starFromAliases(SqlSource src, Refs r) {
        switch (src) {
            case SqlSource.Table t -> r.starred().add(t.alias());
            case SqlSource.SourceUrl u -> r.starred().add(u.alias());
            case SqlSource.Subselect sub -> r.starred().add(sub.alias());
            case SqlSource.Values v -> r.starred().add(v.alias());
            case SqlSource.Pivot p -> r.starred().add(p.alias());
            case SqlSource.Join j -> {
                starFromAliases(j.left(), r);
                starFromAliases(j.right(), r);
            }
        }
    }

    private static void collectSource(SqlSource src, Refs r) {
        switch (src) {
            case SqlSource.Table t -> {
            }
            case SqlSource.SourceUrl u -> {
            }
            case SqlSource.Subselect sub -> collectQuery(sub.inner(), r);
            case SqlSource.Join j -> {
                if (j.on() != null) {   // CROSS / LATERAL-ON-TRUE joins
                    collectExpr(j.on(), r);
                }
                collectSource(j.left(), r);
                collectSource(j.right(), r);
            }
            case SqlSource.Pivot p -> {
                p.on().forEach(e -> collectExpr(e, r));
                p.in().forEach(e -> collectExpr(e, r));
                p.usings().forEach(u -> collectExpr(u.agg(), r));
                // PIVOT implicitly groups by every remaining source column
                // — the whole source is consumed (gate catch: pruning its
                // subselect dropped a pivot key)
                starFromAliases(p.source(), r);
                collectSource(p.source(), r);
            }
            case SqlSource.Values v ->
                    v.rows().forEach(row -> row.forEach(e -> collectExpr(e, r)));
        }
    }

    private static void collectExpr(SqlExpr e, Refs r) {
        switch (e) {
            case SqlExpr.Column c -> r.col(c.table(), c.name());
            case SqlExpr.Star st -> {
                if (st.table() != null) {
                    r.starred().add(st.table());
                } else {
                    // a scope-less star expands its OWN select's whole FROM
                    // — without scope tracking, disable pruning globally
                    // for safety by starring a wildcard marker
                    r.starred().add("*");
                }
            }
            case SqlExpr.StarExcept se -> {
                if (se.table() != null) {
                    r.starred().add(se.table());
                } else {
                    r.starred().add("*");
                }
            }
            case SqlExpr.StringLit v -> {
            }
            case SqlExpr.IntLit v -> {
            }
            case SqlExpr.FloatLit v -> {
            }
            case SqlExpr.DecimalLit v -> {
            }
            case SqlExpr.BoolLit v -> {
            }
            case SqlExpr.NullLit v -> {
            }
            case SqlExpr.DateLit v -> {
            }
            case SqlExpr.TimestampLit v -> {
            }
            case SqlExpr.OrderedListAgg o -> {
                collectExpr(o.value(), r);
                collectExpr(o.orderBy(), r);
            }
            case SqlExpr.ArrayLit a -> a.elements().forEach(x -> collectExpr(x, r));
            case SqlExpr.StructLit sl ->
                    sl.fields().forEach(f -> collectExpr(f.value(), r));
            case SqlExpr.StructGet g -> collectExpr(g.source(), r);
            case SqlExpr.Call c -> c.args().forEach(x -> collectExpr(x, r));
            case SqlExpr.Case cs -> {
                cs.whens().forEach(w -> {
                    collectExpr(w.condition(), r);
                    collectExpr(w.then(), r);
                });
                if (cs.otherwise() != null) {
                    collectExpr(cs.otherwise(), r);
                }
            }
            case SqlExpr.Exists ex -> collectQuery(ex.subquery(), r);
            case SqlExpr.ScalarSubquery sq -> collectQuery(sq.subquery(), r);
            case SqlExpr.JsonObject jo -> jo.kv().forEach(x -> collectExpr(x, r));
            case SqlExpr.JsonArrayAgg ja -> collectExpr(ja.value(), r);
            case SqlExpr.WindowCall w -> {
                w.fn().args().forEach(x -> collectExpr(x, r));
                if (w.fn() instanceof SqlAgg.Reducer red) {
                    red.orderBy().forEach(k -> collectExpr(k.expr(), r));
                }
                w.partitionBy().forEach(x -> collectExpr(x, r));
                w.orderBy().forEach(k -> collectExpr(k.expr(), r));
            }
            case SqlExpr.Lambda l -> collectExpr(l.body(), r);
            case SqlExpr.Cast c -> collectExpr(c.value(), r);
            case SqlExpr.FoldCall f -> {
                collectExpr(f.source(), r);
                collectExpr(f.lambda(), r);
                collectExpr(f.init(), r);
            }
            case SqlAgg.Reducer red -> {
                red.args().forEach(x -> collectExpr(x, r));
                red.orderBy().forEach(k -> collectExpr(k.expr(), r));
            }
        }
    }

    // ===== rewrite =====

    private static SqlQuery rewriteQuery(SqlQuery q, Refs r) {
        return switch (q) {
            case SqlSelect s -> rewriteSelect(s, r);
            case SqlUnion u -> new SqlUnion(
                    u.branches().stream().map(b -> rewriteQuery(b, r)).toList(),
                    u.all(), u.outputs());
        };
    }

    private static SqlSelect rewriteSelect(SqlSelect s, Refs r) {
        if (s.from() == null) {
            return s;
        }
        SqlSource from = rewriteSource(s.from(), r);
        return from == s.from() ? s : s.withFrom(from);
    }

    private static SqlSource rewriteSource(SqlSource src, Refs r) {
        return switch (src) {
            case SqlSource.Table t -> t;
            case SqlSource.SourceUrl u -> u;
            case SqlSource.Subselect sub -> {
                SqlQuery inner = rewriteQuery(sub.inner(), r);
                if (inner instanceof SqlSelect sel) {
                    SqlSelect pruned = pruneProjections(sel, sub.alias(), r);
                    yield pruned == sel && inner == sub.inner() ? sub
                            : new SqlSource.Subselect(pruned, sub.alias());
                }
                // set-operation inner: branches are positional — never
                // pruned themselves (their nested subselects already were)
                yield inner == sub.inner() ? sub
                        : new SqlSource.Subselect(inner, sub.alias());
            }
            case SqlSource.Join j -> {
                SqlSource left = rewriteSource(j.left(), r);
                SqlSource right = rewriteSource(j.right(), r);
                yield left == j.left() && right == j.right() ? j
                        : new SqlSource.Join(left, right, j.kind(), j.on());
            }
            case SqlSource.Pivot p -> {
                SqlSource inner = rewriteSource(p.source(), r);
                yield inner == p.source() ? p
                        : new SqlSource.Pivot(inner, p.on(), p.in(), p.usings(),
                                p.alias(), p.outputs());
            }
            case SqlSource.Values v -> v;
        };
    }

    /** The inner select of an aliased subselect, plain-column projections
     * unreferenced under the alias dropped. */
    private static SqlSelect pruneProjections(SqlSelect sel, String alias,
            Refs r) {
        if (sel.projections().isEmpty() || sel.distinct()
                || !sel.groupBy().isEmpty() || sel.having() != null
                || sel.qualify() != null
                || r.starred().contains("*") || r.starred().contains(alias)) {
            return sel;
        }
        Set<String> used = r.cols().getOrDefault(alias, Set.of());
        List<SqlSelect.Projection> kept = new ArrayList<>();
        Set<String> keptNames = new HashSet<>();
        for (SqlSelect.Projection p : sel.projections()) {
            String out = p.alias() != null ? p.alias()
                    : p.expr() instanceof SqlExpr.Column c ? c.name() : null;
            if (!(p.expr() instanceof SqlExpr.Column) || out == null
                    || used.contains(out) || r.unqualified().contains(out)) {
                kept.add(p);
                if (out != null) {
                    keptNames.add(out);
                }
            }
        }
        if (kept.size() == sel.projections().size()) {
            return sel;
        }
        if (kept.isEmpty()) {
            // an empty SELECT list is not SQL — keep the first projection
            kept.add(sel.projections().get(0));
            SqlSelect.Projection p0 = sel.projections().get(0);
            if (p0.alias() != null) {
                keptNames.add(p0.alias());
            } else if (p0.expr() instanceof SqlExpr.Column c0) {
                keptNames.add(c0.name());
            }
        }
        // outputs stay name-consistent with the projections when they were
        // (root outputs never prune — the root select is not a Subselect)
        List<OutputCol> outs = sel.outputs();
        if (outs.size() == sel.projections().size()) {
            List<OutputCol> keptOuts = new ArrayList<>();
            for (OutputCol oc : outs) {
                if (keptNames.contains(oc.name())
                        || kept.stream().anyMatch(p ->
                                p.alias() == null
                                && p.expr() instanceof SqlExpr.Star)) {
                    keptOuts.add(oc);
                }
            }
            if (!keptOuts.isEmpty()) {
                outs = keptOuts;
            }
        }
        return sel.withProjections(kept, outs);
    }
}
