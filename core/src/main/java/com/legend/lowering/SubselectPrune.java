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

        void col(@com.legend.Nullable String table, String name) {
            if (table == null) {
                unqualified.add(name);
            } else {
                cols.computeIfAbsent(table, k -> new HashSet<>()).add(name);
            }
        }
    }

    static SqlQuery prune(SqlQuery q) {
        // FIXPOINT: dropping a projection can strand references one
        // level deeper (an isolation frame's pruned column was the only
        // reader of a union output — ledger cluster 57), and the
        // reference census is taken once per pass. Terminates: each
        // round strictly removes projections or changes nothing.
        for (int round = 0; round < 16; round++) {
            Refs refs = new Refs();
            collectQuery(q, refs);
            SqlQuery next = rewriteQuery(q, refs);
            if (next.equals(q)) {
                return q;
            }
            q = next;
        }
        return q;
    }

    // ===== reference collection (exhaustive — a new variant must be
    // classified here, same stance as the Lowerer's windowize) =====

    private static void collectQuery(SqlQuery q, Refs r) {
        switch (q) {
            case SqlSelect s -> collectSelect(s, r);
            case SqlUnion u -> u.branches().forEach(b -> collectQuery(b, r));
            case com.legend.sql.SqlWith w -> {
                w.ctes().forEach(c -> collectQuery(c.query(), r));
                collectQuery(w.body(), r);
            }
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
        if (!(s.from() instanceof SqlSource.Dual)) {   // scalar SELECT without FROM
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
            case SqlSource.Dual d -> {
            }
            case SqlSource.Table t -> r.starred().add(t.alias());
            case SqlSource.VarSetPlaceholder vp -> r.starred().add(vp.alias());
            case SqlSource.SourceUrl u -> r.starred().add(u.alias());
            case SqlSource.Subselect sub -> r.starred().add(sub.alias());
            case SqlSource.Values v -> r.starred().add(v.alias());
            case SqlSource.RawSql raw -> r.starred().add(raw.alias());
            case SqlSource.Pivot p -> r.starred().add(p.alias());
            case SqlSource.Join j -> {
                starFromAliases(j.left(), r);
                starFromAliases(j.right(), r);
            }
        }
    }

    private static void collectSource(SqlSource src, Refs r) {
        switch (src) {
            case SqlSource.Dual d -> {
            }
            case SqlSource.Table t -> {
            }
            case SqlSource.SourceUrl u -> {
            }
            case SqlSource.VarSetPlaceholder vp -> {
            }
            case SqlSource.RawSql raw -> {
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
            case SqlExpr.RowOrder ro -> { }
            case SqlExpr.TempTableInSplice ignored -> { }
            case SqlExpr.Membership m -> {
                collectExpr(m.needle(), r);
                collectExpr(m.collection(), r);
            }
            case SqlExpr.ReduceCollection rc -> {
                collectExpr(rc.collection(), r);
                rc.extras().forEach(x -> collectExpr(x, r));
            }
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
            case SqlExpr.PlanParam v -> {
            }
            case SqlExpr.Group g -> collectExpr(g.inner(), r);
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
            case SqlExpr.FormatLit v -> {
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
            case SqlExpr.InSubquery i -> {
                collectExpr(i.value(), r);
                collectQuery(i.subquery(), r);
            }
            case SqlExpr.Quantified q -> {
                collectExpr(q.value(), r);
                collectQuery(q.subquery(), r);
            }
            case SqlExpr.CheckedOne co -> collectExpr(co.list(), r);
            case SqlExpr.CompactList cl -> collectExpr(cl.list(), r);
            case SqlExpr.DeferredTdsString d -> collectQuery(d.inner(), r);
            case SqlExpr.JsonObject jo -> jo.kv().forEach(x -> collectExpr(x, r));
            case SqlExpr.JsonArray ja0 -> ja0.elements().forEach(x -> collectExpr(x, r));
            case SqlExpr.JsonArrayAgg ja -> {
                collectExpr(ja.value(), r);
                // order keys are LIVE column refs — invisible keys let the
                // prune drop/re-alias their sources (dangling ORDER BY)
                ja.orderKeys().forEach(k -> collectExpr(k.expr(), r));
            }
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
            case com.legend.sql.SqlWith w -> new com.legend.sql.SqlWith(
                    w.ctes().stream().map(c -> new com.legend.sql.SqlWith.Cte(
                            c.name(), rewriteQuery(c.query(), r))).toList(),
                    rewriteQuery(w.body(), r));
        };
    }

    private static SqlSelect rewriteSelect(SqlSelect s, Refs r) {
        if (s.from() instanceof SqlSource.Dual) {
            return s;
        }
        SqlSource from = rewriteSource(s.from(), r);
        return from == s.from() ? s : s.withFrom(from);
    }

    private static SqlSource rewriteSource(SqlSource src, Refs r) {
        return switch (src) {
            case SqlSource.Dual d -> d;
            case SqlSource.Table t -> t;
            case SqlSource.SourceUrl u -> u;
            case SqlSource.VarSetPlaceholder vp -> vp;
            case SqlSource.RawSql raw -> raw;   // carried text: a leaf
            case SqlSource.Subselect sub -> {
                SqlQuery inner = rewriteQuery(sub.inner(), r);
                if (inner instanceof SqlSelect sel) {
                    SqlSelect pruned = pruneProjections(sel, sub.alias(), r);
                    yield pruned == sel && inner == sub.inner() ? sub
                            : new SqlSource.Subselect(pruned, sub.alias(), sub.frameName());
                }
                // set-operation inner (ledger cluster 57): arms are
                // POSITIONAL, so pruning happens by position from the
                // union's own output names — never per-arm (that would
                // desynchronise the branches)
                if (inner instanceof SqlUnion u) {
                    SqlSource.Subselect pu = pruneUnion(u, sub, r);
                    if (pu != null) {
                        yield pu;
                    }
                }
                yield inner == sub.inner() ? sub
                        : new SqlSource.Subselect(inner, sub.alias(), sub.frameName());
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

    /** UNION inner: positions unreferenced under the subselect's alias
     * dropped from EVERY branch and from the union's outputs — a
     * position drops regardless of expression kind (an arm slot may be
     * {@code NULL AS ID_1}; the per-arm plain-Column rule would
     * desynchronise the arms). Null when not prunable: a starred alias,
     * a nested set-op / distinct / grouped / having / qualified branch,
     * an arity mismatch, or nothing to drop. At least one position is
     * kept. */
    private static SqlSource.@com.legend.Nullable Subselect pruneUnion(
            SqlUnion u, SqlSource.Subselect sub, Refs r) {
        // a USER-projected TDS union keeps its projected columns even
        // when the outer never reads them (engine testUnionWithGroupBy
        // golden pins firstName in both arms) — only SYNTHESIS unions
        // (class-mapping union extents) prune
        if ("unionAlias".equals(sub.frameName())) {
            return null;
        }
        if (r.starred().contains("*") || r.starred().contains(sub.alias())
                || u.outputs().isEmpty()) {
            return null;
        }
        int n = u.outputs().size();
        if (!unionArmsPrunable(u, n)) {
            return null;
        }
        Set<String> used = r.cols().getOrDefault(sub.alias(), Set.of());
        List<Integer> keep = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            if (used.contains(u.outputs().get(i).name())
                    || r.unqualified().contains(u.outputs().get(i).name())) {
                keep.add(i);
            }
        }
        if (keep.isEmpty() || keep.size() == n) {
            return null;
        }
        return new SqlSource.Subselect(pruneUnionByPosition(u, keep, n),
                sub.alias(), sub.frameName());
    }

    /** Every (transitive) arm is a plain same-arity SqlSelect — nested
     * set-ops share the positional schema, so they prune recursively. */
    private static boolean unionArmsPrunable(SqlUnion u, int n) {
        for (SqlQuery b : u.branches()) {
            if (b instanceof SqlUnion nu) {
                if (nu.outputs().size() != n || !unionArmsPrunable(nu, n)) {
                    return false;
                }
            } else if (!(b instanceof SqlSelect bs) || bs.distinct()
                    || !bs.groupBy().isEmpty() || bs.having() != null
                    || bs.qualify() != null
                    || bs.projections().size() != n) {
                return false;
            }
        }
        return true;
    }

    private static SqlUnion pruneUnionByPosition(SqlUnion u,
            List<Integer> keep, int n) {
        List<SqlQuery> nb = new ArrayList<>();
        for (SqlQuery b : u.branches()) {
            if (b instanceof SqlUnion nu) {
                nb.add(pruneUnionByPosition(nu, keep, n));
                continue;
            }
            SqlSelect bs = (SqlSelect) b;
            // outputs-from-projections: kept projections carry their
            // slots — the pruned branch's outputs rebuild themselves
            List<SqlSelect.Projection> ps = new ArrayList<>();
            for (int i : keep) {
                ps.add(bs.projections().get(i));
            }
            nb.add(bs.withProjections(ps));
        }
        List<OutputCol> uo = new ArrayList<>();
        for (int i : keep) {
            uo.add(u.outputs().get(i));
        }
        return new SqlUnion(nb, u.all(), uo);
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
        boolean narrowed = false;
        for (SqlSelect.Projection p : sel.projections()) {
            // STAR NARROWING (batch 77): a QUALIFIED star `X.*` inside this
            // aliased subselect expands to the starred source's outputs
            // the outer reads under the alias. The join-emission frame
            // `SELECT t7.*, t13.c AS …` over a navigation hop otherwise
            // stars every mapped property of the hop's union; the engine
            // projects a hop's join keys only (unionalias_1 = ID_0, ID_1),
            // and a starred alias blocks the positional union prune. The
            // expanded columns carry the source's own slots; the next
            // fixpoint round prunes the union.
            // (a source alias is visible ONLY inside its own select, so
            // this projection is the alias's one star reader — the census
            // entry it made in `starred` is its own)
            if (p.expr() instanceof SqlExpr.Star st && st.table() != null) {
                String tbl = st.table();
                SqlSource src = findSource(sel.from(), tbl);
                if (src != null && !(src instanceof SqlSource.Pivot)
                        && !src.outputs().isEmpty()) {
                    for (OutputCol oc : src.outputs()) {
                        if (used.contains(oc.name())
                                || r.unqualified().contains(oc.name())) {
                            kept.add(new SqlSelect.Projection(
                                    SqlExpr.Column.of(tbl, oc), oc.name(), oc));
                        }
                    }
                    narrowed = true;
                    continue;
                }
            }
            String out = p.alias() != null ? p.alias()
                    : p.expr() instanceof SqlExpr.Column c ? c.name() : null;
            if (!(p.expr() instanceof SqlExpr.Column) || out == null
                    || used.contains(out) || r.unqualified().contains(out)) {
                kept.add(p);
            }
        }
        if (!narrowed && kept.size() == sel.projections().size()) {
            return sel;
        }
        if (kept.isEmpty()) {
            // an empty SELECT list is not SQL — keep the first projection
            kept.add(sel.projections().get(0));
        }
        // outputs-from-projections: kept projections carry their slots
        return sel.withProjections(kept);
    }

    /** The FROM-tree source bound to {@code alias}, or null. */
    private static @com.legend.Nullable SqlSource findSource(
            SqlSource src, String alias) {
        return switch (src) {
            case SqlSource.Join j -> {
                SqlSource l = findSource(j.left(), alias);
                yield l != null ? l : findSource(j.right(), alias);
            }
            case SqlSource.Dual d -> null;
            default -> alias.equals(src.alias()) ? src : null;
        };
    }
}
