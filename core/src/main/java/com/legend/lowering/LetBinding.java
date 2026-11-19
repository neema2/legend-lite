// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.sql.OutputCol;
import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlUnion;
import com.legend.sql.SqlWith;
import com.legend.sql.SqlType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * P1a, WHAT THE ROOT WRITES TWICE COMPUTED ONCE (E3 of the total-wall program, ruled by the orchestrator 2026-11-12,
 * docs/LEAN_PRODUCT_SQL_DESIGN_2026_11_12.md §5.1): in the root's FROM-less select, every expression written more than
 * once -- a let's or an argument's value at each read, an object read per property, a list read by each arm of a
 * guard -- is computed ONCE, in a one-row derived table under the select ({@code SELECT ... FROM (SELECT <value> AS
 * __l0) __lets}), and read by column ({@link #bindRepeats}). A Pure let and argument are call by value, evaluated once:
 * binding once is Pure's own semantics, and the statement is the same query, smaller.
 *
 * <p>Conservative, as ruled (P1a; eager lets are P1a.2), so no answer changes: an expression is bound only where it is
 * closed (it reads no column and no lambda parameter it does not bind), it can raise nowhere (the laziness rule: a
 * raise stays under its conditions), it aggregates nothing, it reads no subquery, it draws nothing afresh
 * (random(), generateGuid()), its type is a plain SQL type (a tagged value is read by its structure), and binding it
 * saves characters. Bindings sit only in the root select's own terms, never inside a derived table (H2 reads no outer
 * column there; measured, its lane, 2026-11-12).
 */
final class LetBinding {

    private LetBinding() {
    }

    /** The derived table's alias, which every bound let's reference names. */
    static final String ALIAS = "__lets";

    /** One bound let: its column, its lowered value, its SQL type, and the bound lets its value reads. */
    record Bound(String column, SqlExpr value, SqlType type, int level) {
    }

    /** The reference a bound let's reads lower to. */
    static SqlExpr.Column reference(Bound b) {
        return SqlExpr.Column.of(ALIAS, b.column(), b.type(), true, OutputCol.Origin.DERIVED);
    }

    /** Whether {@code e} is worth a binding: an evaluation -- a call (the clock's {@code now()} too: read once, as
     *  Pure's let reads it), a subquery, any expression of parts -- never a column, a literal or a parameter. */
    static boolean worthBinding(SqlExpr e) {
        return !(e instanceof SqlExpr.Column || e instanceof SqlExpr.PlanParam)
                && (e instanceof SqlExpr.Call || !e.children().isEmpty());
    }

    /** The functions whose call can raise on some input: the SQL layer's own raising set ({@link SqlFn#RAISING}: a
     *  planned raise, a parse, a range or interval check, a fresh draw) united with the arithmetic and text domains a
     *  binding must not lift out of a branch either (division, a root, a logarithm, a pattern) -- one union, here
     *  (2026-11-15, PR1's review: this list had missed ADD_INTERVAL, TIMEZONE, MAKE_DATE and MAKE_TIMESTAMP). */
    private static final java.util.Set<SqlFn> RAISING;

    static {
        java.util.EnumSet<SqlFn> all = java.util.EnumSet.copyOf(SqlFn.RAISING);
        all.addAll(java.util.EnumSet.of(SqlFn.DIVIDE, SqlFn.MOD, SqlFn.REM, SqlFn.INT_DIVIDE, SqlFn.SQRT, SqlFn.LN,
                SqlFn.REGEXP_EXTRACT, SqlFn.REGEXP_EXTRACT_ALL, SqlFn.REGEXP_FULL_MATCH, SqlFn.REGEXP_REPLACE));
        RAISING = java.util.Collections.unmodifiableSet(all);
    }

    /** Whether {@code e} can raise, read from its structure alone (the laziness rule, ruled 2026-11-12): a call of a
     *  {@link #RAISING} function, a cast of anything but a literal, a checked one, anywhere in its tree. */
    static boolean canRaise(SqlExpr e) {
        boolean[] raises = {false};
        mapExpr(e, x -> {
            if (x instanceof SqlExpr.Call c && RAISING.contains(c.fn())
                    || x instanceof SqlExpr.Cast cast && castCanRaise(cast)
                    || x instanceof SqlExpr.CheckedOne) {
                raises[0] = true;
            }
            return null;
        });
        return raises[0];
    }

    /** A node's replacement, or null to keep the node and walk its children. */
    @FunctionalInterface
    interface Replace {
        @com.legend.base.Nullable SqlExpr apply(SqlExpr e);
    }

    /** Whether a cast can raise, by its operand's type and its target (structure alone): a literal's cast never
     *  (planning knows it); a text or a JSON value read as any other kind, or an untyped operand, can; a value narrowed
     *  into an integral or a decimal type can (an overflow); any other (a widening, a temporal, anything to text)
     *  cannot. */
    private static boolean castCanRaise(SqlExpr.Cast cast) {
        if (cast.value().children().isEmpty() && !(cast.value() instanceof SqlExpr.Call)
                && !(cast.value() instanceof SqlExpr.Column) && !(cast.value() instanceof SqlExpr.PlanParam)) {
            // a literal's cast: planning knows its value -- but a text read as another kind may not read
            return cast.value() instanceof SqlExpr.StringLit && cast.target() != SqlType.Scalar.VARCHAR;
        }
        if (!(cast.value().type() instanceof com.legend.sql.TypeFact.Typed from)) {
            return true;
        }
        SqlType to = cast.target();
        if (to == SqlType.Scalar.VARCHAR) {
            return false;
        }
        if (from.type() == SqlType.Scalar.VARCHAR || from.type() == SqlType.Scalar.JSON) {
            return true;
        }
        boolean narrowing = to == SqlType.Scalar.INTEGER || to == SqlType.Scalar.BIGINT
                || to instanceof SqlType.Decimal;
        return narrowing && !from.type().equals(to) && from.type() != SqlType.Scalar.INTEGER
                && !(from.type() == SqlType.Scalar.BIGINT && to != SqlType.Scalar.INTEGER);
    }

    /** {@code q} with what its root's FROM-less select writes more than once bound once under it; {@code q} itself
     *  where its root has no FROM-less select at the bottom of its chain of derived tables, or nothing repeats. */
    static SqlQuery bindRepeats(SqlQuery q) {
        if (!(q instanceof SqlSelect s)) {
            return q;
        }
        if (s.from() instanceof SqlSource.Dual) {
            return bindIn(s);
        }
        if (s.from() instanceof SqlSource.Subselect sub) {
            SqlQuery inner = bindRepeats(sub.inner());
            return inner == sub.inner() ? s : s.withFrom(new SqlSource.Subselect(inner, sub.alias(), sub.frameName()));
        }
        return q;
    }

    /** {@code s}'s repeats bound, the most saving first, until none saves (a binding's value is searched too: a
     *  repeat inside two bound values is bound once, beneath them). */
    private static SqlSelect bindIn(SqlSelect s) {
        // a select that unnests in its own terms returns a set, placed by its FROM's shape (H2 places it only over no
        // FROM; measured, its lane, 2026-11-12): it keeps its FROM
        if (Fold.unnestInProjections(s)) {
            return s;
        }
        List<Bound> bound = new ArrayList<>();
        SqlSelect at = s;
        while (true) {
            // insertion-ordered: the same binding chosen on every JVM when two repeats save alike (G10)
            java.util.Map<SqlExpr, Integer> counts = new java.util.LinkedHashMap<>();
            java.util.Set<SqlExpr> always = new java.util.HashSet<>();
            for (SqlExpr t : terms(at)) {
                count(t, counts);
            }
            // what the row evaluates whatever its values: its filter, where it has one (a row the filter drops
            // evaluates none of its projections), else every term
            if (at.where() != null) {
                alwaysEvaluated(at.where(), always);
            } else if (at.having() == null && at.qualify() == null) {
                terms(at).forEach(t -> alwaysEvaluated(t, always));
            }
            for (Bound b : bound) {
                count(b.value(), counts);
                alwaysEvaluated(b.value(), always);
            }
            SqlExpr best = null;
            long saving = 0;
            for (var e : counts.entrySet()) {
                if (e.getValue() < 2) {
                    continue;
                }
                long w = weight(e.getKey());
                // in nodes: k copies of w become k references (about two nodes each, as written: an alias and a
                // column, quoted on some databases) and one value, under a select of its own
                long k = e.getValue();
                long save = (k - 1) * w - 2 * k - 6;
                if (save <= 0 && readsClock(e.getKey())) {
                    save = 1;   // the statement's clock read once, as Pure's let reads it (the design's §5.1)
                }
                if (save > saving && eligible(e.getKey(), always)) {
                    best = e.getKey();
                    saving = save;
                }
            }
            if (best == null) {
                break;
            }
            SqlType type = ((com.legend.sql.TypeFact.Typed) best.type()).type();
            Bound b = new Bound("__l" + bound.size(), best, type, 0);
            SqlExpr ref = reference(b);
            SqlExpr found = best;
            Replace swap = x -> x.equals(found) ? ref : null;
            // swapped where it was counted: the select's own terms, never its FROM nor a subquery's (2026-11-15,
            // PR1's review: the swap had descended into derived tables and subqueries the count never saw, where
            // the binding's alias is not in scope -- H2 reads no outer column there)
            at = swapTerms(at, swap);
            List<Bound> rewritten = new ArrayList<>();
            for (Bound o : bound) {
                rewritten.add(new Bound(o.column(), swapIn(o.value(), swap), o.type(), 0));
            }
            rewritten.add(b);
            bound = rewritten;
        }
        if (bound.isEmpty()) {
            return s;
        }
        // each binding one level above every binding its value reads
        List<Bound> leveled = new ArrayList<>();
        for (Bound b : bound) {
            leveled.add(new Bound(b.column(), b.value(), b.type(), depth(b, bound, new java.util.HashSet<>())));
        }
        return at.withFrom(new SqlSource.Subselect(source(leveled), ALIAS, null));
    }

    private static int depth(Bound b, List<Bound> all, java.util.Set<String> seen) {
        if (!seen.add(b.column())) {
            throw new IllegalStateException("a binding reads itself: " + b.column());
        }
        int d = 0;
        for (Bound o : all) {
            if (o != b && reads(b.value(), o.column())) {
                d = Math.max(d, depth(o, all, new java.util.HashSet<>(seen)) + 1);
            }
        }
        return d;
    }

    private static boolean reads(SqlExpr e, String column) {
        boolean[] r = {false};
        mapExpr(e, x -> {
            if (x instanceof SqlExpr.Column c && ALIAS.equals(c.table()) && column.equals(c.name())) {
                r[0] = true;
            }
            return null;
        });
        return r[0];
    }

    private static boolean readsClock(SqlExpr e) {
        if (e instanceof SqlExpr.Call c && (c.fn() == SqlFn.NOW || c.fn() == SqlFn.TODAY)) {
            return true;
        }
        for (SqlExpr c : e.children()) {
            if (readsClock(c)) {
                return true;
            }
        }
        return false;
    }

    /** {@code s} with {@code f} applied to its own terms ({@link #terms}), as {@link #count} read them. */
    private static SqlSelect swapTerms(SqlSelect s, Replace f) {
        List<SqlSelect.Projection> ps = s.projections().stream()
                .map(p -> new SqlSelect.Projection(swapIn(p.expr(), f), p.alias(), p.out())).toList();
        List<SqlSelect.SortKey> order = s.orderBy().stream()
                .map(k -> new SqlSelect.SortKey(swapIn(k.expr(), f), k.ascending(), k.nullOrder(), k.outputName()))
                .toList();
        return new SqlSelect(ps, s.distinct(), s.from(), s.where() == null ? null : swapIn(s.where(), f),
                s.groupBy().stream().map(g -> swapIn(g, f)).toList(), s.having() == null ? null : swapIn(s.having(), f),
                s.qualify() == null ? null : swapIn(s.qualify(), f), order, s.limit(), s.offset(), s.outputs());
    }

    /** {@code e} with {@code f} applied where {@link #count} walks: its children, never into a query a node carries. */
    private static SqlExpr swapIn(SqlExpr e, Replace f) {
        SqlExpr replaced = f.apply(e);
        return replaced != null ? replaced : e.mapChildren(c -> swapIn(c, f));
    }

    /** The select's own terms (never its FROM): its projections, filters, groups and orders. */
    private static List<SqlExpr> terms(SqlSelect s) {
        List<SqlExpr> out = new ArrayList<>();
        s.projections().forEach(p -> out.add(p.expr()));
        if (s.where() != null) {
            out.add(s.where());
        }
        out.addAll(s.groupBy());
        if (s.having() != null) {
            out.add(s.having());
        }
        if (s.qualify() != null) {
            out.add(s.qualify());
        }
        s.orderBy().forEach(k -> out.add(k.expr()));
        return out;
    }

    /** Each expression's occurrences, by structure; a repeated expression's parts are counted once with it. */
    private static void count(SqlExpr e, java.util.Map<SqlExpr, Integer> counts) {
        if (counts.merge(e, 1, Integer::sum) > 1) {
            return;
        }
        for (SqlExpr c : e.children()) {
            count(c, counts);
        }
    }

    private static long weight(SqlExpr e) {
        long w = 1;
        for (SqlExpr c : e.children()) {
            w += weight(c);
        }
        return w;
    }

    /**
     * Whether {@code e} may be computed once under the root where {@code always} holds the expressions the root's row
     * evaluates whatever its values (step 7's first half, 2026-11-15, brought into PR1 by round 55f: item 9 put
     * MAKE_TIMESTAMP in {@link #RAISING}, and the date family's repeats went unbound on every database): as
     * {@link #eligible(SqlExpr)}, and a value that can raise too where one of its copies is always evaluated -- it
     * raises there already, so computing it once, eagerly, raises nothing the statement did not (the laziness rule: a
     * raise is never moved out of a branch that guards it).
     */
    static boolean eligible(SqlExpr e, java.util.Set<SqlExpr> always) {
        if (!worthBinding(e) || e instanceof SqlExpr.Lambda
                || !(e.type() instanceof com.legend.sql.TypeFact.Typed)) {
            return false;
        }
        return closedAndPlain(e, java.util.Set.of()) && (!canRaise(e) || always.contains(e));
    }

    /** The calls that may skip an argument on some dialect ({@link SqlFn#MAY_SKIP_AN_ARGUMENT}: the SQL layer's own
     *  record of its renderings; round 55k, 2026-11-15, the orchestrator's ruling under the user's delegation: widen the
     *  set, never across a rendering that can skip). */
    private static final java.util.Set<SqlFn> LAZY = SqlFn.MAY_SKIP_AN_ARGUMENT;

    /**
     * Every expression {@code e} evaluates whatever its values, into {@code into}: itself, and through each node that
     * is STRICT on every dialect, its children. One exhaustive switch over {@link SqlExpr}'s variants, no default arm
     * (the orchestrator's ruling, 2026-11-15: a new variant decides strict or skipping at compile time, never by a
     * default). Skipping, so never descended: a CASE past its first condition, a {@link #LAZY} call, a lambda's body,
     * a subquery, an aggregate or window, a fold, and the carriers whose renderings branch (tagged, compacted,
     * membership, checked, raise text).
     */
    static void alwaysEvaluated(SqlExpr e, java.util.Set<SqlExpr> into) {
        into.add(e);
        boolean strict = switch (e) {
            // leaves: nothing below them
            case SqlExpr.Column x -> false;
            case SqlExpr.Star x -> false;
            case SqlExpr.StarExcept x -> false;
            case SqlExpr.StringLit x -> false;
            case SqlExpr.IntLit x -> false;
            case SqlExpr.FloatLit x -> false;
            case SqlExpr.DecimalLit x -> false;
            case SqlExpr.BoolLit x -> false;
            case SqlExpr.NullLit x -> false;
            case SqlExpr.DateLit x -> false;
            case SqlExpr.TimestampLit x -> false;
            case SqlExpr.TimeLit x -> false;
            case SqlExpr.FormatLit x -> false;
            case SqlExpr.DatePartLit x -> false;
            case SqlExpr.JsonKindLit x -> false;
            case SqlExpr.TypeNameLit x -> false;
            case SqlExpr.PlanParam x -> false;
            case SqlExpr.KeyCell x -> false;
            case SqlExpr.RowOrder x -> false;
            case SqlExpr.StoredRead x -> false;
            // strict: every child evaluated on every dialect
            case SqlExpr.Call c -> !LAZY.contains(c.fn());
            case SqlExpr.Cast x -> true;
            case SqlExpr.ArrayLit x -> true;
            case SqlExpr.StructLit x -> true;
            case SqlExpr.StructGet x -> true;
            case SqlExpr.JsonArray x -> true;
            case SqlExpr.JsonObject x -> true;
            case SqlExpr.Group x -> true;
            // a CASE: its first condition alone
            case SqlExpr.Case c -> {
                if (!c.whens().isEmpty()) {
                    alwaysEvaluated(c.whens().get(0).condition(), into);
                }
                yield false;
            }
            // skipping, or evaluated by its own rules: never descended
            case SqlExpr.Lambda x -> false;
            case SqlExpr.Exists x -> false;
            case SqlExpr.ScalarSubquery x -> false;
            case SqlExpr.InSubquery x -> false;
            case SqlExpr.Quantified x -> false;
            case SqlExpr.CheckedOne x -> false;
            case SqlExpr.CheckedDefects x -> false;
            case SqlExpr.CheckedChildValue x -> false;
            case SqlExpr.CompactList x -> false;
            case SqlExpr.Tagged x -> false;
            case SqlExpr.TaggedList x -> false;
            case SqlExpr.RaiseText x -> false;
            case SqlExpr.WindowCall x -> false;
            case SqlExpr.FoldCall x -> false;
            case SqlExpr.JsonArrayAgg x -> false;
            case SqlExpr.OrderedListAgg x -> false;
            case SqlExpr.ReduceCollection x -> false;
            case SqlExpr.Membership x -> false;
            case SqlAgg.Reducer x -> false;
        };
        if (strict) {
            e.children().forEach(child -> alwaysEvaluated(child, into));
        }
    }

    /** Whether {@code e} may be computed once under the root: worth a binding, typed by a plain SQL type, closed, and
     *  holding nothing that aggregates, reads a subquery, is checked, is tagged, draws afresh or can raise. */
    static boolean eligible(SqlExpr e) {
        if (!worthBinding(e) || e instanceof SqlExpr.Lambda
                || !(e.type() instanceof com.legend.sql.TypeFact.Typed)) {
            return false;
        }
        return closedAndPlain(e, java.util.Set.of()) && !canRaise(e);
    }

    private static boolean closedAndPlain(SqlExpr e, java.util.Set<String> params) {
        switch (e) {
            case SqlExpr.Column c -> {
                return c.table() == null && params.contains(c.name());
            }
            case SqlExpr.Lambda l -> {
                java.util.Set<String> inner = new java.util.HashSet<>(params);
                inner.addAll(l.params());
                return closedAndPlain(l.body(), inner);
            }
            case SqlAgg.Reducer r -> {
                return false;
            }
            case SqlExpr.WindowCall w -> {
                return false;
            }
            case SqlExpr.OrderedListAgg a -> {
                return false;
            }
            case SqlExpr.JsonArrayAgg a -> {
                return false;
            }
            case SqlExpr.ScalarSubquery q -> {
                return false;
            }
            case SqlExpr.Exists q -> {
                return false;
            }
            case SqlExpr.InSubquery q -> {
                return false;
            }
            case SqlExpr.Quantified q -> {
                return false;
            }
            case SqlExpr.Tagged t -> {
                return false;
            }
            case SqlExpr.TaggedList t -> {
                return false;
            }
            case SqlExpr.CheckedOne c -> {
                return false;
            }
            case SqlExpr.CheckedDefects c -> {
                return false;
            }
            case SqlExpr.CheckedChildValue c -> {
                return false;
            }
            case SqlExpr.Group g -> {
                return false;
            }
            case SqlExpr.KeyCell k -> {
                return false;
            }
            case SqlExpr.RowOrder r -> {
                return false;
            }
            case SqlExpr.StoredRead r -> {
                return false;
            }
            case SqlExpr.Star st -> {
                return false;
            }
            case SqlExpr.StarExcept st -> {
                return false;
            }
            default -> {
                if (Fold.nondeterministic(e)) {
                    return false;
                }
                for (SqlExpr c : e.children()) {
                    if (!closedAndPlain(c, params)) {
                        return false;
                    }
                }
                return true;
            }
        }
    }

    /** The one-row derived table of {@code bound}: one select per level, each reading the level below. */
    private static SqlQuery source(List<Bound> bound) {
        int top = bound.stream().mapToInt(Bound::level).max().orElseThrow();
        SqlQuery below = null;
        List<Bound> carried = new ArrayList<>();
        for (int level = 0; level <= top; level++) {
            List<SqlSelect.Projection> cols = new ArrayList<>();
            List<OutputCol> outs = new ArrayList<>();
            for (Bound b : carried) {
                cols.add(new SqlSelect.Projection(reference(b), b.column(), new OutputCol(b.column(), b.type(), true)));
                outs.add(new OutputCol(b.column(), b.type(), true));
            }
            for (Bound b : bound) {
                if (b.level() == level) {
                    cols.add(new SqlSelect.Projection(b.value(), b.column(), new OutputCol(b.column(), b.type(), true)));
                    outs.add(new OutputCol(b.column(), b.type(), true));
                    carried.add(b);
                }
            }
            SqlSource from = below == null ? new SqlSource.Dual() : new SqlSource.Subselect(below, ALIAS, null);
            below = new SqlSelect(cols, false, from, null, List.of(), null, null, List.of(), null, null, outs);
        }
        return java.util.Objects.requireNonNull(below, "a binding at level 0");
    }

    // ── the walk: f answers a node's replacement, or null to keep it and walk its children ──

    static SqlQuery map(SqlQuery q, Replace f) {
        return switch (q) {
            case SqlSelect s -> mapSelect(s, f);
            case SqlUnion u -> new SqlUnion(u.branches().stream().map(b -> map(b, f)).toList(), u.all(), u.outputs());
            case SqlWith w -> w.with(w.ctes().stream()
                    .map(c -> new SqlWith.Cte(c.name(), map(c.query(), f), c.materialized())).toList(),
                    map(w.body(), f));
        };
    }

    private static SqlSelect mapSelect(SqlSelect s, Replace f) {
        List<SqlSelect.Projection> ps = s.projections().stream()
                .map(p -> new SqlSelect.Projection(mapExpr(p.expr(), f), p.alias(), p.out())).toList();
        List<SqlSelect.SortKey> order = s.orderBy().stream()
                .map(k -> new SqlSelect.SortKey(mapExpr(k.expr(), f), k.ascending(), k.nullOrder(), k.outputName()))
                .toList();
        return new SqlSelect(ps, s.distinct(), mapSource(s.from(), f), opt(s.where(), f),
                s.groupBy().stream().map(g -> mapExpr(g, f)).toList(), opt(s.having(), f), opt(s.qualify(), f), order,
                s.limit(), s.offset(), s.outputs());
    }

    private static @com.legend.base.Nullable SqlExpr opt(@com.legend.base.Nullable SqlExpr e,
            Replace f) {
        return e == null ? null : mapExpr(e, f);
    }

    private static SqlSource mapSource(SqlSource src, Replace f) {
        return switch (src) {
            case SqlSource.Dual d -> d;
            case SqlSource.Table t -> t;
            case SqlSource.Cte c -> c;
            case SqlSource.SourceUrl u -> u;
            case SqlSource.RawSql r -> r;
            case SqlSource.Catalog c -> c;
            case SqlSource.GridReport g -> new SqlSource.GridReport(map(g.grid(), f), g.part(), g.alias(), g.outputs());
            case SqlSource.Subselect sub -> new SqlSource.Subselect(map(sub.inner(), f), sub.alias(), sub.frameName());
            case SqlSource.Join j -> new SqlSource.Join(mapSource(j.left(), f), mapSource(j.right(), f), j.kind(),
                    opt(j.on(), f), j.outputs());
            case SqlSource.Values v -> new SqlSource.Values(v.rows().stream()
                    .map(row -> row.stream().map(e -> mapExpr(e, f)).toList()).toList(), v.columns(), v.alias(),
                    v.outputs(), v.payload());
            case SqlSource.Pivot p -> new SqlSource.Pivot(mapSource(p.source(), f),
                    p.on().stream().map(e -> mapExpr(e, f)).toList(), p.in().stream().map(e -> mapExpr(e, f)).toList(),
                    p.named().stream().map(e -> mapExpr(e, f)).toList(),
                    p.usings().stream().map(u -> new SqlSource.Pivot.Using(reducer(mapExpr(u.agg(), f)), u.alias(),
                            u.type())).toList(), p.alias(), p.outputs());
        };
    }

    private static SqlAgg.Reducer reducer(SqlExpr e) {
        if (e instanceof SqlAgg.Reducer r) {
            return r;
        }
        throw new IllegalStateException("a pivot's aggregate mapped to a " + e.getClass().getSimpleName()
                + ": a binding never replaces an aggregate");
    }

    /** {@code e} mapped: {@code f}'s replacement where it gives one, else its children mapped -- the four expressions
     *  that carry a query mapping their query too (the children contract leaves those to their owner). */
    static SqlExpr mapExpr(SqlExpr e, Replace f) {
        SqlExpr replaced = f.apply(e);
        if (replaced != null) {
            return replaced;
        }
        SqlExpr withQuery = switch (e) {
            case SqlExpr.ScalarSubquery s -> new SqlExpr.ScalarSubquery(map(s.subquery(), f), s.type());
            case SqlExpr.Exists x -> new SqlExpr.Exists(map(x.subquery(), f), x.type());
            case SqlExpr.InSubquery i -> new SqlExpr.InSubquery(i.value(), map(i.subquery(), f), i.type());
            case SqlExpr.Quantified q -> new SqlExpr.Quantified(q.value(), q.comparison(), q.quantifier(),
                    map(q.subquery(), f), q.type());
            default -> e;
        };
        return withQuery.mapChildren(c -> mapExpr(c, f));
    }
}
