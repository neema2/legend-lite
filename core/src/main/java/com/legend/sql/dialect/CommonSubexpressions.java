// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A large subexpression a projection or a filter repeats is written ONCE, for a database whose statements are capped
 * (SQLite: 1,000,000 bytes, measured "statement too long"). The lowering repeats a value where it reuses one (a
 * float's text canon reads its text a dozen times, each read a regular expression's walk over it; a literal list's
 * canon texts in each comparison of a sort), and a writer spells each use in full: the statement grows with the
 * product of the reuses. Here an equal subtree is one node, and a repeated node becomes a column of a one-row derived
 * table under a scalar subquery: {@code (SELECT e' FROM (SELECT <node> AS v) AS __cseN)}.
 *
 * <p>Only a node whose value cannot depend on where it is evaluated is moved: no aggregate or window (the select's own
 * grouping), no subquery, nothing that raises (a CASE branch not taken must raise nothing: an ERROR, a checked
 * narrowing, a parse, a range check) and no fresh value, and nothing naming a lambda's parameter (it is bound inside
 * the lambda, never above it; a lambda's body is bound within itself first). A projection with an aggregate or window
 * anywhere is left as it is, and so is an explosion.
 */
final class CommonSubexpressions extends com.legend.sql.SqlRewriter {

    /** The expanded size, in nodes, from which a repeated node is bound. */
    private static final int MIN_SIZE = 40;

    private final int[] fresh = {0};

    /** The selects already rewritten, by content: an equal select is rewritten as it was, so equal copies of one
     *  value (a float's bound text in each branch) stay equal, their binds' names too, and are bound as one above
     *  them (step 9, 2026-10-19: each copy's fresh names had made 108 equal canons distinct, 1.29 MB on SQLite). */
    private final Map<SqlSelect, SqlQuery> rewritten = new java.util.HashMap<>();

    @Override
    protected SqlQuery select(SqlSelect s) {
        SqlQuery known = rewritten.get(s);
        if (known != null) {
            return known;
        }
        SqlQuery out = selectOnce(s);
        rewritten.put(s, out);
        return out;
    }

    private SqlQuery selectOnce(SqlSelect s) {
        if (ExplosionMarkers.isMarker(s)) {
            return s;
        }
        List<SqlSelect.Projection> ps = new ArrayList<>(s.projections().size());
        boolean changed = false;
        for (SqlSelect.Projection p : s.projections()) {
            SqlExpr e = ExplosionMarkers.isUnnest(p.expr()) || grouped(p.expr()) ? p.expr() : shared(p.expr());
            changed |= e != p.expr();
            ps.add(e == p.expr() ? p : new SqlSelect.Projection(e, p.alias(), p.out()));
        }
        SqlExpr where = s.where() == null || grouped(s.where()) ? s.where() : shared(s.where());
        changed |= where != s.where();
        return changed ? new SqlSelect(ps, s.distinct(), s.from(), where, s.groupBy(), s.having(), s.qualify(),
                s.orderBy(), s.limit(), s.offset(), s.outputs()) : s;
    }

    /** {@code e} bound ({@link #bound}) after its equal subtrees are made one node; {@code e} itself when nothing was
     *  bound. */
    private SqlExpr shared(SqlExpr e) {
        SqlExpr c = canonical(e, new IdentityHashMap<>(), new java.util.HashMap<>());
        SqlExpr out = bound(c);
        return out == c ? e : out;
    }

    /** {@code e} with its largest repeated movable node bound once, recursively; {@code e} itself when it has none. */
    private SqlExpr bound(SqlExpr e0) {
        SqlExpr e = lambdasBound(e0, new IdentityHashMap<>());
        Set<String> params = new HashSet<>();
        Map<SqlExpr, Integer> uses = new IdentityHashMap<>();
        visit(e, uses, params);
        Map<SqlExpr, Long> sizes = new IdentityHashMap<>();
        Map<SqlExpr, Boolean> movable = new IdentityHashMap<>();
        Map<SqlExpr, Set<String>> names = new IdentityHashMap<>();
        SqlExpr best = null;
        long bestSize = 0;
        for (Map.Entry<SqlExpr, Integer> u : uses.entrySet()) {
            if (u.getValue() < 2 || u.getKey() == e) {
                continue;
            }
            long size = size(u.getKey(), sizes);
            if (size >= MIN_SIZE && size > bestSize && movable(u.getKey(), movable)
                    && java.util.Collections.disjoint(names(u.getKey(), names), params)) {
                best = u.getKey();
                bestSize = size;
            }
        }
        if (best == null) {
            return e;
        }
        String alias = "__cse" + fresh[0]++;
        SqlType type = best.type() instanceof com.legend.sql.TypeFact.Typed t ? t.type() : null;
        SqlExpr ref = type == null ? SqlExpr.Column.derived(alias, "v")
                : SqlExpr.Column.of(alias, "v", type, true, com.legend.sql.OutputCol.Origin.DERIVED);
        SqlExpr outer = bound(replace(e, best, ref, new IdentityHashMap<>()));
        SqlExpr inner = bound(best);
        SqlSelect bind = new SqlSelect(List.of(new SqlSelect.Projection(inner, "v", column(inner))), false,
                new SqlSource.Dual(), null, List.of(), null, null, List.of(), null, null, List.of());
        SqlSelect use = new SqlSelect(List.of(new SqlSelect.Projection(outer, "v", column(outer))), false,
                new SqlSource.Subselect(bind, alias, null), null, List.of(), null, null, List.of(), null, null,
                List.of());
        return new SqlExpr.ScalarSubquery(use);
    }

    /** {@code e} with each subtree equal to another the same node (children first, so equality is a node's own), the
     *  first met kept: a repeated value written apart is a shared one (identity binds it). */
    private static SqlExpr canonical(SqlExpr e, Map<SqlExpr, SqlExpr> done, Map<SqlExpr, SqlExpr> pool) {
        SqlExpr known = done.get(e);
        if (known != null) {
            return known;
        }
        List<SqlExpr> kids = e.children();
        SqlExpr out = e;
        if (!kids.isEmpty()) {
            List<SqlExpr> mapped = new ArrayList<>(kids.size());
            boolean changed = false;
            for (SqlExpr k : kids) {
                SqlExpr m = canonical(k, done, pool);
                changed |= m != k;
                mapped.add(m);
            }
            out = changed ? e.withChildren(mapped) : e;
        }
        SqlExpr shared = pool.putIfAbsent(out, out);
        out = shared != null ? shared : out;
        done.put(e, out);
        return out;
    }

    /** {@code e} with each lambda's body bound within itself. */
    private SqlExpr lambdasBound(SqlExpr e, Map<SqlExpr, SqlExpr> done) {
        SqlExpr known = done.get(e);
        if (known != null) {
            return known;
        }
        SqlExpr out;
        if (e instanceof SqlExpr.Lambda l) {
            SqlExpr body = bound(l.body());
            out = body == l.body() ? l : new SqlExpr.Lambda(l.params(), body, l.type());
        } else {
            List<SqlExpr> kids = e.children();
            List<SqlExpr> mapped = new ArrayList<>(kids.size());
            boolean changed = false;
            for (SqlExpr k : kids) {
                SqlExpr m = lambdasBound(k, done);
                changed |= m != k;
                mapped.add(m);
            }
            out = changed ? e.withChildren(mapped) : e;
        }
        done.put(e, out);
        return out;
    }

    private static com.legend.sql.@com.legend.base.Nullable OutputCol column(SqlExpr e) {
        return e.type() instanceof com.legend.sql.TypeFact.Typed t ? new com.legend.sql.OutputCol("v", t.type(), true)
                : null;
    }

    /** Each node's uses: counted, its children walked at its first; every lambda's parameters collected. */
    private static void visit(SqlExpr e, Map<SqlExpr, Integer> uses, Set<String> params) {
        Integer n = uses.get(e);
        uses.put(e, n == null ? 1 : n + 1);
        if (n == null) {
            if (e instanceof SqlExpr.Lambda l) {
                params.addAll(l.params());
            }
            for (SqlExpr k : e.children()) {
                visit(k, uses, params);
            }
        }
    }

    /** The unqualified column names a node reads (a lambda parameter is one). */
    private static Set<String> names(SqlExpr e, Map<SqlExpr, Set<String>> memo) {
        Set<String> known = memo.get(e);
        if (known != null) {
            return known;
        }
        Set<String> out = new HashSet<>();
        if (e instanceof SqlExpr.Column c && c.table() == null) {
            out.add(c.name());
        }
        for (SqlExpr k : e.children()) {
            out.addAll(names(k, memo));
        }
        memo.put(e, out);
        return out;
    }

    /** The calls written as a regular expression's walk: a statement of its own (SqliteRegex: two kilobytes). */
    private static final Set<SqlFn> WALKS = Set.of(SqlFn.REGEXP_EXTRACT, SqlFn.REGEXP_EXTRACT_ALL,
            SqlFn.REGEXP_REPLACE, SqlFn.REGEXP_FULL_MATCH, SqlFn.MATCHES);

    /** The calls written as a subquery over a list's elements. */
    private static final Set<SqlFn> SUBQUERIES = Set.of(SqlFn.LIST_FILTER, SqlFn.LIST_TRANSFORM,
            SqlFn.LIST_SORT, SqlFn.LIST_SORT_DESC, SqlFn.LIST_DISTINCT, SqlFn.SPLIT, SqlFn.SPLIT_PART,
            SqlFn.PURE_SPLIT_PART);

    /** The node's size as written: its children's, each use in full, and its own (a walk's or a subquery's weight,
     *  else one). */
    private static long size(SqlExpr e, Map<SqlExpr, Long> sizes) {
        Long known = sizes.get(e);
        if (known != null) {
            return known;
        }
        long n = !(e instanceof SqlExpr.Call c) ? 1
                : WALKS.contains(c.fn()) ? 200 : SUBQUERIES.contains(c.fn()) ? 20 : 1;
        // a bind's expressions are no child of its node: its size is theirs
        if (e instanceof SqlExpr.ScalarSubquery q && q.subquery() instanceof SqlSelect use
                && use.from() instanceof SqlSource.Subselect sub && sub.inner() instanceof SqlSelect bind) {
            for (SqlSelect.Projection p : use.projections()) {
                n = Math.min(Long.MAX_VALUE / 2, n + size(p.expr(), sizes));
            }
            for (SqlSelect.Projection p : bind.projections()) {
                n = Math.min(Long.MAX_VALUE / 2, n + size(p.expr(), sizes));
            }
        }
        for (SqlExpr k : e.children()) {
            n = Math.min(Long.MAX_VALUE / 2, n + size(k, sizes));
        }
        sizes.put(e, n);
        return n;
    }

    private static SqlExpr replace(SqlExpr e, SqlExpr target, SqlExpr with, Map<SqlExpr, SqlExpr> done) {
        if (e == target) {
            return with;
        }
        SqlExpr known = done.get(e);
        if (known != null) {
            return known;
        }
        List<SqlExpr> kids = e.children();
        SqlExpr out = e;
        if (!kids.isEmpty()) {
            List<SqlExpr> mapped = new ArrayList<>(kids.size());
            boolean changed = false;
            for (SqlExpr k : kids) {
                SqlExpr m = replace(k, target, with, done);
                changed |= m != k;
                mapped.add(m);
            }
            out = changed ? e.withChildren(mapped) : e;
        }
        done.put(e, out);
        return out;
    }

    /** A node whose value is its operands' alone, and which raises nothing: anything but a raise (an ERROR, a checked
     *  narrowing, a parse, a domain or range check, a JSON cast of a text), a fresh value (a GUID), an aggregate or a
     *  window, or a subquery. */
    private static boolean movable(SqlExpr e, Map<SqlExpr, Boolean> memo) {
        Boolean known = memo.get(e);
        if (known != null) {
            return known;
        }
        boolean ok = switch (e) {
            case SqlExpr.Call c -> !RAISING.contains(c.fn());
            case SqlExpr.Cast c -> c.target() != SqlType.Scalar.JSON;
            case SqlExpr.CheckedOne c -> false;
            case SqlExpr.CheckedDefects c -> false;
            case SqlExpr.CheckedChildValue c -> false;
            case SqlExpr.WindowCall w -> false;
            case SqlExpr.ScalarSubquery q -> isBind(q, memo);
            case SqlExpr.Exists q -> false;
            case SqlExpr.InSubquery q -> false;
            case SqlExpr.Quantified q -> false;
            case SqlExpr.Star st -> false;
            case SqlExpr.StarExcept st -> false;
            case SqlExpr.DeferredTdsString d -> false;
            case SqlExpr.TempTableInSplice t -> false;
            default -> !(e instanceof com.legend.sql.SqlAgg);
        };
        if (ok) {
            for (SqlExpr k : e.children()) {
                if (!movable(k, memo)) {
                    ok = false;
                    break;
                }
            }
        }
        memo.put(e, ok);
        return ok;
    }

    /** A one-row bind ({@code (SELECT e FROM (SELECT x AS v) AS a)}, the shape this pass and a float's bound text
     *  write) of movable expressions: its value is its operands' alone (step 9, 2026-10-19). */
    private static boolean isBind(SqlExpr.ScalarSubquery q, Map<SqlExpr, Boolean> memo) {
        if (!(q.subquery() instanceof SqlSelect use) || !plain(use) || use.projections().size() != 1
                || !(use.from() instanceof SqlSource.Subselect sub) || !(sub.inner() instanceof SqlSelect bind)
                || !plain(bind) || !(bind.from() instanceof SqlSource.Dual)) {
            return false;
        }
        // its expressions are no child of the subquery's node, so a lambda parameter read inside it is refused here
        // (an unqualified column is one), never bound above the lambda
        for (SqlSelect.Projection p : bind.projections()) {
            if (!movable(p.expr(), memo) || grouped(p.expr()) || !names(p.expr(), new IdentityHashMap<>()).isEmpty()) {
                return false;
            }
        }
        SqlExpr spelled = use.projections().get(0).expr();
        return movable(spelled, memo) && !grouped(spelled) && names(spelled, new IdentityHashMap<>()).isEmpty();
    }

    private static boolean plain(SqlSelect s) {
        return !s.distinct() && s.where() == null && s.groupBy().isEmpty() && s.having() == null
                && s.qualify() == null && s.orderBy().isEmpty() && s.limit() == null && s.offset() == null;
    }

    /** An aggregate or a window anywhere: the select's own grouping reads it. */
    private static boolean grouped(SqlExpr e) {
        Map<SqlExpr, Boolean> seen = new IdentityHashMap<>();
        return grouped(e, seen);
    }

    private static boolean grouped(SqlExpr e, Map<SqlExpr, Boolean> seen) {
        if (seen.put(e, Boolean.TRUE) != null) {
            return false;
        }
        if (e instanceof com.legend.sql.SqlAgg || e instanceof SqlExpr.WindowCall) {
            return true;
        }
        for (SqlExpr k : e.children()) {
            if (grouped(k, seen)) {
                return true;
            }
        }
        return false;
    }

    /** The calls that raise (or answer a fresh value) when evaluated: never moved out of a branch. */
    private static final Set<SqlFn> RAISING = Set.of(SqlFn.ERROR, SqlFn.PARSE_INT, SqlFn.PARSE_DATE, SqlFn.ACOS,
            SqlFn.ASIN, SqlFn.ADD_INTERVAL, SqlFn.ADD_INTERVAL_TEMPORAL, SqlFn.STRPTIME, SqlFn.GUID, SqlFn.TIMEZONE,
            SqlFn.MAKE_DATE, SqlFn.MAKE_TIMESTAMP);
}
