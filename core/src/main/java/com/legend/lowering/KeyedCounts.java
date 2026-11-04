// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlUnion;
import com.legend.sql.SqlWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A COUNT OF ROWS A DECLARED KEY MAKES ONE IS ONE (2026-11-04; the timeseries time model's D2, agreed: "the check
 * skipped where uniqueness is known, a declared key"): {@code COUNT(*) OVER (PARTITION BY cols)} where the rows it
 * counts are unique on {@code cols} -- a store table's declared primary key, narrowed by a {@code WHERE col = literal}
 * and carried through plain columns, a join on the other side's whole key, a {@code GROUP BY}, a shared relation's
 * WITH -- is the literal 1. The timeseries library counts each line's rows on each period to refuse two lines that
 * both have several there (D1); a line of a table whose key is its instrument and date has one, and its check then
 * costs nothing (spec-13: 15 such windows, run 73 -> 30 ms on DuckDB, measured 2026-11-04). The fact is the store's,
 * as the declared columns' types are: Knowledge the model holds, read where it is written. Not on the engine-text
 * printer.
 *
 * <p>A key is a set of a source's columns no two of its rows share; the empty set is a key of at most one row. Only
 * {@code COUNT(*)} with no DISTINCT, ORDER BY or FILTER, at a projection's top, in a select of no GROUP BY, whose
 * partition is plain columns of its source, is folded.
 */
final class KeyedCounts {

    private KeyedCounts() {
    }

    /** A source's column, as a select reads it. */
    private record Col(String table, String name) {
    }

    /** {@code q} with each count a key makes one the literal 1; {@code tableKeys} each table read's declared key, by
     *  the alias it is read under. */
    static SqlQuery fold(SqlQuery q, Map<String, List<String>> tableKeys) {
        return new KeyedCounts.Walk(tableKeys).query(q, new HashMap<>());
    }

    private static final class Walk {
        private final Map<String, List<String>> tableKeys;

        Walk(Map<String, List<String>> tableKeys) {
            this.tableKeys = tableKeys;
        }

        /** {@code q} folded; {@code ctes} the keys of the common table expressions in scope, by name. */
        SqlQuery query(SqlQuery q, Map<String, List<Set<String>>> ctes) {
            return switch (q) {
                case SqlWith w -> {
                    Map<String, List<Set<String>>> scope = new HashMap<>(ctes);
                    List<SqlWith.Cte> cs = new ArrayList<>(w.ctes().size());
                    boolean changed = false;
                    for (SqlWith.Cte c : w.ctes()) {
                        SqlQuery folded = query(c.query(), scope);
                        changed |= folded != c.query();
                        cs.add(folded == c.query() ? c : new SqlWith.Cte(c.name(), folded, c.materialized()));
                        scope.put(c.name(), keys(folded, scope));
                        cteEqual.put(c.name(), outEqual(folded));
                    }
                    SqlQuery body = query(w.body(), scope);
                    yield !changed && body == w.body() ? w : w.with(cs, body);
                }
                case SqlUnion u -> {
                    List<SqlQuery> bs = new ArrayList<>(u.branches().size());
                    boolean changed = false;
                    for (SqlQuery b : u.branches()) {
                        SqlQuery f = query(b, ctes);
                        changed |= f != b;
                        bs.add(f);
                    }
                    yield changed ? new SqlUnion(bs, u.all(), u.outputs()) : u;
                }
                case SqlSelect s -> select(s, ctes);
            };
        }

        private SqlSelect select(SqlSelect s, Map<String, List<Set<String>>> ctes) {
            SqlSource from = source(s.from(), ctes);
            SqlSelect out = from == s.from() ? s : s.withFrom(from);
            if (!out.groupBy().isEmpty()) {
                return out;
            }
            List<Set<Col>> rows = rowKeys(out, ctes);
            if (rows.isEmpty()) {
                return out;
            }
            Map<Col, Set<Col>> same = equal(out);
            List<SqlSelect.Projection> ps = new ArrayList<>(out.projections().size());
            boolean changed = false;
            for (SqlSelect.Projection p : out.projections()) {
                if (countOfOne(p.expr(), rows, same)) {
                    ps.add(new SqlSelect.Projection(new SqlExpr.IntLit(1), p.outputName(), p.out()));
                    changed = true;
                } else {
                    ps.add(p);
                }
            }
            return changed ? out.withProjections(ps) : out;
        }

        /** {@code s} (its sources folded) with each nested select folded. */
        private SqlSource source(SqlSource s, Map<String, List<Set<String>>> ctes) {
            return switch (s) {
                case SqlSource.Subselect sub -> {
                    SqlQuery inner = query(sub.inner(), ctes);
                    yield inner == sub.inner() ? sub : new SqlSource.Subselect(inner, sub.alias(), sub.frameName());
                }
                case SqlSource.Join j -> {
                    SqlSource l = source(j.left(), ctes);
                    SqlSource r = source(j.right(), ctes);
                    yield l == j.left() && r == j.right() ? j : new SqlSource.Join(l, r, j.kind(), j.on());
                }
                default -> s;
            };
        }

        /** Whether {@code e} is {@code COUNT(*) OVER (PARTITION BY cols)} over rows one of {@code keys} makes one on
         *  {@code cols}. */
        private static boolean countOfOne(SqlExpr e, List<Set<Col>> keys, Map<Col, Set<Col>> same) {
            if (!(e instanceof SqlExpr.WindowCall w) || !(w.fn() instanceof SqlAgg.Reducer r)
                    || r.fn() != SqlAgg.Fn.COUNT || !r.args().isEmpty() || r.distinct() || !r.orderBy().isEmpty()) {
                return false;
            }
            Set<Col> partition = new HashSet<>();
            for (SqlExpr p : w.partitionBy()) {
                if (!(p instanceof SqlExpr.Column c) || c.table() == null) {
                    return false;
                }
                partition.add(new Col(c.table(), c.name()));
            }
            return keys.stream().anyMatch(k -> k.stream().allMatch(c -> partition.contains(c)
                    || classOf(c, same).stream().anyMatch(partition::contains)));
        }

        private static Set<Col> classOf(Col c, Map<Col, Set<Col>> same) {
            Set<Col> cls = same.get(c);
            return cls != null ? cls : Set.of(c);
        }

        /** The columns of {@code s}'s rows its inner joins and its WHERE make equal, each column's class. */
        private Map<Col, Set<Col>> equal(SqlSelect s) {
            Map<Col, Set<Col>> classes = new HashMap<>();
            // the equal columns its sources bring (a column a subselect names twice, two its join made equal)
            for (Set<Col> given : sourceEqual(s.from())) {
                Set<Col> merged = new HashSet<>();
                for (Col c : given) {
                    merged.addAll(classOf(c, classes));
                }
                for (Col m : merged) {
                    classes.put(m, merged);
                }
            }
            List<SqlExpr> eqs = new ArrayList<>();
            innerOns(s.from(), eqs);
            if (s.where() != null) {
                eqs.addAll(conjuncts(s.where()));
            }
            for (SqlExpr c : eqs) {
                if (c instanceof SqlExpr.Call call && (call.fn() == SqlFn.EQUAL || call.fn() == SqlFn.NULL_SAFE_EQUAL)
                        && call.args().size() == 2 && call.args().get(0) instanceof SqlExpr.Column a
                        && call.args().get(1) instanceof SqlExpr.Column b && a.table() != null && b.table() != null) {
                    Col x = new Col(a.table(), a.name());
                    Col y = new Col(b.table(), b.name());
                    Set<Col> merged = new HashSet<>(classOf(x, classes));
                    merged.addAll(classOf(y, classes));
                    for (Col m : merged) {
                        classes.put(m, merged);
                    }
                }
            }
            return classes;
        }

        /** Each common table expression's equal columns, by name (names are the statement's own: one scope). */
        private final Map<String, List<Set<String>>> cteEqual = new HashMap<>();
        /** Each query's equal output columns, once. */
        private final java.util.IdentityHashMap<SqlQuery, List<Set<String>>> knownEqual =
                new java.util.IdentityHashMap<>();

        /** The sets of {@code src}'s columns equal on every row, as qualified columns. */
        private List<Set<Col>> sourceEqual(SqlSource src) {
            return switch (src) {
                case SqlSource.Subselect sub -> qualified(sub.alias(), outEqual(sub.inner()));
                case SqlSource.Cte c -> qualified(c.alias(), cteEqual.getOrDefault(c.name(), List.of()));
                case SqlSource.Join j -> {
                    List<Set<Col>> out = new ArrayList<>(sourceEqual(j.left()));
                    out.addAll(sourceEqual(j.right()));
                    yield out;
                }
                default -> List.of();
            };
        }

        /** The sets of {@code q}'s output columns equal on every row. */
        List<Set<String>> outEqual(SqlQuery q) {
            List<Set<String>> done = knownEqual.get(q);
            if (done != null) {
                return done;
            }
            List<Set<String>> out = new ArrayList<>();
            if (q instanceof SqlWith w) {
                for (SqlWith.Cte c : w.ctes()) {
                    cteEqual.put(c.name(), outEqual(c.query()));
                }
                out = outEqual(w.body());
            } else if (q instanceof SqlSelect s && s.groupBy().isEmpty() && s.projections().isEmpty()
                    && !(s.from() instanceof SqlSource.Join)) {
                // SELECT *: its source's equal columns, under their own names
                for (Set<Col> given : sourceEqual(s.from())) {
                    Set<String> names = new LinkedHashSet<>();
                    given.forEach(c -> names.add(c.name()));
                    if (names.size() > 1) {
                        out.add(names);
                    }
                }
            } else if (q instanceof SqlSelect s && s.groupBy().isEmpty() && !s.projections().isEmpty()) {
                Map<Col, Set<Col>> same = equal(s);
                Map<Set<Col>, Set<String>> byClass = new HashMap<>();
                boolean joined = s.from() instanceof SqlSource.Join;
                for (SqlSelect.Projection p : s.projections()) {
                    if (p.expr() instanceof SqlExpr.Star st && !(st.table() == null && joined)) {
                        // a star: each column of its source under its own name, in its class
                        for (Set<Col> cls : new java.util.HashSet<>(same.values())) {
                            for (Col c : cls) {
                                if (st.table() == null || st.table().equals(c.table())) {
                                    byClass.computeIfAbsent(cls, k -> new LinkedHashSet<>()).add(c.name());
                                }
                            }
                        }
                        continue;
                    }
                    Set<Col> cls = classOfValue(p.expr(), same);
                    if (cls != null && p.outputName() != null) {
                        byClass.computeIfAbsent(cls, k -> new LinkedHashSet<>()).add(p.outputName());
                    }
                }
                for (Set<String> names : byClass.values()) {
                    if (names.size() > 1) {
                        out.add(names);
                    }
                }
            }
            knownEqual.put(q, out);
            return out;
        }

        /** The class of columns {@code e}'s value is ({@link #valueOf}), or null. */
        private static @com.legend.base.Nullable Set<Col> classOfValue(SqlExpr e, Map<Col, Set<Col>> same) {
            if (e instanceof SqlExpr.Column x && x.table() != null) {
                return classOf(new Col(x.table(), x.name()), same);
            }
            if (e instanceof SqlExpr.Case k && k.whens().size() == 1 && k.otherwise() != null) {
                Set<Col> cls = classOfValue(k.whens().get(0).then(), same);
                return cls != null && valueOf(e, cls) ? cls : null;
            }
            return null;
        }

        /** The conjuncts of each inner join's condition in {@code s}. */
        private static void innerOns(SqlSource s, List<SqlExpr> out) {
            if (s instanceof SqlSource.Join j) {
                innerOns(j.left(), out);
                innerOns(j.right(), out);
                if (j.kind() == SqlSource.Join.Kind.INNER && j.on() != null) {
                    out.addAll(conjuncts(j.on()));
                }
            }
        }

        /** Each query's keys, once: a query is read by every select above it. */
        private final java.util.IdentityHashMap<SqlQuery, List<Set<String>>> known = new java.util.IdentityHashMap<>();

        /** The keys of {@code q}'s rows, by its output names. */
        List<Set<String>> keys(SqlQuery q, Map<String, List<Set<String>>> ctes) {
            List<Set<String>> done = known.get(q);
            if (done != null) {
                return done;
            }
            List<Set<String>> out = fewest(computeKeys(q, ctes));
            known.put(q, out);
            return out;
        }

        private List<Set<String>> computeKeys(SqlQuery q, Map<String, List<Set<String>>> ctes) {
            return switch (q) {
                case SqlWith w -> {
                    Map<String, List<Set<String>>> scope = new HashMap<>(ctes);
                    for (SqlWith.Cte c : w.ctes()) {
                        scope.put(c.name(), keys(c.query(), scope));
                        cteEqual.put(c.name(), outEqual(c.query()));
                    }
                    yield keys(w.body(), scope);
                }
                case SqlUnion u -> List.of();
                case SqlSelect s -> selectKeys(s, ctes);
            };
        }

        private List<Set<String>> selectKeys(SqlSelect s, Map<String, List<Set<String>>> ctes) {
            List<Set<String>> out = new ArrayList<>();
            if (!s.groupBy().isEmpty()) {
                // the groups: unique on the group keys the select names
                Set<String> named = new LinkedHashSet<>();
                for (SqlExpr g : s.groupBy()) {
                    String name = outputOf(s, g);
                    if (name == null) {
                        return List.of();
                    }
                    named.add(name);
                }
                out.add(named);
                return out;
            }
            Map<Col, Set<Col>> same = equal(s);
            for (Set<Col> k : rowKeys(s, ctes)) {
                // a key's column the select names twice (a date as both its period and its date) is a key under each
                List<Set<String>> named = new ArrayList<>(List.of(new LinkedHashSet<>()));
                for (Col c : k) {
                    List<String> names = namesOf(s, c, same);
                    List<Set<String>> next = new ArrayList<>();
                    for (Set<String> partial : named) {
                        for (String n : names) {
                            if (next.size() < MAX_KEYS) {
                                Set<String> grown = new LinkedHashSet<>(partial);
                                grown.add(n);
                                next.add(grown);
                            }
                        }
                    }
                    named = next;
                }
                for (Set<String> n : named) {
                    if (!out.contains(n)) {
                        out.add(n);
                    }
                }
            }
            if (s.distinct() && !s.projections().isEmpty() && !(s.from() instanceof SqlSource.Join)) {
                Set<String> every = new LinkedHashSet<>();
                s.projections().forEach(p -> every.add(p.outputName()));
                if (!every.contains(null)) {
                    out.add(every);
                }
            }
            return out;
        }

        /** The most keys one key becomes where its columns are named more than once: a bound on the work, never on
         *  the answer (a count not proved one is counted). */
        private static final int MAX_KEYS = 16;

        /** {@code keys} without repeats or a key that holds another (a smaller key says more), at most
         *  {@link #MAX_KEYS}. */
        private static <C> List<Set<C>> fewest(List<Set<C>> keys) {
            List<Set<C>> out = new ArrayList<>();
            for (Set<C> k : keys) {
                if (out.size() >= MAX_KEYS) {
                    break;
                }
                if (out.stream().noneMatch(k::containsAll)) {
                    out.removeIf(o -> o.containsAll(k));
                    out.add(k);
                }
            }
            return out;
        }

        /** Every name {@code s} gives the value of column {@code c}: a column equal to it ({@code same}), or the later
         *  or the earlier of two such (the date two lines meeting are dated at, their dates equal by the join). */
        private static List<String> namesOf(SqlSelect s, Col c, Map<Col, Set<Col>> same) {
            boolean joined = s.from() instanceof SqlSource.Join;
            if (s.projections().isEmpty()) {
                // SELECT *: each column under its own name, but a join's two sides may name one twice
                return joined ? List.of() : List.of(c.name());
            }
            Set<Col> cls = classOf(c, same);
            List<String> out = new ArrayList<>();
            for (SqlSelect.Projection p : s.projections()) {
                if (p.outputName() != null && valueOf(p.expr(), cls)) {
                    out.add(p.outputName());
                } else if (p.expr() instanceof SqlExpr.Star st
                        && (st.table() != null ? st.table().equals(c.table()) : !joined)) {
                    out.add(c.name());
                }
            }
            return out;
        }

        /** Whether {@code e}'s value is the value of the columns {@code cls}, all equal on each row. */
        private static boolean valueOf(SqlExpr e, Set<Col> cls) {
            if (e instanceof SqlExpr.Column x) {
                return x.table() != null && cls.contains(new Col(x.table(), x.name()));
            }
            if (e instanceof SqlExpr.Case k && k.whens().size() == 1 && k.otherwise() != null
                    && k.whens().get(0).condition() instanceof SqlExpr.Call cond && cond.args().size() == 2
                    && (cond.fn() == SqlFn.GREATER_EQUAL || cond.fn() == SqlFn.GREATER || cond.fn() == SqlFn.LESS_EQUAL
                    || cond.fn() == SqlFn.LESS)) {
                return valueOf(k.whens().get(0).then(), cls) && valueOf(k.otherwise(), cls);
            }
            return false;
        }

        /** The name {@code s} gives the value of {@code e} -- a plain column, or a group key -- or null. */
        private static @com.legend.base.Nullable String outputOf(SqlSelect s, SqlExpr e) {
            if (s.projections().isEmpty()) {
                // SELECT *: each column under its own name
                return e instanceof SqlExpr.Column c ? c.name() : null;
            }
            for (SqlSelect.Projection p : s.projections()) {
                if (same(p.expr(), e)) {
                    return p.outputName();
                }
                if (p.expr() instanceof SqlExpr.Star st && e instanceof SqlExpr.Column c
                        && (st.table() == null || st.table().equals(c.table()))) {
                    return c.name();
                }
            }
            return null;
        }

        private static boolean same(SqlExpr a, SqlExpr b) {
            if (a instanceof SqlExpr.Column x && b instanceof SqlExpr.Column y) {
                return java.util.Objects.equals(x.table(), y.table()) && x.name().equals(y.name());
            }
            return a.equals(b);
        }

        /** The keys of {@code s}'s rows before its projections: its source's, each less the columns its WHERE fixes
         *  to a literal. */
        private List<Set<Col>> rowKeys(SqlSelect s, Map<String, List<Set<String>>> ctes) {
            List<Set<Col>> keys = sourceKeys(s.from(), ctes);
            if (keys.isEmpty() || s.where() == null) {
                return keys;
            }
            Set<Col> fixed = new HashSet<>();
            for (SqlExpr c : conjuncts(s.where())) {
                if (c instanceof SqlExpr.Call call && call.fn() == SqlFn.EQUAL && call.args().size() == 2) {
                    SqlExpr a = call.args().get(0);
                    SqlExpr b = call.args().get(1);
                    if (a instanceof SqlExpr.Column col && col.table() != null && literal(b)) {
                        fixed.add(new Col(col.table(), col.name()));
                    } else if (b instanceof SqlExpr.Column col && col.table() != null && literal(a)) {
                        fixed.add(new Col(col.table(), col.name()));
                    }
                }
            }
            if (fixed.isEmpty()) {
                return keys;
            }
            List<Set<Col>> out = new ArrayList<>(keys.size());
            for (Set<Col> k : keys) {
                Set<Col> less = new LinkedHashSet<>(k);
                less.removeAll(fixed);
                out.add(less);
            }
            return out;
        }

        /** The keys of a source's rows, as qualified columns. */
        private List<Set<Col>> sourceKeys(SqlSource src, Map<String, List<Set<String>>> ctes) {
            return switch (src) {
                case SqlSource.Table t -> {
                    List<String> key = tableKeys.get(t.alias());
                    if (key == null || key.isEmpty()) {
                        yield List.of();
                    }
                    Set<Col> k = new LinkedHashSet<>();
                    key.forEach(n -> k.add(new Col(t.alias(), n)));
                    yield List.of(k);
                }
                case SqlSource.Subselect sub -> qualified(sub.alias(), keys(sub.inner(), ctes));
                case SqlSource.Cte c -> qualified(c.alias(), ctes.getOrDefault(c.name(), List.of()));
                case SqlSource.Join j -> joinKeys(j, ctes);
                default -> List.of();
            };
        }

        private static List<Set<Col>> qualified(String alias, List<Set<String>> keys) {
            List<Set<Col>> out = new ArrayList<>(keys.size());
            for (Set<String> k : keys) {
                Set<Col> q = new LinkedHashSet<>();
                k.forEach(n -> q.add(new Col(alias, n)));
                out.add(q);
            }
            return out;
        }

        /** A join's keys: each of the left's where a key of the right is bound by its equalities to the left's
         *  columns (each left row meets at most one right row; INNER and LEFT), the same the other way (INNER), and
         *  each pair's union (INNER). */
        private List<Set<Col>> joinKeys(SqlSource.Join j, Map<String, List<Set<String>>> ctes) {
            boolean inner = j.kind() == SqlSource.Join.Kind.INNER;
            if (!inner && j.kind() != SqlSource.Join.Kind.LEFT) {
                return List.of();
            }
            List<Set<Col>> left = sourceKeys(j.left(), ctes);
            List<Set<Col>> right = sourceKeys(j.right(), ctes);
            Set<String> leftAliases = aliases(j.left());
            Set<Col> boundLeft = new HashSet<>();
            Set<Col> boundRight = new HashSet<>();
            if (j.on() != null) {
                for (SqlExpr c : conjuncts(j.on())) {
                    if (c instanceof SqlExpr.Call call && (call.fn() == SqlFn.EQUAL || call.fn() == SqlFn.NULL_SAFE_EQUAL)
                            && call.args().size() == 2 && call.args().get(0) instanceof SqlExpr.Column a
                            && call.args().get(1) instanceof SqlExpr.Column b && a.table() != null
                            && b.table() != null) {
                        boolean aLeft = leftAliases.contains(a.table());
                        boolean bLeft = leftAliases.contains(b.table());
                        if (aLeft != bLeft) {
                            boundLeft.add(aLeft ? new Col(a.table(), a.name()) : new Col(b.table(), b.name()));
                            boundRight.add(aLeft ? new Col(b.table(), b.name()) : new Col(a.table(), a.name()));
                        }
                    }
                }
            }
            List<Set<Col>> out = new ArrayList<>();
            if (right.stream().anyMatch(boundRight::containsAll)) {
                out.addAll(left);
            }
            if (inner) {
                if (left.stream().anyMatch(boundLeft::containsAll)) {
                    out.addAll(right);
                }
                for (Set<Col> l : left) {
                    for (Set<Col> r : right) {
                        if (out.size() >= MAX_KEYS) {
                            return fewest(out);
                        }
                        Set<Col> both = new LinkedHashSet<>(l);
                        both.addAll(r);
                        out.add(both);
                    }
                }
            }
            return fewest(out);
        }

        /** The aliases a source's columns are read under. */
        private static Set<String> aliases(SqlSource s) {
            Set<String> out = new HashSet<>();
            switch (s) {
                case SqlSource.Join j -> {
                    out.addAll(aliases(j.left()));
                    out.addAll(aliases(j.right()));
                }
                case SqlSource.Table t -> out.add(t.alias());
                case SqlSource.Subselect sub -> out.add(sub.alias());
                case SqlSource.Cte c -> out.add(c.alias());
                default -> {
                }
            }
            return out;
        }

        private static List<SqlExpr> conjuncts(SqlExpr e) {
            if (e instanceof SqlExpr.Call c && c.fn() == SqlFn.AND) {
                List<SqlExpr> out = new ArrayList<>();
                c.args().forEach(a -> out.addAll(conjuncts(a)));
                return out;
            }
            return List.of(e);
        }

        private static boolean literal(SqlExpr e) {
            return e instanceof SqlExpr.StringLit || e instanceof SqlExpr.IntLit || e instanceof SqlExpr.DecimalLit
                    || e instanceof SqlExpr.BoolLit || e instanceof SqlExpr.DateLit;
        }
    }
}
