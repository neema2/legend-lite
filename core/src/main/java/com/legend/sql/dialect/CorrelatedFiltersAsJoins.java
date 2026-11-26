// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlRewriter;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * H2-family structural pass (the edition program's step 1, 2026-11-26; the user's ruling of 2026-11-25 on the
 * Pure-mode gaps, "H2's lateral as an inner join"): a lateral whose subquery only FILTERS its own source, correlated
 * to the row before it through its WHERE's top-level conjuncts alone, is that source JOINED on those conjuncts --
 * INNER for a cross lateral, LEFT for a left lateral -- each inner column read through the subquery's output of it.
 * H2 has no LATERAL ({@code Function "LATERAL" not found}), and {@code $t->lateral(x | $t2->filter(y | $y.k == $x.k))}
 * never needed one. The pass rearranges a shape and computes nothing. Any other lateral is left as it is and refused
 * where it is written, as before (no fallback): a subquery that groups, sorts, limits or deduplicates, a projection
 * that is not a plain inner column, a read of the row before it anywhere but the WHERE's conjuncts, a moved conjunct
 * that holds a subquery or reads a column its subquery does not output, an alias of the row before it defined again
 * inside. ClickHouse's renderer holds the same rule at render. The design, with the shapes the lowering writes:
 * docs/records/edition-homework/r3/design-h2-lateral.md.
 */
final class CorrelatedFiltersAsJoins extends SqlRewriter {

    @Override
    protected SqlSource source(SqlSource s) {
        if (!(s instanceof SqlSource.Join j)
                || (j.kind() != SqlSource.Join.Kind.CROSS_LATERAL && j.kind() != SqlSource.Join.Kind.LEFT_LATERAL)
                || !(j.right() instanceof SqlSource.Subselect sub) || !(sub.inner() instanceof SqlSelect in)
                || in.where() == null || !filterOnly(in)) {
            return s;
        }
        Set<String> out = new LinkedHashSet<>();
        topAliases(j.left(), out);
        Set<String> own = new LinkedHashSet<>();
        topAliases(in.from(), own);
        Reads all = Reads.of(in);                       // DEF: every alias defined inside S
        if (!Collections.disjoint(all.defined, out)) {
            return s;                                   // condition 3: an outer alias redefined inside
        }
        if (!all.unreadable.isEmpty() || !all.unqualified.isEmpty()) {
            // a source or a call whose reads the pass cannot see (raw SQL; a value drawn afresh for each row, which
            // a lateral draws once a left row and a join once), or a column with no table, which a join inside a
            // correlated subquery could bind to another scope than the lateral's (the independent audit of
            // 2026-11-26)
            return s;
        }
        Reads body = Reads.of(in.withWhere(null));      // condition 4
        if (!Collections.disjoint(body.read, out) || !all.defined.containsAll(body.read)) {
            return s;
        }
        Map<String, String> outputOf = outputs(in, own);
        if (outputOf == null) {
            return s;                                   // condition 2
        }
        List<SqlExpr> local = new ArrayList<>();
        List<SqlExpr> moved = new ArrayList<>();
        for (SqlExpr c : conjuncts(in.where())) {
            Reads r = Reads.of(c);
            if (Collections.disjoint(r.read, out)) {
                if (!all.defined.containsAll(r.read)) {
                    return s;                           // reads a scope further out
                }
                local.add(c);
                continue;
            }
            Set<String> allowed = new LinkedHashSet<>(out);
            allowed.addAll(own);
            if (!r.subqueries.isEmpty() || !allowed.containsAll(r.read)) {
                return s;                               // condition 5
            }
            Respell rs = new Respell(own, outputOf, sub.alias(), in.outputs());
            SqlExpr x = rs.of(c);
            if (!rs.missing.isEmpty()) {
                return s;                               // condition 6: an inner column not output
            }
            moved.add(x);
        }
        if (moved.isEmpty()) {
            return s;
        }
        if (j.on() != null && !(j.on() instanceof SqlExpr.BoolLit b && b.value())) {
            moved.add(j.on());
        }
        SqlSelect filtered = in.withWhere(and(local));
        return new SqlSource.Join(j.left(), new SqlSource.Subselect(filtered, sub.alias(), sub.frameName()),
                j.kind() == SqlSource.Join.Kind.CROSS_LATERAL ? SqlSource.Join.Kind.INNER : SqlSource.Join.Kind.LEFT,
                java.util.Objects.requireNonNull(and(moved)));
    }

    private static boolean filterOnly(SqlSelect s) {
        return !s.distinct() && s.groupBy().isEmpty() && s.having() == null && s.qualify() == null
                && s.orderBy().isEmpty() && s.limit() == null && s.offset() == null;
    }

    /** Each inner column's output name, "alias.column" -> name; null where a projection is not a plain inner column
     *  or a star, or a star's source declares no columns. */
    private static @com.legend.base.Nullable Map<String, String> outputs(SqlSelect in, Set<String> own) {
        Map<String, String> m = new LinkedHashMap<>();
        if (in.projections().isEmpty()) {
            return stars(in.from(), null, m) && distinctNames(in.outputs()) ? m : null;
        }
        for (SqlSelect.Projection p : in.projections()) {
            if (p.expr() instanceof SqlExpr.Star st && (st.table() == null || own.contains(st.table()))) {
                if (!stars(in.from(), st.table(), m)) {
                    return null;
                }
            } else if (p.expr() instanceof SqlExpr.Column c && c.table() != null && own.contains(c.table())
                    && p.outputName() != null) {
                m.putIfAbsent(c.table() + "." + c.name(), p.outputName());
            } else {
                return null;
            }
        }
        return distinctNames(in.outputs()) ? m : null;
    }

    /** The leaves of {@code src}'s join tree (those {@code table} names, or all), each column under its own name. */
    private static boolean stars(SqlSource src, @com.legend.base.Nullable String table, Map<String, String> into) {
        if (src instanceof SqlSource.Join j) {
            return stars(j.left(), table, into) && stars(j.right(), table, into);
        }
        if (src instanceof SqlSource.Dual || (table != null && !table.equals(src.alias()))) {
            return true;
        }
        if (src.outputs().isEmpty()) {
            return false;                               // a source that declares no columns: its star names none
        }
        src.outputs().forEach(oc -> into.putIfAbsent(src.alias() + "." + oc.name(), oc.name()));
        return true;
    }

    private static boolean distinctNames(List<OutputCol> outs) {
        return outs.stream().map(OutputCol::name).distinct().count() == outs.size();
    }

    private static void topAliases(SqlSource src, Set<String> into) {
        if (src instanceof SqlSource.Join j) {
            topAliases(j.left(), into);
            topAliases(j.right(), into);
        } else if (!(src instanceof SqlSource.Dual)) {
            into.add(src.alias());
        }
    }

    private static List<SqlExpr> conjuncts(SqlExpr e) {
        if (e instanceof SqlExpr.Call c && c.fn() == SqlFn.AND) {
            List<SqlExpr> out = new ArrayList<>();
            c.args().forEach(x -> out.addAll(conjuncts(x)));
            return out;
        }
        return List.of(e);
    }

    private static @com.legend.base.Nullable SqlExpr and(List<SqlExpr> parts) {
        return parts.isEmpty() ? null : parts.size() == 1 ? parts.get(0) : new SqlExpr.Call(SqlFn.AND, parts);
    }

    /** What a query or an expression reads and defines: aliases of every source inside it, the tables its columns
     *  read, its unqualified columns, its subquery nodes. Final fields only (CodeShapeGuardrailTest's mutable-field
     *  rule). */
    private static final class Reads extends SqlRewriter {
        final Set<String> defined = new LinkedHashSet<>();
        final Set<String> read = new LinkedHashSet<>();
        final List<SqlExpr.Column> unqualified = new ArrayList<>();
        final List<SqlExpr> subqueries = new ArrayList<>();
        /** What the pass cannot read through: a raw SQL source, a call that draws a value afresh. */
        final List<Object> unreadable = new ArrayList<>();

        static Reads of(SqlQuery q) {
            Reads r = new Reads();
            r.rewrite(q);
            return r;
        }

        static Reads of(SqlExpr e) {
            Reads r = new Reads();
            r.rewriteExpr(e);
            return r;
        }

        @Override
        protected SqlSource source(SqlSource s) {
            if (!(s instanceof SqlSource.Join) && !(s instanceof SqlSource.Dual)) {
                defined.add(s.alias());
            }
            if (s instanceof SqlSource.RawSql) {
                unreadable.add(s);
            }
            return s;
        }

        @Override
        protected SqlExpr expr(SqlExpr e) {
            if (e instanceof SqlExpr.Column c) {
                if (c.table() == null) {
                    unqualified.add(c);
                } else {
                    read.add(c.table());
                }
            } else if (e instanceof SqlExpr.ScalarSubquery || e instanceof SqlExpr.Exists
                    || e instanceof SqlExpr.InSubquery || e instanceof SqlExpr.Quantified) {
                subqueries.add(e);
            } else if (e instanceof SqlExpr.RowOrder ro && ro.table() != null) {
                read.add(ro.table());                   // a row's position reads its table as a column does
            } else if (e instanceof SqlExpr.Star st && st.table() != null) {
                read.add(st.table());
            } else if (e instanceof SqlExpr.StarExcept sx && sx.table() != null) {
                read.add(sx.table());
            } else if (e instanceof SqlExpr.Call c && c.fn().drawsAfresh()) {
                unreadable.add(e);
            }
            return e;
        }
    }

    /** An inner column read as the subquery's output of it, typed by that output. */
    private static final class Respell extends SqlRewriter {
        private final Set<String> own;
        private final Map<String, String> outputOf;
        private final String alias;
        private final List<OutputCol> outs;
        final List<SqlExpr.Column> missing = new ArrayList<>();

        Respell(Set<String> own, Map<String, String> outputOf, String alias, List<OutputCol> outs) {
            this.own = own;
            this.outputOf = outputOf;
            this.alias = alias;
            this.outs = outs;
        }

        SqlExpr of(SqlExpr e) {
            return rewriteExpr(e);
        }

        @Override
        protected SqlExpr expr(SqlExpr e) {
            if (e instanceof SqlExpr.Column c && c.table() != null && own.contains(c.table())) {
                String name = outputOf.get(c.table() + "." + c.name());
                if (name == null) {
                    missing.add(c);
                    return e;
                }
                return SqlExpr.Column.of(alias, outs, name);   // stamps the output's type and nullability
            }
            return e;
        }
    }
}
