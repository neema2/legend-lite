// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.sql.OutputCol;
import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

/**
 * §4AD batch-6 TAIL — the graph lane's correlated reducer subqueries
 * decorrelate to the GROUPED-JOIN form (the measured reducer cell:
 * grouped subselect joined back on the correlation keys —
 * testSubAggregationMultiLevel; COUNT-zero via COALESCE over the
 * padded read). A candidate is a {@code ScalarSubquery} whose inner
 * select is a bare single-Reducer projection over a WHERE that splits
 * cleanly into ({@code outer.col = inner.col}) equality keys plus
 * inner-only conjuncts. Any shape this rewrite cannot PROVE it
 * preserves keeps the correlated form and its census arm — left loud,
 * never guessed.
 */
final class GraphAggDecorrelate {

    private GraphAggDecorrelate() {
    }

    /** Decorrelate every qualifying reducer subquery inside {@code leaf}
     * against the live frame {@code fr[0]} (updated in place when a
     * hoist lands); returns the (possibly rewritten) expression. */
    static SqlExpr apply(SqlSelect[] fr, SqlExpr leaf,
            Supplier<String> aliasMint) {
        Set<String> outer = RelationPredicates.frameAliases(fr[0]);
        return rewrite(leaf, outer, fr, aliasMint);
    }

    private static SqlExpr rewrite(SqlExpr e, Set<String> outer,
            SqlSelect[] fr, Supplier<String> aliasMint) {
        if (e instanceof SqlExpr.ScalarSubquery ss
                && ss.subquery() instanceof SqlSelect s) {
            SqlExpr done = tryDecorrelate(s, outer, fr, aliasMint);
            if (done != null) {
                return done;
            }
        }
        List<SqlExpr> cs = e.children();
        if (cs.isEmpty()) {
            return e;
        }
        List<SqlExpr> rs = new ArrayList<>(cs.size());
        boolean changed = false;
        for (SqlExpr c : cs) {
            SqlExpr r = rewrite(c, outer, fr, aliasMint);
            changed |= r != c;
            rs.add(r);
        }
        return changed ? e.withChildren(rs) : e;
    }

    private static @com.legend.base.Nullable SqlExpr tryDecorrelate(SqlSelect s,
            Set<String> outer, SqlSelect[] fr, Supplier<String> aliasMint) {
        if (s.distinct() || !s.groupBy().isEmpty() || s.having() != null
                || s.qualify() != null || s.limit() != null
                || s.offset() != null || !s.orderBy().isEmpty()
                || s.projections().size() != 1
                || !(s.projections().get(0).expr()
                        instanceof SqlAgg.Reducer red)) {
            return null;
        }
        Set<String> inner = RelationPredicates.frameAliases(s);
        if (refsOutside(red, inner)) {
            return null;
        }
        List<SqlExpr> conjuncts = new ArrayList<>();
        flattenAnd(s.where(), conjuncts);
        List<SqlExpr> innerPreds = new ArrayList<>();
        List<SqlExpr> outerKeys = new ArrayList<>();
        List<SqlExpr.Column> innerKeys = new ArrayList<>();
        for (SqlExpr c : conjuncts) {
            if (!refsOutside(c, inner)) {
                innerPreds.add(c);
                continue;
            }
            if (c instanceof SqlExpr.Call call && call.fn() == SqlFn.EQUAL
                    && call.args().size() == 2
                    && call.args().get(0) instanceof SqlExpr.Column a
                    && call.args().get(1) instanceof SqlExpr.Column b) {
                SqlExpr.Column o =
                        a.table() != null && outer.contains(a.table()) ? a
                        : b.table() != null && outer.contains(b.table()) ? b
                        : null;
                SqlExpr.Column in = o == a ? b : a;
                if (o != null && in.table() != null
                        && inner.contains(in.table())) {
                    outerKeys.add(o);
                    innerKeys.add(in);
                    continue;
                }
            }
            // an outer reference outside the equality-key shape — this
            // rewrite cannot prove row equivalence; keep the correlated
            // form (its census arm stays the honest register)
            return null;
        }
        if (outerKeys.isEmpty()) {
            // no correlation keys at all: a whole-extent envelope, the
            // extent-* census class — not this rewrite's business
            return null;
        }
        // the grouped select DECLARES its slots (output-less projections
        // contribute no OutputCol — the scalar-envelope rule); a slot
        // whose expression carries no stored type bails the rewrite
        List<SqlSelect.Projection> ps = new ArrayList<>();
        List<SqlExpr> gb = new ArrayList<>();
        for (int i = 0; i < innerKeys.size(); i++) {
            OutputCol ko = declaredSlot("k" + i, innerKeys.get(i));
            if (ko == null) {
                return null;
            }
            ps.add(new SqlSelect.Projection(innerKeys.get(i), "k" + i, ko));
            gb.add(innerKeys.get(i));
        }
        OutputCol ao = declaredSlot("aggCol", red);
        if (ao == null) {
            return null;
        }
        ps.add(new SqlSelect.Projection(red, "aggCol", ao));
        SqlSelect grouped = new SqlSelect(ps, false, s.from(),
                innerPreds.isEmpty() ? null
                        : Fold.mergeAnd(innerPreds.toArray(SqlExpr[]::new)),
                gb, null, null, List.of(), null, null, List.of());
        String ga = aliasMint.get();
        List<OutputCol> gouts = grouped.outputs();
        SqlExpr[] eqs = new SqlExpr[outerKeys.size()];
        for (int i = 0; i < outerKeys.size(); i++) {
            eqs[i] = SqlExpr.Call.of(SqlFn.EQUAL, outerKeys.get(i),
                    derivedRead(ga, gouts, "k" + i));
        }
        fr[0] = fr[0].withFrom(new SqlSource.Join(fr[0].from(),
                new SqlSource.Subselect(grouped, ga, null),
                SqlSource.Join.Kind.LEFT, Fold.mergeAnd(eqs)));
        SqlExpr read = derivedRead(ga, gouts, "aggCol");
        // COUNT over an unmatched parent is ZERO, not NULL (the join-back
        // pad) — the measured COUNT-zero contract of the grouped cell;
        // other reducers are NULL over empty in BOTH forms
        return red.fn() == SqlAgg.Fn.COUNT
                ? SqlExpr.Call.of(SqlFn.COALESCE, read, new SqlExpr.IntLit(0))
                : read;
    }

    /** The grouped select's own slot, DECLARED from the expression's
     * stored fact (§E3: the builder states type and nullability with
     * the same authority) — null when the fact is not in hand. */
    private static @com.legend.base.Nullable OutputCol declaredSlot(String name,
            SqlExpr e) {
        return e.type() instanceof com.legend.sql.TypeFact.Typed t
                ? new OutputCol(name, t.type(), t.nullable(), OutputCol.Origin.DERIVED)
                : null;
    }

    /** A stamped read of the grouped subselect's own DERIVED slot —
     * the case-sensitive H2 renderer walls origin-less references
     * (SQL-IR backend-agnosticism slice 1). */
    private static SqlExpr.Column derivedRead(String table,
            List<OutputCol> outs, String name) {
        OutputCol oc = outs.stream().filter(c -> c.name().equals(name))
                .findFirst().orElseThrow(() -> new IllegalStateException(
                        "decorrelated grouped select lost its own slot '"
                                + name + "'"));
        return SqlExpr.Column.of(table, name, oc.type(), oc.nullable(),
                OutputCol.Origin.DERIVED);
    }

    private static void flattenAnd(@com.legend.base.Nullable SqlExpr e,
            List<SqlExpr> out) {
        if (e == null) {
            return;
        }
        if (e instanceof SqlExpr.Group g) {
            flattenAnd(g.inner(), out);
            return;
        }
        if (e instanceof SqlExpr.Call c && c.fn() == SqlFn.AND) {
            c.args().forEach(x -> flattenAnd(x, out));
            return;
        }
        out.add(e);
    }

    /** Any reference this scope does not bind: an alias outside
     * {@code inner}, a bare (unqualified) column, or a nested subquery
     * (its own scope — conservatively outside). */
    private static boolean refsOutside(SqlExpr e, Set<String> inner) {
        if (e instanceof SqlExpr.Column c) {
            return c.table() == null || !inner.contains(c.table());
        }
        if (e instanceof SqlExpr.ScalarSubquery
                || e instanceof SqlExpr.Exists) {
            return true;
        }
        for (SqlExpr c : e.children()) {
            if (refsOutside(c, inner)) {
                return true;
            }
        }
        return false;
    }
}
