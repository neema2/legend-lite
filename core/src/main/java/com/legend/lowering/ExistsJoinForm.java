// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

/**
 * The engine's SECOND exists emission (pureToSQLQuery chooser L5607-5609,
 * {@code buildExistsAsJoinWithNullCheck} L5636): a WHERE-level
 * {@code [NOT] EXISTS} whose subquery predicate is FULLY LOCAL to the
 * navigated sources — every outer reference is an equality correlation
 * key — rewrites to
 * {@code LEFT JOIN (SELECT DISTINCT <keys> FROM <inner> WHERE <local>)
 * ON <outer=key…> … WHERE <key> IS [NOT] NULL}. DISTINCT over the join
 * keys keeps the form row-count-preserving (at most one match per outer
 * row); a NULL outer key misses the join exactly as the correlated
 * EXISTS comparison fails. Anything non-local (or any shape beyond a
 * plain filtered select) keeps the correlated EXISTS — the engine's own
 * fallback form.
 */
final class ExistsJoinForm {

    private ExistsJoinForm() {
    }

    /** Rewrites every qualifying WHERE conjunct of {@code outer};
     * returns {@code outer} unchanged when none qualifies. {@code zones}
     * looks up a subquery WHERE's resolver-zone split — the DISTINCT-key
     * subselect spells temporal conds FIRST (the engine applies the
     * child's milestoning during child processing; the user pred
     * concatenates after), while the merged WHERE arrives user-first. */
    static SqlSelect rewrite(SqlSelect outer, Supplier<String> alias,
            java.util.function.Function<SqlExpr, WhereMerge.@com.legend.base.Nullable Zones> zones) {
        if (outer.where() == null) {
            return outer;
        }
        List<SqlExpr> conjs = new ArrayList<>();
        flatten(outer.where(), conjs);
        Set<String> outerAliases = new LinkedHashSet<>();
        aliases(outer.from(), outerAliases);
        SqlSource from = outer.from();
        List<SqlExpr> keep = new ArrayList<>();
        boolean changed = false;
        for (SqlExpr c : conjs) {
            boolean negated = false;
            SqlExpr e = ungroup(c);
            if (e instanceof SqlExpr.Call nc && nc.fn() == SqlFn.NOT
                    && nc.args().size() == 1) {
                negated = true;
                e = ungroup(nc.args().get(0));
            }
            if (!(e instanceof SqlExpr.Exists ex)
                    || !(ex.subquery() instanceof SqlSelect sub)
                    || !plainShape(sub)) {
                keep.add(c);
                continue;
            }
            Set<String> inner = new LinkedHashSet<>();
            aliases(sub.from(), inner);
            List<SqlExpr> subConjs = new ArrayList<>();
            if (sub.where() != null) {
                flatten(sub.where(), subConjs);
            }
            List<CorrPair> corr = new ArrayList<>();
            List<SqlExpr> local = new ArrayList<>();
            boolean eligible = true;
            for (SqlExpr sc : subConjs) {
                CorrPair pair = corrPair(
                        ungroup(sc), outerAliases, inner);
                if (pair != null) {
                    corr.add(pair);
                } else if (refsOutside(sc, inner)) {
                    eligible = false;
                    break;
                } else {
                    local.add(sc);
                }
            }
            if (!eligible || corr.isEmpty()) {
                keep.add(c);
                continue;
            }
            // DISTINCT key subselect: one projection per correlation's
            // INNER side. Identity is TABLE+NAME (audit §3a: dedupe by
            // bare name collapsed T_PERSON.NAME and T_DEPT.NAME into
            // one projection and the ON below then compared the outer
            // against the WRONG side's column — 0 rows where 1 was
            // right, on every main path). A name carried by two inner
            // tables gets a DISAMBIGUATED output alias; the common
            // single-table case keeps the bare name (golden-stable).
            List<SqlSelect.Projection> projs = new ArrayList<>();
            java.util.Map<String, String> aliasFor = new java.util.LinkedHashMap<>();
            Set<String> usedNames = new LinkedHashSet<>();
            for (CorrPair p2 : corr) {
                SqlExpr.Column in = p2.inner();
                String key2 = in.table() + " " + in.name();
                if (aliasFor.containsKey(key2)) {
                    continue;
                }
                String outName = in.name();
                int k = 1;
                while (!usedNames.add(outName)) {
                    outName = in.name() + "_" + k++;
                }
                aliasFor.put(key2, outName);
                OutputCol oc = outputColOf(sub.from(), in);
                projs.add(new SqlSelect.Projection(in,
                        outName.equals(in.name()) ? null : outName,
                        outName.equals(oc.name()) ? oc
                                : new OutputCol(outName, oc.type(), oc.nullable())));
            }
            SqlSelect keys = new SqlSelect(projs, true, sub.from(),
                    keysWhere(local, sub.where() == null ? null
                            : zones.apply(sub.where())),
                    List.of(), null, null, List.of(), null, null, List.of());
            SqlSource.Subselect side = new SqlSource.Subselect(
                    keys, alias.get(),
                    SqlSource.Subselect.EXISTS_KEYS_FRAME);
            List<SqlExpr> on = new ArrayList<>();
            for (CorrPair p2 : corr) {
                on.add(SqlExpr.Call.of(SqlFn.EQUAL, p2.outer(),
                        SqlExpr.Column.derived(side.alias(),
                        java.util.Objects.requireNonNull(aliasFor.get(
                                p2.inner().table() + " "
                                        + p2.inner().name()),
                                "exists key alias missing"))));
            }
            from = new SqlSource.Join(from, side, SqlSource.Join.Kind.LEFT,
                    Fold.mergeAnd(on.toArray(SqlExpr[]::new)));
            SqlExpr key = SqlExpr.Column.derived(side.alias(),
                    java.util.Objects.requireNonNull(
                            aliasFor.get(corr.get(0).inner().table() + " "
                                    + corr.get(0).inner().name()),
                            "exists key alias missing"));
            keep.add(SqlExpr.Call.of(
                    negated ? SqlFn.IS_NULL : SqlFn.IS_NOT_NULL, key));
            outerAliases.add(side.alias());
            changed = true;
        }
        if (!changed) {
            return outer;
        }
        // §4AD census (batch-7 re-adjudication): this form now serves
        // ONLY genuine emptiness-family predicates — row-count-preserving
        // BY THEIR OWN SEMANTICS, and the engine's own
        // buildExistsAsJoinWithNullCheck shape. Predicate-reads left this
        // channel for the fan-out route (charter decision 2).
        return outer.withFrom(from).withWhere(
                Fold.mergeAnd(keep.toArray(SqlExpr[]::new)));
    }

    /** An {@code outerCol = innerCol} correlation equality. */
    private record CorrPair(SqlExpr.Column outer, SqlExpr.Column inner) {
    }

    /** outer = inner equality (either operand order), or null. */
    private static @com.legend.base.Nullable CorrPair corrPair(SqlExpr e,
            Set<String> outer, Set<String> inner) {
        if (e instanceof SqlExpr.Call eq && eq.fn() == SqlFn.EQUAL
                && eq.args().size() == 2
                && eq.args().get(0) instanceof SqlExpr.Column a
                && eq.args().get(1) instanceof SqlExpr.Column b) {
            boolean aOut = a.table() != null && outer.contains(a.table());
            boolean bOut = b.table() != null && outer.contains(b.table());
            boolean aIn = a.table() != null && inner.contains(a.table());
            boolean bIn = b.table() != null && inner.contains(b.table());
            if (aOut && bIn && !bOut) {
                return new CorrPair(a, b);
            }
            if (bOut && aIn && !aOut) {
                return new CorrPair(b, a);
            }
        }
        return null;
    }

    /** TRUE when the expression reads any alias outside {@code allowed}
     * or carries a nested subquery (conservatively non-local). */
    private static boolean refsOutside(SqlExpr e, Set<String> allowed) {
        if (e instanceof SqlExpr.Column c) {
            return c.table() != null && !allowed.contains(c.table());
        }
        if (e instanceof SqlExpr.Exists
                || e instanceof SqlExpr.ScalarSubquery) {
            return true;
        }
        for (SqlExpr child : e.children()) {
            if (refsOutside(child, allowed)) {
                return true;
            }
        }
        return false;
    }

    /** Only a plain filtered select converts — any other clause means
     * the engine's local-predicate gate fails and EXISTS stands. */
    /** The DISTINCT-key subselect's WHERE: engine order is TEMPORAL
     * conds first, user pred after (the child's milestoning applies
     * during child processing, buildExistsAsJoinWithNullCheck
     * concatenates the pred). Applies only when the zone split exactly
     * accounts for the non-correlation conds — a user pred that itself
     * contributed a correlation pair falls back to encounter order. */
    private static @com.legend.base.Nullable SqlExpr keysWhere(List<SqlExpr> local,
            WhereMerge.@com.legend.base.Nullable Zones z) {
        if (local.isEmpty()) {
            return null;
        }
        if (z != null && z.temporal() != null) {
            SqlExpr reordered = z.user() == null ? z.temporal()
                    : Fold.mergeAnd(z.temporal(), z.user());
            List<SqlExpr> flat = new ArrayList<>();
            flatten(reordered, flat);
            if (flat.size() == local.size()
                    && new java.util.HashSet<>(flat)
                            .equals(new java.util.HashSet<>(local))) {
                return reordered;
            }
        }
        return Fold.mergeAnd(local.toArray(SqlExpr[]::new));
    }

    private static boolean plainShape(SqlSelect s) {
        return !s.distinct() && s.groupBy().isEmpty() && s.having() == null
                && s.qualify() == null && s.orderBy().isEmpty()
                && s.limit() == null && s.offset() == null;
    }

    private static SqlExpr ungroup(SqlExpr e) {
        return e instanceof SqlExpr.Group g ? ungroup(g.inner()) : e;
    }

    private static void flatten(SqlExpr e, List<SqlExpr> out) {
        SqlExpr u = e instanceof SqlExpr.Group g
                && g.inner() instanceof SqlExpr.Call c
                && c.fn() == SqlFn.AND ? g.inner() : e;
        if (u instanceof SqlExpr.Call c2 && c2.fn() == SqlFn.AND) {
            c2.args().forEach(x -> flatten(x, out));
        } else {
            out.add(e);
        }
    }

    private static void aliases(SqlSource s, Set<String> out) {
        switch (s) {
            case SqlSource.Join j -> {
                aliases(j.left(), out);
                aliases(j.right(), out);
            }
            case SqlSource.Dual ignored -> {
            }
            default -> out.add(s.alias());
        }
    }

    /** The inner column's schema entry, found by alias in the source
     * tree; loud when absent — a correlation key MUST be resolvable. */
    private static OutputCol outputColOf(SqlSource s, SqlExpr.Column c) {
        List<SqlSource> stack = new ArrayList<>(List.of(s));
        while (!stack.isEmpty()) {
            SqlSource cur = stack.remove(stack.size() - 1);
            if (cur instanceof SqlSource.Join j) {
                stack.add(j.left());
                stack.add(j.right());
                continue;
            }
            if (cur instanceof SqlSource.Dual) {
                continue;
            }
            if (cur.alias().equals(c.table())) {
                for (OutputCol oc : cur.outputs()) {
                    if (oc.name().equals(c.name())) {
                        return oc;
                    }
                }
            }
        }
        throw new IllegalStateException("exists join-form key '" + c.table()
                + "." + c.name() + "' has no schema entry in the inner"
                + " source tree");
    }
}
