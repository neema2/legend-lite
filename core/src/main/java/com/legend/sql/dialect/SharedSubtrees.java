// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * THE SUBTREES A SELECT'S COLUMNS SHARE, worth computing once (the user, 2026-10-27: "shared subexpression on
 * deephaven"; DuckDB's the same day, the desk in the tab). The compiler inlines a function where it is called, its
 * arguments written wherever its body reads them (UserCallInliner), so a value a function reads twice is written twice:
 * the trading desk's Black and Scholes, written as a textbook writes it, reads d1 in its distribution's variable, its
 * density, its tail and its sign, and d2 reads d1 again -- the option chain's SQL on DuckDB 364,053 characters. A
 * dialect that can name a column for later columns of the same select to read (Deephaven's view, DuckDB's lateral
 * aliases) computes each of these once, as a helper column ({@code __e<i>}), before the first column that reads it.
 *
 * <p>A helper is computed on every row, so only a subtree some column computes on every row is one: never one that
 * only a branch reads (a CASE's, the right side of an AND or an OR, a COALESCE's fallback), which a row not taking the
 * branch never computes -- and may not be able to. No subtree that is not the same on every evaluation is one (a random
 * number, the time, the user), nor one holding what a select's columns do not compute row by row (an aggregate, a
 * window, a subquery: those are read by their own columns). Largest first: a shared subtree's copies become one, so what
 * is inside it is written that many fewer times, and is a helper of its own only where it is still written twice.
 *
 * @param minNodes the fewest nodes a shared subtree has for a helper of its own: below it, writing it twice costs the
 *                 dialect less than a helper does (Deephaven's formulas, compiled by the server: 5; DuckDB's helpers, a
 *                 select of their own: 50)
 */
record SharedSubtrees(int minNodes) {

    /** The subtrees of {@code exprs} (a select's columns, in order) worth computing once, smallest first: a helper reads
     *  the helpers inside it by their names. */
    List<SqlExpr> shared(List<SqlExpr> exprs) {
        Map<SqlExpr, Integer> count = new HashMap<>();
        Set<SqlExpr> always = new HashSet<>();
        for (SqlExpr e : exprs) {
            countIn(e, count);
            always(e, always);
        }
        List<SqlExpr> candidates = new ArrayList<>(count.keySet());
        candidates.sort(Comparator.comparingInt(SharedSubtrees::nodes).reversed());
        List<SqlExpr> shared = new ArrayList<>();
        for (SqlExpr c : candidates) {
            int n = count.getOrDefault(c, 0);
            if (n < 2 || !always.contains(c)) {
                continue;
            }
            shared.add(c);
            Map<SqlExpr, Integer> inside = new HashMap<>();
            for (SqlExpr child : children(c)) {
                countIn(child, inside);
            }
            inside.forEach((d, k) -> count.merge(d, -(n - 1) * k, Integer::sum));
        }
        shared.sort(Comparator.comparingInt(SharedSubtrees::nodes));
        return shared;
    }

    /** Every candidate subtree of {@code e}, once: what a column holds that a helper may be. */
    Set<SqlExpr> within(SqlExpr e) {
        Set<SqlExpr> out = new HashSet<>();
        countIn(e, out);
        return out;
    }

    /** Every candidate subtree of {@code e} ({@link #candidate}), each time it is written. */
    private void countIn(SqlExpr e, Map<SqlExpr, Integer> count) {
        SqlExpr x = bare(e);
        if (!composite(x)) {
            return;
        }
        if (candidate(x)) {
            count.merge(x, 1, Integer::sum);
        }
        for (SqlExpr child : children(x)) {
            countIn(child, count);
        }
    }

    private void countIn(SqlExpr e, Set<SqlExpr> within) {
        SqlExpr x = bare(e);
        if (!composite(x)) {
            return;
        }
        if (candidate(x)) {
            within.add(x);
        }
        for (SqlExpr child : children(x)) {
            countIn(child, within);
        }
    }

    /** The candidate subtrees {@code e} computes whenever it is computed: not those only a branch of it reads. */
    private void always(SqlExpr e, Set<SqlExpr> out) {
        SqlExpr x = bare(e);
        if (!composite(x)) {
            return;
        }
        if (candidate(x)) {
            out.add(x);
        }
        switch (x) {
            // the right side read only when the left does not decide it; a COALESCE's fallback only for a null
            case SqlExpr.Call c when c.fn() == SqlFn.AND || c.fn() == SqlFn.OR || c.fn() == SqlFn.COALESCE -> {
                if (!c.args().isEmpty()) {
                    always(c.args().get(0), out);
                }
            }
            case SqlExpr.Case c -> out.addAll(always(c.whens(), c.otherwise(), 0));
            default -> children(x).forEach(child -> always(child, out));
        }
    }

    /** What a CASE from its {@code i}th branch computes whenever it is computed: its condition, and what every way on
     *  from it computes. */
    private Set<SqlExpr> always(List<SqlExpr.Case.When> whens, @com.legend.base.Nullable SqlExpr otherwise, int i) {
        Set<SqlExpr> out = new HashSet<>();
        if (i == whens.size()) {
            if (otherwise != null) {
                always(otherwise, out);
            }
            return out;
        }
        always(whens.get(i).condition(), out);
        Set<SqlExpr> taken = new HashSet<>();
        always(whens.get(i).then(), taken);
        taken.retainAll(always(whens, otherwise, i + 1));
        out.addAll(taken);
        return out;
    }

    /** A subtree worth a column of its own: one that computes, of {@link #minNodes} nodes or more, made only of what a
     *  select's columns compute row by row, and the same on every evaluation. */
    private boolean candidate(SqlExpr e) {
        return composite(e) && nodes(e) >= minNodes && pure(e);
    }

    /** A call, a CASE or a cast, whose parts this reads. */
    private static boolean composite(SqlExpr e) {
        return e instanceof SqlExpr.Call || e instanceof SqlExpr.Case || e instanceof SqlExpr.Cast;
    }

    /** Whether {@code e} is made only of columns, values, parameters and the calls, CASEs and casts over them, and is the
     *  same on every evaluation. */
    private static boolean pure(SqlExpr e) {
        return switch (bare(e)) {
            case SqlExpr.Column c -> true;
            case SqlExpr.StringLit l -> true;
            case SqlExpr.IntLit l -> true;
            case SqlExpr.FloatLit l -> true;
            case SqlExpr.DecimalLit l -> true;
            case SqlExpr.BoolLit l -> true;
            case SqlExpr.NullLit l -> true;
            case SqlExpr.DateLit l -> true;
            case SqlExpr.TimestampLit l -> true;
            case SqlExpr.TimeLit l -> true;
            case SqlExpr.PlanParam p -> true;
            // a guard's error is the same for the same row (a square root's of a negative number, written in its CASE): a
            // subtree holding one is computed only where a column computes it on every row anyway (#always)
            case SqlExpr.Call c -> switch (c.fn()) {
                case RANDOM, NOW, TODAY, GUID, CURRENT_USER_FN -> false;
                default -> c.args().stream().allMatch(SharedSubtrees::pure);
            };
            case SqlExpr.Case c -> children(c).stream().allMatch(SharedSubtrees::pure);
            case SqlExpr.Cast c -> pure(c.value());
            default -> false;
        };
    }

    /** {@code e}'s parts: a call's arguments, a CASE's conditions, results and fallback, a cast's value. */
    private static List<SqlExpr> children(SqlExpr e) {
        return switch (bare(e)) {
            case SqlExpr.Call c -> c.args();
            case SqlExpr.Case c -> {
                List<SqlExpr> parts = new ArrayList<>();
                for (SqlExpr.Case.When when : c.whens()) {
                    parts.add(when.condition());
                    parts.add(when.then());
                }
                if (c.otherwise() != null) {
                    parts.add(c.otherwise());
                }
                yield parts;
            }
            case SqlExpr.Cast c -> List.of(c.value());
            default -> List.of();
        };
    }

    /** The nodes of {@code e}, its parentheses not counted. */
    static int nodes(SqlExpr e) {
        int n = 1;
        for (SqlExpr child : children(e)) {
            n += nodes(child);
        }
        return n;
    }

    /** {@code e} without its parentheses. */
    static SqlExpr bare(SqlExpr e) {
        return e instanceof SqlExpr.Group g ? bare(g.inner()) : e;
    }
}
