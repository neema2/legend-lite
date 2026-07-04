package com.legend.sql;

import java.util.List;

/**
 * Aggregate-position functions, POSITION-TYPED (the current-branch idea worth
 * keeping): {@link Reducer}s are valid both under GROUP BY and inside
 * {@code OVER(...)}; {@link RankingFn}/{@link ValueFn} are window-only.
 * {@link Reducer} implements {@link SqlExpr} so it can sit in a grouped
 * select's projections/HAVING; the window-only kinds deliberately do NOT
 * &mdash; {@code LAG} in a GROUP BY position is a javac error, not a runtime one.
 */
public sealed interface SqlAgg {

    String fn();

    List<SqlExpr> args();

    /** GROUP-BY-valid aggregate (also usable inside a window): SUM, COUNT, MIN, ... */
    record Reducer(String fn, List<SqlExpr> args, boolean distinct) implements SqlAgg, SqlExpr {
        public static Reducer of(String fn, SqlExpr... args) {
            return new Reducer(fn, List.of(args), false);
        }
    }

    /** Window-only ranking function: ROW_NUMBER, RANK, NTILE, CUME_DIST, ... */
    record RankingFn(String fn, List<SqlExpr> args) implements SqlAgg {
    }

    /** Window-only value function: LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE. */
    record ValueFn(String fn, List<SqlExpr> args) implements SqlAgg {
    }
}
