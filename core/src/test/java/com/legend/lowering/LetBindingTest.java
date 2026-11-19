// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.Compiler;
import com.legend.database.Databases;
import com.legend.executionplan.ExecutionPlan;
import com.legend.testbackend.StorelessRuntime;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * P1a, a query's lets bound once ({@link LetBinding}; E3, ruled 2026-11-12): a let read more than once is computed
 * once, under the root, and read by column -- Pure's own call-by-value let -- and answers exactly as before; a let
 * whose value can raise, or read once, stays at its reads.
 */
class LetBindingTest {

    private static final String MODEL = StorelessRuntime.with("", Databases.PLATFORM);

    private static ExecutionPlan plan(String query) {
        return Compiler.query(Compiler.compileModel(MODEL), query).executionPlan(StorelessRuntime.RUNTIME);
    }

    private static String sql(String query) {
        return plan(query).answerSql().statement();
    }

    private static List<List<Object>> rows(String query) throws Exception {
        try (java.sql.Connection c = com.legend.testbackend.TestBackends.platform().open()) {
            com.legend.result.Collector collector = new com.legend.result.Collector();
            var session = com.legend.testbackend.TestSessions.over(c);
            com.legend.Execution.run(MODEL, query, StorelessRuntime.RUNTIME,
                    com.legend.exec.SessionSource.given(session), com.legend.exec.PlanSessions.of(session),
                    new com.legend.result.Output.Rows(collector));
            return collector.collected().rows();
        }
    }

    private static int count(String text, String part) {
        int n = 0;
        for (int i = text.indexOf(part); i >= 0; i = text.indexOf(part, i + 1)) {
            n++;
        }
        return n;
    }

    /** A let of the clock read twice reads the clock ONCE (the latent deviation the design found: two reads were two
     *  clocks, held equal only by each database's per-statement clock), and answers true. */
    @Test
    void aLetOfTheClockReadTwiceReadsTheClockOnce() throws Exception {
        String q = "|let t = meta::pure::functions::date::now(); $t == $t;";
        String sql = sql(q);
        assertEquals(1, count(sql.toLowerCase(java.util.Locale.ROOT), "now()"), sql);
        assertTrue(sql.contains(LetBinding.ALIAS), sql);
        assertEquals(List.of(List.of(true)), rows(q));
    }

    /** A list read three times: its size, read by the guards of both {@code at}s and the sum (five sorts before),
     *  is computed once, and the answer is the one it was. The list itself is left at its three places: a binding of
     *  it would cost about what it saves (LetBinding's measure, in nodes). */
    @Test
    void aLetReadThriceIsBoundWhereItSaves_andAnswersTheSame() throws Exception {
        String q = "|let xs = [3, 1, 2]->meta::pure::functions::collection::sort(); $xs->meta::pure::functions::collection::size()"
                + " + $xs->meta::pure::functions::collection::at(0) + $xs->meta::pure::functions::collection::at(2);";
        String sql = sql(q);
        assertTrue(sql.contains(LetBinding.ALIAS), sql);
        assertEquals(3, count(sql, "list_sort("), sql);
        assertEquals(List.of(List.of(7L)), rows(q));
    }

    /** A list built by a lambda and read twice is built once (the lambda's parameter is the binding's own). */
    @Test
    void aListBuiltByALambdaAndReadTwiceIsBuiltOnce() throws Exception {
        String q = "|let a = [1, 2, 3]->meta::pure::functions::collection::map(x | $x * 10); $a->meta::pure::functions::collection::size()"
                + " + $a->meta::pure::functions::collection::size();";
        String sql = sql(q);
        assertEquals(1, count(sql, "list_transform("), sql);
        assertEquals(List.of(List.of(6L)), rows(q));
    }

    /** A value that can raise, read four times where every read is evaluated, is computed once (step 7's first half,
     *  2026-11-15, in PR1 since round 55f): it raises there already, so computing it eagerly raises nothing the
     *  statement did not -- a formula's division, or a date's MAKE_TIMESTAMP, is no reason to repeat it. */
    @Test
    void aRaisingValueEvaluatedAtEveryReadIsBoundOnce() throws Exception {
        String q = "|let r = (2.0 + 3.0) / (1.0 + 1.0); $r + $r + $r + $r;";
        String sql = sql(q);
        assertTrue(sql.contains(LetBinding.ALIAS), sql);
        assertEquals(1, count(sql, "/"), sql);
        assertEquals(List.of(List.of(10.0)), rows(q));
    }

    /** A let whose value can raise stays at its reads (the laziness rule, P1a's conservative half: eager lets are
     *  P1a.2), and a let read once is never bound. */
    @Test
    void aRaisingLetOrALetReadOnceStaysInline() throws Exception {
        String raising = "|let x = 1 / 0; if(false, |$x, |2.0);";
        assertFalse(sql(raising).contains(LetBinding.ALIAS), sql(raising));
        // read three times under an untaken branch: repeated enough to bind, so only the laziness rule keeps it in
        // place (2026-11-15, PR1's review: the case above reads its raising value once, so it could not fail)
        String repeatedRaise = "|let n = 0; if($n == 0, |0.0, |(10 / $n) + (10 / $n) + (10 / $n));";
        assertFalse(sql(repeatedRaise).contains(LetBinding.ALIAS), sql(repeatedRaise));
        assertEquals(List.of(List.of(0.0)), rows(repeatedRaise));
        String once = "|let xs = [3, 1, 2]->meta::pure::functions::collection::sort(); $xs->meta::pure::functions::collection::size();";
        assertFalse(sql(once).contains(LetBinding.ALIAS), sql(once));
        assertEquals(List.of(List.of(3L)), rows(once));
    }

    /** The always-evaluated set descends through a strict call and never into a skipping one (round 55k, 2026-11-15,
     *  the orchestrator's ruling: widen the set, never across a rendering that can skip): a raising division under
     *  ABS's PLUS is always evaluated (CONCAT's is not since A6: it may skip); the same division as COALESCE's second argument, or in a CASE's later
     *  branch, is not, so a repeat there is never bound. */
    @Test
    void theAlwaysEvaluatedSetCrossesStrictCallsAndNeverASkippingOne() {
        com.legend.sql.SqlExpr raising = com.legend.sql.SqlExpr.Call.of(com.legend.sql.SqlFn.DIVIDE,
                new com.legend.sql.SqlExpr.IntLit(10), new com.legend.sql.SqlExpr.IntLit(0));
        java.util.Set<com.legend.sql.SqlExpr> strict = new java.util.HashSet<>();
        LetBinding.alwaysEvaluated(com.legend.sql.SqlExpr.Call.of(com.legend.sql.SqlFn.ABS,
                com.legend.sql.SqlExpr.Call.of(com.legend.sql.SqlFn.PLUS, new com.legend.sql.SqlExpr.IntLit(1),
                        raising)), strict);
        assertTrue(strict.contains(raising), "under strict calls: " + strict);
        // CONCAT branches on MySQL-family dialects and SQLite, so it is may-skip since the PR1 re-review's A6
        // (2026-11-16): never crossed
        java.util.Set<com.legend.sql.SqlExpr> concatenated = new java.util.HashSet<>();
        LetBinding.alwaysEvaluated(com.legend.sql.SqlExpr.Call.of(com.legend.sql.SqlFn.CONCAT,
                new com.legend.sql.SqlExpr.StringLit("a"), raising), concatenated);
        assertFalse(concatenated.contains(raising), "CONCAT may skip an argument on some dialect: " + concatenated);
        java.util.Set<com.legend.sql.SqlExpr> coalesced = new java.util.HashSet<>();
        LetBinding.alwaysEvaluated(com.legend.sql.SqlExpr.Call.of(com.legend.sql.SqlFn.COALESCE,
                new com.legend.sql.SqlExpr.IntLit(1), raising), coalesced);
        assertFalse(coalesced.contains(raising), "a COALESCE may skip its second argument: " + coalesced);
    }
}
