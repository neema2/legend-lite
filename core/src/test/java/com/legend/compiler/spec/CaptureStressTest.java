// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.test.StorelessRuntime;

import com.legend.Execution;
import com.legend.exec.ExecutionResult;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Capture-avoiding substitution under depth (rebuild W0.6 push 1). Each program is generated twice: once with
 * every binder spelled differently, once with every binder spelled the same. The two differ only in bound
 * names, so they must return the same rows at every depth; before the fix the same-name forms of the nested
 * lets and the nested matches returned other values. The timing half of this stress (compile time against
 * the previous commit, by size) is a harness run alone, recorded in GATES; a test asserts no time.
 */
class CaptureStressTest {

    private static final String SOURCE = "[1, 2]->filter(z | $z > 0)";

    private static List<String> values(String model, String query) throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:")) {
            ExecutionResult r = Execution.execute(StorelessRuntime.with(model, DatabaseType.DuckDB), query, StorelessRuntime.RUNTIME, c);
            if (!(r instanceof ExecutionResult.Collection col)) {
                throw new IllegalStateException("expected a collection frame for " + query + ", got " + r);
            }
            return col.values().stream().map(String::valueOf).toList();
        }
    }

    /** d nested lambdas, a let at every level, the innermost lambda reading every let. */
    private static String nestedLets(int d, boolean sameNames) {
        StringBuilder q = new StringBuilder("|");
        StringBuilder close = new StringBuilder();
        for (int k = 1; k <= d; k++) {
            String x = sameNames ? "x" : "x" + k;
            q.append(SOURCE).append("->map({").append(x).append(" | let c").append(k).append(" = $").append(x)
                    .append(k == 1 ? "" : " + $c" + (k - 1)).append("; ");
            close.insert(0, "->sum();})");
        }
        String y = sameNames ? "x" : "y";
        q.append("[10, 20]->filter(z | $z > 0)->map(").append(y).append(" | $").append(y);
        for (int k = 1; k <= d; k++) {
            q.append(" + $c").append(k);
        }
        return q.append(")").append(close).toString();
    }

    /** d nested statically dispatched matches, the innermost lambda reading every arm parameter. */
    private static String nestedMatch(int d, boolean sameNames) {
        StringBuilder q = new StringBuilder("|" + SOURCE + "->map(x | $x");
        StringBuilder close = new StringBuilder();
        for (int k = 1; k <= d; k++) {
            q.append("->match([i").append(k).append(": Integer[1] | ");
            if (k < d) {
                q.append("($i").append(k).append(" + 1)");
            }
            close.append("])");
        }
        String w = sameNames ? "x" : "w";
        q.append("[10, 20]->filter(z | $z > 0)->map(").append(w).append(" | $").append(w);
        for (int k = 1; k <= d; k++) {
            q.append(" + $i").append(k);
        }
        return q.append(")->sum()").append(close).append(")").toString();
    }

    /** A chain of d user functions, each with two lets and a lambda spelled like the caller's binder. */
    private static String callChainModel(int d) {
        StringBuilder m = new StringBuilder();
        for (int k = 1; k <= d; k++) {
            m.append("function t::f").append(k).append("(x: Integer[1]): Integer[1] { let a = ").append(SOURCE)
                    .append("->map(w | $w + $x)->sum(); let b = $a + ").append(k).append("; ")
                    .append(k == 1 ? "$b" : "t::f" + (k - 1) + "($b)").append("; }\n");
        }
        return m.toString();
    }

    @Test
    void nestedLetsAtDepth() throws Exception {
        // depth 4 by hand: c1 = x1, c2 = x2 + c1, ...; all x = 1 gives 10 + 1+2+3+4 and 20 + 1+2+3+4 = 50,
        // and the sixteen rows of the four nested sources sum to 448 and 512 by outermost x
        assertEquals(List.of("448", "512"), values("", nestedLets(4, false)));
        // depth 8 is 256 innermost rows; depth 16 would be 65,536 and cost the lane twelve seconds
        for (int d : new int[] {4, 8}) {
            assertEquals(values("", nestedLets(d, false)), values("", nestedLets(d, true)), "depth " + d);
        }
    }

    @Test
    void nestedMatchesAtDepth() throws Exception {
        // depth 4, x = 1: the arms bind 1, 2, 3, 4; (10 + 10) + (20 + 10) = 50; x = 2: 58
        assertEquals(List.of("50", "58"), values("", nestedMatch(4, false)));
        for (int d : new int[] {4, 8, 16}) {
            assertEquals(values("", nestedMatch(d, false)), values("", nestedMatch(d, true)), "depth " + d);
        }
    }

    @Test
    void callChainAtDepth() throws Exception {
        String model = callChainModel(8);
        assertEquals(values(model, "|[5, 6]->filter(z | $z > 0)->map(q | t::f8($q))"),
                values(model, "|[5, 6]->filter(z | $z > 0)->map(w | t::f8($w))"));
    }

    @Test
    void unrolledFoldWithAGrowingAccumulator() throws Exception {
        // inside a call frame the fold over a spelled list unrolls; the accumulator starts as the outer binder
        // a and is substituted under the inner binder a at every step. The inner lambda adds 0, so the result
        // is a + (10 + ... + 60). (Kept short: DuckDB's own planning time grows steeply with lambda nesting.)
        String model = "function t::k(xs: Integer[*], ws: Integer[*]): Integer[*] { $xs->map(a |"
                + " [10, 20, 30, 40, 50, 60]->fold({x, acc | $ws->map(a | $acc + $a)->sum() + $x}, $a)) }";
        assertEquals(List.of("211", "212", "213"), values(model,
                "|t::k([1, 2, 3]->filter(z | $z > 0), [0]->filter(z | $z >= 0))"));
    }
}
