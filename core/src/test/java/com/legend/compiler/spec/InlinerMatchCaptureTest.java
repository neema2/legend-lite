// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.test.StorelessRuntime;

import com.legend.Compiler;
import com.legend.exec.ExecutionResult;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Capture-avoiding substitution, end to end (rebuild W0.6 push 1; review #9 was the first case). A term
 * substituted under a binder that spells one of the term's free variables must not be captured by it, in
 * any of the three substitution engines: the inliner ({@code UserCallInliner}), the typer's source-level
 * let fold ({@code SourceSubst}) and the lowering's static match fold ({@code MatchFold}). Each case
 * returned the value in its comment before the fix. DuckDB only: the capture happens before SQL.
 */
class InlinerMatchCaptureTest {

    private static List<String> values(String query) throws Exception {
        return values("", query);
    }

    private static List<String> values(String model, String query) throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:")) {
            ExecutionResult r = Compiler.execute(StorelessRuntime.with(model, DatabaseType.DuckDB), query, StorelessRuntime.RUNTIME, c);
            if (!(r instanceof ExecutionResult.Collection col)) {
                throw new IllegalStateException("expected a collection frame for " + query + ", got " + r);
            }
            return col.values().stream().map(String::valueOf).toList();
        }
    }

    @Test
    void matchArmInputIsNotCapturedByAnInnerBinder() throws Exception {
        // control: the outer binder spelled differently cannot be captured -- (y+10)+(y+20)
        assertEquals(List.of("32", "34", "36"), values(
                "|[1, 2, 3]->map(y | $y->match([i: Integer[1] | [10, 20]->map(x | $i + $x)->sum()]));"));
        // was [60, 60, 60]
        assertEquals(List.of("32", "34", "36"), values(
                "|[1, 2, 3]->map(x | $x->match([i: Integer[1] | [10, 20]->map(x | $i + $x)->sum()]));"));
    }

    @Test
    void matchExtraArgumentIsNotCaptured() throws Exception {
        // was [90, 90, 90]
        assertEquals(List.of("34", "38", "42"), values("|[1, 2, 3]->map(x | $x->match(["
                + "{i: Integer[1], k: Integer[1] | [10, 20]->map(x | $i + $x + $k)->sum()}], $x));"));
    }

    @Test
    void threeLevelsOfTheSameBinder() throws Exception {
        // was [600, 600, 600]
        assertEquals(List.of("232", "234", "236"), values("|[1, 2, 3]->map(x | $x->match([i: Integer[1] |"
                + " [10, 20]->map(x | $x->match([j: Integer[1] | [100]->map(x | $i + $j + $x)->sum()]))->sum()]));"));
    }

    @Test
    void matchOverAComputedInput() throws Exception {
        // was [62.0, 62.0]
        assertEquals(List.of("43.0", "47.0"), values("|[5.5, 7.5]->filter(z | $z > 0)->map(n | ($n + 1)->match(["
                + "i: Integer[1] | 0, m: Number[1] | [10.0, 20.0]->filter(z | $z > 0)->map(n | $m + $n)->sum()]))"));
    }

    @Test
    void literalUnrollUnderACallFrame() throws Exception {
        // the outer binder y survives (a source that is not spelled); was [60, 60, 60]
        assertEquals(List.of("32", "34", "36"), values(
                "function t::g(xs: Integer[*], ws: Integer[*]): Integer[*]"
                        + " { $xs->map(y | [$y]->map(e | $ws->map(y | $e + $y)->sum())->toOne()) }",
                "|t::g([1, 2, 3]->filter(z | $z > 0), [10, 20]->filter(z | $z > 0))"));
    }

    @Test
    void foldAccumulatorUnderACallFrame() throws Exception {
        // was [30, 30, 30]
        assertEquals(List.of("31", "32", "33"), values(
                "function t::k(xs: Integer[*], ws: Integer[*]): Integer[*]"
                        + " { $xs->map(a | [10, 20]->fold({x, acc | $ws->map(a | $acc + $a)->sum() + $x}, $a)) }",
                "|t::k([1, 2, 3]->filter(z | $z > 0), [0]->filter(z | $z >= 0))"));
    }

    @Test
    void typerSideLetIsNotCaptured() throws Exception {
        // SourceSubst folds the let while typing; was [60, 60, 60]
        assertEquals(List.of("32", "34", "36"), values(
                "|[1, 2, 3]->map(x | let c = $x; [10, 20]->map(x | $c + $x)->sum();)"));
    }

    @Test
    void letOfAnEvaluatedLambdaIsNotCaptured() throws Exception {
        // was [60, 60, 60]
        assertEquals(List.of("32", "34", "36"), values(
                "|[1, 2, 3]->map(y | {| let c = $y; [10, 20]->map(y | $c + $y)->sum();}->eval())"));
    }

    @Test
    void loweringSideMatchFoldIsNotCaptured() throws Exception {
        // the runtime match survives to the lowerer, whose static fold substitutes the input
        String control = "|[1.5, 2.5]->map(y | $y->toOne()->cast(@Number)->match(["
                + "i: Integer[1] | 0, n: Number[1] | [10, 20]->map(x | $n + $x)->sum()]))";
        assertEquals(List.of("33.0", "35.0"), values(control));
        // was [60, 60]
        assertEquals(List.of("33.0", "35.0"), values(control.replace("map(y | $y->", "map(x | $x->")));
    }

    @Test
    void callArgumentsWereAlreadySafe() throws Exception {
        // a regression guard: the call frame's own capture set
        assertEquals(List.of("7", "8"), values(
                "function t::f(p: Integer[1]): Integer[*] { let z = $p + 1; [1, 2]->map(p | $z + $p); }",
                "|[5]->map(p | t::f($p))"));
    }
}
