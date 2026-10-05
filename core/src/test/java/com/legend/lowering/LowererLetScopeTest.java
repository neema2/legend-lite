// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.test.StorelessRuntime;

import com.legend.Compiler;
import com.legend.Execution;
import com.legend.compiler.element.ClassLayouts;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.exec.ExecutionResult;
import com.legend.sql.SqlQuery;
import com.legend.sql.dialect.DuckDb;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * The lowerer's query scope (rebuild W0.6 push 2; review #8 was the first case). Query-level lets, plan parameters
 * and lets met in expression position share one flat map that is read ahead of every row scope, so a binder spelled
 * like one of them used to read the map instead of its own scope: {@code |let x = 10; [1, 2, 3]->map(x | $x + 1)}
 * returned {@code [11, 11, 11]}. Now every such binder is renamed before lowering ({@code Lowerer.lower(List)}), so
 * the map never meets a name it does not own. Each case's old value is in its comment.
 */
class LowererLetScopeTest {

    private static List<String> values(String query) throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:")) {
            ExecutionResult r = Execution.execute(StorelessRuntime.with("", DatabaseType.DuckDB), query, StorelessRuntime.RUNTIME, c);
            if (r instanceof ExecutionResult.Collection col) {
                return col.values().stream().map(String::valueOf).toList();
            }
            if (r instanceof ExecutionResult.Scalar s) {
                return List.of(String.valueOf(s.value()));
            }
            throw new IllegalStateException("unexpected result for " + query + ": " + r);
        }
    }

    @Test
    void mapParameterIsNotTheQueryLet() throws Exception {
        assertEquals(List.of("2", "3", "4"), values("|[1, 2, 3]->map(x | $x + 1);"));
        assertEquals(List.of("2", "3", "4"), values("|let y = 10; [1, 2, 3]->map(x | $x + 1);"));
        // was [11, 11, 11]
        assertEquals(List.of("2", "3", "4"), values("|let x = 10; [1, 2, 3]->map(x | $x + 1);"));
    }

    @Test
    void filterParameterIsNotTheQueryLet() throws Exception {
        // was [1, 2, 3]
        assertEquals(List.of("2", "3"), values("|let x = 10; [1, 2, 3]->filter(x | $x > 1);"));
    }

    @Test
    void everyBinderKindShadowsTheQueryLet() throws Exception {
        assertEquals(List.of("true"), values("|let x = 10; [1, 2, 3]->exists(x | $x == 2);"));      // was false
        assertEquals(List.of("true"), values("|let x = 10; [1, 2, 3]->forAll(x | $x < 5);"));       // was false
        assertEquals(List.of("6"), values("|let x = 10; [1, 2, 3]->fold({x, a | $x + $a}, 0);"));    // was 30
        assertEquals(List.of("6"), values("|let a = 100; [1, 2, 3]->fold({x, a | $x + $a}, 0);"));   // was 103
        assertEquals(List.of("1", "2", "3"), values("|let x = 10; [3, 1, 2]->sortBy(x | $x);"));    // was [3, 1, 2]
        // a property read on a binder spelled like a struct-valued let; was 0
        assertEquals(List.of("5"), values("|let p = ^Pair<Integer, Integer>(first = 0, second = 0);"
                + " [^Pair<Integer, Integer>(first = 5, second = 6)]->map(p | $p.first);"));
    }

    @Test
    void nestedBindersAndLetReadsThroughLambdas() throws Exception {
        // three levels spelled x; was [202, 202]
        assertEquals(List.of("32", "32"), values("|let x = 100; [1, 2]->map(x | [10, 20]->map(x | $x + 1)->sum());"));
        // a let read at depth two still resolves
        assertEquals(List.of("232", "234"),
                values("|let y = 100; [1, 2]->map(x | [10, 20]->map(z | $x + $z + $y)->sum());"));
        assertEquals(List.of("11", "12", "13"), values("|let y = 10; [1, 2, 3]->map(x | $x + $y);"));
        // a let in expression position inside a lambda, spelled like the query let
        assertEquals(List.of("3", "5", "7"), values("|let x = 10; [1, 2, 3]->map(y | {| let x = $y * 2; $x + 1;}->eval());"));
        // several statements: the second let's value and the final expression both bind x; was [100, 100, 100]
        assertEquals(List.of("4", "9", "16"), values("|let x = 10; let z = [1, 2, 3]->map(x | $x + 1); $z->map(x | $x * $x);"));
    }

    @Test
    void planParameterIsNotTheLambdaParameter() {
        // a plan parameter x bound into the query scope, then a lambda whose binder is also x: before the fix the
        // binder read the parameter and the placeholder reached an executable dialect (a loud failure there, a wrong
        // ${x} in the plan text on the engine-text path)
        ModelContext ctx = Compiler.compileModel("");
        TypedSpec typed = Compiler.query(Compiler.compileModel(""), "|[1, 2, 3]->map(x | $x + 1)").expression();
        List<TypedSpec> body = typed instanceof TypedLambda lam ? lam.body() : List.of(typed);
        Lowerer lw = new Lowerer(t -> ClassLayouts.layoutOf(ctx, t), f -> ctx.findClass(f).isPresent(),
                ctx.implementations()).bindPlanParam("x", false);
        SqlQuery plan = lw.lower(body);
        String sql = new DuckDb().render(plan);
        assertFalse(sql.contains("${x}"), sql);
    }
}
