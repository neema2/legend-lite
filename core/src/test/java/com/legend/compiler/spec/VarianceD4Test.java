// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.test.StorelessRuntime;

import com.legend.Compiler;
import com.legend.exec.ExecutionResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * DEEP_AUDIT §5b (D4) — function-parameter slots are CONTRAVARIANT
 * (reference TypeMatch matches params with {@code !covariant}): the
 * actual's parameter must be a SUPERTYPE of the formal's. The old
 * covariant order both wrong-ACCEPTED the unsound direction (an
 * Integer[1] lambda receiving 1.5 returned 2.5) and wrong-REJECTED the
 * sound one. Plus: an inheritance cycle was a StackOverflowError in
 * ModelContext.isSubtype (visited set added).
 */
class VarianceD4Test {

    private static final String FNS = """
            function m::callN(f: Function<{Number[1]->String[1]}>[1]): String[1]
            { $f->eval(1.5) }
            function m::relay(g: Function<{Integer[1]->String[1]}>[1]): String[1]
            { m::callN($g) }
            """;

    @Test
    @DisplayName("wrong-accept dies: an Integer[1] lambda cannot fill a Number[1] slot")
    void unsoundDirectionRejected() throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:")) {
            var ex = assertThrows(Exception.class, () -> Compiler.execute(StorelessRuntime.with(FNS, DatabaseType.DuckDB), "{|m::relay({i: Integer[1] | ($i + 1)->toString()})}", StorelessRuntime.RUNTIME,
                    c));
            assertTrue(ex.getMessage() != null,
                    "expected a type error, got: " + ex);
        }
    }

    @Test
    @DisplayName("sound direction accepted: a wider-param function fills a narrower slot")
    void soundDirectionAccepted() throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:")) {
            // a Number-taking function in an Integer-taking slot — the
            // USEFUL case the covariant order refused
            ExecutionResult r = Compiler.execute(StorelessRuntime.with("""
                    function m::callI(f: Function<{Integer[1]->String[1]}>[1]): String[1]
                    { $f->eval(7) }
                    function m::wide(n: Number[1]): String[1]
                    { $n->toString() }
                    """, DatabaseType.DuckDB),
                    "{|m::callI(m::wide_Number_1__String_1_)}", StorelessRuntime.RUNTIME, c);
            assertEquals("7", ((ExecutionResult.Scalar) r).value());
        }
    }

    @Test
    @DisplayName("an inheritance cycle refuses/answers finitely (no StackOverflowError)")
    void inheritanceCycleIsFinite() throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:")) {
            try {
                Compiler.execute(StorelessRuntime.with("""
                        Class m::A extends m::B {}
                        Class m::B extends m::A {}
                        Class m::D {}
                        """, DatabaseType.DuckDB), "{|m::D}", StorelessRuntime.RUNTIME, c);
            } catch (StackOverflowError e) {
                throw new AssertionError(
                        "inheritance cycle still overflows", e);
            } catch (Exception acceptable) {
                // any FINITE outcome (compile error preferred) is the pin
            }
        }
    }
}
