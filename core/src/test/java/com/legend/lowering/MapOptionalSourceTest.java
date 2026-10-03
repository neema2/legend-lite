// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.test.StorelessRuntime;

import com.legend.Compiler;
import com.legend.exec.ExecutionResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * KEPT platform fixes from the Phase-4 arc (the redesign's re-judgment,
 * 2026-08-19 — the equality seam arms this file once also pinned are
 * DELETED under Charter Clause 2c; verdicts live in World 1):
 *
 * <p>1. {@code map} over a TO-ONE/[0..1] source wraps as a singleton
 * list and a to-one RESULT unwraps back to the scalar wire
 * ({@code ListEncodings.map}) — a real user-visible fix:
 * {@code head()->map(...)} was a DuckDB Binder error.
 *
 * <p>2. A RIGID type-variable binding whose declared type is an
 * abstract value head (Number, Date) accepts actuals up the m3 lattice
 * and keeps the declared type — {@code eval({a:Number[1]|...}, 1.0)}
 * is the essential-math spec shape (testNumberExp/Log/Pow).
 */
class MapOptionalSourceTest {

    private static Connection conn;

    @BeforeAll
    static void open() throws Exception {
        conn = DriverManager.getConnection("jdbc:duckdb:");
    }

    @AfterAll
    static void close() throws Exception {
        conn.close();
    }

    private static Object run(String query) throws Exception {
        ExecutionResult r = Compiler.execute(StorelessRuntime.with("", DatabaseType.DuckDB), query, StorelessRuntime.RUNTIME, conn);
        return r instanceof ExecutionResult.Scalar s ? s.value()
                : r instanceof ExecutionResult.Collection c ? c.values() : r;
    }

    @Test
    @DisplayName("map over a present [0..1] source applies to the one value")
    void mapOverPresentOptional() throws Exception {
        assertEquals("a!", run("{|['a','b']->head()->map(x|$x + '!')}"));
    }

    @Test
    @DisplayName("map over an empty [0..1] source is EMPTY, never f(NULL)")
    void mapOverEmptyOptional() throws Exception {
        assertEquals(null,
                run("{|[]->head()->map(x|'reached')}"));
    }

    @Test
    @DisplayName("map over a to-one source is the mapped one value")
    void mapOverToOne() throws Exception {
        assertEquals(6L, ((Number) run("{|3->toOne()->map(x|$x * 2)}"))
                .longValue());
    }

    @Test
    @DisplayName("rigid Number parameter accepts a Float actual (m3 lattice)")
    void rigidNumberAcceptsFloat() throws Exception {
        // NUMERIC CHARTER Rule 2: a Number-DECLARED result keeps the
        // database's kind (the engine's value rule: cast(@Number) as-is) —
        // the lattice fact under test is the VALUE, judged numerically
        assertEquals(2.0, ((Number) run("{|{a:Number[1]|$a + 1}->eval(1.0)}")).doubleValue());
    }

    @Test
    @DisplayName("rigid Number parameter accepts an Integer actual")
    void rigidNumberAcceptsInteger() throws Exception {
        assertEquals(2L, ((Number) run("{|{a:Number[1]|$a + 1}->eval(1)}"))
                .longValue());
    }
}
