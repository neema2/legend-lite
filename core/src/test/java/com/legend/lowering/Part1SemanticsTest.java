// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.test.StorelessRuntime;

import com.legend.Compiler;
import com.legend.exec.ExecutionResult;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Part-1 silent-value WITNESSES (VERDICT_AND_PCT_AUDIT "still open"
 * list, machine-verified and fixed 2026-08-26 — the audit read a
 * pre-F10 commit, so every item was probed against current main
 * before any fix; two of its items were already adjudicated
 * pure-faithful and stay pinned in BurnLaneTest).
 *
 * <p>Oracle receipts per pin: integer/integer divide goes through
 * pure's BigDecimal accumulator lane and RAISES on zero
 * (NumericAccumulator — only the double lane yields Infinity);
 * times() preserves the Integer kind; []-&gt;map(f) is the empty
 * value (NullLit is its one cell representation); a ^new omitting a
 * required [1] property is a COMPILE error (NewValidator:132).
 */
class Part1SemanticsTest {

    private static final String MODEL =
            "Class my::A { name: String[1]; age: Integer[1]; }";

    private static Object scalar(String q, Connection conn) throws Exception {
        ExecutionResult r = Compiler.execute(StorelessRuntime.with(MODEL, DatabaseType.DuckDB), q, StorelessRuntime.RUNTIME, conn);
        return r instanceof ExecutionResult.Scalar s ? s.value()
                : r instanceof ExecutionResult.Collection c ? c.values() : r;
    }

    @Test
    void part1Semantics() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
            // integer / integer by zero RAISES (pure's BigDecimal lane)
            Exception dz = assertThrows(Exception.class,
                    () -> scalar("{|1 / 0}", conn));
            assertTrue(String.valueOf(dz.getMessage())
                            .contains("Division by zero"),
                    "integer 1/0 must raise: " + dz.getMessage());
            // the FLOAT lane keeps IEEE semantics (pure's double arm)
            assertEquals("Infinity",
                    String.valueOf(scalar("{|1.0 / 0.0}", conn)));
            // times() preserves the Integer kind (was Double 6.0 —
            // LIST_PRODUCT degrades to DOUBLE; list_reduce keeps kind)
            Object t = scalar("{|[2,3]->times()}", conn);
            assertEquals("6", String.valueOf(t));
            assertTrue(!(t instanceof Double),
                    "times() over Integer[*] must stay integer-kinded");
            // []->map(f) is the EMPTY value, not a stamp-invariant crash
            assertEquals("null",
                    String.valueOf(scalar("{|[]->map(v|$v)}", conn)));
            // VALUE-LANE toOne over an empty RAISES (user ruling
            // 2026-08-26: "if not relational, follow the type system
            // literally" — the interpreted lane is the oracle for
            // store-free expressions; row-lane reads keep the
            // adjudicated relational flow)
            Exception to = assertThrows(Exception.class,
                    () -> scalar("{|[]->first()->toOne()}", conn));
            assertTrue(String.valueOf(to.getMessage()).contains(
                            "Cannot cast a collection of size 0"),
                    "value-lane toOne over empty must raise: "
                            + to.getMessage());
            assertEquals("1",
                    String.valueOf(scalar("{|[1,2]->first()->toOne()}", conn)));
            // ^new omitting a required [1] is a COMPILE error with
            // pure's own message (NewValidator format)
            Exception miss = assertThrows(Exception.class,
                    () -> scalar("{|^my::A(name='x').age}", conn));
            assertTrue(String.valueOf(miss.getMessage()).contains(
                            "Missing value(s) for required property 'age'"),
                    "missing required [1] must reject at compile: "
                            + miss.getMessage());
        }
    }
}
