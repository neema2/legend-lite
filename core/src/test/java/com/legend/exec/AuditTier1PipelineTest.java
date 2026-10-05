// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.test.StorelessRuntime;

import com.legend.Compiler;
import com.legend.Execution;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tier-1 regression pins for audit findings B and F (2026-08-18),
 * driven through the WHOLE pipeline ({@code Compiler.execute} — the
 * harness-runs-through-the-platform rule).
 *
 * <p><b>B</b> — a grid chain's {@code ->toString()} rides the SQL
 * projection. The old Java {@code String.valueOf} fabricated the TEXT
 * {@code "null"} for a NULL cell; in SQL, CAST keeps NULL NULL — the
 * pure EMPTY.
 *
 * <p><b>F</b> — a collection-shaped result that shrinks below its
 * declared lower bound (NULL cells dropped, rows missing) is a LOUD
 * defect, never a quiet count.
 */
class AuditTier1PipelineTest {

    private static final String CONN_LET =
            "{| let c = ^meta::external::store::relational::runtime::TestDatabaseConnection("
                    + "type=meta::relational::runtime::DatabaseType.DuckDB);\n";

    private static Connection conn;

    @BeforeAll
    static void open() throws Exception {
        conn = DriverManager.getConnection("jdbc:duckdb:");
    }

    @AfterAll
    static void close() throws Exception {
        conn.close();
    }

    @Test
    @DisplayName("B: a NULL grid cell through ->toString() is EMPTY, not the text \"null\"")
    void nullCellToStringIsEmpty() throws Exception {
        ExecutionResult r = Execution.execute(StorelessRuntime.with("", DatabaseType.DuckDB), CONN_LET
                + "meta::relational::metamodel::execute::executeInDb("
                + "'select null as X', $c, 0, 1000)"
                + ".rows->at(0).value('X')->toString();}", StorelessRuntime.RUNTIME, conn);
        Object v = ((ExecutionResult.Scalar) r).value();
        assertNull(v, "String.valueOf(null) used to fabricate the text"
                + " \"null\" here — a NULL cell is a pure EMPTY");
    }

    @Test
    @DisplayName("B control: a real cell through ->toString() still reads its text")
    void realCellToStringReads() throws Exception {
        ExecutionResult r = Execution.execute(StorelessRuntime.with("", DatabaseType.DuckDB), CONN_LET
                + "meta::relational::metamodel::execute::executeInDb("
                + "'select \\'hi\\' as X', $c, 0, 1000)"
                + ".rows->at(0).value('X')->toString();}", StorelessRuntime.RUNTIME, conn);
        assertEquals("hi", ((ExecutionResult.Scalar) r).value());
    }

    @Test
    @DisplayName("F: a [1..*]-typed result with zero values raises IN SQL")
    void lowerBoundShrinkWallsLoudly() {
        // multiplicity audit slice 3: toOneMany is CHECKED at-least-one
        // in the emitted SQL (tenet #1 — the database raises, with
        // pure's own message), replacing the old egress-side wall.
        Exception e = assertThrows(Exception.class,
                () -> Execution.execute(StorelessRuntime.with("", DatabaseType.DuckDB),
                        "{|[1, 2, 3]->filter(x | $x > 10)->toOneMany();}", StorelessRuntime.RUNTIME,
                        conn));
        assertTrue(String.valueOf(e.getMessage())
                        .contains("Cannot cast a collection of size 0"),
                "expected pure's size-0 cast error, got: "
                + e.getMessage());
    }

    @Test
    @DisplayName("F control: a satisfied [1..*] collection still flows")
    void satisfiedLowerBoundFlows() throws Exception {
        ExecutionResult r = Execution.execute(StorelessRuntime.with("", DatabaseType.DuckDB),
                "{|[1, 2, 3]->filter(x | $x > 2)->toOneMany();}", StorelessRuntime.RUNTIME, conn);
        java.util.List<Object> values =
                ((ExecutionResult.Collection) r).values();
        assertEquals(1, values.size());
        assertEquals(3L, ((Number) values.get(0)).longValue());
    }
}
