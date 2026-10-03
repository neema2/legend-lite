// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.test.StorelessRuntime;

import com.legend.Compiler;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PHASE 1 AUDIT PINS (One-Platform Plan; verdict: PARTIAL — the
 * execution is platform-honest, the GENERALITY is not yet).
 *
 * <p>Pin 1 — bare {@code .rows} returns REAL Row objects with real
 * cells (user-forced honesty 2026-08-18: "don't we execute and get
 * something back?"), including rows whose FIRST column is NULL (the
 * shape that broke both prior stand-ins).
 *
 * <p>Pin 2 — the GENERALITY TRIPWIRE: {@code .rows->size()} is a
 * perfectly well-typed expression that today WALLS, because ResultNav
 * is a closed-vocabulary recognizer, not a typed platform feature.
 * This pin holds the honest current behavior and MUST FLIP when the
 * execution-backed metamodel-classes leg lands (Phase 3a: model
 * Result/ResultSet/Row so ANY well-typed expression compiles) — a
 * green flip here is that leg's acceptance test.
 */
class Phase1AuditTest {

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
    @DisplayName("Pin 1: bare .rows = real Row objects, real cells, NULL-first-column included")
    void bareRowsAreRealRows() throws Exception {
        ExecutionResult r = Compiler.execute(StorelessRuntime.with("", DatabaseType.DuckDB), CONN_LET
                + "meta::relational::metamodel::execute::executeInDb("
                + "'select null as A, 7 as B union all"
                + " select null, 8 order by 2', $c, 0, 1000).rows;}", StorelessRuntime.RUNTIME, conn);
        // bare .rows = REAL Row carriers with real cells, whichever
        // route serves it during the slice-3/4 migration (the ResultNav
        // arm yields a Collection of Rows; the pipeline yields the
        // relation as TABULAR — the user-ratified two-worlds design)
        List<Row> rows = r instanceof ExecutionResult.Tabular t ? t.rows()
                : ((ExecutionResult.Collection) r).values().stream()
                        .map(Row.class::cast).toList();
        assertEquals(2, rows.size(),
                "NULL-first-column rows must COUNT (both prior stand-ins"
                + " failed exactly here)");
        assertEquals(7L, ((Number) rows.get(0).get(1)).longValue(),
                "the Row holds its REAL cells");
    }

    @Test
    @DisplayName("Pin 2 FLIPPED (Phase 1c): .rows->size() is an ORDINARY expression — the generality landed")
    void rowsSizeIsOrdinary() throws Exception {
        // the tripwire's contract fulfilled: `.rows` types as a
        // relation (the splice arm -> TypedRawSqlRelation -> the one
        // Lowerer -> COUNT in the database); no recognizer vocabulary
        ExecutionResult r = Compiler.execute(StorelessRuntime.with("", DatabaseType.DuckDB), CONN_LET
                + "meta::relational::metamodel::execute::executeInDb("
                + "'select 1 as A union all select 2', $c, 0, 1000)"
                + ".rows->size();}", StorelessRuntime.RUNTIME, conn);
        assertEquals(2L, ((Number) ((ExecutionResult.Scalar) r).value())
                .longValue());
    }

    /** TRIPWIRE #2 (Phase 1c slice 2) — SPEC-FAITHFUL spelling
     * (user-ratified: the SURFACE stays legend-pure-spec-exact even
     * where the implementation is relation/TDS underneath):
     * {@code $r.value('A')} is the spec's OWN accessor
     * (Row.value(name)), and composing it under filter must lower to a
     * column read. Today it refuses; flips to {@code assertEquals(2)}
     * when slice 2 lands. Note {@code $r.A} is NON-spec and its type
     * error is correct forever, not a gap. */
    @Test
    @DisplayName("slice 3: .columnNames is a probed SCHEMA FACT through the ordinary pipeline")
    void columnNamesIsASchemaFact() throws Exception {
        ExecutionResult r = Compiler.execute(StorelessRuntime.with("", DatabaseType.DuckDB), CONN_LET
                + "meta::relational::metamodel::execute::executeInDb("
                + "'select 1 as A, 2 as B', $c, 0, 1000).columnNames;}", StorelessRuntime.RUNTIME,
                conn);
        assertEquals(List.of("A", "B"),
                ((ExecutionResult.Collection) r).values());
    }

    @Test
    @DisplayName("slice 3: a fetchDb catalog grid composes as a relation too")
    void fetchDbGridComposes() throws Exception {
        try (var st = conn.createStatement()) {
            st.execute("CREATE TABLE PHASE1_T(X INT)");
        }
        ExecutionResult r = Compiler.execute(StorelessRuntime.with("", DatabaseType.DuckDB), CONN_LET
                + "meta::relational::metamodel::execute::fetchDbTablesMetaData("
                + "$c, [], 'PHASE1_T').rows->size();}", StorelessRuntime.RUNTIME, conn);
        assertEquals(1L, ((Number) ((ExecutionResult.Scalar) r).value())
                .longValue());
    }

    @Test
    @DisplayName("tripwire #2 FLIPPED (slice 2): filter via the SPEC accessor value('A') composes")
    void filterViaSpecAccessorComposes() throws Exception {
        ExecutionResult r = Compiler.execute(StorelessRuntime.with("", DatabaseType.DuckDB), CONN_LET
                + "meta::relational::metamodel::execute::executeInDb("
                + "'select 1 as A union all select 2 union all select 3',"
                + " $c, 0, 1000)"
                + ".rows->filter(r | $r.value('A')->cast(@Integer) > 1)"
                + "->size();}", StorelessRuntime.RUNTIME, conn);
        assertEquals(2L, ((Number) ((ExecutionResult.Scalar) r).value())
                .longValue());
    }
}
