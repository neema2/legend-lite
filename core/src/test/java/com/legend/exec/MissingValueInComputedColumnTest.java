// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.Execution;
import com.legend.model.ConnectionDefinition.DatabaseType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Build rebuild Phase 3b, item 3, homework H5 (PHASE_3B_HOMEWORK_2026_10_09.md): the reference lane's 34 OVERLOAD
 * rows where legend-pure types a column argument {@code [0..1]} and we type it {@code [1]} ({@code greaterThan},
 * {@code in}, {@code contains}, {@code startsWith}, ...). In a filter the rows are the same either way. In a computed
 * column, legend-pure's own value for a missing argument is {@code false} (the {@code [0..1]} body over an empty
 * value), and so is ours: the lowering guards the missing value ({@code coalesce(x IN (...), FALSE)},
 * {@code x IS NOT NULL AND x > 3}), so the computed value is {@code false} on DuckDB and on H2, where legend-engine's
 * plain SQL ({@code x IN (...)}, {@code x > 3}) would give NULL. Pinned here as the evidence for the recorded,
 * type-only difference ({@code reasons.tsv} OVERLOAD rows; SEMANTICS_REGISTER S40).
 */
class MissingValueInComputedColumnTest {

    /** The table's runtime declares the database it executes on (the connection the test opens); one whole model
     *  per database, so each literal is a model the own-corpus parity lane can read. */
    private static final String DUCKDB_MODEL = """
            ###Relational
            Database h5::db
            (
              Table T (ID INTEGER PRIMARY KEY, STR VARCHAR(10), N INTEGER)
            )
            ###Connection
            RelationalDatabaseConnection h5::Conn { store: h5::db; type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime h5::RT { mappings: []; connections: [ h5::db: [ c: h5::Conn ] ]; }
            """;
    private static final String H2_MODEL = """
            ###Relational
            Database h5::db
            (
              Table T (ID INTEGER PRIMARY KEY, STR VARCHAR(10), N INTEGER)
            )
            ###Connection
            RelationalDatabaseConnection h5::Conn { store: h5::db; type: H2; specification: LocalH2 { }; auth: DefaultH2; }
            ###Runtime
            Runtime h5::RT { mappings: []; connections: [ h5::db: [ c: h5::Conn ] ]; }
            """;

    private static String model(DatabaseType type) {
        return type == DatabaseType.H2 ? H2_MODEL : DUCKDB_MODEL;
    }

    private static List<Row> run(Connection conn, DatabaseType type, String query) throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE T (ID INTEGER PRIMARY KEY, STR VARCHAR(10), N INTEGER)");
            st.execute("INSERT INTO T VALUES (1, 'a', 5), (2, NULL, NULL)");
        }
        ExecutionResult r = Execution.execute(model(type), query, "h5::RT", conn);
        return ((ExecutionResult.Tabular) r).rows();
    }

    private static void probe(Connection conn, DatabaseType type) throws Exception {
        // a computed column over a missing value: false, legend-pure's own value (the lowering guards the missing
        // value; plain SQL three-valued logic would give NULL), on both databases
        List<Row> rows = run(conn, type, "|#>{h5::db.T}#->extend(~[inA: c|$c.STR->in(['a']), big: c|$c.N > 3])"
                + "->sort(~ID->ascending())->select(~[ID, inA, big])");
        assertEquals(2, rows.size());
        assertEquals(true, rows.get(0).values().get(1), type + " row 1 inA");
        assertEquals(true, rows.get(0).values().get(2), type + " row 1 big");
        assertEquals(false, rows.get(1).values().get(1), type + ": a missing value is not in ['a']");
        assertEquals(false, rows.get(1).values().get(2), type + ": a missing value is not > 3");
        // the same predicates in a filter keep the same rows as legend-pure would (the missing row is out)
        try (Statement st = conn.createStatement()) {
            st.execute("DROP TABLE T");
        }
        // (parenthesized: Pure reads binary operators left to right, without precedence)
        List<Row> kept = run(conn, type, "|#>{h5::db.T}#->filter(c|($c.STR->in(['a'])) && ($c.N > 3))->select(~[ID])");
        assertEquals(1, kept.size(), type + " filter keeps the present row only");
        assertEquals(1L, ((Number) kept.get(0).values().get(0)).longValue());
    }

    @Test
    @DisplayName("DuckDB: a computed column over a missing value is false; a filter keeps the same rows")
    void duckdb() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
            probe(conn, DatabaseType.DuckDB);
        }
    }

    @Test
    @DisplayName("H2: a computed column over a missing value is false; a filter keeps the same rows")
    void h2() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:h5probe" + System.nanoTime())) {
            probe(conn, DatabaseType.H2);
        }
    }
}
