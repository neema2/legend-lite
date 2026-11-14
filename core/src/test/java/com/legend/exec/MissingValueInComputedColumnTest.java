// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.testing.Lean;
import com.legend.testbackend.TestBackend;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Connection;
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

    /** The table's runtime declares the database it executes on: the connection the test opens, on each in-process
     *  database (the dialect program's Phase 6, 2026-10-16: until then a literal model each for DuckDB and H2). */
    private static String model(TestBackend backend) {
        return """
                ###Relational
                Database h5::db
                (
                  Table T (ID INTEGER PRIMARY KEY, STR VARCHAR(10), N INTEGER)
                )
                ###Connection
                RelationalDatabaseConnection h5::Conn { store: h5::db; %s }
                ###Runtime
                Runtime h5::RT { mappings: []; connections: [ h5::db: [ c: h5::Conn ] ]; }
                """.formatted(backend.connectionBody());
    }

    private static List<List<Object>> run(Connection conn, TestBackend type, String query) throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE T (ID INTEGER PRIMARY KEY, STR VARCHAR(10), N INTEGER)");
            st.execute("INSERT INTO T VALUES (1, 'a', 5), (2, NULL, NULL)");
        }
        Lean.Result r = Lean.run(model(type), query, "h5::RT", conn);
        return r.rows();
    }

    private static void probe(Connection conn, TestBackend type) throws Exception {
        // a computed column over a missing value: false, legend-pure's own value (the lowering guards the missing
        // value; plain SQL three-valued logic would give NULL), on every database
        List<List<Object>> rows = run(conn, type, "|#>{h5::db.T}#->extend(~[inA: c|$c.STR->in(['a']), big: c|$c.N > 3])"
                + "->sort(~ID->ascending())->select(~[ID, inA, big])");
        assertEquals(2, rows.size());
        assertEquals(true, rows.get(0).get(1), type + " row 1 inA");
        assertEquals(true, rows.get(0).get(2), type + " row 1 big");
        assertEquals(false, rows.get(1).get(1), type + ": a missing value is not in ['a']");
        assertEquals(false, rows.get(1).get(2), type + ": a missing value is not > 3");
        // the same predicates in a filter keep the same rows as legend-pure would (the missing row is out)
        try (Statement st = conn.createStatement()) {
            st.execute("DROP TABLE T");
        }
        // (parenthesized: Pure reads binary operators left to right, without precedence)
        List<List<Object>> kept = run(conn, type, "|#>{h5::db.T}#->filter(c|($c.STR->in(['a'])) && ($c.N > 3))->select(~[ID])");
        assertEquals(1, kept.size(), type + " filter keeps the present row only");
        assertEquals(1L, ((Number) kept.get(0).get(0)).longValue());
    }

    @ParameterizedTest(name = "{0}: a computed column over a missing value is false; a filter keeps the same rows")
    @MethodSource("com.legend.testbackend.TestBackends#inProcess")
    void aMissingValueInAComputedColumn(TestBackend backend) throws Exception {
        try (Connection conn = backend.open()) {
            probe(conn, backend);
        }
    }
}
