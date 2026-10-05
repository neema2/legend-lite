// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.Execution;
import com.legend.exec.ExecutionResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * COMPARISON-SITE null tolerance (audit 20a H2): the pure {@code [0..1]}
 * comparison overloads' bodies ({@code $x->isNotEmpty() && ...} —
 * legend-pure inequality/greaterThan.pure, engine stringExtension.pure)
 * inline as {@code X IS NOT NULL AND <cmp>} at the comparison itself, so
 * the semantics hold in EVERY context — negated, value position,
 * composed — not just the corpus's directly-pinned spellings. The
 * not-rule stays the engine's processNot: bare, with only the equal/in
 * arms (dbExtension.pure).
 */
class NullSemanticsTest {

    private static final String MODEL = """
            Class m::A { name: String[1]; street: String[0..1]; n: Integer[0..1]; }
            ###Relational
            Database s::DB ( Table A (NAME VARCHAR(50), STREET VARCHAR(50), N INTEGER) )
            ###Mapping
            Mapping m::M ( *m::A: Relational { ~mainTable [s::DB] A
                name: A.NAME, street: A.STREET, n: A.N } )
            ###Connection
            RelationalDatabaseConnection s::DBDuckDB { store: s::DB; type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime m::RT { mappings: [m::M]; connections: [ s::DB: [ c0: s::DBDuckDB ] ]; }
            """;

    private static Connection conn;

    @BeforeAll
    static void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE A (NAME VARCHAR, STREET VARCHAR, N INTEGER)");
            st.execute("INSERT INTO A VALUES ('a','Loop',5),('b','Main',NULL),"
                    + "('c',NULL,30)");
        }
    }

    @AfterAll
    static void tearDown() throws SQLException {
        conn.close();
    }

    private static List<String> names(String query) throws SQLException {
        ExecutionResult r = Execution.execute(MODEL, query, "m::RT", conn);
        // F6.2: the map-binder channel is a VALUE COLLECTION (nulls are
        // pure empties and never arrive)
        if (r instanceof ExecutionResult.Collection c) {
            return c.values().stream().map(String::valueOf).sorted().toList();
        }
        return ((ExecutionResult.Tabular) r).rows().stream()
                .map(row -> String.valueOf(row.values().get(0))).sorted().toList();
    }

    @Test
    @DisplayName("col-vs-col == is NULL-SAFE: both-null rows are EQUAL (pure empty==empty)")
    void colToColEqualNullSafe() throws SQLException {
        // FAILS-BEFORE (functions/tests testConsistencyWithNulls
        // col-to-col): SQL 'street = street' yields NULL for the
        // null-street row and DROPS it; pure empty==empty is TRUE —
        // engine isEqualsFromFilter emits 'a = b OR (a is null AND
        // b is null)' (dbExtension.pure:926/:947). All 3 rows satisfy
        // street == street.
        assertEquals(List.of("a", "b", "c"),
                names("|m::A.all()->filter(a|$a.street == $a.street)"
                        + "->project([a|$a.name], ['name'])"));
        // and the partition contract holds: == plus != covers all rows
        assertEquals(List.of(),
                names("|m::A.all()->filter(a|$a.street != $a.street)"
                        + "->project([a|$a.name], ['name'])"));
    }

    @Test
    @DisplayName("partition contract: pred + notPred == all (null operands land on the NOT side)")
    void partitionContract() throws SQLException {
        assertEquals(List.of("c"),
                names("{| m::A.all()->filter(x|$x.n > 10)->map(x|$x.name);}"));
        assertEquals(List.of("a", "b"),
                names("{| m::A.all()->filter(x|!($x.n > 10))->map(x|$x.name);}"),
                "null n: guard makes the comparison FALSE, so not() admits");
    }

    @Test
    @DisplayName("COMPOSED negation: !(startsWith || cmp) — the wrap-at-not layer got this wrong")
    void composedNegation() throws SQLException {
        // a: F||F -> admitted; b: startsWith('Main') true -> dropped;
        // c: null street guard FALSE, but 30>10 -> dropped
        assertEquals(List.of("a"), names(
                "{| m::A.all()->filter(x|!($x.street->startsWith('Main')"
                        + " || ($x.n > 10)))->map(x|$x.name);}"));
    }

    @Test
    @DisplayName("VALUE position: a [0..1] comparison yields pure's false, never SQL NULL")
    void valuePosition() throws SQLException {
        ExecutionResult r = Execution.execute(MODEL,
                "{| m::A.all()->project([x|$x.street->endsWith('p'), x|$x.name],"
                        + " ['e','nm']);}", "m::RT", conn);
        var rows = ((ExecutionResult.Tabular) r).rows();
        assertEquals(3, rows.size());
        for (var row : rows) {
            if ("c".equals(row.values().get(1))) {
                assertEquals(false, row.values().get(0),
                        "null street: the guard folds to FALSE, not NULL");
            }
        }
    }
}
