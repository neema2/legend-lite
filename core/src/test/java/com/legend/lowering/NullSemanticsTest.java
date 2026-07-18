// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.Compiler;
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
            Database s::DB ( Table A (NAME VARCHAR(50), STREET VARCHAR(50), N INTEGER) )
            Mapping m::M ( *m::A: Relational { ~mainTable [s::DB] A
                name: A.NAME, street: A.STREET, n: A.N } )
            Runtime m::RT { mappings: [m::M]; }
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
        ExecutionResult r = Compiler.execute(MODEL, query, "m::RT", conn);
        return ((ExecutionResult.Tabular) r).rows().stream()
                .map(row -> String.valueOf(row.values().get(0))).sorted().toList();
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
        ExecutionResult r = Compiler.execute(MODEL,
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
