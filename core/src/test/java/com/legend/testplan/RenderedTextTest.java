// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.testplan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.error.AssertFailed;
import com.legend.testbackend.TestBackends;
import com.legend.testing.Program;

import java.sql.Connection;
import java.sql.Statement;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * A RENDERED TEXT asserted against its golden. In lite's own runs the two strings ARE the verdict (the user,
 * 2026-11-05: after a verdict the harness may only explain a failure) -- under the database judge the database computes
 * the render and the equality. Only in backwards-compatibility mode does a ruled comparison decide where the strings
 * differ: an unsorted query's tokens as a multiset (S131). Run under each judge by the lane ({@code legend.test.judge}).
 */
class RenderedTextTest {
    private static final String MODEL = """
            Class r::P { name: String[1]; }
            ###Relational
            Database r::DB ( Table PT (NAME VARCHAR(20)) )
            ###Mapping
            Mapping r::M (
              *r::P: Relational { ~mainTable [r::DB] PT name: PT.NAME }
            )
            ###Connection
            RelationalDatabaseConnection r::C { store: r::DB; type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime r::RT { mappings: [r::M]; connections: [ r::DB: [ c0: r::C ] ]; }
            """;
    private Connection conn;

    @BeforeEach
    void setUp() throws Exception {
        conn = TestBackends.platform().open();
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE PT (NAME VARCHAR)");
            st.execute("INSERT INTO PT VALUES ('ann'), ('bob')");
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        conn.close();
    }

    /** The names, sorted (lite's own tests read no order the program never defined), joined. */
    private Object run(String golden) throws Exception {
        return Program.last(MODEL, "{| assertEquals('" + golden
                + "', r::P.all()->map(p|$p.name)->sort()->joinStrings(','));}", "r::RT", conn).value();
    }

    /** The names in the query's arrival order, joined, judged in backwards-compatibility mode. */
    private Object runCompat(String golden) throws Exception {
        return Program.last(MODEL, "{| assertEquals('" + golden + "', r::P.all()->map(p|$p.name)->joinStrings(','));}",
                "r::RT", conn, com.legend.ExecuteOptions.NONE.withJudgeMode(Program.laneJudge())
                        .withEngineCompat(true)).value();
    }

    @Test
    @DisplayName("the strings equal: the comparison holds")
    void equalStrings() throws Exception {
        assertEquals(Boolean.TRUE, run("ann,bob"));
    }

    @Test
    @DisplayName("clean mode: the strings differ, so the assert fails -- no comparison after it")
    void theStringsAreTheVerdict() {
        AssertFailed e = assertThrows(AssertFailed.class, () -> run("bob,ann"));
        assertTrue(e.getMessage().contains("bob,ann") && e.getMessage().contains("ann,bob"), e.getMessage());
    }

    @Test
    @DisplayName("compatibility mode: the strings differ, the unsorted query's tokens agree -- the ruled comparison"
            + " holds (S131)")
    void compatTokensAgree() throws Exception {
        assertEquals(Boolean.TRUE, runCompat("bob,ann"));
    }

    @Test
    @DisplayName("compatibility mode: the tokens differ -- the ruled comparison fails with its own message")
    void compatTokensDiffer() {
        AssertFailed e = assertThrows(AssertFailed.class, () -> runCompat("ann,cy"));
        assertTrue(e.getMessage().contains("cy"), e.getMessage());
    }
}
