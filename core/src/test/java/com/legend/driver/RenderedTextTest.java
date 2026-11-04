// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.driver;

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
 * A RENDERED TEXT asserted against its golden, generic first (2026-11-04, the core program: every assert on its own
 * merit, a ruled fallback only after it fails): the two strings compare as strings -- under the database judge the
 * database computes the render and the equality -- and only where they differ does the ruled comparison decide, an
 * unsorted query's tokens as a multiset (S131). Run under each judge by the lane ({@code legend.test.judge}).
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

    private Object run(String golden) throws Exception {
        return Program.last(MODEL, "{| assertEquals('" + golden + "', r::P.all()->map(p|$p.name)->joinStrings(','));}",
                "r::RT", conn).value();
    }

    @Test
    @DisplayName("the strings equal: the generic comparison holds")
    void equalStrings() throws Exception {
        assertEquals(Boolean.TRUE, run("ann,bob"));
    }

    @Test
    @DisplayName("the strings differ, the unsorted query's tokens agree: the ruled fallback holds (S131)")
    void tokensAgree() throws Exception {
        assertEquals(Boolean.TRUE, run("bob,ann"));
    }

    @Test
    @DisplayName("the tokens differ: the fallback fails with its own message")
    void tokensDiffer() {
        AssertFailed e = assertThrows(AssertFailed.class, () -> run("ann,cy"));
        assertTrue(e.getMessage().contains("cy"), e.getMessage());
    }
}
