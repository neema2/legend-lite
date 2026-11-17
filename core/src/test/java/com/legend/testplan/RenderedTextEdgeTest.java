// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.testplan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.error.AssertFailed;
import com.legend.testbackend.TestBackends;
import com.legend.testing.EdgeProgram;

import java.sql.Connection;
import java.sql.Statement;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * A RENDERED TEXT asserted against its golden ON THE EDGE'S DOOR (register S131; census C1's K14, the orchestrator's
 * ruling of 2026-11-17: a test of upstream's behaviour is an edge test -- moved from {@code RenderedTextTest}): where the
 * strings differ, the compatibility rules compare an unsorted query's tokens as a multiset. Under the lane's judge.
 */
class RenderedTextEdgeTest {
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

    /** The names in the query's arrival order, joined, judged by the compatibility rules on the edge's door. */
    private Object runCompat(String golden) throws Exception {
        return EdgeProgram.run(MODEL, "{| assertEquals('" + golden + "', r::P.all()->map(p|$p.name)->joinStrings(','));}",
                "r::RT", conn).side().value();
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
