// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.database.Databases;
import com.legend.error.ModelException;
import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackends;
import com.legend.testing.EdgeProgram;
import com.legend.testing.Program;
import java.sql.Connection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * A RELATION COUNTED AS ONE VALUE BY THE EDGE (register S137; census C1's K14, the orchestrator's ruling of 2026-11-17:
 * a test of upstream's behaviour is an edge test -- moved from {@code RelationCountAssertsTest}): the compatibility
 * rules on the edge's door count a relation passed to {@code assertSize} or {@code assertNotEmpty} as ONE value, as
 * upstream does. Under the lane's judge.
 */
class RelationCountAssertsEdgeTest {

    private static final String MODEL = "###Pure\nClass t::Marker {}\n";
    private static final String TWO_ROWS = "#TDS\n  a\n  1\n  2\n#";
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        conn = TestBackends.platform().open();
    }

    @AfterAll
    static void tearDown() throws Exception {
        conn.close();
    }

    private static Program.Report run(String body) {
        return EdgeProgram.run(StorelessRuntime.with(MODEL, Databases.PLATFORM), body, StorelessRuntime.RUNTIME, conn);
    }

    @Test
    @DisplayName("backwards-compatibility mode counts the relation as ONE value, as upstream does")
    void upstreamInCompat() {
        assertTrue(run("|assertNotEmpty(" + TWO_ROWS + ")").passed());
        assertTrue(run("|assertSize(" + TWO_ROWS + ", 1)").passed());
        com.legend.error.AssertFailed size = assertThrows(com.legend.error.AssertFailed.class,
                () -> run("|assertSize(" + TWO_ROWS + ", 2)"));
        // (item 3, 2026-11-21: the catalog's words on every route, Q15 = A; the count upstream's, one value)
        assertTrue(size.getMessage().contains("expected size: 2, actual size: 1"), size.getMessage());
        // assertNotSize, the mirror (legend-pure's assertNotSize.pure: assertNotEq($size, $collection->size(), ...) over
        // collection:Any[*]; held to the rule since 2026-11-26): the relation is one value, so it holds for every size
        // but 1, whatever rows the relation has
        assertTrue(run("|assertNotSize(" + TWO_ROWS + ", 2)").passed());
        com.legend.error.AssertFailed notSize = assertThrows(com.legend.error.AssertFailed.class,
                () -> run("|assertNotSize(" + TWO_ROWS + ", 1)"));
        assertTrue(notSize.getMessage().contains("size should not equal: 1"), notSize.getMessage());
        // upstream fails it with '%s is not empty' printing the relation itself, a text lite does not spell: refused
        ModelException empty = assertThrows(ModelException.class, () -> run("|assertEmpty(" + TWO_ROWS + ")"));
        assertTrue(empty.getMessage().contains("which lite does not spell"), empty.getMessage());
    }
}
