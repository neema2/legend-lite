// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.database.Databases;
import com.legend.error.ModelException;
import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackends;
import com.legend.testing.Program;
import java.sql.Connection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * A RELATION COUNTED AS ONE VALUE BY THE EDGE (register S137; census C1, the orchestrator's ruling of 2026-11-17): the
 * compatibility rules' run -- the edge's style and its run hook -- counts a relation passed to {@code assertSize} or
 * {@code assertNotEmpty} as ONE value, as upstream does. Under the lane's judge.
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

    private static Program.Report run(String body, boolean compat) {
        return Program.run(StorelessRuntime.with(MODEL, Databases.PLATFORM), body, StorelessRuntime.RUNTIME, conn,
                ExecuteOptions.NONE.withJudgeMode(Program.laneJudge()).withEngineCompat(compat)
                        .withStyle(com.legend.edge.PureText.UPSTREAM),
                com.legend.edge.AppealRun.with(null));
    }

    @Test
    @DisplayName("backwards-compatibility mode counts the relation as ONE value, as upstream does")
    void upstreamInCompat() {
        assertTrue(run("|assertNotEmpty(" + TWO_ROWS + ")", true).passed());
        assertTrue(run("|assertSize(" + TWO_ROWS + ", 1)", true).passed());
        com.legend.error.AssertFailed size = assertThrows(com.legend.error.AssertFailed.class,
                () -> run("|assertSize(" + TWO_ROWS + ", 2)", true));
        assertTrue(size.getMessage().contains("expected size: 2, actual size: 1"), size.getMessage());
        // upstream fails it with '%s is not empty' printing the relation itself, a text lite does not spell: refused
        ModelException empty = assertThrows(ModelException.class, () -> run("|assertEmpty(" + TWO_ROWS + ")", true));
        assertTrue(empty.getMessage().contains("which lite does not spell"), empty.getMessage());
    }
}
