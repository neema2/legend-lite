// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.database.Databases;
import com.legend.error.ModelException;
import com.legend.test.StorelessRuntime;
import com.legend.testbackend.TestBackends;
import com.legend.testing.Program;
import java.sql.Connection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * A RELATION COUNTED AS ONE VALUE (register S137, the user's ruling of 2026-11-05): {@code assertSize},
 * {@code assertEmpty} and {@code assertNotEmpty} take a collection, so upstream counts a relation passed to them as
 * one value. Backwards-compatibility mode does exactly that; lite's own runs refuse the three over a relation at
 * planning and name the assert that says what the test means. Under the lane's judge.
 */
class RelationCountAssertsTest {

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
                ExecuteOptions.NONE.withJudgeMode(Program.laneJudge()).withEngineCompat(compat));
    }

    @Test
    @DisplayName("lite refuses assertSize, assertEmpty and assertNotEmpty over a relation at planning, naming what to say")
    void refusedInLite() {
        ModelException empty = assertThrows(ModelException.class, () -> run("|assertEmpty(" + TWO_ROWS + ")", false));
        assertTrue(empty.getMessage().contains("assertEmpty over a relation counts the relation as ONE value")
                && empty.getMessage().contains("say assertEquals(0, $relation->size())"), empty.getMessage());
        ModelException size = assertThrows(ModelException.class, () -> run("|assertSize(" + TWO_ROWS + ", 2)", false));
        assertTrue(size.getMessage().contains("say assertEquals(<size>, $relation->size())"), size.getMessage());
        ModelException notEmpty = assertThrows(ModelException.class,
                () -> run("|assertNotEmpty(" + TWO_ROWS + ")", false));
        assertTrue(notEmpty.getMessage().contains("say assertNotEquals(0, $relation->size())"), notEmpty.getMessage());
    }

    @Test
    @DisplayName("what lite asks for instead holds: the relation's size")
    void sizeSaysIt() {
        assertTrue(run("|assertEquals(2, " + TWO_ROWS + "->size())", false).passed());
    }

    @Test
    @DisplayName("backwards-compatibility mode does not refuse them")
    void notRefusedInCompat() {
        // upstream's one-value count is not built there yet: lite counts the rows (a literal relation's emptiness
        // is refused by name, NotImplemented; an execute's $result.values is counted as one, AssertVerdictSpliceTest)
        assertTrue(run("|assertNotEmpty(" + TWO_ROWS + ")", true).passed());
    }
}
