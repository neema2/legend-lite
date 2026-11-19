// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

import com.legend.error.DataError;
import com.legend.testexecutionplan.RootLoop;
import com.legend.testexecutionplan.TestPlan;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A segment's run that holds the database's rejection of one of its fused statements (PR #127's review, 2026-11-19):
 *  its root without a row raises the database's own error, and the held rejection is raised whole, where nothing
 *  raised it; a run with none raises nothing. */
class RootLoopsRejectionTest {

    private static final TestPlan.Verdict FIRST = new TestPlan.Verdict(1, "assertEquals@1", "equals", List.of(), null,
            null);
    private static final TestPlan.Verdict SECOND = new TestPlan.Verdict(2, "assertEquals@2", "equals", List.of(), null,
            null);

    private static RootLoops.Run run() {
        return new RootLoops.Run(new RootLoop(List.of(
                new RootLoop.Root("assertEquals@1", List.of(new RootLoop.Row("assertEquals@1", 1, null, null,
                        RootLoop.NO_APPEAL, null)), null),
                new RootLoop.Root("assertEquals@2", List.of(new RootLoop.Row("assertEquals@2", 2, null, null,
                        RootLoop.NO_APPEAL, null)), null)), Map.of()));
    }

    @Test
    void theFirstRootWithoutARowRaisesTheDatabasesOwnError() {
        RootLoops.Run run = run();
        DataError refused = new DataError("date field value out of range: \"-21457-01-08T20:48:00\"");
        // the first group answered its row; the second, its own fused statement, was rejected
        run.received(FIRST, List.of(true));
        run.fused(refused);
        run.received(SECOND, null);
        assertTrue(run.rejected());
        List<Object> judged = new java.util.ArrayList<>();
        DataError raised = assertThrows(DataError.class, () -> run.judge(null, null, false,
                (name, row) -> judged.add(name), i -> null, null));
        assertSame(refused, raised);
        // the first assert was judged on its row before the second raised the database's error
        assertTrue(judged.contains("assertEquals@1"), judged.toString());
        assertSame(refused, assertThrows(DataError.class, run::raiseRejection));
    }

    @Test
    void aRunWithNoRejectionRaisesNothing() {
        RootLoops.Run run = run();
        assertFalse(run.rejected());
        assertDoesNotThrow(run::raiseRejection);
    }
}
