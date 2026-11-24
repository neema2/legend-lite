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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** A segment judged on the database's rejection of one of its fused statements (PR #127's review, 2026-11-19; the
 *  rejection handed to the judging step since 2026-11-20, the user's "Yes remove mutable next"): its root without a row
 *  raises the database's own error after the roots before it are judged on their rows, and where every root held the
 *  rejection is raised whole; a segment judged with no rejection raises nothing. */
class RootLoopsRejectionTest {

    private static final TestPlan.Verdict FIRST = new TestPlan.Verdict(1, "assertEquals@1", "equals", List.of(), null,
            null);
    private static final TestPlan.Verdict SECOND = new TestPlan.Verdict(2, "assertEquals@2", "equals", List.of(), null,
            null);

    /** A typed row's report (item 3's S4: every database row is typed). */
    private static final RootLoop.Report CONDITION = new RootLoop.Report(RootLoop.Family.CONDITION, true, null, null,
            null, -1);

    private static RootLoops.Run run() {
        return new RootLoops.Run(new RootLoop(List.of(
                new RootLoop.Root("assertEquals@1", List.of(new RootLoop.Row("assertEquals@1", 1, CONDITION)),
                        null),
                new RootLoop.Root("assertEquals@2", List.of(new RootLoop.Row("assertEquals@2", 2, CONDITION)),
                        null)), Map.of()));
    }

    private static final DataError REFUSED = new DataError("date field value out of range: \"-21457-01-08T20:48:00\"");

    @Test
    void theFirstRootWithoutARowRaisesTheDatabasesOwnError() {
        RootLoops.Run run = run();
        // the first group answered its row; the second, its own fused statement, was rejected
        run.received(FIRST, List.of(true));
        run.fused(REFUSED);
        List<Object> judged = new java.util.ArrayList<>();
        DataError raised = assertThrows(DataError.class, () -> run.judgeRejected(REFUSED, null, null, false,
                (name, row, report) -> judged.add(name)));
        assertSame(REFUSED, raised);
        // the first assert was judged on its row before the second raised the database's error
        assertEquals(List.of("assertEquals@1"), judged);
    }

    /** Not reached by any plan (a rejected group's verdicts are members, read with no row), held to all the same. */
    @Test
    void whereEveryRootHeld_theRejectionIsRaisedWhole() {
        RootLoops.Run run = run();
        run.received(FIRST, List.of(true));
        run.received(SECOND, List.of(true));
        List<Object> judged = new java.util.ArrayList<>();
        assertSame(REFUSED, assertThrows(DataError.class, () -> run.judgeRejected(REFUSED, null, null, false,
                (name, row, report) -> judged.add(name))));
        assertEquals(List.of("assertEquals@1", "assertEquals@2"), judged);
    }

    @Test
    void aSegmentJudgedWithNoRejectionRaisesNothing() {
        RootLoops.Run run = run();
        run.received(FIRST, List.of(true));
        run.received(SECOND, List.of(true));
        assertDoesNotThrow(() -> run.judge(null, null, false, (name, row, report) -> { }));
    }
}
