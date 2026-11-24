// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

import com.legend.base.Nullable;
import com.legend.testexecutionplan.AssertListener;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Isolated BOTH's conclusion, each outcome pinned (the orchestrator's ruling on IsolatedRun's broad catch, 2026-11-17):
 * a run's recorded failure is rethrown or JudgesDisagree is raised; nothing is swallowed.
 */
class IsolatedRunTest {

    private static IsolatedRun.Recording run(@Nullable RuntimeException failure, Object... verdicts) {
        IsolatedRun.Recording r = new IsolatedRun.Recording();
        for (int i = 0; i < verdicts.length; i += 2) {
            r.verdict("t::a" + i, (Boolean) verdicts[i], (String) verdicts[i + 1]);
        }
        r.failure = failure;
        return r;
    }

    private static List<String> heard(IsolatedRun.Recording host, IsolatedRun.Recording database) {
        List<String> heard = new ArrayList<>();
        AssertListener listener = new AssertListener() {
            @Override
            public void verdict(String assertName, boolean pass, @Nullable String detail) {
                heard.add(pass + " " + detail);
            }
        };
        try {
            IsolatedRun.conclude(host, database, listener);
        } catch (RuntimeException e) {
            heard.add("threw " + e.getClass().getSimpleName());
        }
        return heard;
    }

    @Test
    void agreeingPassingRunsPassAndTheCallerHearsTheHostsVerdicts() {
        assertDoesNotThrow(() -> IsolatedRun.conclude(run(null, true, null), run(null, true, null), null));
        assertEquals(List.of("true null"), heard(run(null, true, null), run(null, true, null)));
    }

    @Test
    void agreeingFailedRunsRethrowTheHostsOwnFailure() {
        RuntimeException hostFailure = new IllegalStateException("expected: 1, actual: 2");
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> IsolatedRun.conclude(
                run(hostFailure, false, "expected: 1, actual: 2"),
                run(new IllegalStateException("expected: 1, actual: 2"), false, "expected: 1, actual: 2"), null));
        assertSame(hostFailure, thrown);
    }

    @Test
    void aDifferentMessageRaisesJudgesDisagreeQuotingBoth() {
        IsolatedRun.JudgesDisagree e = assertThrows(IsolatedRun.JudgesDisagree.class, () -> IsolatedRun.conclude(
                run(null, false, "expected: 1, actual: 2"), run(null, false, "expected 1, got 3"), null));
        assertTrue(e.getMessage().contains("\"expected: 1, actual: 2\"") && e.getMessage().contains("\"expected 1, got 3\""),
                e.getMessage());
    }

    @Test
    void aDifferentVerdictOrCountRaisesJudgesDisagree() {
        assertThrows(IsolatedRun.JudgesDisagree.class,
                () -> IsolatedRun.conclude(run(null, true, null), run(null, false, "x"), null));
        assertThrows(IsolatedRun.JudgesDisagree.class,
                () -> IsolatedRun.conclude(run(null, true, null), run(null), null));
    }

    @Test
    void aDifferentEndingRaisesJudgesDisagreeEvenWithEqualVerdicts() {
        IsolatedRun.JudgesDisagree e = assertThrows(IsolatedRun.JudgesDisagree.class, () -> IsolatedRun.conclude(
                run(null, true, null), run(new IllegalStateException("boom"), true, null), null));
        assertTrue(e.getMessage().contains("ended differently") && e.getMessage().contains("boom"), e.getMessage());
    }

    @Test
    void theNamesAreNotCompared() {
        IsolatedRun.Recording host = new IsolatedRun.Recording();
        host.verdict("t::helper", true, null);
        IsolatedRun.Recording database = new IsolatedRun.Recording();
        database.verdict("meta::pure::functions::asserts::assert", true, null);
        assertDoesNotThrow(() -> IsolatedRun.conclude(host, database, null));
    }

    // (underTheCompatibilityRulesTheVerdictsAloneAreCompared and underTheCompatibilityRulesAnAssertTheDatabaseDeclined-
    // IsNotCompared: deleted with the comparison they pinned, A4 of the compatibility program, stage A ratified by the
    // user 2026-11-19 -- the host judge is the only judge in compatibility, so no compatibility body runs both judges
    // isolated, and an isolated run always compares the two judges' messages, both writing lite's sentences)
}
