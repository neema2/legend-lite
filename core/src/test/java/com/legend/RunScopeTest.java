// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.base.RunScope;
import com.legend.exec.Sessions;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE WALL's proof ({@link RunScope}; made total 2026-11-06): each crossing tried, each refused by name -- through
 * the real seam (a session opened by {@link Sessions}, its statements taken through its one door), not a model of it.
 * Rule 1: a run plans nothing. Rule 2: planning opens no session, starts no run, sends nothing. Rule 3: a session's
 * statements and its check are a run's alone. Rule 4: a run starts no run, with no exception (the named residue door
 * went 2026-11-06). A refusal names the callers that asked (read off a throwable's trace since 2026-11-05, which the
 * WebAssembly planner's class library has).
 */
class RunScopeTest {

    /** The platform's own in-process database (its test backend: the owner that names a database). */
    private static final DatabaseType PLATFORM = TestBackends.platform().type();

    @Test
    void planningOutsideARunIsFree() {
        assertFalse(RunScope.running());
        RunScope.planning("lower a spec");
    }

    @Test
    void rule1PlanningInsideARunIsRefusedByNameWithItsCallers() {
        try (var run = RunScope.enter()) {
            assertTrue(RunScope.running());
            RunScope.Refused refused = assertThrows(RunScope.Refused.class, () -> RunScope.planning("lower a spec"));
            String message = refused.getMessage();
            assertTrue(message.startsWith("the runtime wall: lower a spec while a plan runs (lower a spec <- "), message);
            assertTrue(message.contains("RunScopeTest."), message);
            // a planning scope cannot be entered inside a run either
            assertThrows(RunScope.Refused.class, () -> RunScope.plans("a test body"));
        }
        assertFalse(RunScope.running());
        RunScope.planning("lower a spec");
    }

    /** A run's caller called back (a sink, a session source) plans nothing: the full audit's must-fix, 2026-11-09 --
     *  a session source planned the query again inside the run, where the wall did not look. */
    @Test
    void aRunsCallerCalledBackPlansNothing() {
        try (var called = RunScope.callingBack()) {
            assertThrows(RunScope.Refused.class, () -> RunScope.planning("lower a spec"));
            assertThrows(RunScope.Refused.class, () -> RunScope.plans("a query"));
        }
        RunScope.planning("lower a spec");
    }

    /** A held appeal decided after its body's run (P1.2, 2026-11-07): it plans nothing and plans no body, but it may
     *  run the plans planned ahead for it, each a run of its own. */
    @Test
    void aJudgmentAfterARunPlansNothingButRunsItsPlans() {
        try (var judging = RunScope.judging()) {
            RunScope.Refused refused = assertThrows(RunScope.Refused.class, () -> RunScope.planning("lower a spec"));
            assertTrue(refused.getMessage().startsWith(
                    "the runtime wall: lower a spec while a judgment is decided after its run, or a run's caller is "
                            + "called back for it ("), refused.getMessage());
            assertThrows(RunScope.Refused.class, () -> RunScope.plans("a test body"));
            try (var run = RunScope.enter()) {
                assertTrue(RunScope.running());
            }
        }
        RunScope.planning("lower a spec");
    }

    @Test
    void rule2PlanningOpensNoSessionStartsNoRunAndSendsNothing() {
        try (var planning = RunScope.plans("a test body")) {
            assertTrue(RunScope.isPlanning());
            RunScope.Refused opened = assertThrows(RunScope.Refused.class,
                    () -> Sessions.openPrivate(com.legend.planner.ConnectionPlans.type(PLATFORM), null));
            assertTrue(opened.getMessage().contains("a session opened while a test body is planned"),
                    opened.getMessage());
            RunScope.Refused ran = assertThrows(RunScope.Refused.class, RunScope::enter);
            assertTrue(ran.getMessage().contains("a plan run while a test body is planned"), ran.getMessage());
            // an inner scope keeps the outer's name
            try (var inner = RunScope.plans("its asserts")) {
                assertThrows(RunScope.Refused.class, () -> RunScope.opening("a transaction bracket"));
            }
            assertTrue(RunScope.isPlanning());
        }
        assertFalse(RunScope.isPlanning());
    }

    @Test
    void rule2ASessionOpenedOnFirstUseOpensNothingWhileItIsOnlyNamed() {
        int[] opened = {0};
        try (Sessions.Session lazy = Sessions.lazy(() -> {
            opened[0]++;
            return Sessions.openPrivate(com.legend.planner.ConnectionPlans.type(PLATFORM), null);
        })) {
            try (var planning = RunScope.plans("a system-routed body")) {
                // a planner names the session (its state is its own) and opens nothing
                lazy.state();
                assertEquals(0, opened[0]);
                // reaching its statements while planning is refused before anything opens
                assertThrows(RunScope.Refused.class, lazy::statements);
                assertEquals(0, opened[0]);
            }
            try (var run = RunScope.enter()) {
                lazy.statements().execute(com.legend.testbackend.Unplanned.sql("SELECT 1"));
                assertEquals(1, opened[0]);
                lazy.statements().execute(com.legend.testbackend.Unplanned.sql("SELECT 2"));
                assertEquals(1, opened[0], "opened once");
            }
        }
    }

    @Test
    void rule3ASessionsStatementsAndItsCheckAreARunsAlone() throws Exception {
        try (Sessions.Session session = Sessions.openPrivate(com.legend.planner.ConnectionPlans.type(PLATFORM), null)) {
            RunScope.Refused sent = assertThrows(RunScope.Refused.class, session::statements);
            assertTrue(sent.getMessage().contains("a session's statements outside a plan's run"), sent.getMessage());
            assertThrows(RunScope.Refused.class, () -> Sessions.check(session, com.legend.planner.ConnectionPlans.type(PLATFORM)));
            assertThrows(RunScope.Refused.class, () -> Sessions.view(session).statements());
            try (var run = RunScope.enter()) {
                Sessions.check(session, com.legend.planner.ConnectionPlans.type(PLATFORM));
                session.statements().execute(com.legend.testbackend.Unplanned.sql("SELECT 1"));
            }
            // the owner's bracket is the owner's: free outside a run, refused while planning
            assertEquals(1, (int) session.inTransaction(() -> 1));
            try (var planning = RunScope.plans("a test body")) {
                assertThrows(RunScope.Refused.class, () -> session.inTransaction(() -> 1));
            }
        }
    }

    @Test
    void rule4ARunStartsNoRun() {
        try (var outer = RunScope.enter()) {
            RunScope.Refused nested = assertThrows(RunScope.Refused.class, RunScope::enter);
            assertTrue(nested.getMessage().contains("a run started inside a run"), nested.getMessage());
            assertTrue(RunScope.running());
            assertThrows(RunScope.Refused.class, () -> RunScope.planning("render a statement"));
        }
        assertEquals(false, RunScope.running());
    }
}
