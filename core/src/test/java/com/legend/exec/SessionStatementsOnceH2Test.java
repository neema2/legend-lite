// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.ExecutionPlan;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * A target's session statements run ONCE per connection (D66, 2026-10-30; the session's memo, {@link SessionState}):
 * until then the plan runner re-sent them before every statement, a round trip per statement on a server. A
 * rollback clears the memo -- Postgres's {@code SET} is transactional, so a list run inside a transaction that rolled
 * back is undone with it -- and the next statement runs them again. Measured on H2, which keeps a session variable
 * under the same {@code SET @name} spelling as a server does. Inside a transaction the seam opened, the list runs once
 * too (the C final audit's J6, 2026-11-12). The count is the SESSION mark's: the target's session statements, and the
 * settings read-back a session the platform did not open is sent once ({@link #READ_BACK}; the C checkpoint's SHOULD-FIX
 * 3, 2026-11-12), every connection here being the test's own.
 */
class SessionStatementsOnceH2Test {

    private static final List<String> SESSION = List.of("SET @legend_session = 1");

    /** The H2 target's settings read-back, sent once on a session the platform did not open, counted as the session's:
     *  every connection here is the test's own. */
    private static final int READ_BACK = 1;

    private static ExecutionPlan plan(ExecutionPlan.Target target, String statement) {
        return ExecutionPlan.reading(List.of(), PlanRunnerTest.cell(ExecutionPlan.Sql.of(statement, target)));
    }

    private static long sessionStatements() {
        return StatementOrigin.snapshot()[StatementOrigin.SESSION.ordinal()];
    }

    @Test
    void aTargetsSessionStatementsRunOncePerConnection_andAgainAfterARollback() throws Exception {
        try (Connection c = TestBackends.of(DatabaseType.H2).open()) {
            PlanSessions.Source on = PlanSessions.of(com.legend.testbackend.TestSessions.over(c));
            ExecutionPlan.Target target = new ExecutionPlan.Target(new ExecutionPlan.Database.Platform(com.legend.planner.ConnectionPlans.type(DatabaseType.H2), null),
                    SESSION, List.of(), null, com.legend.testbackend.TestBackends.settings(DatabaseType.H2));
            ExecutionPlan read = plan(target, "SELECT CAST(@legend_session AS VARCHAR)");
            long before = sessionStatements();
            assertEquals("1", PlanRunnerTest.run(read, on), "the session statement ran before the first statement");
            assertEquals("1", PlanRunnerTest.run(read, on));
            assertEquals("1", PlanRunnerTest.run(read, on));
            assertEquals(READ_BACK + 1, sessionStatements() - before, "three statements, the session statements once");
            // another target's list on the same connection: its own, once
            ExecutionPlan.Target other = new ExecutionPlan.Target(new ExecutionPlan.Database.Platform(com.legend.planner.ConnectionPlans.type(DatabaseType.H2), null),
                    List.of("SET @legend_other = 2"), List.of(), null, com.legend.testbackend.TestBackends.settings(DatabaseType.H2));
            assertEquals("2", PlanRunnerTest.run(plan(other, "SELECT CAST(@legend_other AS VARCHAR)"), on));
            assertEquals("2", PlanRunnerTest.run(plan(other, "SELECT CAST(@legend_other AS VARCHAR)"), on));
            assertEquals(READ_BACK + 2, sessionStatements() - before, "another list, once more");
            // a rollback on the session (the seam's own bracket, as a failed effect segment rolls back): the memo
            // forgets, and the next statement for each target runs its list again
            IllegalStateException failed = assertThrows(IllegalStateException.class,
                    () -> com.legend.testbackend.TestSessions.over(c).inTransaction(() -> {
                        throw new IllegalStateException("the work failed");
                    }));
            assertEquals("the work failed", failed.getMessage());
            assertEquals("1", PlanRunnerTest.run(read, on));
            assertEquals(READ_BACK + 3, sessionStatements() - before, "after the rollback: the session statements again, once");
            assertEquals("1", PlanRunnerTest.run(read, on));
            assertEquals(READ_BACK + 3, sessionStatements() - before);
        }
    }

    @Test
    void insideATransactionTheCallerHolds_theListRunsBeforeEveryStatement_andIsNotRemembered() throws Exception {
        try (Connection c = TestBackends.of(DatabaseType.H2).open()) {
            PlanSessions.Source on = PlanSessions.of(com.legend.testbackend.TestSessions.over(c));
            ExecutionPlan.Target target = new ExecutionPlan.Target(new ExecutionPlan.Database.Platform(com.legend.planner.ConnectionPlans.type(DatabaseType.H2), null),
                    SESSION, List.of(), null, com.legend.testbackend.TestBackends.settings(DatabaseType.H2));
            ExecutionPlan read = plan(target, "SELECT CAST(@legend_session AS VARCHAR)");
            long before = sessionStatements();
            // the caller holds a transaction: what a session statement sets there goes with the caller's rollback
            // (Postgres), which the memo cannot see, so the list runs before every statement, as it always did
            c.setAutoCommit(false);
            assertEquals("1", PlanRunnerTest.run(read, on));
            assertEquals("1", PlanRunnerTest.run(read, on));
            assertEquals(READ_BACK + 2, sessionStatements() - before, "inside the caller's transaction: every statement");
            c.rollback();
            c.setAutoCommit(true);
            // the caller's transaction over: the memo holds nothing from it, so the list runs once more, then not
            assertEquals("1", PlanRunnerTest.run(read, on));
            assertEquals("1", PlanRunnerTest.run(read, on));
            assertEquals(READ_BACK + 3, sessionStatements() - before, "after it: once per connection again");
        }
    }

    /** Inside a transaction the SEAM opened (a program's own, a write's), whose rollback clears the memo, the list runs
     *  once, as outside one (the C final audit's J6, 2026-11-12: until then autocommit off read as the caller's, and
     *  the list -- H2's non-keywords on every plan -- was sent before every statement of the transaction). */
    @Test
    void insideTheSeamsOwnTransaction_theListRunsOnce() throws Exception {
        try (Connection c = TestBackends.of(DatabaseType.H2).open()) {
            Sessions.Session over = com.legend.testbackend.TestSessions.over(c);
            PlanSessions.Source on = PlanSessions.of(over);
            ExecutionPlan.Target target = new ExecutionPlan.Target(new ExecutionPlan.Database.Platform(
                    com.legend.planner.ConnectionPlans.type(DatabaseType.H2), null), SESSION, List.of(), null,
                    com.legend.testbackend.TestBackends.settings(DatabaseType.H2));
            ExecutionPlan read = plan(target, "SELECT CAST(@legend_session AS VARCHAR)");
            long before = sessionStatements();
            List<String> answers = over.inTransaction(() -> {
                try {
                    return List.of(PlanRunnerTest.run(read, on), PlanRunnerTest.run(read, on),
                            PlanRunnerTest.run(read, on));
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            });
            assertEquals(List.of("1", "1", "1"), answers);
            assertEquals(READ_BACK + 1, sessionStatements() - before, "inside the seam's transaction: once");
            // committed: still remembered after it
            assertEquals("1", PlanRunnerTest.run(read, on));
            assertEquals(READ_BACK + 1, sessionStatements() - before);
        }
    }

    @Test
    void aTargetWithNoSessionStatementsSendsNone() throws Exception {
        try (Connection c = TestBackends.of(DatabaseType.H2).open()) {
            ExecutionPlan.Target target = new ExecutionPlan.Target(new ExecutionPlan.Database.Platform(com.legend.planner.ConnectionPlans.type(DatabaseType.H2), null),
                    List.of(), List.of(), null, com.legend.testbackend.TestBackends.settings(DatabaseType.H2));
            long before = sessionStatements();
            assertEquals("1", PlanRunnerTest.run(plan(target, "SELECT '1'"), Map.of(), PlanSessions.of(com.legend.testbackend.TestSessions.over(c))));
            assertEquals(READ_BACK, sessionStatements() - before, "the settings read-back alone");
        }
    }
}
