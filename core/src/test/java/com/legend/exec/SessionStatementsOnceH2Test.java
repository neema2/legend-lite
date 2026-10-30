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
 * under the same {@code SET @name} spelling as a server does.
 */
class SessionStatementsOnceH2Test {

    private static final List<String> SESSION = List.of("SET @legend_session = 1");

    private static ExecutionPlan plan(ExecutionPlan.Target target, String statement) {
        return new ExecutionPlan(List.of(), PlanRunnerTest.cell(ExecutionPlan.Sql.of(statement, target)));
    }

    private static long sessionStatements() {
        return StatementOrigin.snapshot()[StatementOrigin.SESSION.ordinal()];
    }

    @Test
    void aTargetsSessionStatementsRunOncePerConnection_andAgainAfterARollback() throws Exception {
        try (Connection c = TestBackends.of(DatabaseType.H2).open()) {
            PlanSessions.Source on = PlanSessions.of(com.legend.testbackend.TestSessions.over(c));
            ExecutionPlan.Target target = new ExecutionPlan.Target(new ExecutionPlan.Database.Platform(DatabaseType.H2),
                    new ExecutionPlan.Servers.Every(), SESSION, List.of());
            ExecutionPlan read = plan(target, "SELECT CAST(@legend_session AS VARCHAR)");
            long before = sessionStatements();
            assertEquals("1", PlanRunnerTest.run(read, on), "the session statement ran before the first statement");
            assertEquals("1", PlanRunnerTest.run(read, on));
            assertEquals("1", PlanRunnerTest.run(read, on));
            assertEquals(1, sessionStatements() - before, "three statements, the session statements once");
            // another target's list on the same connection: its own, once
            ExecutionPlan.Target other = new ExecutionPlan.Target(new ExecutionPlan.Database.Platform(DatabaseType.H2),
                    new ExecutionPlan.Servers.Every(), List.of("SET @legend_other = 2"), List.of());
            assertEquals("2", PlanRunnerTest.run(plan(other, "SELECT CAST(@legend_other AS VARCHAR)"), on));
            assertEquals("2", PlanRunnerTest.run(plan(other, "SELECT CAST(@legend_other AS VARCHAR)"), on));
            assertEquals(2, sessionStatements() - before, "another list, once more");
            // a rollback on the session (the seam's own bracket, as a failed effect segment rolls back): the memo
            // forgets, and the next statement for each target runs its list again
            IllegalStateException failed = assertThrows(IllegalStateException.class,
                    () -> com.legend.testbackend.TestSessions.over(c).inTransaction(() -> {
                        throw new IllegalStateException("the work failed");
                    }));
            assertEquals("the work failed", failed.getMessage());
            assertEquals("1", PlanRunnerTest.run(read, on));
            assertEquals(3, sessionStatements() - before, "after the rollback: the session statements again, once");
            assertEquals("1", PlanRunnerTest.run(read, on));
            assertEquals(3, sessionStatements() - before);
        }
    }

    @Test
    void insideATransactionTheCallerHolds_theListRunsBeforeEveryStatement_andIsNotRemembered() throws Exception {
        try (Connection c = TestBackends.of(DatabaseType.H2).open()) {
            PlanSessions.Source on = PlanSessions.of(com.legend.testbackend.TestSessions.over(c));
            ExecutionPlan.Target target = new ExecutionPlan.Target(new ExecutionPlan.Database.Platform(DatabaseType.H2),
                    new ExecutionPlan.Servers.Every(), SESSION, List.of());
            ExecutionPlan read = plan(target, "SELECT CAST(@legend_session AS VARCHAR)");
            long before = sessionStatements();
            // the caller holds a transaction: what a session statement sets there goes with the caller's rollback
            // (Postgres), which the memo cannot see, so the list runs before every statement, as it always did
            c.setAutoCommit(false);
            assertEquals("1", PlanRunnerTest.run(read, on));
            assertEquals("1", PlanRunnerTest.run(read, on));
            assertEquals(2, sessionStatements() - before, "inside the caller's transaction: every statement");
            c.rollback();
            c.setAutoCommit(true);
            // the caller's transaction over: the memo holds nothing from it, so the list runs once more, then not
            assertEquals("1", PlanRunnerTest.run(read, on));
            assertEquals("1", PlanRunnerTest.run(read, on));
            assertEquals(3, sessionStatements() - before, "after it: once per connection again");
        }
    }

    @Test
    void aTargetWithNoSessionStatementsSendsNone() throws Exception {
        try (Connection c = TestBackends.of(DatabaseType.H2).open()) {
            ExecutionPlan.Target target = new ExecutionPlan.Target(new ExecutionPlan.Database.Platform(DatabaseType.H2),
                    new ExecutionPlan.Servers.Every(), List.of(), List.of());
            long before = sessionStatements();
            assertEquals("1", PlanRunnerTest.run(plan(target, "SELECT '1'"), Map.of(), PlanSessions.of(com.legend.testbackend.TestSessions.over(c))));
            assertEquals(0, sessionStatements() - before);
        }
    }
}
