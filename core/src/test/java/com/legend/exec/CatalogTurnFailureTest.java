// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.ExecutionPlan;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.TestBackends;
import com.legend.testbackend.TestSessions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.sql.Connection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A failed statement inside a run's TURN on the catalog (docs/DATA_DELIVERY_DESIGN_2026_11_10.md §3 rule 3, A4): DuckDB
 * aborts the turn's whole transaction and refuses every statement after it until a rollback, so the seam rolls the turn
 * back at once and clears the session's memo -- the run's next statement runs, and its registrations establish again at
 * its next step. A database that rolls back the failed statement alone (H2) keeps the turn's work.
 */
class CatalogTurnFailureTest {

    private record Key(String name) {
    }

    private static Statements statements(Sessions.Session session, DatabaseType type) {
        return PlanRunner.opened(session, new ExecutionPlan.Target(new ExecutionPlan.Database.Platform(
                com.legend.planner.ConnectionPlans.type(type), null), List.of(), List.of(), null));
    }

    @Test
    void duckDbsAbortedTurnIsRolledBackSoTheRunsNextStatementRuns() throws Exception {
        try (Connection c = TestBackends.of(DatabaseType.DuckDB).open()) {
            Sessions.Session session = TestSessions.over(c);
            try (var run = com.legend.base.RunScope.enter()) {
                Sessions.beginTurn(session);
                try {
                    Statements st = statements(session, DatabaseType.DuckDB);
                    st.execute(com.legend.testbackend.Unplanned.sql("CREATE TEMPORARY TABLE TURN_T (A INTEGER)"));
                    st.execute(com.legend.testbackend.Unplanned.sql("INSERT INTO TURN_T VALUES (1)"));
                    session.state().register(new Key("registration"));
                    assertThrows(com.legend.error.DataError.class, () -> st.execute(com.legend.testbackend.Unplanned.sql("SELECT * FROM NO_SUCH_TABLE")));
                    // the turn rolled back: its registration is forgotten, so the run's next step makes it again
                    assertFalse(session.state().registered(new Key("registration")));
                    // and the run's next statement runs, in a fresh transaction (aborted until A4: "Current transaction
                    // is aborted (please ROLLBACK)")
                    st.execute(com.legend.testbackend.Unplanned.sql("CREATE TEMPORARY TABLE TURN_T (A INTEGER)"));
                    st.execute(com.legend.testbackend.Unplanned.sql("INSERT INTO TURN_T VALUES (2)"));
                } finally {
                    Sessions.endTurn(session);
                }
            }
        }
    }

    /**
     * A WRITE inside the turn leaves the run's registration in place (the B checkpoint's correctness audit, C1,
     * 2026-11-11): a registration is the model's rows inserted into the catalog's keyed tables, which no write of the
     * run undoes, so the registration's next step skips it -- until then a write forgot it, and the next step inserted
     * the rows again (a duplicate key on every database). A rollback forgets it, and the next step registers again.
     */
    @ParameterizedTest
    @EnumSource(value = DatabaseType.class, names = {"DuckDB", "H2", "SQLite"})
    void aWriteInsideTheTurnLeavesTheRegistrationInPlace(DatabaseType type) throws Exception {
        try (Connection c = TestBackends.of(type).open()) {
            Sessions.Session session = TestSessions.over(c);
            ExecutionPlan.Target target = new ExecutionPlan.Target(new ExecutionPlan.Database.Platform(
                    com.legend.planner.ConnectionPlans.type(type), null), List.of(), List.of(), null);
            // the catalog's keyed table, made by a load of its own before the turn; the registration's row into it
            ExecutionPlan.Load table = load(ExecutionPlan.Load.Cadence.PER_ACQUISITION, target,
                    "CREATE TABLE REG_T (A INTEGER PRIMARY KEY)");
            ExecutionPlan.Load registration = load(ExecutionPlan.Load.Cadence.PER_TRANSACTION, target,
                    "INSERT INTO REG_T VALUES (1)");
            try (var run = com.legend.base.RunScope.enter()) {
                PlanRunner.establishOn(table, session);
                Sessions.beginTurn(session);
                try {
                    Statements st = statements(session, type);
                    PlanRunner.seed(registration, session, st, com.legend.executionplan.DataStores.none(), ran -> { });
                    // a staged table's CREATE ... AS, a store write: the session writes
                    session.state().markWriting();
                    PlanRunner.seed(registration, session, st, com.legend.executionplan.DataStores.none(), ran -> { });
                    assertEquals(1, count(c), type + ": registered once");
                    // the turn's transaction rolled back, as after a statement that aborted it (JdbcStatements)
                    c.rollback();
                    session.state().rolledBack();
                    assertEquals(0, count(c), type + ": the rollback took the row back");
                    PlanRunner.seed(registration, session, st, com.legend.executionplan.DataStores.none(), ran -> { });
                    assertEquals(1, count(c), type + ": registered again after a rollback");
                } finally {
                    Sessions.endTurn(session);
                }
            }
        }
    }

    /** A load of {@code cadence} on {@code target} sending {@code statement}, its identity its content's. */
    private static ExecutionPlan.Load load(ExecutionPlan.Load.Cadence cadence, ExecutionPlan.Target target,
            String statement) {
        List<ExecutionPlan.LoadStep> steps = List.of(new ExecutionPlan.LoadStep.Statement(statement));
        return new ExecutionPlan.Load(ExecutionPlan.Load.IDENTITY_PREFIX + com.legend.cache.Hash.ofUtf8(
                ExecutionPlan.Load.canonical(cadence, steps)).hex(), cadence, steps, target, java.util.Map.of());
    }

    private static int count(Connection c) throws java.sql.SQLException {
        try (java.sql.Statement plain = c.createStatement();
             java.sql.ResultSet rs = plain.executeQuery("SELECT COUNT(*) FROM REG_T")) {
            rs.next();
            return rs.getInt(1);
        }
    }

    @Test
    void h2KeepsTheTurnsWorkAfterAFailedStatement() throws Exception {
        try (Connection c = TestBackends.of(DatabaseType.H2).open()) {
            Sessions.Session session = TestSessions.over(c);
            try (var run = com.legend.base.RunScope.enter()) {
                Sessions.beginTurn(session);
                try {
                    Statements st = statements(session, DatabaseType.H2);
                    st.execute(com.legend.testbackend.Unplanned.sql("CREATE LOCAL TEMPORARY TABLE TURN_T (A INTEGER) TRANSACTIONAL"));
                    st.execute(com.legend.testbackend.Unplanned.sql("INSERT INTO TURN_T VALUES (1)"));
                    session.state().register(new Key("registration"));
                    assertThrows(com.legend.error.DataError.class, () -> st.execute(com.legend.testbackend.Unplanned.sql("SELECT * FROM NO_SUCH_TABLE")));
                    assertTrue(session.state().registered(new Key("registration")),
                            "H2 rolls back the failed statement alone: the turn's registration stands");
                    try (java.sql.Statement plain = c.createStatement();
                         java.sql.ResultSet rs = plain.executeQuery("SELECT COUNT(*) FROM TURN_T")) {
                        rs.next();
                        assertEquals(1, rs.getInt(1));
                    }
                } finally {
                    Sessions.endTurn(session);
                }
            }
        }
    }
}
