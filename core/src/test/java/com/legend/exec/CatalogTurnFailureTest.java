// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.ExecutionPlan;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.TestBackends;
import com.legend.testbackend.TestSessions;
import org.junit.jupiter.api.Test;

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
                com.legend.planner.ConnectionPlans.type(type), null), List.of(), List.of()));
    }

    @Test
    void duckDbsAbortedTurnIsRolledBackSoTheRunsNextStatementRuns() throws Exception {
        try (Connection c = TestBackends.of(DatabaseType.DuckDB).open()) {
            Sessions.Session session = TestSessions.over(c);
            try (var run = com.legend.base.RunScope.enter()) {
                Sessions.beginTurn(session);
                try {
                    Statements st = statements(session, DatabaseType.DuckDB);
                    st.execute("CREATE TEMPORARY TABLE TURN_T (A INTEGER)");
                    st.execute("INSERT INTO TURN_T VALUES (1)");
                    session.state().establish(new Key("registration"));
                    assertThrows(com.legend.error.DataError.class, () -> st.execute("SELECT * FROM NO_SUCH_TABLE"));
                    // the turn rolled back: its registration is forgotten, so the run's next step makes it again
                    assertFalse(session.state().established(new Key("registration")));
                    // and the run's next statement runs, in a fresh transaction (aborted until A4: "Current transaction
                    // is aborted (please ROLLBACK)")
                    st.execute("CREATE TEMPORARY TABLE TURN_T (A INTEGER)");
                    st.execute("INSERT INTO TURN_T VALUES (2)");
                } finally {
                    Sessions.endTurn(session);
                }
            }
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
                    st.execute("CREATE LOCAL TEMPORARY TABLE TURN_T (A INTEGER) TRANSACTIONAL");
                    st.execute("INSERT INTO TURN_T VALUES (1)");
                    session.state().establish(new Key("registration"));
                    assertThrows(com.legend.error.DataError.class, () -> st.execute("SELECT * FROM NO_SUCH_TABLE"));
                    assertTrue(session.state().established(new Key("registration")),
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
