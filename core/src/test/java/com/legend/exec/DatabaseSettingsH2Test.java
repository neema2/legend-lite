// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.ExecutionPlan;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.RecordingConnection;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * H2'S DATABASE SETTINGS ARE THE PLAN'S (the C checkpoint's SHOULD-FIX 3, the orchestrator's ruling of 2026-11-12):
 * {@code MODE=LEGACY} and {@code DEFAULT_NULL_ORDERING=HIGH} are the database's, and their {@code SET} commits an open
 * transaction (measured on 2.5.252), so they are no session statement: the target carries them as values, exec writes
 * them on the URL of an in-memory H2 it opens, and a session it did not open is read back with the target's own
 * statement and refused by name where a value differs -- never a plan run with other semantics. The non-keywords,
 * a session's setting that commits nothing, are the dialect's session statement.
 */
class DatabaseSettingsH2Test {

    private static ExecutionPlan.Target h2Target() {
        return com.legend.planner.Targets.of(new ExecutionPlan.Database.Platform(
                com.legend.planner.ConnectionPlans.type(DatabaseType.H2), null), new com.legend.sql.dialect.H2());
    }

    private static List<String> reported(Connection c) throws Exception {
        List<String> out = new ArrayList<>();
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery("SELECT SETTING_NAME, SETTING_VALUE FROM"
                + " INFORMATION_SCHEMA.SETTINGS WHERE SETTING_NAME IN ('MODE', 'DEFAULT_NULL_ORDERING')"
                + " ORDER BY SETTING_NAME")) {
            while (rs.next()) {
                out.add(rs.getString(1) + "=" + rs.getString(2));
            }
        }
        return out;
    }

    @Test
    void theDialectWritesThemIntoThePlansTarget() {
        ExecutionPlan.Target target = h2Target();
        assertEquals(List.of("SET NON_KEYWORDS " + com.legend.sql.dialect.H2.NON_KEYWORDS), target.session());
        assertEquals(List.of(new ExecutionPlan.Setting("MODE", "LEGACY"),
                new ExecutionPlan.Setting("DEFAULT_NULL_ORDERING", "HIGH")), target.settings().required());
    }

    /** An in-memory H2 the platform opens is MADE with the target's settings, and is not read back for them. */
    @Test
    void aSessionThePlatformOpensIsMadeWithThem_andNotReadBack() throws Exception {
        ExecutionPlan.Target target = h2Target();
        try (var run = com.legend.base.RunScope.enter();
                Sessions.Session session = Sessions.openPrivate(target.database().type(), target.settings())) {
            assertTrue(session.state().holdsSettings(target.settings()));
            long before = StatementOrigin.snapshot()[StatementOrigin.SESSION.ordinal()];
            session.statements().check(target);
            assertEquals(before, StatementOrigin.snapshot()[StatementOrigin.SESSION.ordinal()], "no read-back");
        }
        try (Connection bare = H2Connector.INSTANCE.openPrivate(null);
                Connection made = H2Connector.INSTANCE.openPrivate(target.settings())) {
            assertEquals(List.of("DEFAULT_NULL_ORDERING=LOW", "MODE=REGULAR"), reported(bare), "H2's own defaults");
            assertEquals(List.of("DEFAULT_NULL_ORDERING=HIGH", "MODE=LEGACY"), reported(made), "the target's");
        }
    }

    /** A session the platform did not open (a caller's) is read back ONCE, with the target's own statement, counted as
     *  the session's; a session that has the settings runs on. */
    @Test
    void aCallersSessionIsReadBackOnce_withTheTargetsOwnStatement() throws Exception {
        ExecutionPlan.Target target = h2Target();
        List<String> sent = new ArrayList<>();
        try (Connection c = RecordingConnection.wrap(TestBackends.of(DatabaseType.H2).open(), sent);
                var run = com.legend.base.RunScope.enter()) {
            JdbcStatements st = new JdbcStatements(c, new SessionState());
            long before = StatementOrigin.snapshot()[StatementOrigin.SESSION.ordinal()];
            st.check(target);
            st.check(target);
            assertEquals(List.of(target.settings().readBack()), sent, "the target's read-back, once");
            assertEquals(1, StatementOrigin.snapshot()[StatementOrigin.SESSION.ordinal()] - before, "the session's");
        }
    }

    /** A caller's session whose database has other settings is refused by name: the plan never runs on it (its null
     *  ordering would order the answer otherwise). */
    @Test
    void aCallersSessionWithOtherSettingsIsRefusedByName() throws Exception {
        ExecutionPlan.Target target = h2Target();
        try (Connection bare = DriverManager.getConnection("jdbc:h2:mem:settings_bare", "sa", "");
                var run = com.legend.base.RunScope.enter()) {
            JdbcStatements st = new JdbcStatements(bare, new SessionState());
            com.legend.error.NotImplementedException refused = assertThrows(
                    com.legend.error.NotImplementedException.class, () -> st.check(target));
            assertEquals("the session's database does not have the settings the plan's target declares (MODE is"
                    + " REGULAR, not LEGACY; DEFAULT_NULL_ORDERING is LOW, not HIGH): a session this platform did not"
                    + " open is read back, and never runs a plan with other settings", refused.getMessage());
        }
    }

    /** Why they are no session statement: each {@code SET} commits the open transaction (a caller's savepoints, the
     *  catalog's staged read would be committed under them); the non-keywords commit nothing. */
    @Test
    void theirSetCommitsAnOpenTransaction_theNonKeywordsDoNot() throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:h2:mem:settings_commit", "sa", "")) {
            try (Statement st = c.createStatement()) {
                st.execute("CREATE TABLE t(x INT)");
            }
            c.setAutoCommit(false);
            for (String set : List.of("SET NON_KEYWORDS VALUE", "SET MODE LEGACY", "SET DEFAULT_NULL_ORDERING HIGH")) {
                try (Statement st = c.createStatement()) {
                    st.execute("DELETE FROM t");
                    c.commit();
                    st.execute("INSERT INTO t VALUES (1)");
                    st.execute(set);
                }
                c.rollback();
                try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM t")) {
                    rs.next();
                    assertEquals(set.startsWith("SET NON_KEYWORDS") ? 0 : 1, rs.getInt(1), set);
                }
            }
        }
    }
}
