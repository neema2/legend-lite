// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.ExecutionPlan;
import com.legend.model.AuthenticationSpec;
import com.legend.model.ConnectionDefinition;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.model.ConnectionSpecification;
import com.legend.testbackend.RecordingConnection;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.nio.file.Files;
import java.nio.file.Path;
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
        try (Connection bare = DriverManager.getConnection("jdbc:h2:mem:settings_defaults", "sa", "");
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
            assertEquals("the session's database does not have the settings the plan's target declares (it has"
                    + " DEFAULT_NULL_ORDERING=LOW, MODE=REGULAR; the target declares MODE=LEGACY,"
                    + " DEFAULT_NULL_ORDERING=HIGH): a session this platform did not open is read back, and never runs"
                    + " a plan with other settings", refused.getMessage());
        }
    }

    /** An H2 target on a connection {@code spec} declares (a file, a server). */
    private static ExecutionPlan.Target declared(ConnectionSpecification spec) {
        return com.legend.planner.Targets.of(new ExecutionPlan.Database.Declared(com.legend.planner.ConnectionPlans.of(
                new ConnectionDefinition("s::H2Url", "s::DB", DatabaseType.H2, spec, new AuthenticationSpec.TestAuth())),
                null), new com.legend.sql.dialect.H2());
    }

    /** The null ordering the target's database has, read by a plan run on it. */
    private static String nullOrdering(ExecutionPlan.Target target) throws Exception {
        return PlanRunnerTest.run(PlanRunnerTest.plan("SELECT SETTING_VALUE FROM INFORMATION_SCHEMA.SETTINGS"
                + " WHERE SETTING_NAME = 'DEFAULT_NULL_ORDERING'", target), PlanSessions.shared());
    }

    /** An H2 database reached by a FILE is opened with the target's settings on its URL, as an in-memory one is, and a
     *  query runs on it (the C final audit's J1, 2026-11-12: until then its URL carried none, so its session was read
     *  back as H2's defaults and every plan on it refused). */
    @Test
    void aFileDatabaseIsOpenedWithThem_andAQueryRunsOnIt() throws Exception {
        Path dir = Files.createTempDirectory(Path.of(System.getenv("TEST_TMPDIR")), "h2file");
        assertEquals("HIGH", nullOrdering(declared(new ConnectionSpecification.LocalFile(dir.resolve("db").toString()))));
    }

    /** So is one reached through a SERVER (an in-process TCP server here). */
    @Test
    void aServersDatabaseIsOpenedWithThem_andAQueryRunsOnIt() throws Exception {
        Path dir = Files.createTempDirectory(Path.of(System.getenv("TEST_TMPDIR")), "h2tcp");
        org.h2.tools.Server server = org.h2.tools.Server.createTcpServer("-tcpPort", "0", "-baseDir", dir.toString(),
                "-ifNotExists").start();
        try {
            assertEquals("HIGH", nullOrdering(declared(new ConnectionSpecification.StaticDatasource("localhost",
                    server.getPort(), "j1"))));
        } finally {
            server.stop();
        }
    }

    /** An H2 target that declares no settings is refused by name, where exec opens its database and where a session is
     *  checked against it; so is a target declaring settings on a database that takes none (the C final audit's J4,
     *  2026-11-12: until then H2 opened in its own defaults, other semantics). */
    @Test
    void aTargetWithoutThemIsRefusedByName() throws Exception {
        com.legend.error.NotImplementedException opened = assertThrows(com.legend.error.NotImplementedException.class,
                () -> H2Connector.INSTANCE.openPrivate(null));
        assertEquals("an H2 database is opened with the settings the plan's target declares, and it declares none:"
                + " H2's own defaults would run the plan with other semantics", opened.getMessage());
        ExecutionPlan.Target none = new ExecutionPlan.Target(h2Target().database(), h2Target().session(), List.of(),
                null, null);
        try (Connection c = TestBackends.of(DatabaseType.H2).open(); var run = com.legend.base.RunScope.enter()) {
            JdbcStatements st = new JdbcStatements(c, new SessionState());
            com.legend.error.NotImplementedException refused = assertThrows(
                    com.legend.error.NotImplementedException.class, () -> st.check(none));
            assertEquals("a plan's target on H2 declares the settings its database must have, and this one declares"
                    + " none: never run on other semantics", refused.getMessage());
        }
        ExecutionPlan.Target duck = com.legend.planner.Targets.of(new ExecutionPlan.Database.Platform(
                com.legend.planner.ConnectionPlans.type(DatabaseType.DuckDB), null), new com.legend.sql.dialect.DuckDb());
        ExecutionPlan.Target duckWithSettings = new ExecutionPlan.Target(duck.database(), duck.session(), List.of(),
                null, h2Target().settings());
        try (Connection c = TestBackends.of(DatabaseType.DuckDB).open(); var run = com.legend.base.RunScope.enter()) {
            JdbcStatements st = new JdbcStatements(c, new SessionState());
            com.legend.error.NotImplementedException refused = assertThrows(
                    com.legend.error.NotImplementedException.class, () -> st.check(duckWithSettings));
            assertEquals("a plan's target on DuckDB declares no settings, and this one declares"
                    + " [Setting[name=MODE, value=LEGACY], Setting[name=DEFAULT_NULL_ORDERING, value=HIGH]]: never run"
                    + " on other semantics", refused.getMessage());
        }
    }

    /** A setting the platform does not write is refused by name where exec would write it on a URL: only the dialect's
     *  two (J4: until then any plain word). */
    @Test
    void aSettingOtherThanTheDialectsIsRefusedByName() {
        IllegalArgumentException refused = assertThrows(IllegalArgumentException.class, () -> H2Connector.url(
                new ExecutionPlan.DatabaseSettings(List.of(new ExecutionPlan.Setting("IGNORECASE", "TRUE")),
                        "SELECT 1")));
        assertEquals("an H2 database setting a plan's target declares is one of [DEFAULT_NULL_ORDERING, MODE]:"
                + " Setting[name=IGNORECASE, value=TRUE]", refused.getMessage());
    }
}
