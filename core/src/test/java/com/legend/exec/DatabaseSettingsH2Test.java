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
 * H2'S DATABASE SETTINGS ARE THE PLAN'S (the C checkpoint's SHOULD-FIX 3, the orchestrator's ruling of 2026-11-12) --
 * and only for a database the platform MAKES (the third re-audit's MUST-FIX 2, 2026-11-12: a declared file or server is
 * the user's, run as it is, never modified):
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

    /**
     * Every SETTING of the database's {@code INFORMATION_SCHEMA.SETTINGS}, in order: what a declared database keeps byte
     * for byte. Not its {@code info.*} rows (2026-11-13, main's Windows run 43451235254, the user's "fix now"): they are
     * the store's statistics -- {@code info.CHUNK_COUNT}, {@code info.CURRENT_VERSION}, {@code info.PAGE_COUNT} and the
     * rest -- which H2's background writer changes on its own whenever it flushes (WRITE_DELAY, 500 ms by default), so
     * two reads straddling a flush differ with nothing sent and no setting changed. A setting is what a statement
     * changes; those rows are what the store's housekeeping does.
     */
    private static List<String> everySetting(Connection c) throws Exception {
        List<String> out = new ArrayList<>();
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery("SELECT SETTING_NAME, SETTING_VALUE FROM"
                + " INFORMATION_SCHEMA.SETTINGS WHERE SETTING_NAME NOT LIKE 'info.%' ORDER BY SETTING_NAME")) {
            while (rs.next()) {
                out.add(rs.getString(1) + "=" + rs.getString(2));
            }
        }
        assertTrue(out.stream().anyMatch(r -> r.startsWith("DEFAULT_NULL_ORDERING=")), "the settings read: " + out);
        return out;
    }

    /** The store written out (a {@code CHECKPOINT}) before the settings are first read, so the background writer has
     *  nothing pending to flush while the test runs. */
    private static List<String> quietSettings(Connection c) throws Exception {
        try (Statement st = c.createStatement()) {
            st.execute("CHECKPOINT");
        }
        return everySetting(c);
    }

    /**
     * A DECLARED H2 database -- a file -- is the user's, and lite's query leaves it as it found it (the third re-audit's
     * MUST-FIX 2, 2026-11-12): its target declares the database's own settings, none written and none read back, and
     * every one of its settings reads the same before and after, read by a client of the user's that holds it open
     * across the run, as any other client would. Until then (the C final audit's J1) the dialect's settings were
     * written on its URL, and H2 applies a URL's settings to the whole open database -- measured on 2.5.252: the
     * holding client's null ordering became HIGH and stayed so after lite's session closed.
     */
    @Test
    void aDeclaredFileDatabaseIsNeverModified() throws Exception {
        Path dir = Files.createTempDirectory(Path.of(System.getenv("TEST_TMPDIR")), "h2file");
        String path = dir.resolve("db").toString();
        try (Connection holder = DriverManager.getConnection("jdbc:h2:file:" + path)) {
            List<String> before = quietSettings(holder);
            ExecutionPlan.Target target = declared(new ConnectionSpecification.LocalFile(path));
            assertEquals(ExecutionPlan.DatabaseSettings.THE_DATABASES_OWN, target.settings(), "the database's own");
            assertEquals("LOW", nullOrdering(target), "run as it is: H2's own null ordering, never the dialect's");
            assertEquals(before, everySetting(holder), "every setting as the user's database had it");
        }
    }

    /**
     * So is one reached through a SERVER, and a query the PRODUCT plans -- a sort over a nullable column -- answers its
     * own null order there, the engine's, on a database whose own is H2's (LOW): the product writes every null's order
     * (D's measurement, the ruling's premise). The server's database is held open by a client of the user's across the
     * run, and every setting it reads is unchanged.
     */
    @Test
    void aDeclaredServerDatabaseIsNeverModified_andAProductSortAnswersItsOwnNullOrder() throws Exception {
        Path dir = Files.createTempDirectory(Path.of(System.getenv("TEST_TMPDIR")), "h2tcp");
        org.h2.tools.Server server = org.h2.tools.Server.createTcpServer("-tcpPort", "0", "-baseDir", dir.toString(),
                "-ifNotExists").start();
        try (Connection holder = DriverManager.getConnection("jdbc:h2:tcp://localhost:" + server.getPort()
                + "/declared")) {
            try (Statement st = holder.createStatement()) {
                st.execute("CREATE TABLE T (ID INTEGER PRIMARY KEY, V INTEGER)");
                st.execute("INSERT INTO T VALUES (1, 3), (2, NULL), (3, 1), (4, NULL), (5, 2)");
            }
            List<String> before = quietSettings(holder);
            String model = """
                    ###Relational
                    Database s::DB ( Table T ( ID INTEGER PRIMARY KEY, V INTEGER ) )
                    ###Connection
                    RelationalDatabaseConnection s::Conn
                    {
                        store: s::DB;
                        type: H2;
                        specification: Static { host: 'localhost'; port: %d; name: 'declared'; };
                        auth: DefaultH2;
                    }
                    ###Runtime
                    Runtime s::RT { mappings: []; connections: [ s::DB: [ c1: s::Conn ] ]; }
                    """.replace("%d", Integer.toString(server.getPort()));
            com.legend.compiler.element.ModelContext ctx = com.legend.Compiler.compileModel(model);
            // the engine's null order (DEFAULT_NULL_ORDERING=HIGH: null is largest), which the product writes into its
            // text, so a database ordering nulls otherwise answers it all the same
            for (List<String> direction : List.of(List.of("ascending", "[1, 2, 3, null, null]"),
                    List.of("descending", "[null, null, 3, 2, 1]"))) {
                ExecutionPlan plan = com.legend.Compiler.query(ctx, "|#>{s::DB.T}#->select(~[ID, V])->sort(~V->"
                        + direction.get(0) + "())").executionPlan("s::RT");
                assertEquals(ExecutionPlan.DatabaseSettings.THE_DATABASES_OWN, plan.answerSql().target().settings());
                List<String> ours = new ArrayList<>();
                for (List<Object> row : PlanRunner.rows(plan, java.util.Map.of(), PlanSessions.shared(),
                        java.util.UUID.randomUUID()).rows()) {
                    ours.add(String.valueOf(row.get(1)));
                }
                assertEquals(direction.get(1), ours.toString(), direction.get(0) + ": the product's own null order,"
                        + " on a database whose own is LOW");
            }
            assertEquals(before, everySetting(holder), "every setting as the user's database had it");
        } finally {
            server.stop();
        }
    }

    /**
     * The check bites (2026-11-13, with the {@code info.*} rows left out): a {@code SET} another session sends to the
     * declared server database -- what a run that modified it would do -- changes the settings the holding client
     * reads, and the comparison the two tests above make fails. That a run sends only its plan's statements is
     * witnessed apart, on every road ({@code com.legend.planner.SentStatementsAreThePlansTest}); this pins that the
     * database's state is read where it would change.
     */
    @Test
    void aSetSentToTheDeclaredServerDatabaseIsSeen() throws Exception {
        Path dir = Files.createTempDirectory(Path.of(System.getenv("TEST_TMPDIR")), "h2set");
        org.h2.tools.Server server = org.h2.tools.Server.createTcpServer("-tcpPort", "0", "-baseDir", dir.toString(),
                "-ifNotExists").start();
        String url = "jdbc:h2:tcp://localhost:" + server.getPort() + "/declared";
        try (Connection holder = DriverManager.getConnection(url)) {
            List<String> before = quietSettings(holder);
            try (Connection planted = DriverManager.getConnection(url); Statement st = planted.createStatement()) {
                st.execute("SET DEFAULT_NULL_ORDERING HIGH");
            }
            List<String> after = everySetting(holder);
            assertTrue(before.contains("DEFAULT_NULL_ORDERING=LOW") && after.contains("DEFAULT_NULL_ORDERING=HIGH"),
                    before + " -> " + after);
            assertTrue(!before.equals(after), "the comparison sees the SET");
        } finally {
            server.stop();
        }
    }

    /** Exec never writes settings on a declared database's URL: a target declaring some there is a planner defect,
     *  refused by name before anything is opened (the third re-audit's MUST-FIX 2, 2026-11-12). */
    @Test
    void aDeclaredDatabaseIsNeverOpenedWithSettingsToWrite() {
        IllegalStateException refused = assertThrows(IllegalStateException.class, () -> Sessions.open(
                new Sessions.Opening.Url("jdbc:h2:mem:never_opened"), H2Connector.INSTANCE, h2Target().settings()));
        assertTrue(refused.getMessage().startsWith("a H2 database a connection declares is opened as declared and"
                + " never modified"), refused.getMessage());
    }

    /** An H2 target that declares no settings is refused by name, where exec opens its database and where a session is
     *  checked against it (the C final audit's J4, 2026-11-12: until then H2 opened in its own defaults, other
     *  semantics). A target declaring settings on a database that takes none: {@code TargetSettingsTest}. */
    @Test
    void aTargetWithoutThemIsRefusedByName() throws Exception {
        com.legend.error.NotImplementedException opened = assertThrows(com.legend.error.NotImplementedException.class,
                () -> H2Connector.INSTANCE.openPrivate(null));
        assertEquals("an H2 database is opened with the settings the plan's target declares, and it declares none:"
                + " H2's own defaults would run the plan with other semantics", opened.getMessage());
        ExecutionPlan.Target none = new ExecutionPlan.Target(h2Target().database(), h2Target().session(), List.of(),
                null, null, null);
        try (Connection c = TestBackends.of(DatabaseType.H2).open(); var run = com.legend.base.RunScope.enter()) {
            JdbcStatements st = new JdbcStatements(c, new SessionState());
            com.legend.error.NotImplementedException refused = assertThrows(
                    com.legend.error.NotImplementedException.class, () -> st.check(none));
            assertEquals("a plan's target on H2 declares the settings its database must have, and this one declares"
                    + " none: never run on other semantics", refused.getMessage());
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
