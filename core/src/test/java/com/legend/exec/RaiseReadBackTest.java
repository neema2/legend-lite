// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.ExecutionPlan;
import com.legend.executionplan.PlanJson;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.sql.dialect.SqlDialect;
import com.legend.testbackend.RecordingConnection;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE RAISE A SESSION HOLDS, READ BACK BY THE STATEMENTS THE PLAN DECLARES (C2, 2026-11-12, D4; the charter's A3(c),
 * "declared error read-back"): MariaDB's and MySQL's DIALECT declares the two statements ({@link SqlDialect#raiseReadBack}:
 * the read of {@code @legend_raise}, its reset), the plan's target carries them ({@code ExecutionPlan.Target
 * #raiseReadBack}, its JSON too), and the seam sends exactly them after a failure -- and nothing where a target declares
 * none. Until C2 the connector declared them and the seam sent them outside every plan (D65's
 * {@code DatabaseConnector.raiseRecovery}). H2 keeps session variables under the same spelling ({@code SET @name},
 * {@code SELECT @name}, its {@code SET(@name, value)} function in a select), so the run is measured here on the texts
 * MariaDB's dialect declares, verbatim; the MariaDB and MySQL lanes themselves are manual ({@code //gates:heavy}).
 */
class RaiseReadBackTest {

    private static final ExecutionPlan.RaiseReadBack MARIADB =
            new ExecutionPlan.RaiseReadBack("SELECT @legend_raise", "SET @legend_raise = NULL");

    /** A statement that keeps a text in {@code @legend_raise}, wrapped as a raise is, then fails AT RUN with H2's own
     *  error, which carries no raise -- as MariaDB's dialect raises ("Subquery returns more than 1 row"). The sentinels
     *  are made by the database (no constant H2 folds while preparing, whose error would echo the statement's text). */
    private static final String RAISING = "SELECT CASE WHEN SET(@legend_raise, CHAR(" + (int) RaisedErrors.SENTINEL
            + ") || 'the raised text' || CHAR(" + (int) RaisedErrors.SENTINEL + ")) IS NULL THEN 0 ELSE 1 / z END"
            + " FROM (VALUES (0)) AS t(z)";

    @Test
    void mariaDbAndMySqlDeclareTheReadAndTheReset_everyOtherDialectNone() {
        for (DatabaseType type : new DatabaseType[] {DatabaseType.MariaDB, DatabaseType.MySQL}) {
            assertEquals(new SqlDialect.RaiseReadBack(MARIADB.read(), MARIADB.reset()),
                    com.legend.database.Databases.dialect(type).raiseReadBack(), type.toString());
        }
        for (DatabaseType type : new DatabaseType[] {DatabaseType.H2, DatabaseType.DuckDB, DatabaseType.SQLite,
                DatabaseType.Postgres, DatabaseType.Trino, DatabaseType.ClickHouse, DatabaseType.Snowflake,
                DatabaseType.Databricks, DatabaseType.BigQuery, DatabaseType.MongoDB}) {
            assertNull(com.legend.database.Databases.dialect(type).raiseReadBack(), type.toString());
        }
    }

    /** A table of one row on a connection of {@code type}: planning alone, no database. */
    private static ExecutionPlan planned(String type) {
        String model = """
                ###Relational
                Database s::DB ( Table T ( ID INTEGER PRIMARY KEY ) )
                ###Connection
                RelationalDatabaseConnection s::Conn
                {
                    store: s::DB;
                    type: %s;
                    specification: LocalH2 { testDataSetupCSV: 'default\\nT\\nID\\n1\\n'; };
                    auth: DefaultH2;
                }
                ###Runtime
                Runtime s::RT { mappings: []; connections: [ s::DB: [ c1: s::Conn ] ]; }
                """.formatted(type);
        return com.legend.Compiler.query(com.legend.Compiler.compileModel(model), "|#>{s::DB.T}#->select(~[ID])")
                .executionPlan("s::RT");
    }

    @Test
    void aPlansTargetCarriesItsDialectsReadBack_andItsJsonKeepsIt() {
        for (String type : List.of("MariaDB", "MySQL")) {
            ExecutionPlan plan = planned(type);
            assertEquals(MARIADB, plan.answerSql().target().raiseReadBack(), type);
            String json = PlanJson.write(plan);
            assertTrue(json.contains("\"raiseReadBack\":{\"read\":\"SELECT @legend_raise\",\"reset\":\"SET @legend_raise = NULL\"}"),
                    json);
            assertEquals(MARIADB, PlanJson.read(json).answerSql().target().raiseReadBack(), "the JSON keeps it");
            // a key that could carry anything else is refused by name
            IllegalArgumentException refused = assertThrows(IllegalArgumentException.class, () -> PlanJson.read(
                    json.replace("\"reset\":\"SET @legend_raise = NULL\"",
                            "\"reset\":\"SET @legend_raise = NULL\",\"extra\":\"SELECT 1\"")));
            assertTrue(refused.getMessage().contains("has no key extra"), refused.getMessage());
        }
        ExecutionPlan duck = planned("DuckDB");
        assertNull(duck.answerSql().target().raiseReadBack(), "DuckDB's errors carry the raise");
        assertFalse(PlanJson.write(duck).contains("raiseReadBack"), "no key where a target declares none");
    }

    /** {@link #RAISING} run as a plan's answer on H2, its target declaring {@code readBack}: the failure's words, and
     *  every statement the seam sent after the plan's own. */
    private static String failure(ExecutionPlan.RaiseReadBack readBack, List<String> sentAfter) throws Exception {
        List<String> sent = new ArrayList<>();
        try (Connection c = RecordingConnection.wrap(TestBackends.of(DatabaseType.H2).open(), sent)) {
            ExecutionPlan.Target target = new ExecutionPlan.Target(new ExecutionPlan.Database.Platform(
                    com.legend.planner.ConnectionPlans.type(DatabaseType.H2), null), List.of(), List.of(), readBack, null);
            com.legend.error.DataError failed = assertThrows(com.legend.error.DataError.class, () -> PlanRunnerTest.run(
                    ExecutionPlan.reading(List.of(), PlanRunnerTest.cell(ExecutionPlan.Sql.of(RAISING, target))),
                    PlanSessions.of(Sessions.borrowed(c))));
            int at = sent.indexOf(RAISING);
            assertTrue(at >= 0, "the plan's statement was sent: " + sent + " (failed: " + failed.getMessage() + ")");
            sentAfter.addAll(sent.subList(at + 1, sent.size()));
            return failed.getMessage();
        }
    }

    @Test
    void theSeamSendsExactlyTheTargetsDeclaredStatementsAfterAFailure_andNoneWhereItDeclaresNone() throws Exception {
        List<String> after = new ArrayList<>();
        assertEquals("the raised text", failure(MARIADB, after), "the raise read back by the declared read");
        assertEquals(List.of(MARIADB.read(), MARIADB.reset()), after, "the declared read and reset, as written, alone");
        List<String> none = new ArrayList<>();
        String own = failure(null, none);
        assertTrue(own.contains("Division by zero"), "no read-back declared: the database's own error, " + own);
        assertEquals(List.of(), none, "nothing sent the plan does not declare");
    }

    @Test
    void theSeamReadsTheHeldTextOnceAndClearsIt() throws SQLException {
        try (Connection c = TestBackends.of(DatabaseType.H2).open(); var run = com.legend.base.RunScope.enter();
                java.sql.Statement st = c.createStatement()) {
            assertNull(JdbcStatements.held(c, MARIADB), "nothing held yet");
            st.execute("SET @legend_raise = 'Cannot cast a collection of size 0 to multiplicity [1]'");
            assertEquals("Cannot cast a collection of size 0 to multiplicity [1]", JdbcStatements.held(c, MARIADB));
            assertNull(JdbcStatements.held(c, MARIADB), "the reset: a later failure reads no stale text");
            assertNull(JdbcStatements.held(c, (ExecutionPlan.RaiseReadBack) null), "a target that declares none");
        }
    }

    /** The reset is sent whatever the read did, so a raise held now never attaches to a later failure; and the read-back
     *  is counted as a session's statement (the C checkpoint's code audit, 2026-11-12: until then a failed read skipped
     *  the reset, and neither was counted). */
    @Test
    void theResetIsSentWhenTheReadFails_andTheReadBackIsCountedAsTheSessions() throws SQLException {
        ExecutionPlan.RaiseReadBack unreadable = new ExecutionPlan.RaiseReadBack("SELECT * FROM NO_SUCH_TABLE",
                MARIADB.reset());
        List<String> sent = new ArrayList<>();
        try (Connection c = RecordingConnection.wrap(TestBackends.of(DatabaseType.H2).open(), sent);
                var run = com.legend.base.RunScope.enter()) {
            long before = StatementOrigin.snapshot()[StatementOrigin.SESSION.ordinal()];
            assertThrows(SQLException.class, () -> JdbcStatements.held(c, unreadable));
            assertEquals(List.of(unreadable.read(), unreadable.reset()), sent, "the reset follows the failed read");
            assertEquals(2, StatementOrigin.snapshot()[StatementOrigin.SESSION.ordinal()] - before,
                    "the read and the reset, each a session's statement");
            sent.clear();
            assertNull(JdbcStatements.held(c, MARIADB));
            assertEquals(List.of(MARIADB.read(), MARIADB.reset()), sent, "and after a read that answers");
        }
    }

    @Test
    void aReadTheSessionRefusesIsNoText_theStatementsOwnErrorStands() throws SQLException {
        Connection c = TestBackends.of(DatabaseType.H2).open();
        SessionState state = new SessionState();
        // a session no target declared a read-back for: none sent, no text
        assertNull(JdbcStatements.held(c, state, new SQLException("the statement's")));
        c.close();
        // a session that cannot be asked (closed: any statement refused) holds no text, and the refusal is kept beside
        // the statement's error, never dropped -- a failure of the read is never the statement's error (the A delta
        // audit's 6, 2026-11-11)
        state.declareRaiseReadBack(MARIADB);
        SQLException statements = new SQLException("the statement's");
        try (var run = com.legend.base.RunScope.enter()) {
            assertNull(JdbcStatements.held(c, state, statements));
        }
        assertEquals(1, statements.getSuppressed().length, "the read's failure kept beside the statement's");
    }

    /** No target is made without saying its read-back (the C checkpoint's code audit, SHOULD-FIX 3, 2026-11-12, invariant
     *  4): the constructor that defaulted it to none -- and so lost a MariaDB target's raise text silently, as
     *  Synthesize's did -- is gone, and stays gone. */
    @Test
    void aTargetIsMadeOnlyBySayingItsReadBack() {
        java.lang.reflect.Constructor<?>[] made = ExecutionPlan.Target.class.getDeclaredConstructors();
        assertEquals(1, made.length, "one constructor: the record's own");
        assertEquals(List.of(ExecutionPlan.Database.class, List.class, List.class, ExecutionPlan.RaiseReadBack.class),
                List.of(made[0].getParameterTypes()), "the read-back is said, never defaulted");
    }

    @Test
    void theSessionIsAskedOnlyWhereTheErrorsOwnTextCarriesNoRaise() {
        int[] asked = {0};
        java.util.function.Supplier<String> held = () -> {
            asked[0]++;
            return RaisedErrors.SENTINEL + "the raise's text" + RaisedErrors.SENTINEL;
        };
        SQLException carried = new SQLException(RaisedErrors.SENTINEL + "carried" + RaisedErrors.SENTINEL, "45000");
        assertEquals("carried", RaisedErrors.unwrapped(carried, held).getMessage());
        assertEquals(0, asked[0], "the error carried its raise: the session is not asked");
        SQLException own = new SQLException("Subquery returns more than 1 row", "21000");
        SQLException told = RaisedErrors.unwrapped(own, held);
        assertEquals(1, asked[0], "no raise in the text: the session is asked once");
        assertEquals("the raise's text", told.getMessage());
        assertEquals("21000", told.getSQLState());
        assertSame(own, RaisedErrors.unwrapped(own, () -> null), "nothing held: the error as it is");
    }
}
