// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.database.Databases;
import com.legend.executionplan.DataStores;
import com.legend.executionplan.ExecutionPlan;
import com.legend.executionplan.PlanJson;
import com.legend.testbackend.TestBackends;
import com.legend.testbackend.TestSessions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A PROGRAM run by the program runner (the planner-driver seam program's S1b, 2026-10-30; the plan-as-data design's
 * step 3, 2026-11-08: exec is the one walker and calls back for a step it does not know): the product's steps in order
 * on one session -- an effect's script, a seed step, a keyed step's snapshot -- the program's answer after them, and a
 * CALLBACK at its place, reading only the statements it declared, through the one open program. The verdict groups a
 * test body's callbacks run are the driver's ({@code driver/TestRunTest}). These programs are built by hand on the
 * platform's database.
 */
final class ProgramRunnerTest {

    private static Connection conn;
    private static ExecutionPlan.Target target;

    @BeforeAll
    static void setUp() throws SQLException {
        conn = TestBackends.platform().open();
        target = new ExecutionPlan.Target(new ExecutionPlan.Database.Platform(com.legend.planner.ConnectionPlans.type(Databases.PLATFORM), null),
                List.of(), List.of());
    }

    @AfterAll
    static void tearDown() throws SQLException {
        conn.close();
    }

    private static PlanSessions.Source sessions() {
        return PlanSessions.acquired(TestSessions.over(conn), target);
    }

    /** The test-is-the-product plan's M10a: a target naming the graph's SYSTEM database opens the session its holder
     *  hands the source for it (a non-closing view), every other target the caller's own; the shared source and a
     *  writing open refuse a System target by name (the runner knows no database). */
    @Test
    void aSystemTargetOpensTheSystemOpenersSession() throws Exception {
        try (Connection other = TestBackends.platform().open()) {
            Sessions.Session body = TestSessions.over(conn);
            Sessions.Session system = TestSessions.over(other);
            ExecutionPlan.Target systemTarget = new ExecutionPlan.Target(
                    new ExecutionPlan.Database.System(com.legend.planner.ConnectionPlans.type(Databases.PLATFORM), "a-graph"),
                    List.of(), List.of());
            // the run's leases of the system databases (2026-11-09: the leases themselves, where a function was handed)
            try (SystemDatabase.Leases leases = SystemDatabase.leases(java.util.Map.of())) {
                PlanSessions.Source source = PlanSessions.of(body, null, leases);
                assertEquals(Sessions.identity(leases.sessionFor(
                                (ExecutionPlan.Database.System) systemTarget.database())),
                        Sessions.identity(source.open(systemTarget)), "the System target opens the leased session");
                assertEquals(Sessions.identity(body), Sessions.identity(source.open(target)),
                        "any other target the caller's own");
            }
            assertEquals(Sessions.identity(body), Sessions.identity(PlanSessions.of(body, null, null).open(systemTarget)),
                    "no opener: the caller's own for every target");
            var refused = org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                    () -> PlanSessions.shared().open(systemTarget));
            assertTrue(refused.getMessage().contains("system database"), refused.getMessage());
            var refusedWrite = org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                    () -> PlanSessions.shared().openToChange(systemTarget));
            assertTrue(refusedWrite.getMessage().contains("read-only"), refusedWrite.getMessage());
        }
    }

    private static ExecutionPlan.Column col(String name, String type, String slot) {
        return new ExecutionPlan.Column(name, type, new ExecutionPlan.Multiplicity(1, 1), slot, null, null);
    }

    private static ExecutionPlan.TdsResult relation(String sql, List<ExecutionPlan.Column> columns) {
        return new ExecutionPlan.TdsResult(ExecutionPlan.TdsResult.Shape.RELATION, columns,
                ExecutionPlan.Sql.of(sql, target));
    }

    /** What the program's sink received, in order. */
    private static class Recording implements ProgramRunner.ProgramSink {
        final List<String> events = new ArrayList<>();
        List<ExecutionPlan.Column> answerColumns;
        final List<List<Object>> answerRows = new ArrayList<>();

        @Override
        public void columns(List<ExecutionPlan.Column> columns) {
            events.add("columns");
            answerColumns = columns;
        }

        @Override
        public void batch(com.legend.result.Batch batch) {
            events.add("batch");
            for (int i = 0; i < batch.size(); i++) {
                answerRows.add(batch.row(i));
            }
        }

        @Override
        public void end() {
            events.add("end");
        }
    }

    /** {@code steps}, each a product node, as a program: the plan reads back from its JSON as written. */
    private static ProgramRunner.Program program(ExecutionPlan.Node... steps) {
        for (ExecutionPlan.Node n : steps) {
            assertEquals(n, PlanJson.read(PlanJson.write(ExecutionPlan.reading(List.of(), n))).root(),
                    "a step reads back as written");
        }
        List<ProgramRunner.Step> out = new ArrayList<>();
        for (ExecutionPlan.Node n : steps) {
            out.add(new ProgramRunner.Node(n, null));
        }
        return new ProgramRunner.Program(List.of(), out);
    }

    private static final ProgramRunner.Callbacks NONE = (id, open) -> {
        throw new IllegalStateException("no callback in this program");
    };

    private static Recording run(ProgramRunner.Program program) throws IOException {
        Recording sink = new Recording();
        ProgramRunner.run(program, Map.of(), sessions(), DataStores.none(), NONE, sink);
        return sink;
    }

    // ---- a callback (the plan-as-data design's step 3, 2026-11-08) ----

    @Test
    @DisplayName("a callback at its place reads what it declared, through the one open program; its reads are the"
            + " program's, and the answer follows the steps")
    void aCallbackReadsWhatItDeclared() throws IOException {
        ExecutionPlan.TdsResult declared = relation("SELECT 5 AS value", List.of(col("value", "Integer", "INTEGER")));
        ExecutionPlan.TdsResult answer = relation("SELECT 7 AS value", List.of(col("value", "Integer", "INTEGER")));
        List<Object> read = new ArrayList<>();
        ProgramRunner.OpenProgram[] open = new ProgramRunner.OpenProgram[2];
        Recording sink = new Recording();
        ProgramRunner.run(new ProgramRunner.Program(List.of(), List.of(
                new ProgramRunner.Callback(1, List.of(declared), null),
                new ProgramRunner.Node(answer, null),
                new ProgramRunner.Callback(2, List.of(), null))), Map.of(), sessions(), DataStores.none(), (id, o) -> {
                    open[id - 1] = o;
                    if (id == 1) {
                        read.add(o.read(declared, null, Map.of()).rows().get(0).get(0));
                    }
                }, sink);
        assertEquals(List.of(5L), read);
        assertTrue(open[0] == open[1], "one open program for the whole program");
        assertEquals(List.of("columns", "batch", "end"), sink.events);
        assertEquals(List.of(List.of(7L)), sink.answerRows);
        IllegalStateException after = assertThrows(IllegalStateException.class,
                () -> open[0].read(declared, null, Map.of()));
        assertTrue(after.getMessage().contains("once the program has ended"), after.getMessage());
    }

    @Test
    @DisplayName("a statement no callback declared is refused by name, never sent (exec runs only the plan's statements)")
    void anUndeclaredReadIsRefused() {
        ExecutionPlan.TdsResult declared = relation("SELECT 5 AS value", List.of(col("value", "Integer", "INTEGER")));
        ExecutionPlan.TdsResult other = relation("SELECT * FROM no_such_table_undeclared",
                List.of(col("value", "Integer", "INTEGER")));
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> ProgramRunner.run(
                new ProgramRunner.Program(List.of(), List.of(new ProgramRunner.Callback(1, List.of(declared), null))),
                Map.of(), sessions(), DataStores.none(), (id, o) -> o.read(other, null, Map.of()), new Recording()));
        assertTrue(e.getMessage().contains("no step of the program declared"), e.getMessage());
    }

    @Test
    @DisplayName("a guarded read inside the program's transaction: the savepoint set and released, or rolled back to"
            + " where the read fails, the failure raised as it was")
    void aGuardedReadSetsAndReleasesItsSavepoint() throws Exception {
        // the savepoint's three texts are stand-ins that log their step (DuckDB has no savepoint)
        exec("CREATE OR REPLACE TABLE m3c_sp (step VARCHAR)");
        exec("CREATE OR REPLACE TABLE m3c_w (a INTEGER)");
        ProgramRunner.Savepoint sp = new ProgramRunner.Savepoint("INSERT INTO m3c_sp VALUES ('set')",
                "INSERT INTO m3c_sp VALUES ('rollbackTo')", "INSERT INTO m3c_sp VALUES ('release')");
        ExecutionPlan.TdsResult good = relation("SELECT 5 AS value", List.of(col("value", "Integer", "INTEGER")));
        ExecutionPlan.TdsResult bad = relation("SELECT * FROM no_such_table_m3c",
                List.of(col("value", "Integer", "INTEGER")));
        List<String> raised = new ArrayList<>();
        ProgramRunner.run(new ProgramRunner.Program(List.of(), List.of(
                new ProgramRunner.Node(new ExecutionPlan.Update(ExecutionPlan.Sql.of("INSERT INTO m3c_w VALUES (1)",
                        target), null), null),
                new ProgramRunner.Callback(1, List.of(good, bad), null))), Map.of(), sessions(), DataStores.none(), (id, o) -> {
                    assertTrue(o.insideATransaction(), "a program that writes runs in one transaction");
                    o.guarded(sp, () -> o.read(good, null, Map.of()), e -> {
                        throw new AssertionError(e);
                    });
                    o.guarded(sp, () -> o.read(bad, null, Map.of()), e -> {
                        raised.add(e instanceof com.legend.error.DataError ? "raised" : "other: " + e);
                        return null;
                    });
                }, new Recording());
        assertEquals(List.of("raised"), raised);
        assertEquals(List.of("set", "release", "set", "rollbackTo"), column("SELECT step FROM m3c_sp"));
    }

    // ---- M3b (the test-is-the-product plan's B12): an effect's script, a seed step ----

    private static void exec(String sql) throws SQLException {
        try (java.sql.Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    private static long count(String table) throws SQLException {
        try (java.sql.Statement st = conn.createStatement();
                java.sql.ResultSet rs = st.executeQuery("SELECT count(*) FROM " + table)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private static List<String> column(String sql) throws SQLException {
        List<String> out = new ArrayList<>();
        try (java.sql.Statement st = conn.createStatement(); java.sql.ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                out.add(rs.getString(1));
            }
        }
        return out;
    }

    /** {@code statements} as one effect step, scripted in the platform dialect's bracket. */
    private static ExecutionPlan.Effect script(List<String> statements) {
        com.legend.sql.dialect.SqlDialect dialect = Databases.dialect(Databases.PLATFORM);
        return new ExecutionPlan.Effect(ExecutionPlan.Sql.of(String.join(";\n", statements) + ";", target),
                new ExecutionPlan.Effect.Script(statements, dialect.script(statements), dialect.scriptAbort()));
    }

    @Test
    @DisplayName("an effect script runs in its dialect's bracket; a failure aborts the bracket and applies nothing")
    void anEffectScriptRunsBracketedAndAFailureAppliesNothing() throws Exception {
        run(program(script(List.of("CREATE OR REPLACE TABLE m3b_s (a INTEGER)", "INSERT INTO m3b_s VALUES (1)"))));
        assertEquals(1, count("m3b_s"));
        // the second statement fails at run time: DuckDB's bracket applies neither, and the abort closes it
        assertThrows(com.legend.error.DataError.class, () -> run(program(script(List.of("INSERT INTO m3b_s VALUES (2)",
                "INSERT INTO m3b_s VALUES ('not a number')")))));
        assertEquals(1, count("m3b_s"), "the failed script applied nothing, and the session is usable");
    }

    @Test
    @DisplayName("a seed step runs once on a session, is skipped while the session holds its seeds, re-runs after a write")
    void aSeedStepRunsOnceUntilAWrite() throws Exception {
        ExecutionPlan.Load seeds = com.legend.planner.LoadPlanner.load(ExecutionPlan.Load.Cadence.BEFORE_STEP, List.of(new ExecutionPlan.LoadStep.Statement(
                "CREATE OR REPLACE TABLE m3b_seed AS SELECT 1 AS a")), target);
        ExecutionPlan.TdsResult read = relation("SELECT count(*) AS n FROM m3b_seed",
                List.of(col("n", "Integer", "BIGINT")));
        run(program(seeds, read));
        assertEquals(1, count("m3b_seed"));
        exec("INSERT INTO m3b_seed VALUES (2)");   // outside the runner: the session's memo does not see it
        run(program(seeds, read));
        assertEquals(2, count("m3b_seed"), "skipped: the session holds the seeds and ran no write");
        run(program(new ExecutionPlan.Effect(ExecutionPlan.Sql.of("INSERT INTO m3b_seed VALUES (3)", target)), seeds,
                read));
        assertEquals(1, count("m3b_seed"), "re-seeded after the program's write");
    }

    // ---- the M3 audit's findings (2026-11-01) ----

    /** B2: a keyed step's snapshot holds the program in a transaction, so an effect's script is sent bare inside it
     *  (asked at each step), never its BEGIN ... COMMIT bracket within the open transaction. */
    @Test
    @DisplayName("an effect before a keyed step runs bare inside the keyed step's snapshot transaction")
    void anEffectBeforeAKeyedStepRunsInsideItsSnapshot() throws Exception {
        ExecutionPlan.Sql keys = ExecutionPlan.Sql.of("SELECT 'k' AS k, 'k__|__v' AS n", target);
        String bodyText = "SELECT 1 AS a, ? AS _pk0_v";
        ExecutionPlan.Sql keyedBody = new ExecutionPlan.Sql(bodyText,
                List.of(new ExecutionPlan.Slot("", new ExecutionPlan.Binding.Key(0))), target, null,
                List.of(new ExecutionPlan.Region(13, bodyText.length(), "", ", ", "", 0, 1, List.of())));
        ExecutionPlan.Keyed keyed = new ExecutionPlan.Keyed(keys, List.of("k", "n"),
                List.of(new ExecutionPlan.Keyed.Fixed(col("a", "Integer", "INTEGER"), "a"),
                        new ExecutionPlan.Keyed.PerKey(col("v", "String", "VARCHAR"), 1, "v")),
                1, keyedBody, ExecutionPlan.Keyed.Snapshot.TRANSACTION, null);
        run(program(script(List.of("CREATE OR REPLACE TABLE m3x_k (a INTEGER)", "INSERT INTO m3x_k VALUES (1)")),
                keyed));
        assertEquals(1, count("m3x_k"));
    }

    /** B3: a seed step carries the program's target, so seeds before a constant answer open the session. */
    @Test
    @DisplayName("seeds before a constant answer: the seed step opens the program's session")
    void seedsBeforeAConstantOpenTheSession() throws Exception {
        ExecutionPlan.Load seeds = com.legend.planner.LoadPlanner.load(ExecutionPlan.Load.Cadence.BEFORE_STEP, List.of(new ExecutionPlan.LoadStep.Statement(
                "CREATE OR REPLACE TABLE m3x_c AS SELECT 1 AS a")), target);
        ExecutionPlan.Constant answer = new ExecutionPlan.Constant(List.of(new ExecutionPlan.Column("value", "String",
                new ExecutionPlan.Multiplicity(1, 1), null, null, null)), List.of(List.of("x")));
        Recording sink = run(program(seeds, answer));
        assertEquals(List.of(List.of("x")), sink.answerRows);
        assertEquals(1, count("m3x_c"));
    }

    /** B1: a rollback may undo seeds run inside the transaction, so the next program's seed step runs again. */
    @Test
    @DisplayName("a seed step undone by a rollback runs again in the next program")
    void aSeedUndoneByARollbackRunsAgain() throws Exception {
        exec("DROP TABLE IF EXISTS m3x_r");
        ExecutionPlan.Load seeds = com.legend.planner.LoadPlanner.load(ExecutionPlan.Load.Cadence.BEFORE_STEP, List.of(new ExecutionPlan.LoadStep.Statement(
                "CREATE TABLE m3x_r AS SELECT 1 AS a")), target);
        ExecutionPlan.Update failing = new ExecutionPlan.Update(ExecutionPlan.Sql.of(
                "INSERT INTO no_such_table_m3x VALUES (1)", target), null);
        assertThrows(RuntimeException.class, () -> run(program(seeds, failing)));
        ExecutionPlan.TdsResult read = relation("SELECT count(*) AS n FROM m3x_r", List.of(col("n", "Integer",
                "BIGINT")));
        Recording sink = run(program(seeds, read));
        assertEquals(1L, ((Number) sink.answerRows.get(0).get(0)).longValue());
    }
}
