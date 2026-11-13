// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.Compiler;
import com.legend.executionplan.DataRef;
import com.legend.executionplan.DataStores;
import com.legend.executionplan.ExecutionPlan;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.RecordingConnection;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import com.legend.testcases.PlanCases;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE RUNNER SENDS A PLAN'S TEXT AS IT IS, IN THE PLAN'S ORDER (the tenets re-audit's B-6, 2026-11-09; widened by the
 * C checkpoint's MUST-FIX 1, 2026-11-12, which found this proof covered one road): every statement a session is sent
 * while a plan runs is text the plan carries, character for character, on every road exec runs -- the product's query
 * (every parameter kind), a keyed body (the plan's text with each region written once per key, the charter's A3(a), the
 * one sanctioned writing at run), the program runner's road (a program's steps, a file's load among them, the test
 * door's road beneath it) and the test door's own (a body's test plan) -- and in the plan's order. The type says the
 * same ({@code exec.Statements} takes a {@code PlannedSql}, minted only where a plan is read:
 * {@code ArchitectureTest.onlyThePlansReadersMintPlannedSql}); this is the run's own witness of it. Nothing is set
 * aside: the per-execution trace comment the seam wrote in front of every statement until C1 (2026-11-12) is gone.
 */
class SentStatementsAreThePlansTest {

    /** The product's query, every parameter kind on every database this process runs: each sent statement the plan's,
     *  in the plan's order (its target's session statements, its loads' statements, its answer). */
    @Test
    void everySentStatementIsThePlansText_inThePlansOrder() throws Exception {
        int checked = 0;
        for (DatabaseType type : PlanMakerTest.IN_PROCESS) {
            for (PlanCases.Parameterised q : PlanCases.scalars("T")) {
                ExecutionPlan plan = Compiler.query(Compiler.compileModel(PlanMakerTest.model(type.name())),
                        q.withParameters()).executionPlan("s::RT");
                List<String> planned = ordered(plan);
                List<String> sent = new ArrayList<>();
                try (Connection c = RecordingConnection.wrap(TestBackends.of(type).open(), sent)) {
                    PlanCases.write(plan, c, q.values(), PlanCases.Written.JSON::writer);
                } catch (com.legend.error.DataError refused) {
                    // a value finer than its slot keeps, refused by the statement's own raise, which was sent as the
                    // plan wrote it (S181, A7, 2026-11-10: DuckDB's microsecond slot and the nanosecond case)
                    assertTrue(String.valueOf(refused.getMessage()).contains(com.legend.error.ErrorKind.FRACTION_NOT_KEPT.sentence()),
                            type + " " + q.withParameters() + ": " + refused.getMessage());
                }
                assertFalse(sent.isEmpty(), type + " " + q.withParameters());
                inOrder(sent, planned, type + " " + q.withParameters());
                assertEquals(plan.answerSql().statement(), sent.get(sent.size() - 1),
                        "the answer's statement was sent last, as written");
                checked += sent.size();
            }
        }
        assertTrue(checked > 50, "too few statements checked to prove anything: " + checked);
    }

    /** A dynamic pivot, a keyed plan, on every database this process runs: its keys probe sent as the plan holds it, then
     *  its body as the plan holds it with each region written once per key -- nothing else, in that order. */
    @Test
    void aKeyedBodyIsThePlansTextWrittenOncePerKey() throws Exception {
        String query = "#TDS\n city, year, treePlanted\n NYC, 2011, 5\n NYC, 2000, 15\n SAN, 2011, 2\n SAN, 2000, 1\n#"
                + "->pivot(~[year], ~['newCol' : x | $x.treePlanted : y | $y->plus()])->sort(~city->ascending())";
        for (DatabaseType type : PlanMakerTest.IN_PROCESS) {
            ExecutionPlan plan = Compiler.query(Compiler.compileModel(PlanMakerTest.model(type.name())), query)
                    .executionPlan("s::RT");
            ExecutionPlan.Keyed keyed = (ExecutionPlan.Keyed) plan.root();
            assertFalse(keyed.body().regions().isEmpty(), type + ": a keyed body has its per-key regions");
            List<String> sent = new ArrayList<>();
            try (Connection c = RecordingConnection.wrap(TestBackends.of(type).open(), sent)) {
                PlanCases.write(plan, c, Map.of(), PlanCases.Written.JSON::writer);
            }
            List<String> head = sent.subList(0, sent.size() - 1);
            List<String> planned = targetTexts(keyed.keys().target());
            planned.add(keyed.keys().statement());
            inOrder(head, planned, type + ": the target's statements, then the keys probe");
            assertEquals(keyed.keys().statement(), head.get(head.size() - 1), type + ": the probe, as written");
            // two keys (2000, 2011): the body is the plan's text with each region written twice, and nothing else
            assertEquals(expanded(keyed.body(), 2), sent.get(sent.size() - 1), type + ": the body, once per key");
        }
    }

    /**
     * The program runner's road (the test door's beneath it): a program's steps -- a load of a statement and a file the
     * database reads by the plan's own reader, a result, an effect -- on each database this process runs whose dialect
     * reads a file, each sent statement the step's own text, in the program's order: the table made, the file's reading and its
     * undoing (H2's path variable), the result's statement, the effect's.
     */
    @Test
    void aProgramSendsItsStepsTexts_aFileLoadAmongThem_inOrder(@TempDir Path dir) throws Exception {
        byte[] csv = "NAME,AGE\nJoe,21\nJill,31\n".getBytes(StandardCharsets.UTF_8);
        Files.write(dir.resolve("people.csv"), csv);
        int reading = 0;
        for (TestBackend b : TestBackends.inProcess()) {
            ExecutionPlan.Target target = Targets.of(new ExecutionPlan.Database.Platform(b.planType(), null),
                    b.dialect());
            com.legend.sql.dialect.SqlDialect.CsvLoad reads;
            try {
                reads = b.dialect().csvLoad(new com.legend.sql.SqlDml.LoadCsv(null, "sent_people",
                        com.legend.sql.NameQuoting.BARE, List.of("NAME", "AGE"), Set.of(), List.of(
                                new com.legend.sql.SqlDdl.ColumnType.Sized("VARCHAR", 20),
                                new com.legend.sql.SqlDdl.ColumnType.Plain(com.legend.sql.SqlDdl.ColumnType.Kind.INTEGER)),
                        ',', '"', 500));
            } catch (com.legend.sql.DialectCapability noFile) {
                continue;   // a database that reads no file says so by name (DataLoadsTest)
            }
            reading++;
            ExecutionPlan.LoadStep.File file = new ExecutionPlan.LoadStep.File(new DataRef(DataRef.Namespace.RESOURCE,
                    "people.csv", DataStores.sha256(csv), csv.length, DataRef.Csv.DEFAULT),
                    new ExecutionPlan.LoadStep.Delivery.Reads(reads.statements(), reads.after()), 500);
            String create = "CREATE TABLE sent_people (NAME VARCHAR(20), AGE INTEGER)";
            ExecutionPlan.Load load = LoadPlanner.load(ExecutionPlan.Load.Cadence.AT_STEP,
                    List.of(new ExecutionPlan.LoadStep.Statement(create), file), target);
            String select = "SELECT NAME, AGE FROM sent_people ORDER BY AGE";
            ExecutionPlan.TdsResult result = new ExecutionPlan.TdsResult(ExecutionPlan.TdsResult.Shape.RELATION,
                    List.of(column("NAME", "String", "VARCHAR"), column("AGE", "Integer", "INTEGER")),
                    ExecutionPlan.Sql.of(select, target));
            String drop = "DROP TABLE sent_people";
            ExecutionPlan.Effect effect = new ExecutionPlan.Effect(ExecutionPlan.Sql.of(drop, target), null);
            List<String> sent = new ArrayList<>();
            List<List<Object>> rows = new ArrayList<>();
            try (Connection c = RecordingConnection.wrap(b.open(), sent)) {
                com.legend.exec.ProgramRunner.run(new com.legend.exec.ProgramRunner.Program(List.of(), List.of(
                        new com.legend.exec.ProgramRunner.Node(load, null),
                        new com.legend.exec.ProgramRunner.Node(result, null),
                        new com.legend.exec.ProgramRunner.Node(effect, null))), Map.of(),
                        com.legend.exec.PlanSessions.acquired(com.legend.testbackend.TestSessions.over(c), target),
                        DataStores.resources(dir), (id, open) -> {
                            throw new IllegalStateException("no callback in this program");
                        }, sink(rows), java.util.UUID.randomUUID());
            }
            assertEquals(List.of(List.of("Joe", 21L), List.of("Jill", 31L)), rows, b.name() + ": the file was read");
            List<String> planned = new ArrayList<>();
            // a caller's session, read back for the settings its database must have (H2's: SHOULD-FIX 3)
            if (target.settings() != null) {
                planned.add(target.settings().readBack());
            }
            planned.addAll(target.session());
            planned.add(create);
            planned.addAll(reads.statements());
            planned.addAll(reads.after());
            planned.add(select);
            planned.add(drop);
            assertEquals(planned, sent, b.name() + ": the program's texts, each as the plan holds it, in its order");
        }
        assertTrue(reading >= 2, "the databases that read a file: " + reading);
    }

    /** The test door's road: a body planned whole into its test plan and run -- every sent statement one the test plan
     *  carries (its JSON's texts, a load's statements blob read as its statements), on every database this process
     *  runs, under each judge. */
    @Test
    void aTestBodySendsOnlyItsTestPlansTexts() throws Exception {
        String body = "{|let n = #>{s::DB.T}#->filter(r|$r.ID > 1)->size();\n"
                + "assertEquals(2, $n);\n"
                + "#>{s::DB.T}#->filter(r|$r.ID == 1)->select(~[NAME]);}";
        int checked = 0;
        for (TestBackend b : TestBackends.inProcess()) {
            var ctx = Compiler.compileModel(PlanMakerTest.model(b.type().name()));
            for (com.legend.testplan.PlanningOptions.JudgeMode judge : List.of(
                    com.legend.testplan.PlanningOptions.JudgeMode.HOST,
                    com.legend.testplan.PlanningOptions.JudgeMode.DATABASE)) {
                com.legend.ExecuteOptions options = com.legend.ExecuteOptions.NONE.withJudgeMode(judge);
                com.legend.testexecutionplan.TestPlan plan = com.legend.TestExecution.planBody(typed(ctx, body), "s::RT",
                        options);
                Set<String> planned = texts(com.legend.testexecutionplan.TestPlanJson.write(plan));
                List<String> sent = new ArrayList<>();
                List<String> verdicts = new ArrayList<>();
                try (Connection c = RecordingConnection.wrap(b.open(), sent)) {
                    com.legend.TestExecution.runBody(plan, com.legend.testbackend.TestSessions.over(c),
                            (name, pass, detail) -> verdicts.add(pass + " " + detail), null, options, null);
                }
                assertEquals(List.of("true null"), verdicts, b.name() + " " + judge);
                assertFalse(sent.isEmpty(), b.name() + " " + judge);
                for (String statement : sent) {
                    assertTrue(planned.contains(statement), b.name() + " " + judge + ": sent a statement the test"
                            + " plan does not carry:\n" + statement);
                    checked++;
                }
                // and in the plan's order (the third re-audit's SHOULD-FIX 10, 2026-11-12: until then membership alone)
                inOrder(sent, ordered(plan), b.name() + " " + judge);
            }
        }
        assertTrue(checked > 10, "too few statements checked to prove anything: " + checked);
    }

    /** The order check refuses a statement sent before one the plan holds ahead of it, and one the plan does not hold;
     *  it lets a statement the run had no need of go unsent (the third re-audit's SHOULD-FIX 10, 2026-11-12). */
    @Test
    void theOrderCheckRefusesAStatementOutOfItsPlansOrder() {
        List<String> planned = List.of("SET a", "SELECT 1", "SELECT 2");
        inOrder(List.of("SET a", "SELECT 2"), planned, "a statement skipped");
        org.junit.jupiter.api.Assertions.assertThrows(AssertionError.class,
                () -> inOrder(List.of("SELECT 2", "SELECT 1"), planned, "out of order"));
        org.junit.jupiter.api.Assertions.assertThrows(AssertionError.class,
                () -> inOrder(List.of("SELECT 3"), planned, "not the plan's"));
    }

    /** The witness's connection leads nowhere past it (the third re-audit's SHOULD-FIX 10, 2026-11-12): {@code unwrap}
     *  is refused by name and {@code isWrapperFor} answers no, on the connection, a statement and the metadata; a
     *  statement's and the metadata's {@code getConnection} answer the recording connection, whose statements are
     *  recorded. Until then {@code unwrap} reached the driver beneath, where a statement went unrecorded. */
    @Test
    void theRecordingConnectionLeadsNowherePastIt() throws Exception {
        List<String> sent = new ArrayList<>();
        try (Connection c = RecordingConnection.wrap(TestBackends.platform().open(), sent)) {
            for (java.sql.Wrapper w : List.<java.sql.Wrapper>of(c, c.createStatement(), c.prepareStatement("SELECT 1"),
                    c.getMetaData())) {
                java.sql.SQLException refused = org.junit.jupiter.api.Assertions.assertThrows(java.sql.SQLException.class,
                        () -> w.unwrap(Connection.class));
                assertTrue(refused.getMessage().startsWith("a recording connection refuses unwrap"), refused.getMessage());
                assertFalse(w.isWrapperFor(Connection.class));
            }
            try (java.sql.Statement st = c.createStatement().getConnection().createStatement()) {
                st.execute("SELECT 2");
            }
            try (java.sql.Statement st = c.getMetaData().getConnection().createStatement()) {
                st.execute("SELECT 3");
            }
        }
        assertEquals(List.of("SELECT 2", "SELECT 3"), sent, "every statement recorded, whichever way it was reached");
    }

    /** Every text {@code plan} carries for a session, in the order a run sends it: its target's session statements,
     *  each load's statements and staging, the error read-back the target declares (sent after a failure alone), then
     *  its answer's statement. */
    private static List<String> ordered(ExecutionPlan plan) {
        ExecutionPlan.Target target = plan.answerSql().target();
        List<String> out = targetTexts(target);
        // the error read-back the target declares, sent after a failure (C2, 2026-11-12: MariaDB's and MySQL's)
        if (target.raiseReadBack() != null) {
            out.add(target.raiseReadBack().read());
            out.add(target.raiseReadBack().reset());
        }
        out.add(plan.answerSql().statement());
        return out;
    }

    /** Every text {@code plan} carries for a session, in the order its run sends them: the body's session target's
     *  (its settings' read-back, its session statements, its loads), then each step's statements in the steps' order --
     *  a product step's node, a frame run's check, a verdict group's or a branch's statements -- each with its own
     *  target's first. */
    private static List<String> ordered(com.legend.testexecutionplan.TestPlan plan) {
        List<String> out = targetTexts(plan.session());
        for (com.legend.testexecutionplan.TestPlan.TestStep step : plan.steps()) {
            List<ExecutionPlan.Node> nodes = switch (step) {
                case com.legend.testexecutionplan.TestPlan.Run r -> List.of(r.node());
                case com.legend.testexecutionplan.TestPlan.FrameRun f -> List.of(f.check());
                case com.legend.testexecutionplan.TestPlan.Verdicts v ->
                        com.legend.testexecutionplan.TestPlan.statements(v);
                case com.legend.testexecutionplan.TestPlan.Branch br ->
                        com.legend.testexecutionplan.TestPlan.statements(br);
            };
            for (ExecutionPlan.Node node : nodes) {
                nodeTexts(node, out);
            }
        }
        return out;
    }

    /** {@code node}'s statements, each with its target's texts first, in the order a run sends them. */
    private static void nodeTexts(ExecutionPlan.Node node, List<String> out) {
        switch (node) {
            case ExecutionPlan.Sequence q -> q.steps().forEach(step -> nodeTexts(step, out));
            case ExecutionPlan.TdsResult t -> sqlTexts(t.sql(), out);
            case ExecutionPlan.Keyed k -> {
                sqlTexts(k.keys(), out);
                sqlTexts(k.body(), out);
            }
            case ExecutionPlan.Update u -> sqlTexts(u.sql(), out);
            case ExecutionPlan.Check c -> sqlTexts(c.sql(), out);
            case ExecutionPlan.Effect e -> sqlTexts(e.sql(), out);
            case ExecutionPlan.Load l -> {
                out.addAll(targetTexts(l.target().bare()));
                loadTexts(l, out);
            }
            case ExecutionPlan.Constant c -> {
            }
        }
    }

    private static void sqlTexts(ExecutionPlan.Sql sql, List<String> out) {
        out.addAll(targetTexts(sql.target()));
        out.add(sql.statement());
    }

    /** {@code target}'s texts in the order a session acquired for it sends them: its session statements, then each
     *  load's statements and staging. */
    private static List<String> targetTexts(ExecutionPlan.Target target) {
        List<String> out = new ArrayList<>();
        // a session the runner did not open is read back for the settings its database must have (H2's: the C
        // checkpoint's SHOULD-FIX 3), the target's own statement
        if (target.settings() != null && target.settings().readBack() != null) {
            out.add(target.settings().readBack());
        }
        out.addAll(target.session());
        for (ExecutionPlan.Load load : target.loads()) {
            out.addAll(load.target().session());
            loadTexts(load, out);
        }
        return out;
    }

    /** {@code load}'s statements and staging, in its steps' order. */
    private static void loadTexts(ExecutionPlan.Load load, List<String> out) {
        for (ExecutionPlan.LoadStep step : load.steps()) {
            {
                switch (step) {
                    case ExecutionPlan.LoadStep.Statement s -> out.add(s.sql());
                    case ExecutionPlan.LoadStep.Statements st -> out.addAll(DataRef.Statements.decode(
                            load.content().get(st.ref().digest())));
                    case ExecutionPlan.LoadStep.File f -> {
                        ExecutionPlan.LoadStep.Delivery.Reads reads = (ExecutionPlan.LoadStep.Delivery.Reads) f.delivery();
                        out.addAll(reads.statements());
                        out.addAll(reads.after());
                    }
                    case ExecutionPlan.LoadStep.Bound b -> throw new IllegalStateException("no host's bound statement"
                            + " in these plans");
                    case ExecutionPlan.LoadStep.Stream r -> throw new IllegalStateException("no host's stream in these"
                            + " plans");
                    case ExecutionPlan.LoadStep.Attach a -> throw new IllegalStateException("no host's attached value in"
                            + " these plans");
                }
            }
        }
    }

    /** {@code sent} is {@code planned} in its order, a statement skipped where the run had no need of it (a session's
     *  statement its connection already ran, the read-back where nothing failed) -- never one out of order, never one
     *  the plan does not hold. */
    private static void inOrder(List<String> sent, List<String> planned, String what) {
        int at = 0;
        for (String statement : sent) {
            int found = planned.subList(at, planned.size()).indexOf(statement);
            assertTrue(found >= 0, what + ": sent a statement the plan does not carry where it was sent:\n" + statement
                    + "\nthe plan's, in order:\n" + String.join("\n---\n", planned) + "\nsent:\n"
                    + String.join("\n---\n", sent));
            at += found + 1;
        }
    }

    /** {@code sql} written for {@code keys} keys, by the rule its regions declare ({@link ExecutionPlan.Region}): the
     *  text outside them as it is; each region, with no key, its {@code empty}; else {@code lead}, then one copy per key,
     *  {@code separator} between, the key's position written at each counter. */
    private static String expanded(ExecutionPlan.Sql sql, int keys) {
        String text = sql.statement();
        StringBuilder out = new StringBuilder();
        int from = 0;
        for (ExecutionPlan.Region r : sql.regions()) {
            out.append(text, from, r.start());
            if (keys == 0) {
                out.append(r.empty());
            } else {
                out.append(r.lead());
                for (int k = 0; k < keys; k++) {
                    if (k > 0) {
                        out.append(r.separator());
                    }
                    int at = r.start();
                    for (int counter : r.counters()) {
                        out.append(text, at, counter).append(k);
                        at = counter;
                    }
                    out.append(text, at, r.end());
                }
            }
            from = r.end();
        }
        out.append(text, from, text.length());
        return out.toString();
    }

    /** Every text a test plan's JSON holds: each string, and each statement of a string that is a statements blob (a
     *  load's content, a JSON array of its statements). */
    private static Set<String> texts(String json) {
        Set<String> out = new LinkedHashSet<>();
        collect(com.legend.json.Json.parse(json), out);
        return out;
    }

    private static void collect(com.legend.json.Json.Node node, Set<String> out) {
        switch (node) {
            case com.legend.json.Json.Str s -> {
                out.add(s.value());
                if (s.value().startsWith("[\"")) {
                    try {
                        out.addAll(DataRef.Statements.decode(s.value()));
                    } catch (RuntimeException notABlob) {
                        // a text that only looks like one
                    }
                }
            }
            case com.legend.json.Json.Obj o -> o.fields().values().forEach(v -> collect(v, out));
            case com.legend.json.Json.Arr a -> a.items().forEach(v -> collect(v, out));
            default -> {
            }
        }
    }

    /** {@code body}, a lambda's text, typed as a test body. */
    private static com.legend.TypedQuery typed(com.legend.compiler.element.ModelContext ctx, String body) {
        com.legend.protocol.spec.ValueSpecification parsed = Compiler.parseQuery(body);
        List<com.legend.protocol.spec.ValueSpecification> statements =
                parsed instanceof com.legend.protocol.spec.LambdaFunction lf && lf.parameters().isEmpty() ? lf.body()
                        : List.of(parsed);
        return Compiler.queryResolved(ctx, Compiler.resolveQuery(statements,
                new com.legend.model.ImportScope(List.of()), ctx));
    }

    private static ExecutionPlan.Column column(String name, String type, String slot) {
        return new ExecutionPlan.Column(name, type, new ExecutionPlan.Multiplicity(1, 1), slot, null, null);
    }

    /** A program's sink that keeps its answer's rows. */
    private static com.legend.exec.ProgramRunner.ProgramSink sink(List<List<Object>> rows) {
        return new com.legend.exec.ProgramRunner.ProgramSink() {
            @Override
            public void columns(List<ExecutionPlan.Column> columns) {
            }

            @Override
            public void batch(com.legend.result.Batch batch) {
                for (int i = 0; i < batch.size(); i++) {
                    rows.add(batch.row(i));
                }
            }

            @Override
            public void end() {
            }
        };
    }
}
