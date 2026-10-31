// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.Compiler;
import com.legend.database.Databases;
import com.legend.exec.PlanRunner;
import com.legend.exec.PlanSessions;
import com.legend.executionplan.ExecutionPlan;
import com.legend.test.StorelessRuntime;
import com.legend.testbackend.TestBackends;
import com.legend.testbackend.TestSessions;
import com.legend.testcases.PlanRoundTrip;
import java.sql.Connection;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The product's statement road (the test-is-the-product plan's M2, B11): a query's and a program's statements planned
 * by the test road's front half. A String or a Boolean literal answer is the plan's own value, a CONSTANT node that
 * sends nothing (D26, step 6); a raw grid's rows or column names are a KEYED result whose keys are the grid's
 * reported columns, as the test road plans them (D29, step 7; until then the product refused them, "per-key regions
 * are a keyed result's body").
 */
class ProductStatementsTest {

    private static final String CONN = "let c = ^meta::external::store::relational::runtime::TestDatabaseConnection("
            + "type=meta::relational::runtime::DatabaseType.DuckDB);\n";

    private static final String WRITES = """
            ###Relational
            Database s::DB ( Table T ( ID INTEGER PRIMARY KEY, NAME VARCHAR(20) ) )
            ###Connection
            RelationalDatabaseConnection s::Conn
            {
                store: s::DB;
                type: DuckDB;
                specification: LocalH2 { };
                auth: DefaultH2;
            }
            ###Runtime
            Runtime s::RT { mappings: [ ]; connections: [ s::DB: [ c1: s::Conn ] ]; }
            """;

    private static ExecutionPlan plan(String query) {
        ExecutionPlan plan = Compiler.query(Compiler.compileModel(StorelessRuntime.with("", Databases.PLATFORM)), query)
                .executionPlan(StorelessRuntime.RUNTIME);
        PlanRoundTrip.assertRoundTrips(plan, query);
        return plan;
    }

    private static List<List<Object>> rows(ExecutionPlan plan) throws Exception {
        try (Connection c = TestBackends.platform().open()) {
            return PlanRunner.rows(plan, Map.of(), plan.sendsTheAnswer()
                    ? PlanSessions.setUp(TestSessions.over(c), plan.answerSql().target())
                    : PlanSessions.of(TestSessions.over(c))).rows();
        }
    }

    @Test
    void aStringOrBooleanLiteralAnswerIsAConstant() throws Exception {
        ExecutionPlan text = plan("|'hello'");
        ExecutionPlan.Constant c = assertInstanceOf(ExecutionPlan.Constant.class, text.root());
        assertEquals(List.of(List.of("hello")), c.cells());
        assertEquals(ExecutionPlan.TdsResult.Shape.VALUE, text.answerShape());
        assertEquals(false, text.sendsTheAnswer());
        assertEquals(List.of(List.of("hello")), rows(text));
        // a Boolean's cell is the Boolean, as the database's row would carry it
        assertEquals(List.of(List.of(Boolean.TRUE)), rows(plan("|true")));
        // an Integer literal is the database's to spell: a statement, as before
        assertInstanceOf(ExecutionPlan.TdsResult.class, plan("|1").root());
    }

    @Test
    void aProgramsLastLiteralIsAConstantStepAndAnEarlierOneRunsNothing() {
        ExecutionPlan plan = Compiler.query(Compiler.compileModel(WRITES),
                "{|'first'; #>{s::DB.T}#->delete(r|$r.ID == 9); 'done';}").executionPlan("s::RT");
        PlanRoundTrip.assertRoundTrips(plan, "a program ending in a literal");
        ExecutionPlan.Sequence seq = assertInstanceOf(ExecutionPlan.Sequence.class, plan.root());
        assertEquals(2, seq.steps().size(), "the earlier literal plans no step: " + seq.steps());
        assertInstanceOf(ExecutionPlan.Update.class, seq.steps().get(0));
        ExecutionPlan.Constant last = assertInstanceOf(ExecutionPlan.Constant.class, seq.steps().get(1));
        assertEquals(List.of(List.of("done")), last.cells());
    }

    @Test
    void aRawGridsRowsAreAKeyedResult() throws Exception {
        ExecutionPlan values = plan("{|" + CONN
                + "meta::relational::metamodel::execute::executeInDb('select 7 as A', $c).rows->map(r|$r.values);}");
        assertInstanceOf(ExecutionPlan.Keyed.class, values.root());
        // a value read off the grid answers as a value, not a relation (M2's audit: a writer chooses its layout by the
        // answer's shape, and the server refuses a value's answer by name)
        assertEquals(ExecutionPlan.TdsResult.Shape.VALUE, values.answerShape());
        // the test road's own answer to the same statement (its rows plan, JudgePlans.rowsPlan, keyed the same way)
        try (Connection c = TestBackends.platform().open()) {
            assertEquals(com.legend.testing.Program.last(StorelessRuntime.with("", Databases.PLATFORM), "{|" + CONN
                    + "meta::relational::metamodel::execute::executeInDb('select 7 as A', $c).rows->map(r|$r.values);}",
                    StorelessRuntime.RUNTIME, c).rows(), rows(values));
        }
        // the column names too: the grid's reported columns are the keys
        ExecutionPlan names = plan("{|" + CONN
                + "meta::relational::metamodel::execute::executeInDb('select 7 as A, 8 as B', $c).columnNames;}");
        assertInstanceOf(ExecutionPlan.Keyed.class, names.root());
        assertEquals(ExecutionPlan.TdsResult.Shape.VALUE, names.answerShape());
        try (Connection c = TestBackends.platform().open()) {
            assertEquals(com.legend.testing.Program.last(StorelessRuntime.with("", Databases.PLATFORM), "{|" + CONN
                    + "meta::relational::metamodel::execute::executeInDb('select 7 as A, 8 as B', $c).columnNames;}",
                    StorelessRuntime.RUNTIME, c).rows(), rows(names));
        }
        assertEquals(List.of(List.of("A"), List.of("B")), rows(names));
        // the grid itself is a relation of the columns its result reports, as before
        ExecutionPlan grid = plan("{|" + CONN
                + "meta::relational::metamodel::execute::executeInDb('select 7 as A', $c, 0, 1000);}");
        ExecutionPlan.TdsResult result = assertInstanceOf(ExecutionPlan.TdsResult.class, grid.root());
        assertEquals(List.of(), result.columns());
    }

    @Test
    void aHandleIsRefusedAsAnAnswerAndPlansNoStepBeforeTheLast() {
        // a connection answered by a query: it has no value a plan can answer, refused by name
        com.legend.error.NotImplementedException refused = assertThrows(
                com.legend.error.NotImplementedException.class, () -> plan("{|" + CONN + "$c;}"));
        assertTrue(refused.getMessage().contains("orchestration handle"), refused.getMessage());
        // before a program's last statement: nothing to run, no step
        ExecutionPlan plan = Compiler.query(Compiler.compileModel(WRITES), "{|" + CONN
                + "$c; #>{s::DB.T}#->delete(r|$r.ID == 9);}").executionPlan("s::RT");
        ExecutionPlan.Sequence seq = assertInstanceOf(ExecutionPlan.Sequence.class, plan.root());
        assertTrue(seq.steps().stream().noneMatch(st -> st instanceof ExecutionPlan.Constant), seq.steps().toString());
        assertInstanceOf(ExecutionPlan.Update.class, seq.steps().get(0));
    }

    private static final String LEGACY = """
            ###Relational
            Database test::DB ( Table T (id INTEGER PRIMARY KEY, name VARCHAR(200)) )
            ###Mapping
            Mapping test::M ( )
            """;

    @Test
    void aStagedToSqlStringIsThePlansConstantAndTheTestRoadsText() throws Exception {
        // D14: toSQLString's engine text is a plan-time value (staged), so the product's plan answers it as its own
        // constant; the text is the test road's for the same statement
        String query = "|meta::relational::functions::sqlstring::toSQLString(|#>{test::DB.T}#->select(~[id, name]),"
                + " test::M, meta::relational::runtime::DatabaseType.H2, [])";
        String model = StorelessRuntime.with(LEGACY, Databases.PLATFORM);
        ExecutionPlan plan = Compiler.query(Compiler.compileModel(model), query).executionPlan(StorelessRuntime.RUNTIME);
        ExecutionPlan.Constant c = assertInstanceOf(ExecutionPlan.Constant.class, plan.root());
        try (Connection conn = TestBackends.platform().open()) {
            assertEquals(String.valueOf(com.legend.testing.Program.last(model, query, StorelessRuntime.RUNTIME, conn)
                    .one()), c.cells().get(0).get(0));
        }
        assertTrue(c.cells().get(0).get(0).startsWith("select "), c.cells().toString());
    }

    @Test
    void aDdlTextIsThePlansConstantAndHasNoSqlText() {
        // D17: a DDL text generator's text is the model's, answered with nothing sent; the query's SQL text (the
        // lowering, TypedQuery.lower) has none, refused by name
        String query = "|meta::relational::functions::toDDL::createTableStatement(test::DB, 'T')";
        com.legend.TypedQuery typed = Compiler.query(Compiler.compileModel(StorelessRuntime.with(LEGACY,
                Databases.PLATFORM)), query);
        ExecutionPlan.Constant c = assertInstanceOf(ExecutionPlan.Constant.class,
                typed.executionPlan(StorelessRuntime.RUNTIME).root());
        assertTrue(c.cells().get(0).get(0).toUpperCase(java.util.Locale.ROOT).startsWith("CREATE TABLE"),
                c.cells().toString());
        com.legend.error.NotImplementedException none = assertThrows(com.legend.error.NotImplementedException.class,
                () -> typed.lower(StorelessRuntime.RUNTIME, false));
        assertTrue(none.getMessage().contains("DDL text"), none.getMessage());
    }

    @Test
    void aProgramOfConstantsAloneRunsWithNoSession() throws Exception {
        // B2: a program that sends nothing answers its constant; no session is opened
        ExecutionPlan plan = plan("|'alone'");
        com.legend.result.Collector collector = new com.legend.result.Collector();
        PlanSessions.Source none = target -> {
            throw new AssertionError("a program that sends nothing opened a session");
        };
        PlanRunner.program(plan, Map.of(), none, PlanRunner.Judging.HOST, new PlanRunner.ProgramSink() {
                    @Override
                    public void columns(List<ExecutionPlan.Column> columns) throws java.io.IOException {
                        collector.columns(columns);
                    }

                    @Override
                    public void batch(com.legend.result.Batch batch) throws java.io.IOException {
                        collector.batch(batch);
                    }

                    @Override
                    public void end() throws java.io.IOException {
                        collector.end();
                    }

                    @Override
                    public void verdict(ExecutionPlan.Verdict verdict,
                            @com.legend.base.Nullable List<Object> row) {
                        throw new AssertionError("no verdicts");
                    }

                    @Override
                    public void fused(ExecutionPlan.Verdicts group,
                            com.legend.error.@com.legend.base.Nullable DataError rejected) {
                        throw new AssertionError("no verdicts");
                    }

                    @Override
                    public void sides(ExecutionPlan.Verdict verdict, List<PlanRunner.Rows> sides) {
                        throw new AssertionError("no verdicts");
                    }
                });
        assertEquals(List.of(List.of("alone")), collector.collected().rows());
    }

    @Test
    void aDynamicPivotAsAProgramsLastStatementIsKeyed() {
        // D29's other half: a pivot no cast names, answered by a program that writes, is the keyed result the query
        // door plans (until M2 a program's last statement was a plain typed result, its per-key columns unread)
        ExecutionPlan plan = Compiler.query(Compiler.compileModel(WRITES), "{|#>{s::DB.T}#->delete(r|$r.ID == 9);"
                + " #>{s::DB.T}#->pivot(~[NAME], ~['n' : x | $x.ID : y | $y->plus()]);}").executionPlan("s::RT");
        PlanRoundTrip.assertRoundTrips(plan, "a program ending in a dynamic pivot");
        ExecutionPlan.Sequence seq = assertInstanceOf(ExecutionPlan.Sequence.class, plan.root());
        ExecutionPlan.Keyed keyed = assertInstanceOf(ExecutionPlan.Keyed.class, seq.steps().get(seq.steps().size() - 1));
        assertEquals(ExecutionPlan.TdsResult.Shape.RELATION, keyed.shape());
    }

    @Test
    void aFromOverAnInlineRuntimeTakesItsConnectionsTimeZone() {
        // D4: the outermost from()'s context is the frame; its connection's time zone is the lowering's (the test
        // road's rule, BodyPlanner.prepare): a DateTime literal compared under it is written in that zone
        String model = """
                ###Relational
                Database s::DB ( Table T ( ID INTEGER PRIMARY KEY, TS TIMESTAMP ) )
                ###Connection
                RelationalDatabaseConnection s::Conn
                {
                    store: s::DB;
                    type: DuckDB;
                    specification: LocalH2 { };
                    auth: DefaultH2;
                }
                ###Runtime
                Runtime s::RT { mappings: [ ]; connections: [ s::DB: [ c1: s::Conn ] ]; }
                """;
        String filter = "#>{s::DB.T}#->filter(r|$r.TS > %2020-01-01T10:00:00)->select(~[ID])";
        String zoned = "|" + filter + "->from(^meta::core::runtime::Runtime(connectionStores="
                + "^meta::core::runtime::ConnectionStore(element=s::DB, connection="
                + "^meta::external::store::relational::runtime::TestDatabaseConnection("
                + "type=meta::relational::runtime::DatabaseType.DuckDB, timeZone='US/Arizona'))))";
        String plain = "|" + filter + "->from(^meta::core::runtime::Runtime(connectionStores="
                + "^meta::core::runtime::ConnectionStore(element=s::DB, connection="
                + "^meta::external::store::relational::runtime::TestDatabaseConnection("
                + "type=meta::relational::runtime::DatabaseType.DuckDB))))";
        com.legend.compiler.element.ModelContext ctx = Compiler.compileModel(model);
        String zonedSql = Compiler.query(ctx, zoned).executionPlan("s::RT").answerSql().statement();
        String plainSql = Compiler.query(ctx, plain).executionPlan("s::RT").answerSql().statement();
        // 10:00 UTC is 03:00 in Arizona (UTC-7, no daylight saving): the literal is written in the frame's zone
        assertTrue(zonedSql.contains("TIMESTAMP '2020-01-01T03:00:00'"), zonedSql);
        assertTrue(plainSql.contains("TIMESTAMP '2020-01-01T10:00:00'"), plainSql);
    }
}
