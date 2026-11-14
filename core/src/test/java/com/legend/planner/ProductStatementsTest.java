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
import com.legend.testbackend.StorelessRuntime;
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
 * sends nothing (D26, step 6). A raw grid ({@code executeInDb}'s read) is the TEST road's alone since 2026-11-13 (the
 * user's ruling (a) on the orchestrator's item 5): its keyed result is pinned on the test door (TestDoorTest), its
 * refusal on the product's doors (QueryDoorTest).
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
                    ? PlanSessions.acquired(TestSessions.over(c), plan.answerSql().target())
                    : PlanSessions.of(TestSessions.over(c)), java.util.UUID.randomUUID()).rows();
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

    /** The test-data seed's one entry (the user's ruling of 2026-11-13, option (b): it replaces TypedQuery.lower for the
     *  generator's two callers) refuses a query reaching a statement's own SQL text by name, inside itself, as every
     *  product door refuses it; the same query without that read is lowered. */
    @Test
    void theTestDataSeedsEntryRefusesRawSqlInsideItself() {
        com.legend.compiler.element.ModelContext ctx = Compiler.compileModel(WRITES);
        com.legend.TypedQuery raw = Compiler.query(ctx, "{|" + CONN + "let n = meta::relational::metamodel::execute"
                + "::executeInDb('select 7 as A', $c).columnNames;\n#>{s::DB.T}#->select(~[ID]);}");
        com.legend.error.NotImplementedException refused = assertThrows(
                com.legend.error.NotImplementedException.class, () -> ProductStatements.lowerTestDataSeed(raw, "s::RT"));
        assertTrue(refused.getMessage().startsWith("an executeInDb's SQL in a query"), refused.getMessage());
        com.legend.sql.SqlQuery lowered = ProductStatements.lowerTestDataSeed(Compiler.query(ctx,
                "|#>{s::DB.T}#->select(~[ID])"), "s::RT");
        assertInstanceOf(com.legend.sql.SqlSelect.class, lowered);
    }

    /** The lowering's own steps are the planner's (the C4 audit's M5, 2026-11-13; the follow-up audit's G1,
     *  2026-11-14): ProductStatements' {@code planned}, {@code lower}, both {@code lowerer}s and {@code storeWrite},
     *  BodyPlanner's {@code lowerAndPrepare}s, {@code lowererFor}, {@code prepare} and {@code bare}, PlanMaker's
     *  {@code copyInto} and EnginePlans' {@code engineSql}s, {@code engineTestSql} and {@code planModel}, package-private;
     *  the public lowering statics that remain, and the Lowerer's constructors, pinned to the planners by
     *  ArchitectureTest.PLANNER_ENTRIES_ONLY (default-deny: a listed entry point alone). A class past the
     *  planner reaches a lowering by these public routes alone, each refusing an executeInDb's read by name first or
     *  pinned to its callers: the query doors (TypedQuery's plans, Execution's runs, QueryDoor first);
     *  {@code ProductStatements.prepare}, whose callers ArchitectureTest pins to the planner and the test planner; the
     *  seed's one entry ({@code lowerTestDataSeed}); and {@code Compiler.lowerResolved}, which refuses inside itself
     *  ({@link #theRootsLowerResolvedRefusesRawSqlInsideItself}). The tests' helper {@link Lowered} refuses raw SQL
     *  first, as the product's doors and the seed's entry do. */
    @Test
    void theLoweringsStepsArePackagePrivateAndTheTestsHelperRefusesRawSql() {
        java.util.Set<String> open = new java.util.TreeSet<>();
        for (java.lang.reflect.Method m : ProductStatements.class.getDeclaredMethods()) {
            if (java.util.Set.of("planned", "lower", "lowerer", "configured", "storeWrite").contains(m.getName())
                    && java.lang.reflect.Modifier.isPublic(m.getModifiers())) {
                open.add("ProductStatements." + m.getName());
            }
        }
        // the follow-up audit's G1, its second part (2026-11-14): the snapshot's copy, the engine text's lowerings and the
        // body planner's bare lowering, whose callers are the planner's own
        Map<Class<?>, java.util.Set<String>> narrowed = Map.of(
                BodyPlanner.class, java.util.Set.of("lowerAndPrepare", "lowererFor", "prepare", "bare"),
                PlanMaker.class, java.util.Set.of("copyInto"),
                // the final audit of 9af16fbe58's J1 (2026-11-14): their callers the planner's own
                SidePlanner.class, java.util.Set.of("prepare"),
                PlanAllocations.class, java.util.Set.of("node"),
                EnginePlans.class, java.util.Set.of("engineSql", "engineTestSql", "planModel"));
        narrowed.forEach((owner, names) -> {
            for (java.lang.reflect.Method m : owner.getDeclaredMethods()) {
                if (names.contains(m.getName()) && java.lang.reflect.Modifier.isPublic(m.getModifiers())) {
                    open.add(owner.getSimpleName() + "." + m.getName());
                }
            }
        });
        assertEquals(java.util.Set.of(), open, "the lowering's steps reachable past the planner");
        com.legend.compiler.element.ModelContext ctx = Compiler.compileModel(WRITES);
        com.legend.TypedQuery raw = Compiler.query(ctx, "{|" + CONN + "let n = meta::relational::metamodel::execute"
                + "::executeInDb('select 7 as A', $c).columnNames;\n#>{s::DB.T}#->select(~[ID]);}");
        com.legend.error.NotImplementedException refused = assertThrows(
                com.legend.error.NotImplementedException.class, () -> Lowered.lower(raw, "s::RT", false));
        assertTrue(refused.getMessage().startsWith("an executeInDb's SQL in a query"), refused.getMessage());
    }

    /** The root's {@code Compiler.lowerResolved} types, resolves and lowers on its own; it refuses an executeInDb's
     *  read by name inside itself, as the seed's entry does (the follow-up audit's G1, 2026-11-14: until then it lowered
     *  one, reproduced from a class past the planner); the same query without the read is lowered. */
    @Test
    void theRootsLowerResolvedRefusesRawSqlInsideItself() {
        com.legend.compiler.element.ModelContext ctx = Compiler.compileModel(WRITES);
        com.legend.protocol.spec.ValueSpecification raw = Compiler.parseQuery("{|" + CONN
                + "let n = meta::relational::metamodel::execute::executeInDb('select 7 as A', $c).columnNames;\n"
                + "#>{s::DB.T}#->select(~[ID]);}");
        com.legend.protocol.spec.ValueSpecification resolved = Compiler.resolveQuery(
                ((com.legend.protocol.spec.LambdaFunction) raw).body(), com.legend.model.ImportScope.empty(), ctx);
        com.legend.error.NotImplementedException refused = assertThrows(com.legend.error.NotImplementedException.class,
                () -> Compiler.lowerResolved(resolved, ctx, "s::RT", false));
        assertTrue(refused.getMessage().startsWith("an executeInDb's SQL in a query"), refused.getMessage());
        com.legend.protocol.spec.ValueSpecification plain = Compiler.resolveQuery(((com.legend.protocol.spec.LambdaFunction)
                Compiler.parseQuery("|#>{s::DB.T}#->select(~[ID])")).body(), com.legend.model.ImportScope.empty(), ctx);
        assertInstanceOf(com.legend.sql.SqlSelect.class, Compiler.lowerResolved(plain, ctx, "s::RT", false));
    }

    /** The plan handles' scans answer "no rows" for a lowering's failure, but never for the raw-SQL refusal, which
     *  propagates by name (the final audit of 9af16fbe58's J7, 2026-11-14: the catch swallowed lowerResolved's). */
    @Test
    void theLineageScansLetTheRawSqlRefusalThrough() {
        com.legend.compiler.element.ModelContext ctx = Compiler.compileModel(WRITES);
        com.legend.protocol.spec.ValueSpecification resolved = Compiler.resolveQuery(((com.legend.protocol.spec
                .LambdaFunction) Compiler.parseQuery("{|" + CONN + "let n = meta::relational::metamodel::execute"
                + "::executeInDb('select 7 as A', $c).columnNames;\n#>{s::DB.T}#->select(~[ID]);}")).body(),
                com.legend.model.ImportScope.empty(), ctx);
        com.legend.error.NotImplementedException refusal = assertThrows(com.legend.error.NotImplementedException.class,
                () -> Compiler.lowerResolved(resolved, ctx, "s::RT", false));
        assertTrue(QueryDoor.isRawSqlRefusal(refusal));
        assertEquals(false, PlanAllocations.noLineageRows(refusal));
        assertEquals(true, PlanAllocations.noLineageRows(new com.legend.error.NotImplementedException("a wall")));
        assertEquals(true, PlanAllocations.noLineageRows(new IllegalStateException("a wall")));
        // the same words from another refusal are not it: the exact refusal alone
        assertEquals(true, PlanAllocations.noLineageRows(new IllegalStateException(refusal.getMessage())));
    }

    @Test
    void aDdlTextIsThePlansConstantAndHasNoSqlText() {
        // D17: a DDL text generator's text is the model's, answered with nothing sent; the query's SQL text (the
        // lowering, the planner's road: Lowered.lower in tests) has none, refused by name
        String query = "|meta::relational::functions::toDDL::createTableStatement(test::DB, 'T')";
        com.legend.TypedQuery typed = Compiler.query(Compiler.compileModel(StorelessRuntime.with(LEGACY,
                Databases.PLATFORM)), query);
        ExecutionPlan.Constant c = assertInstanceOf(ExecutionPlan.Constant.class,
                typed.executionPlan(StorelessRuntime.RUNTIME).root());
        assertTrue(c.cells().get(0).get(0).toUpperCase(java.util.Locale.ROOT).startsWith("CREATE TABLE"),
                c.cells().toString());
        com.legend.error.NotImplementedException none = assertThrows(com.legend.error.NotImplementedException.class,
                () -> Lowered.lower(typed, StorelessRuntime.RUNTIME, false));
        assertTrue(none.getMessage().contains("DDL text"), none.getMessage());
    }

    @Test
    void aProgramOfConstantsAloneRunsWithNoSession() throws Exception {
        // B2: a program that sends nothing answers its constant; no session is opened
        ExecutionPlan plan = plan("|'alone'");
        com.legend.result.Collector collector = new com.legend.result.Collector();
        PlanSessions.Source none = PlanSessions.refusing("a program that sends nothing opened a session");
        // a product plan of one constant: a program of that one step (the plan-as-data design's step 3, 2026-11-08: exec's
        // program runner, its verdict steps the test plan's)
        com.legend.exec.ProgramRunner.run(new com.legend.exec.ProgramRunner.Program(plan.parameters(),
                List.of(new com.legend.exec.ProgramRunner.Node(plan.root(), null))), Map.of(), none,
                com.legend.executionplan.DataStores.none(), (id, open) -> {
                    throw new AssertionError("no callback");
                }, new com.legend.exec.ProgramRunner.ProgramSink() {
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
                }, java.util.UUID.randomUUID());
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
