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
}
