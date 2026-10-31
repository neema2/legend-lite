// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.legend.Compiler;
import com.legend.TypedQuery;
import com.legend.database.Databases;
import com.legend.error.NotImplementedException;
import com.legend.executionplan.ExecutionPlan;
import com.legend.test.StorelessRuntime;
import com.legend.exec.PlanRunner;
import com.legend.exec.PlanSessions;
import com.legend.testbackend.TestBackends;
import com.legend.testbackend.TestSessions;
import com.legend.testcases.PlanRoundTrip;
import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * THE TEST DOOR (the test-is-the-product plan's M3c, B12 items 1 and 3): a body planned whole as one program on the
 * product's program road; its target carries no setup (R17 C6: a test's seeds are its own steps, M3e); a kind only a
 * test's program holds is the test-planning library's, refused by name until the sub-step that plans it.
 */
class TestDoorTest {

    private static final String SEEDED = """
            Class x::Firm { name: String[1]; }
            ###Relational
            Database x::DB ( Table FIRM (ID INTEGER PRIMARY KEY, NAME VARCHAR(32)) )
            ###Mapping
            Mapping x::M ( *x::Firm: Relational { ~mainTable [x::DB] FIRM name: [x::DB] FIRM.NAME } )
            ###Connection
            RelationalDatabaseConnection x::Conn
            {
                store: x::DB;
                type: H2;
                specification: LocalH2 { testDataSetupCSV: 'default\\nFIRM\\nID,NAME\\n1,Acme\\n'; };
                auth: DefaultH2;
            }
            ###Runtime
            Runtime x::RT { mappings: [x::M]; connections: [ x::DB: [ env: x::Conn ] ]; }
            """;

    @Test
    void aBodyIsOneProgramWhoseTargetCarriesNoSetup() {
        TypedQuery body = Compiler.query(Compiler.compileModel(SEEDED),
                "|x::Firm.all()->project([f|$f.name], ['name'])");
        ExecutionPlan test = TestDoor.plan(body, "x::RT", Set.of(), null);
        PlanRoundTrip.assertRoundTrips(test, "a body through the test door");
        ExecutionPlan.Sequence seq = assertInstanceOf(ExecutionPlan.Sequence.class, test.root());
        assertEquals(1, seq.steps().size());
        ExecutionPlan.TdsResult answer = assertInstanceOf(ExecutionPlan.TdsResult.class, seq.steps().get(0));
        assertEquals(List.of(), answer.sql().target().setup(), "R17 C6: the test door's target carries no setup");
        // the statement is the query door's: one road
        ExecutionPlan query = body.executionPlan("x::RT");
        assertEquals(query.answerSql().statement(), answer.sql().statement());
        assertFalse(query.answerSql().target().setup().isEmpty(), "the query door's target: the declared rows");
    }

    @Test
    void aTestsKindIsTheLibrarysRefusedByNameUntilItsStep() {
        TypedQuery body = Compiler.query(Compiler.compileModel(StorelessRuntime.with("", Databases.PLATFORM)),
                "{|let x = 1; assertEquals(1, $x);}");
        assertEquals("an assert on the test door's program road: not planned yet (the test-is-the-product plan's M6,"
                + " the verdict record)", assertThrows(NotImplementedException.class,
                        () -> TestDoor.plan(body, StorelessRuntime.RUNTIME, Set.of(), null)).getMessage());
    }

    private static final String CONN = "{| let c = ^meta::external::store::relational::runtime::TestDatabaseConnection("
            + "type=meta::relational::runtime::DatabaseType.DuckDB);\n";
    private static final String EXEC = "meta::relational::metamodel::execute::executeInDb(";

    private static ExecutionPlan storeless(String body) {
        ExecutionPlan plan = TestDoor.plan(Compiler.query(Compiler.compileModel(StorelessRuntime.with("",
                Databases.PLATFORM)), body), StorelessRuntime.RUNTIME, Set.of(), null);
        PlanRoundTrip.assertRoundTrips(plan, body);
        return plan;
    }

    private static List<List<Object>> run(ExecutionPlan plan, Connection c) throws Exception {
        return PlanRunner.rows(plan, Map.of(), PlanSessions.of(TestSessions.over(c))).rows();
    }

    private static long count(Connection c, String table) throws Exception {
        try (var st = c.createStatement(); var rs = st.executeQuery("SELECT count(*) FROM " + table)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    /** M3d: consecutive effects are ONE script, any other statement closes it (the walk's segment rule); the script
     *  is its dialect's bracket, run where the session owns its transaction. */
    @Test
    void consecutiveEffectsAreOneScriptAndAnotherStatementClosesIt() throws Exception {
        ExecutionPlan plan = storeless(CONN + EXEC + "'Create Table m3d_a(i INT);', $c, 0, 1000);\n"
                + EXEC + "'Insert into m3d_a values (1);', $c, 0, 1000);\n"
                + "let x = 2;\n"
                + EXEC + "'Insert into m3d_a values (2);', $c, 0, 1000);\n"
                + "'done';}");
        ExecutionPlan.Sequence seq = assertInstanceOf(ExecutionPlan.Sequence.class, plan.root());
        assertEquals(3, seq.steps().size(), seq.steps().toString());
        ExecutionPlan.Effect first = assertInstanceOf(ExecutionPlan.Effect.class, seq.steps().get(0));
        assertEquals(List.of("Create Table m3d_a(i INT)", "Insert into m3d_a values (1)"),
                first.script().statements());
        assertEquals("BEGIN TRANSACTION;\nCreate Table m3d_a(i INT);\nInsert into m3d_a values (1);\nCOMMIT;",
                first.script().bracketed());
        assertEquals("ROLLBACK", first.script().abort());
        assertEquals("Create Table m3d_a(i INT);\nInsert into m3d_a values (1);", first.sql().statement());
        ExecutionPlan.Effect second = assertInstanceOf(ExecutionPlan.Effect.class, seq.steps().get(1));
        assertEquals(List.of("Insert into m3d_a values (2)"), second.script().statements());
        assertInstanceOf(ExecutionPlan.Constant.class, seq.steps().get(2));
        try (Connection c = TestBackends.platform().open()) {
            assertEquals(List.of(List.of("done")), run(plan, c));
            assertEquals(2, count(c, "m3d_a"));
        }
    }

    /** M3d, R17 C11: an effect as the program's last statement answers as the walk's arm does -- no value for
     *  executeInDb, true for the DDL that drops and creates. */
    @Test
    void anEffectAsTheLastStatementAnswersAsTheWalksArm() throws Exception {
        ExecutionPlan plan = storeless(CONN + EXEC + "'Create Table m3d_b(i INT);', $c, 0, 1000);}");
        ExecutionPlan.Sequence seq = assertInstanceOf(ExecutionPlan.Sequence.class, plan.root());
        assertEquals(2, seq.steps().size(), seq.steps().toString());
        assertInstanceOf(ExecutionPlan.Effect.class, seq.steps().get(0));
        ExecutionPlan.Constant answer = assertInstanceOf(ExecutionPlan.Constant.class, seq.steps().get(1));
        assertNull(answer.cells().get(0).get(0));
        try (Connection c = TestBackends.platform().open()) {
            run(plan, c);
            assertEquals(0, count(c, "m3d_b"));
        }
    }

    /** M3d: an effect's String argument is a literal folded; a computed one is the T9 kind M9 plans, refused by
     *  name. */
    @Test
    void aComputedStringArgumentIsRefusedByName() {
        assertEquals("a computed String argument of meta::relational::metamodel::execute::executeInDb on the test"
                + " door's program road: its value is the database's to compute before the effect is written (the T9"
                + " kind the test-is-the-product plan's M9 plans)", assertThrows(NotImplementedException.class,
                        () -> storeless(CONN + EXEC + "'Create Table m3d_' + 1->toString() + '(i INT);', $c, 0,"
                                + " 1000); 1;}")).getMessage());
    }

    /** M3d, R18 (both doors): print plans no step, its argument never evaluated; as the last statement it answers no
     *  value. */
    @Test
    void printPlansNoStepOnEitherDoor() {
        ExecutionPlan.Sequence seq = assertInstanceOf(ExecutionPlan.Sequence.class,
                storeless("{|print('x'); 'y';}").root());
        assertEquals(1, seq.steps().size(), seq.steps().toString());
        ExecutionPlan query = Compiler.query(Compiler.compileModel(StorelessRuntime.with("", Databases.PLATFORM)),
                "|print('x')").executionPlan(StorelessRuntime.RUNTIME);
        ExecutionPlan.Constant none = assertInstanceOf(ExecutionPlan.Constant.class, query.root());
        assertNull(none.cells().get(0).get(0));
    }

    /** M3d: a test's program ending on a handle (a setup that returns its runtime) answers no value, as the walk
     *  does; the query door refuses a handle as an answer. */
    @Test
    void aProgramEndingOnAHandleAnswersNoValueOnTheTestDoorAlone() {
        String handle = CONN + "$c;}";
        ExecutionPlan.Sequence seq = assertInstanceOf(ExecutionPlan.Sequence.class, storeless(handle).root());
        ExecutionPlan.Constant none = assertInstanceOf(ExecutionPlan.Constant.class, seq.steps().get(0));
        assertNull(none.cells().get(0).get(0));
        assertThrows(NotImplementedException.class, () -> Compiler.query(Compiler.compileModel(
                StorelessRuntime.with("", Databases.PLATFORM)), handle).executionPlan(StorelessRuntime.RUNTIME));
    }
}
