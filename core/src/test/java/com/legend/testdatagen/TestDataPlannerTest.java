// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testdatagen;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.database.Databases;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.setup.CsvSeed;
import com.legend.sql.dialect.SqlDialect;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE GENERATOR AS ONE PLANNED STATEMENT (B2d, the user's approved design of 2026-11-10): on every database with an
 * in-memory instance, the planner's one read-only statement answers the same test-data text as the interleaved run
 * that fetched into temporary tables (TestDataGeneratorRun, the test side's, deleted when the test road takes this
 * plan) -- a two-table join, a self-join (one clause per navigation step), a text cell scrubbed (a quote, a comma) and
 * hashed -- and as many fetch texts; it creates nothing on the database.
 */
class TestDataPlannerTest {

    private static final String MODEL = """
            ###Relational
            Database t::DB
            (
              Table PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(40), MANAGERID INTEGER)
              Table BICYCLE (ID INTEGER PRIMARY KEY, PERSONID INTEGER)
              Join Person_Bicycle(PERSON.ID = BICYCLE.PERSONID)
              Join Person_Manager(PERSON.MANAGERID = {target}.ID)
            )
            ###Pure
            Class t::Person { name: String[0..1]; bicycles: t::Bicycle[*]; manager: t::Person[0..1]; }
            Class t::Bicycle { id: Integer[1]; }
            ###Mapping
            Mapping t::M
            (
              t::Person: Relational { ~mainTable [t::DB]PERSON name: [t::DB]PERSON.NAME,
                bicycles: [t::DB]@Person_Bicycle, manager: [t::DB]@Person_Manager }
              t::Bicycle: Relational { ~mainTable [t::DB]BICYCLE id: [t::DB]BICYCLE.ID }
            )
            """;

    private static final String CSV = "default\nPERSON\nID,NAME,MANAGERID\n1,boss,\n2,ann,1\n3,\"o'neil, jr\",1\n"
            + "4,dan,2\n-----\ndefault\nBICYCLE\nID,PERSONID\n10,2\n11,3\n12,3\n13,4\n";

    static List<DatabaseType> inMemory() throws java.sql.SQLException {
        return ReservedNamesGenerationTest.inMemory();
    }

    @ParameterizedTest
    @MethodSource("inMemory")
    void thePlannedStatementAnswersTheRunsTestData(DatabaseType type) throws Exception {
        SqlDialect dialect = Databases.dialect(type);
        ModelContext ctx = Compiler.compileModel(MODEL);
        List<TestDataGenerator.TableRowIds> twoAndThree = List.of(new TestDataGenerator.TableRowIds("default",
                "PERSON", List.of(new TestDataGenerator.RowId(List.of("ID"), List.of(2L)),
                        new TestDataGenerator.RowId(List.of("ID"), List.of(3L)))));
        List<TestDataGenerator.TableRowIds> four = List.of(new TestDataGenerator.TableRowIds("default", "PERSON",
                List.of(new TestDataGenerator.RowId(List.of("ID"), List.of(4L)))));
        try (Connection c = com.legend.exec.Connectors.of(com.legend.planner.ConnectionPlans.type(type)).openPrivate()) {
            try (Statement s = c.createStatement()) {
                for (String sql : CsvSeed.sqls(CSV, "t::DB", ctx, dialect)) {
                    s.execute(sql);
                }
            }
            // a two-table join, its text scrubbed, then hashed
            for (boolean hash : List.of(false, true)) {
                assertSame(type, c, ctx, dialect, "|t::Person.all()->project([p|$p.name, p|$p.bicycles.id],"
                        + " ['n', 'b'])", twoAndThree, hash);
            }
            // a self-join two steps deep: one clause per navigation step
            assertSame(type, c, ctx, dialect, "|t::Person.all()->project([p|$p.manager.manager.name], ['m'])", four,
                    false);
        }
    }

    /** The planned statement's text, run, equals the run's; one fetch text per fetch; no table made. */
    private static void assertSame(DatabaseType type, Connection c, ModelContext ctx, SqlDialect dialect, String query,
            List<TestDataGenerator.TableRowIds> ids, boolean hash) throws Exception {
        TestDataGeneratorRun.Result run = TestDataGeneratorRun.generate(ctx, query(query), "t::M", ids, null, hash,
                com.legend.exec.PlanSessions.of(com.legend.testbackend.TestSessions.over(c)), target(type), dialect,
                (db, view, temps) -> {
                    throw new AssertionError("no view in this model: " + view);
                });
        TestDataPlanner.Planned planned = TestDataPlanner.plan(ctx, query(query), "t::M", ids, null, hash);
        String sql = dialect.render(planned.csv());
        assertFalse(sql.toUpperCase(java.util.Locale.ROOT).contains("CREATE"), sql);
        String text;
        try (Statement s = c.createStatement(); ResultSet r = s.executeQuery(sql)) {
            assertTrue(r.next(), sql);
            text = r.getString(1);
        }
        assertEquals(run.dataCsvString(), text, type + " " + query + (hash ? " hashed" : "") + ":\n" + sql);
        assertEquals(run.sqls().size(), planned.fetches().size(), type + " " + query + ": one text per fetch");
        // each fetch's own text runs, read-only
        for (com.legend.sql.SqlQuery fetch : planned.fetches()) {
            try (Statement s = c.createStatement(); ResultSet r = s.executeQuery(dialect.render(fetch))) {
                while (r.next()) {
                    r.getObject(1);
                }
            }
        }
    }

    private static com.legend.executionplan.ExecutionPlan.Target target(DatabaseType type) {
        return new com.legend.executionplan.ExecutionPlan.Target(new com.legend.executionplan.ExecutionPlan.Database
                .Platform(com.legend.planner.ConnectionPlans.type(type), null), List.of(), List.of());
    }

    private static LambdaFunction query(String text) {
        return (LambdaFunction) com.legend.compiler.NameResolver.resolveQuery(Compiler.parseQuery(text));
    }
}
