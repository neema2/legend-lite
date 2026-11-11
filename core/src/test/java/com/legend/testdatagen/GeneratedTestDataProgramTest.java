// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testdatagen;

import com.legend.Compiler;
import com.legend.ExecuteOptions;
import com.legend.TestExecution;
import com.legend.compiler.element.ModelContext;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.setup.CsvSeed;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import com.legend.testplan.PlanningOptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE TEST-DATA GENERATOR IN A TEST BODY, ON THE PROGRAM ROAD (B2d.2 of the total-wall program, 2026-11-11): a let bound
 * to {@code generateTestData(...)} is planned by {@link TestDataPlanner} before the body is classed -- its
 * {@code sqls} the fetches' texts, planning constants a judge reads as constants; its {@code dataCsvString} the one
 * read-only statement's cell, read where it stands -- and the body runs through {@code TestExecution.runProgram} on
 * every database with an in-process instance (DuckDB, H2, SQLite), each verdict the platform's.
 */
class GeneratedTestDataProgramTest {

    private static final String STORE = """
            ###Relational
            Database t::DB
            (
              Table PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(40), MANAGERID INTEGER)
              Table BICYCLE (ID INTEGER PRIMARY KEY, PERSONID INTEGER)
              Join Person_Bicycle(PERSON.ID = BICYCLE.PERSONID)
            )
            ###Pure
            import meta::relational::metamodel::*;
            Class t::Person { name: String[0..1]; bicycles: t::Bicycle[*]; }
            Class t::Bicycle { id: Integer[1]; }
            function meta::relational::testDataGeneration::getTable(database: Database[1], schema: String[1],
                table: String[1]):meta::relational::metamodel::relation::Table[1]
            {
               $database->meta::relational::metamodel::schema($schema)->toOne()
                   ->meta::relational::metamodel::table($table)->toOne()
            }
            ###Mapping
            Mapping t::M
            (
              t::Person: Relational { ~mainTable [t::DB]PERSON name: [t::DB]PERSON.NAME,
                bicycles: [t::DB]@Person_Bicycle }
              t::Bicycle: Relational { ~mainTable [t::DB]BICYCLE id: [t::DB]BICYCLE.ID }
            )
            """;

    private static final String CSV = "default\nPERSON\nID,NAME,MANAGERID\n1,boss,\n2,ann,1\n3,\"o'neil, jr\",1\n"
            + "-----\ndefault\nBICYCLE\nID,PERSONID\n10,2\n11,3\n12,3\n";

    private static final String QUERY = "|t::Person.all()->project([p|$p.name, p|$p.bicycles.id], ['n', 'b'])";

    /** The body's lets: the query, the mapping, the runtime, the row identifiers of PERSON 2 and 3, the generator. */
    private static final String LETS = "let query = {" + QUERY + "};\n"
            + "let mapping = t::M;\n"
            + "let runtime = t::RT.runtimeValue;\n"
            + "let ids = ^meta::relational::testDataGeneration::TableRowIdentifiers(table ="
            + " meta::relational::testDataGeneration::getTable(t::DB, 'default', 'PERSON'), rowIdentifiers ="
            + " [^meta::relational::testDataGeneration::RowIdentifier(columnValuePairs = zip(['ID'], [2])),"
            + " ^meta::relational::testDataGeneration::RowIdentifier(columnValuePairs = zip(['ID'], [3]))]);\n"
            + "let testData = meta::relational::testDataGeneration::generateTestData($query, $mapping, $runtime, $ids,"
            + " [])->toOne();\n";

    /** The slice of PERSON 2 and 3: the person rows, then their bicycles, each table's lines in order. */
    private static final String SLICE = "default\nPERSON\nID,NAME\n2,ann\n3,\"o'neil, jr\"\n-----\n"
            + "default\nBICYCLE\nID,PERSONID\n10,2\n11,3\n12,3\n-----\n";

    /** Every in-process backend under every judge: the host's, the database's, both. */
    static List<org.junit.jupiter.params.provider.Arguments> backendsAndJudges() {
        List<org.junit.jupiter.params.provider.Arguments> out = new ArrayList<>();
        for (TestBackend b : TestBackends.inProcess()) {
            for (PlanningOptions.JudgeMode m : PlanningOptions.JudgeMode.values()) {
                out.add(org.junit.jupiter.params.provider.Arguments.of(b, m));
            }
        }
        return out;
    }

    @ParameterizedTest
    @MethodSource("backendsAndJudges")
    void theGeneratorsResultIsPlannedAndJudged(TestBackend backend, PlanningOptions.JudgeMode judge) throws Exception {
        ModelContext ctx = Compiler.compileModel(model(backend));
        List<String> texts = new ArrayList<>();
        for (com.legend.sql.SqlQuery f : TestDataPlanner.plan(ctx, query(QUERY), "t::M", List.of(
                new TestDataGenerator.TableRowIds("default", "PERSON", List.of(
                        new TestDataGenerator.RowId(List.of("ID"), List.of(2L)),
                        new TestDataGenerator.RowId(List.of("ID"), List.of(3L))))), null, false).fetches()) {
            texts.add(backend.dialect().render(f));
        }
        assertEquals(2, texts.size(), "one text per fetch");
        // the fetch texts: constants of the plan, judged as constants; the text: the one statement's cell
        assertEquals(List.of("assertSize true", "assertEquals true", "assertEquals true", "assertEquals true"),
                run(backend, judge, ctx, "{|" + LETS
                        + "assertSize($testData.sqls, 2);\n"
                        + "assertEquals('" + pure(texts.get(0)) + "', $testData.sqls->at(0));\n"
                        + "assertEquals('" + pure(texts.get(1)) + "', $testData.sqls->at(1));\n"
                        + "assertEquals('" + pure(SLICE) + "', $testData.dataCsvString);}"),
                backend.name() + " " + judge);
        // a text that differs is the assert's failure, the database's cell read at its turn
        List<String> failed = run(backend, judge, ctx, "{|" + LETS + "assertEquals('nothing', $testData.dataCsvString);}");
        assertEquals(1, failed.size(), backend.name() + " " + judge + ": " + failed);
        assertTrue(failed.get(0).startsWith("assertEquals false"), backend.name() + " " + judge + ": " + failed);
        // a count that differs fails as a count of constants
        assertEquals(List.of("assertSize false"), run(backend, judge, ctx, "{|" + LETS + "assertSize($testData.sqls, 3);}")
                .stream().map(v -> v.substring(0, v.indexOf(' ', "assertSize ".length()))).toList(), backend.name() + " " + judge);
        // the generator in place, its text read where it stands
        assertEquals(List.of("assertEquals true"), run(backend, judge, ctx, "{|" + LETS.substring(0, LETS.indexOf("let testData"))
                + "assertEquals('" + pure(SLICE) + "', meta::relational::testDataGeneration::generateTestData($query,"
                + " $mapping, $runtime, $ids, [])->toOne().dataCsvString);}"), backend.name() + " " + judge);
        // a read of the result the program road does not plan: refused by name
        assertEquals(List.of("NotImplementedException: the test-data generator's result's relationTree: not planned (B2d"
                + " answers its sqls and its dataCsvString)"), run(backend, judge, ctx, "{|" + LETS
                + "assertEquals(1, $testData.relationTree->size());}"), backend.name() + " " + judge);
    }

    /** The store, its data's tables as the harness makes them, and a runtime on {@code backend}'s in-process database. */
    private static String model(TestBackend backend) {
        return STORE + "\n###Connection\nRelationalDatabaseConnection t::C { store: t::DB; " + backend.connectionBody()
                + " }\n###Runtime\nRuntime t::RT { mappings: [t::M]; connections: [ t::DB: [ c0: t::C ] ]; }\n";
    }

    /** {@code body} run through the program entry on a fresh {@code backend} session holding {@link #CSV}: each verdict
     *  as {@code "<assert> <pass> <detail>"}, or the run's refusal as {@code "<exception>: <message>"}. */
    private static List<String> run(TestBackend backend, PlanningOptions.JudgeMode judge, ModelContext ctx,
            String body) throws Exception {
        List<String> verdicts = new ArrayList<>();
        try (Connection c = backend.open()) {
            try (Statement s = c.createStatement()) {
                for (String sql : CsvSeed.sqls(CSV, "t::DB", ctx, backend.dialect())) {
                    s.execute(sql);
                }
            }
            com.legend.testexecutionplan.AssertListener listener = new com.legend.testexecutionplan.AssertListener() {
                @Override
                public void verdict(String assertName, boolean pass, @com.legend.base.Nullable String detail) {
                    verdicts.add(assertName.substring(assertName.lastIndexOf(':') + 1) + " " + pass
                            + (pass ? "" : " " + detail));
                }
            };
            com.legend.protocol.spec.ValueSpecification parsed = Compiler.parseQuery(body);
            List<com.legend.protocol.spec.ValueSpecification> statements =
                    parsed instanceof LambdaFunction lf && lf.parameters().isEmpty() ? lf.body() : List.of(parsed);
            try {
                TestExecution.runProgram(Compiler.queryResolved(ctx, Compiler.resolveQuery(statements,
                        new com.legend.model.ImportScope(List.of()), ctx)), "t::RT",
                        com.legend.exec.SessionSource.given(com.legend.testbackend.TestSessions.over(c)), listener, null,
                        ExecuteOptions.NONE.withJudgeMode(judge));
            } catch (com.legend.error.NotImplementedException e) {
                verdicts.add("NotImplementedException: " + e.getMessage());
            } catch (AssertionError | RuntimeException e) {
                // a failed verdict ends the body (the listener told it); anything else is the test's failure
                if (verdicts.isEmpty() || verdicts.get(verdicts.size() - 1).contains(" true")) {
                    throw e;
                }
            }
        }
        return verdicts;
    }

    /** {@code text} as a Pure string literal's body. */
    static String pure(String text) {
        return text.replace("\\", "\\\\").replace("'", "\\'").replace("\n", "\\n");
    }

    private static LambdaFunction query(String text) {
        return (LambdaFunction) com.legend.compiler.NameResolver.resolveQuery(Compiler.parseQuery(text));
    }
}
