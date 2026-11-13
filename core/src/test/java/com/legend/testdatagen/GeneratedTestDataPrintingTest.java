// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testdatagen;

import com.legend.Compiler;
import com.legend.ExecuteOptions;
import com.legend.TestExecution;
import com.legend.compiler.element.ModelContext;
import com.legend.setup.CsvSeed;
import com.legend.testbackend.TestBackend;
import com.legend.testplan.PlanningOptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A TEST BODY'S TEST-DATA TEXT PRINTS A TIMESTAMP AS THE BODY IS JUDGED (L9; the user's ruling of 2026-11-12: "Our clean
 * sheet lite plan should always do the clean/nice/right thing. In backwards compat we can chop the timestamp"): under
 * lite's own rules in full, lite's writers' canonical text; under the compatibility rules
 * ({@code ExecuteOptions.withEngineCompat}) as upstream's text writes it, the fraction cut. On every in-process database,
 * under every judge.
 */
class GeneratedTestDataPrintingTest {

    private static final String STORE = """
            ###Relational
            Database t::DB
            (
              Table EVENT (ID INTEGER PRIMARY KEY, STAMP TIMESTAMP)
            )
            ###Pure
            import meta::relational::metamodel::*;
            Class t::E { id: Integer[1]; stamp: DateTime[0..1]; }
            function meta::relational::testDataGeneration::getTable(database: Database[1], schema: String[1],
                table: String[1]):meta::relational::metamodel::relation::Table[1]
            {
               $database->meta::relational::metamodel::schema($schema)->toOne()
                   ->meta::relational::metamodel::table($table)->toOne()
            }
            ###Mapping
            Mapping t::M
            (
              t::E: Relational { ~mainTable [t::DB]EVENT id: [t::DB]EVENT.ID, stamp: [t::DB]EVENT.STAMP }
            )
            """;

    private static final String CSV = "default\nEVENT\nID,STAMP\n1,2020-01-02 03:04:05.123\n2,---null---\n";

    /** The generator over EVENT 1 and 2, its text asserted equal to {@code expected}. */
    private static String body(String expected) {
        return "{|let q = {|t::E.all()->project([e|$e.stamp], ['s'])};\n"
                + "let ids = ^meta::relational::testDataGeneration::TableRowIdentifiers(table ="
                + " meta::relational::testDataGeneration::getTable(t::DB, 'default', 'EVENT'), rowIdentifiers ="
                + " [^meta::relational::testDataGeneration::RowIdentifier(columnValuePairs = zip(['ID'], [1])),"
                + " ^meta::relational::testDataGeneration::RowIdentifier(columnValuePairs = zip(['ID'], [2]))]);\n"
                + "let testData = meta::relational::testDataGeneration::generateTestData($q, t::M, t::RT.runtimeValue,"
                + " $ids, [])->toOne();\n"
                + "assertEquals('" + expected.replace("\\", "\\\\").replace("'", "\\'").replace("\n", "\\n")
                + "', $testData.dataCsvString);}";
    }

    static List<org.junit.jupiter.params.provider.Arguments> backendsAndJudges() {
        return GeneratedTestDataProgramTest.backendsAndJudges();
    }

    @ParameterizedTest
    @MethodSource("backendsAndJudges")
    void theTextPrintsATimestampAsTheBodyIsJudged(TestBackend backend, PlanningOptions.JudgeMode judge)
            throws Exception {
        String model = STORE + "\n###Connection\nRelationalDatabaseConnection t::C { store: t::DB; "
                + backend.connectionBody() + " }\n###Runtime\n"
                + "Runtime t::RT { mappings: [t::M]; connections: [ t::DB: [ c0: t::C ] ]; }\n";
        ModelContext ctx = Compiler.compileModel(model);
        String what = backend.name() + " " + judge;
        // lite's own rules: the timestamp in full
        assertEquals(List.of("assertEquals true"), run(backend, ctx, body("default\nEVENT\nID,STAMP\n"
                + "1,2020-01-02T03:04:05.123000000+0000\n2,---null---\n-----\n"),
                ExecuteOptions.NONE.withJudgeMode(judge)), what);
        // the compatibility rules: upstream's text, the fraction cut
        assertEquals(List.of("assertEquals true"), run(backend, ctx, body("default\nEVENT\nID,STAMP\n"
                + "1,2020-01-02 03:04:05\n2,---null---\n-----\n"),
                ExecuteOptions.NONE.withJudgeMode(judge).withEngineCompat(true)), what + " compat");
    }

    /** {@code body} run through the program entry on a fresh {@code backend} session holding {@link #CSV}: each verdict
     *  as {@code "<assert> <pass> <detail>"}. */
    private static List<String> run(TestBackend backend, ModelContext ctx, String body, ExecuteOptions options)
            throws Exception {
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
            try {
                TestExecution.runProgram(GeneratedTestDataProgramTest.typed(ctx, body), "t::RT",
                        com.legend.exec.SessionSource.given(com.legend.testbackend.TestSessions.over(c)), listener,
                        null, options);
            } catch (AssertionError | RuntimeException e) {
                // a failed verdict ends the body; the listener told it
                if (verdicts.isEmpty()) {
                    throw e;
                }
            }
        }
        return verdicts;
    }
}
