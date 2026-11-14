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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
              Table FLAG (ID INTEGER PRIMARY KEY, ACTIVE BIT)
              Table pet (id INTEGER PRIMARY KEY, name VARCHAR(20))
              Join Person_Bicycle(PERSON.ID = BICYCLE.PERSONID)
            )
            ###Pure
            import meta::relational::metamodel::*;
            Class t::Person { name: String[0..1]; bicycles: t::Bicycle[*]; }
            Class t::Bicycle { id: Integer[1]; }
            Class t::Flag { id: Integer[1]; active: Boolean[0..1]; }
            Class t::Pet { name: String[0..1]; }
            // a user function that reads the database, and one that reaches nothing
            function t::names():String[*] { t::Person.all().name }
            function t::two():Integer[1] { 1 + 1 }
            function meta::relational::testDataGeneration::getTable(database: Database[1], schema: String[1],
                table: String[1]):meta::relational::metamodel::relation::Table[1]
            {
               $database->meta::relational::metamodel::schema($schema)->toOne()
                   ->meta::relational::metamodel::table($table)->toOne()
            }
            // upstream's own test helpers (core_relational/relational/testDataGeneration/tests/testDataGeneration.pure),
            // the extensions argument dropped (lite's platform has no relationalExtensions program)
            function t::assertTestData(s1 : String[1], s2 : String[1], db: Database[1]):Boolean[1]
            {
               let setUpSQLs1 = meta::alloy::service::execution::setUpDataSQLs($s1, $db);
               let setUpSQLs2 = meta::alloy::service::execution::setUpDataSQLs($s2, $db);
               assertSameElements($setUpSQLs1, $setUpSQLs2);
            }
            function t::loadAndTestExecution(query: FunctionDefinition<Any>[1], parametersValues:Any[*],
                mapping: meta::pure::mapping::Mapping[1], runtime: meta::core::runtime::Runtime[1], data:String[1],
                db: Database[1]):Boolean[1]
            {
               let testConnection = $runtime.connectionStores.connection->toOne()
                   ->cast(@meta::external::store::relational::runtime::TestDatabaseConnection);
               let setUpSQLs = meta::alloy::service::execution::setUpDataSQLs($data, $db);
               $setUpSQLs->map(sql | meta::relational::metamodel::execute::executeInDb($sql, $testConnection));
               let plan = meta::pure::executionPlan::executionPlan($query, $mapping, $runtime, []);
               let result = $plan->meta::pure::executionPlan::execute($parametersValues, []);
               $result.values->isNotEmpty() && (!$result.values->at(0)->instanceOf(meta::pure::tds::TabularDataSet)
                   || $result.values->at(0)->cast(@meta::pure::tds::TabularDataSet).rows->isNotEmpty());
            }
            ###Mapping
            Mapping t::M
            (
              t::Person: Relational { ~mainTable [t::DB]PERSON name: [t::DB]PERSON.NAME,
                bicycles: [t::DB]@Person_Bicycle }
              t::Bicycle: Relational { ~mainTable [t::DB]BICYCLE id: [t::DB]BICYCLE.ID }
              t::Flag: Relational { ~mainTable [t::DB]FLAG id: [t::DB]FLAG.ID, active: [t::DB]FLAG.ACTIVE }
              t::Pet: Relational { ~mainTable [t::DB]pet name: [t::DB]pet.name }
            )
            """;

    /** The harness's rows, read by lite's test-data reader: a quoted cell with a comma, one with a line break (the
     *  reader quote-aware, B2d.2), a NULL, two flags. A sixth person, whose name is an empty text, is inserted by
     *  {@link #run} (the reader reads an empty cell as NULL). */
    private static final String CSV = "default\nPERSON\nID,NAME,MANAGERID\n1,boss,\n2,ann,1\n3,\"o'neil, jr\",1\n"
            + "4,\"two\nlines\",1\n5,---null---,1\n-----\ndefault\nBICYCLE\nID,PERSONID\n10,2\n11,3\n12,3\n13,5\n"
            + "-----\ndefault\nFLAG\nID,ACTIVE\n1,true\n2,false\n-----\ndefault\npet\nid,name\n1,rex\n2,tom\n3,kit\n";

    private static final String QUERY = "|t::Person.all()->project([p|$p.name, p|$p.bicycles.id], ['n', 'b'])";

    /** The generator over PERSON 4 and 5: a cell with a line break, a NULL. */
    private static final String LETS_45 = "let query = {" + "|t::Person.all()->project([p|$p.name, p|$p.bicycles.id],"
            + " ['n', 'b'])" + "};\n"
            + "let ids = ^meta::relational::testDataGeneration::TableRowIdentifiers(table ="
            + " meta::relational::testDataGeneration::getTable(t::DB, 'default', 'PERSON'), rowIdentifiers ="
            + " [^meta::relational::testDataGeneration::RowIdentifier(columnValuePairs = zip(['ID'], [4])),"
            + " ^meta::relational::testDataGeneration::RowIdentifier(columnValuePairs = zip(['ID'], [5]))]);\n"
            + "let testData = meta::relational::testDataGeneration::generateTestData($query, t::M, t::RT.runtimeValue,"
            + " $ids, [])->toOne();\n";

    /** The body's lets: the query, the mapping, the runtime, the row identifiers of PERSON 2 and 3, the generator. */
    static final String LETS = "let query = {" + QUERY + "};\n"
            + "let mapping = t::M;\n"
            + "let runtime = t::RT.runtimeValue;\n"
            + "let ids = ^meta::relational::testDataGeneration::TableRowIdentifiers(table ="
            + " meta::relational::testDataGeneration::getTable(t::DB, 'default', 'PERSON'), rowIdentifiers ="
            + " [^meta::relational::testDataGeneration::RowIdentifier(columnValuePairs = zip(['ID'], [2])),"
            + " ^meta::relational::testDataGeneration::RowIdentifier(columnValuePairs = zip(['ID'], [3]))]);\n"
            + "let testData = meta::relational::testDataGeneration::generateTestData($query, $mapping, $runtime, $ids,"
            + " [])->toOne();\n";

    /** The slice of PERSON 2 and 3: the person rows, then their bicycles, each table's lines in order. */
    static final String SLICE = "default\nPERSON\nID,NAME\n2,ann\n3,\"o'neil, jr\"\n-----\n"
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
                        new TestDataGenerator.RowId(List.of("ID"), List.of(3L))))), null, false,
                PlannerViews.of(ctx), TestDataPlanner.Printing.CLEAN).fetches()) {
            texts.add(backend.dialect().render(f));
        }
        assertEquals(2, texts.size(), "one text per fetch");
        // one text as a literal, not computed by the code under test (the audit's L2): DuckDB's first fetch
        if ("duckdb".equals(backend.name())) {
            assertEquals("WITH tdg_0 AS (SELECT root.ID AS ID, root.NAME AS NAME\n  FROM PERSON AS root\n"
                    + "  WHERE (root.ID = 2) OR (root.ID = 3)\n  ORDER BY root.ID NULLS LAST\n  LIMIT 20)\n"
                    + "SELECT *\nFROM tdg_0 AS tdg_0", texts.get(0));
        }
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

    /**
     * assertTestData (B2d.2; the user's ruling of 2026-11-11, "Agreed": it passes or fails EXACTLY as upstream decides,
     * the failure's words lite's own): upstream's helper, inlined, compares the load statements of two test-data texts
     * -- one insert per row, each cell as {@code convertValuesToCsvH2} writes it, the two lists sorted -- so here each
     * table's rows, each cell by that rule, are counted on both sides by the database: rows and tables in any order the
     * same; a cell, a row, a table, or one row as many times, that differs fails; an empty and a {@code ---null---} cell
     * are one NULL; a header's case and a wider row's extra cells do not count; a header-only block compares nothing;
     * the texts' own faults fail at planning, upstream's lookups with upstream's own words.
     */
    @ParameterizedTest
    @MethodSource("backendsAndJudges")
    void theGeneratedTextIsComparedAsUpstreamDecides(TestBackend backend, PlanningOptions.JudgeMode judge)
            throws Exception {
        ModelContext ctx = Compiler.compileModel(model(backend));
        String what = backend.name() + " " + judge;
        String ok = "assert true";
        // every case checked, each that does not hold named (so each is seen on its own)
        List<String> wrong = new ArrayList<>();
        // the same inserts: the slice, its rows and tables in another order, its header in another case, a row wider
        // than its header (upstream reads the header's cells alone), a header-only block (no insert)
        String reordered = "default\nBICYCLE\nID,PERSONID\n12,3\n11,3\n10,2\n-----\n"
                + "default\nPERSON\nID,NAME\n3,\"o'neil, jr\"\n2,ann\n-----\n";
        // and the slice saved with CRLF line endings: a CRLF is a line break (the user's G7 ruling, register S302)
        for (String expected : List.of(SLICE, reordered, SLICE.replace("ID,NAME", "id,Name"),
                SLICE.replace("2,ann", "2,ann,extra"), SLICE + "default\nPERSON\nNAME,MANAGERID\n-----\n",
                SLICE.replace("\n", "\r\n"))) {
            expect(wrong, expected, List.of(ok), compare(backend, judge, ctx, LETS, expected));
        }
        // a cell with a line break and a NULL, as the generated text writes them and the reader reads them back; an
        // empty cell is the NULL too
        String fourFive = "default\nPERSON\nID,NAME\n4,\"two\nlines\"\n5,---null---\n-----\ndefault\nBICYCLE\n"
                + "ID,PERSONID\n13,5\n-----\n";
        for (String expected : List.of(fourFive, fourFive.replace("5,---null---", "5,"))) {
            expect(wrong, expected, List.of(ok), compare(backend, judge, ctx, LETS_45, expected));
        }
        // a cell, a missing row, an extra row, a missing table, a row twice: each fails, the database's verdict; and a
        // padded cell, its spaces kept as upstream keeps them (register S302)
        for (String differs : List.of(SLICE.replace("2,ann", "2,anne"), SLICE.replace("2,ann", "2, ann"),
                SLICE.replace("11,3\n", ""),
                SLICE.replace("12,3\n", "12,3\n14,3\n"), SLICE.substring(0, SLICE.indexOf("default\nBICYCLE")),
                SLICE.replace("10,2\n", "10,2\n10,2\n"))) {
            expectDiffers(wrong, differs, compare(backend, judge, ctx, LETS, differs));
        }
        // the failure's words: the rows of one side the other does not hold as often, each with its count
        expect(wrong, "the words", List.of("assert false the generated test data's rows are not the expected's (each"
                + " table's rows counted, upstream's comparison); the rows of one side the other does not hold as"
                + " often:\ndefault.BICYCLE (ID,PERSONID) expected x2: 10,2\n"
                + "default.BICYCLE (ID,PERSONID) generated x1: 10,2\n"),
                compare(backend, judge, ctx, LETS, SLICE.replace("10,2\n", "10,2\n10,2\n")));
        // decided at planning: a header the generator does not write, a table it did not fetch, a row narrower than
        // its header, and upstream's lookups -- a schema, a table, a column -- with upstream's own words
        Map<String, String> planned = new LinkedHashMap<>();
        planned.put(SLICE.replace("ID,NAME\n2,ann\n3,\"o'neil, jr\"", "NAME,ID\nann,2\n\"o'neil, jr\",3"),
                "the generated test data writes no rows of default.PERSON (NAME,ID): 2 expected");
        planned.put(SLICE + "default\nFLAG\nACTIVE,ID\ntrue,1\n",
                "the generated test data writes no rows of default.FLAG (ACTIVE,ID): 1 expected");
        planned.put(SLICE.replace("2,ann", "2"), "refused: the expected test data's row 1 of default.PERSON has 1"
                + " cell(s) for 2 column(s) (upstream's load fails reading the missing cell)");
        planned.put("other\nPERSON\nID\n2\n", "There should be exactly 1 schema with name other found in the provided"
                + " runtime database(s) [t::DB] at the given connection.");
        planned.put("default\nNOPE\nID\n2\n", "Table \"NOPE\" not found in Schema \"default\" in Database(s) t::DB");
        // a padded schema line read as written, as upstream reads it (register S302)
        planned.put(" default\nPERSON\nID\n2\n", "There should be exactly 1 schema with name  default found in the"
                + " provided runtime database(s) [t::DB] at the given connection.");
        planned.put("default\nPERSON\nID,NOPE\n2,x\n",
                "Column \"NOPE\" not found in Table \"PERSON\" in Database(s) t::DB");
        planned.put("default\nPERSON\nID,NAME\n2,\"ann\n", "the expected test data: the test data ends inside the"
                + " quoted cell that starts on line 4");
        for (Map.Entry<String, String> e : planned.entrySet()) {
            expect(wrong, e.getKey(), List.of("assert false " + e.getValue()),
                    compare(backend, judge, ctx, LETS, e.getKey()));
        }
        // two constant texts: their load statements, compared as they always were
        // (legend-engine's DDL text: the edge's style, S309)
        expect(wrong, "two constants", List.of("assertSameElements true"), run(backend, judge, ctx,
                "{|t::assertTestData('" + pure(SLICE) + "', '" + pure(SLICE) + "', t::DB);}", new ArrayList<>(),
                com.legend.edge.PureText.UPSTREAM));
        assertEquals(List.of(), wrong, what);
    }

    /**
     * A BIT cell compared as upstream's insert writes it ({@code bitValueFromString}, h2Extension2_1_214.pure:131-145):
     * trimmed and lower-cased, {@code y}, {@code 1} and {@code true} are {@code true}, {@code n}, {@code 0} and
     * {@code false} are {@code false}, any other text itself; the generated flags, as each database spells them, the
     * same.
     */
    @ParameterizedTest
    @MethodSource("backendsAndJudges")
    void aBitCellIsComparedAsUpstreamWritesIt(TestBackend backend, PlanningOptions.JudgeMode judge) throws Exception {
        ModelContext ctx = Compiler.compileModel(model(backend));
        String what = backend.name() + " " + judge;
        String lets = "let ids = ^meta::relational::testDataGeneration::TableRowIdentifiers(table ="
                + " meta::relational::testDataGeneration::getTable(t::DB, 'default', 'FLAG'), rowIdentifiers ="
                + " [^meta::relational::testDataGeneration::RowIdentifier(columnValuePairs = zip(['ID'], [1])),"
                + " ^meta::relational::testDataGeneration::RowIdentifier(columnValuePairs = zip(['ID'], [2]))]);\n"
                + "let testData = meta::relational::testDataGeneration::generateTestData({|t::Flag.all()->project("
                + "[f|$f.id, f|$f.active], ['i', 'a'])}, t::M, t::RT.runtimeValue, $ids, [])->toOne();\n";
        List<String> wrong = new ArrayList<>();
        for (String flags : List.of("true,1\nfalse,2", "y,1\nn,2", "1,1\n0,2", " TRUE ,1\nFalse,2", "Y,1\nN,2")) {
            expect(wrong, flags, List.of("assert true"), compare(backend, judge, ctx, lets,
                    "default\nFLAG\nACTIVE,ID\n" + flags + "\n"));
        }
        for (String flags : List.of("false,1\nfalse,2", "yes,1\nfalse,2", "true,1\ntrue,1\nfalse,2")) {
            expectDiffers(wrong, flags, compare(backend, judge, ctx, lets, "default\nFLAG\nACTIVE,ID\n" + flags
                    + "\n"));
        }
        assertEquals(List.of(), wrong, what);
    }

    /** {@code actual} is {@code expected}, else the case {@code label} is named in {@code wrong}. */
    private static void expect(List<String> wrong, String label, Object expected, Object actual) {
        if (!expected.equals(actual)) {
            wrong.add(label + " -> " + actual);
        }
    }

    /** {@code verdicts} are the comparison's one failure, the database's, else the case is named in {@code wrong}. */
    private static void expectDiffers(List<String> wrong, String label, List<String> verdicts) {
        if (verdicts.size() != 1 || !verdicts.get(0).startsWith("assert false the generated test data's rows are not"
                + " the expected's")) {
            wrong.add(label + " -> " + verdicts);
        }
    }

    /** {@code t::assertTestData(expected, $testData.dataCsvString, t::DB)} after {@code lets}, run. */
    private static List<String> compare(TestBackend backend, PlanningOptions.JudgeMode judge, ModelContext ctx,
            String lets, String expected) throws Exception {
        // upstream's helper compares the two texts' load statements in legend-engine's DDL text: the compatibility
        // edge's style (S309, RULED by the user 2026-11-14: the product refuses engine text by name)
        return run(backend, judge, ctx, "{|" + lets + "t::assertTestData('" + pure(expected)
                + "', $testData.dataCsvString, t::DB);}", new ArrayList<>(), com.legend.edge.PureText.UPSTREAM);
    }

    /**
     * loadAndTestExecution (B2d.2, the user's ruling of 2026-11-11, "Read only reload is fine"): upstream reloads the
     * generated text into every table of the database and runs the query again; here the reload sends nothing and the
     * query after it is planned again with each table of the database read from the slice -- the slice's rows, or none --
     * the real tables untouched. Upstream's helper runs whole; a query after the reload sees the slice's rows alone (2
     * persons of 5), a table the slice does not hold empty (4 bicycles in the database); nothing is written (the
     * statements the run sends hold no write); a direct read of the database after the reload is refused by name.
     */
    @ParameterizedTest
    @MethodSource("backendsAndJudges")
    void theQueryRunsOverTheSlice(TestBackend backend, PlanningOptions.JudgeMode judge) throws Exception {
        ModelContext ctx = Compiler.compileModel(model(backend));
        String what = backend.name() + " " + judge;
        // upstream's whole shape: the generator, its text compared, the slice reloaded and the query run over it --
        // every statement sent a read
        List<String> sent = new ArrayList<>();
        assertEquals(List.of("assert true"), run(backend, judge, ctx, "{|" + LETS + "t::assertTestData('" + pure(SLICE)
                + "', $testData.dataCsvString, t::DB);\n"
                + "t::loadAndTestExecution($query, [], $mapping, $runtime, $testData.dataCsvString, t::DB);}", sent),
                what);
        assertFalse(sent.isEmpty(), what);
        for (String statement : sent) {
            String bare = statement.replaceAll("(?s)^(--[^\n]*\n)*\s*", "").toUpperCase(java.util.Locale.ROOT);
            assertTrue(bare.startsWith("SELECT") || bare.startsWith("WITH") || bare.startsWith("SET")
                    || bare.startsWith("PRAGMA") || bare.startsWith("SAVEPOINT") || bare.startsWith("RELEASE")
                    || bare.startsWith("ROLLBACK"), what + ": a statement that is no read: " + statement);
        }
        String reload = "let conn = $runtime.connectionStores.connection->toOne()"
                + "->cast(@meta::external::store::relational::runtime::TestDatabaseConnection);\n"
                + "let loads = meta::alloy::service::execution::setUpDataSQLs($testData.dataCsvString, t::DB);\n"
                + "$loads->map(sql | meta::relational::metamodel::execute::executeInDb($sql, $conn));\n";
        // the query over the slice: its 2 persons, not the database's 5 (an execute, and an execution plan's execute)
        assertEquals(List.of("assertSize true"), run(backend, judge, ctx, "{|" + LETS + reload
                + "let r = meta::pure::router::execute(|t::Person.all()->project([p|$p.name], ['n']), t::M, $runtime,"
                + " []);\nassertSize($r.values.rows, 2);}"), what);
        assertEquals(List.of("assertSize true"), run(backend, judge, ctx, "{|" + LETS + reload
                + "let plan = meta::pure::executionPlan::executionPlan(|t::Person.all()->project([p|$p.name], ['n']),"
                + " t::M, $runtime, []);\nlet r = $plan->meta::pure::executionPlan::execute([], []);\n"
                + "assertSize($r.values->at(0)->cast(@meta::pure::tds::TabularDataSet).rows, 2);}"), what);
        // a table of the database the slice does not hold: no row (upstream re-creates it empty)
        String personsOnly = LETS.replace("p|$p.name, p|$p.bicycles.id], ['n', 'b']", "p|$p.name], ['n']");
        assertEquals(List.of("assertSize true"), run(backend, judge, ctx, "{|" + personsOnly + reload
                + "let r = meta::pure::router::execute(|t::Bicycle.all()->project([b|$b.id], ['id']), t::M, $runtime,"
                + " []);\nassertSize($r.values.rows, 0);}"), what);
        // a text cell the slice holds empty reloads as NULL, as upstream's reload reads it (convertValuesToCsvH2; the
        // audit's M4): person 6's name is an empty text in the database, none in the slice
        String six = LETS.replace("p|$p.name, p|$p.bicycles.id], ['n', 'b']", "p|$p.name], ['n']").replace(
                "zip(['ID'], [2])), ^meta::relational::testDataGeneration::RowIdentifier(columnValuePairs ="
                        + " zip(['ID'], [3]))", "zip(['ID'], [6]))");
        assertEquals(List.of("assertSize true"), run(backend, judge, ctx, "{|" + six + reload
                + "let r = meta::pure::router::execute(|t::Person.all()->filter(p|$p.name->isEmpty())->project("
                + "[p|$p.name], ['n']), t::M, $runtime, []);\nassertSize($r.values.rows, 1);}"), what);
    }

    /**
     * A table and its columns declared in lower case, unquoted, read over the reload (the B checkpoint's G4, 2026-11-12):
     * the slice labels each column by its declared name and the swap reads it by that label, spelled as the plan's own
     * names are, so the pruner and every database read the one spelling -- the slice's DISTINCT dropped, the slice's
     * select is pruned to what the query reads, and a read left in the table's physical spelling would be folded by H2
     * ({@code name} read as {@code NAME}) and miss the label {@code "name"}.
     */
    @ParameterizedTest
    @MethodSource("backendsAndJudges")
    void theReloadReadsALowerCaseColumnByItsOwnLabel(TestBackend backend, PlanningOptions.JudgeMode judge)
            throws Exception {
        ModelContext ctx = Compiler.compileModel(model(backend));
        String lets = "let runtime = t::RT.runtimeValue;\n"
                + "let ids = ^meta::relational::testDataGeneration::TableRowIdentifiers(table ="
                + " meta::relational::testDataGeneration::getTable(t::DB, 'default', 'pet'), rowIdentifiers ="
                + " [^meta::relational::testDataGeneration::RowIdentifier(columnValuePairs = zip(['id'], [1])),"
                + " ^meta::relational::testDataGeneration::RowIdentifier(columnValuePairs = zip(['id'], [2]))]);\n"
                + "let testData = meta::relational::testDataGeneration::generateTestData({|t::Pet.all()->project("
                + "[x|$x.name], ['n'])}, t::M, $runtime, $ids, [])->toOne();\n"
                + "let conn = $runtime.connectionStores.connection->toOne()"
                + "->cast(@meta::external::store::relational::runtime::TestDatabaseConnection);\n"
                + "let loads = meta::alloy::service::execution::setUpDataSQLs($testData.dataCsvString, t::DB);\n"
                + "$loads->map(sql | meta::relational::metamodel::execute::executeInDb($sql, $conn));\n";
        String what = backend.name() + " " + judge;
        assertEquals(List.of("assertSize true"), run(backend, judge, ctx, "{|" + lets
                + "let r = meta::pure::router::execute(|t::Pet.all()->project([x|$x.name], ['n']), t::M, $runtime,"
                + " []);\nassertSize($r.values.rows, 2);}"), what);
        assertEquals(List.of("assertSize true"), run(backend, judge, ctx, "{|" + lets
                + "let r = meta::pure::router::execute(|t::Pet.all()->filter(x|$x.name == 'tom')->project([x|$x.name],"
                + " ['n']), t::M, $runtime, []);\nassertSize($r.values.rows, 1);}"), what);
    }

    /**
     * What the read-only reload cannot answer, refused by name (B2d.2's audit, 2026-11-11): after the reload upstream's
     * database holds the slice to the body's end, so any statement but the query run over it that reaches the database
     * -- a read, a write, an {@code executeInDb}, a user function whose body does, before or after that query -- is
     * refused (H1), never assumed to restore it; a user function that reaches nothing runs. The query must run on the
     * runtime whose connection the slice was reloaded on (M1). A database effect between the generator's let and a
     * read of its result is refused (M2: here the generator's statement runs where its result is read). The
     * generator's runtime argument must be the body's runtime (M5).
     */
    @ParameterizedTest
    @MethodSource("backendsAndJudges")
    void whatTheReloadCannotAnswerIsRefusedByName(TestBackend backend, PlanningOptions.JudgeMode judge)
            throws Exception {
        ModelContext ctx = Compiler.compileModel(model(backend));
        String what = backend.name() + " " + judge;
        String conn = "let conn = $runtime.connectionStores.connection->toOne()"
                + "->cast(@meta::external::store::relational::runtime::TestDatabaseConnection);\n";
        String reload = conn + "let loads = meta::alloy::service::execution::setUpDataSQLs($testData.dataCsvString,"
                + " t::DB);\n$loads->map(sql | meta::relational::metamodel::execute::executeInDb($sql, $conn));\n";
        String query = "let r = meta::pure::router::execute(|t::Person.all()->project([p|$p.name], ['n']), t::M,"
                + " $runtime, []);\nassertSize($r.values.rows, 2);\n";
        String read = "assertSize(t::Person.all()->project([p|$p.name], ['n'])->from(t::M, $runtime), 6);\n";
        String after = "NotImplementedException: a statement that reads or writes the database after the test-data"
                + " generator's slice was reloaded, other than the query run over it (";
        Map<String, String> refusals = new LinkedHashMap<>();
        refusals.put(reload + read, after + "before that query)");
        refusals.put(reload + query + read, after + "after that query)");
        refusals.put(reload + query + "meta::relational::metamodel::execute::executeInDb('DELETE FROM BICYCLE',"
                + " $conn);\n", after + "after that query)");
        refusals.put(reload + query + "assertEquals(6, t::names()->size());\n", after + "after that query)");
        refusals.put(reload + "meta::relational::metamodel::execute::executeInDb('SELECT 1', $conn);\n" + query,
                after + "before that query)");
        refusals.put(reload.replace("$runtime.connectionStores", "t::RT2.runtimeValue.connectionStores") + query,
                "NotImplementedException: the query run over the test-data generator's reloaded slice on a runtime"
                        + " other than the one whose connection the slice was reloaded on: not planned (B2d)");
        String effect = "NotImplementedException: a database effect between the test-data generator's let and a read"
                + " of its result: not planned (B2d: the generator's statement runs where its result is read,"
                + " upstream's at its let, and across the effect the two read different data)";
        refusals.put(conn + "meta::relational::metamodel::execute::executeInDb('DELETE FROM BICYCLE', $conn);\n"
                + "assertEquals('" + pure(SLICE) + "', $testData.dataCsvString);\n", effect);
        refusals.put(conn + "let csv = $testData.dataCsvString;\n"
                + "meta::relational::metamodel::execute::executeInDb('DELETE FROM BICYCLE', $conn);\n"
                + "assertEquals('" + pure(SLICE) + "', $csv);\n", effect);
        refusals.put(conn + "meta::relational::metamodel::execute::executeInDb('DELETE FROM BICYCLE', $conn);\n"
                + "assertSize($testData.sqls, 2);\n", effect);
        List<String> wrong = new ArrayList<>();
        for (Map.Entry<String, String> e : refusals.entrySet()) {
            List<String> verdicts = run(backend, judge, ctx, "{|" + LETS + e.getKey() + "}");
            if (verdicts.size() != 1 || !verdicts.get(0).startsWith(e.getValue())) {
                wrong.add(e.getKey() + " -> " + verdicts);
            }
        }
        // a user function that reaches nothing, after the query: it runs
        expect(wrong, "t::two()", List.of("assertSize true", "assertEquals true"), run(backend, judge, ctx, "{|" + LETS
                + reload + query + "assertEquals(2, t::two());}"));
        // the generator's runtime argument: another runtime element, named or a let's, refused by name
        String other = "NotImplementedException: the test-data generator's runtime argument is t::RT2, not the body's"
                + " runtime t::RT: not planned (B2d sends the generator's statement on the body's session, so its"
                + " runtime must be the body's)";
        expect(wrong, "a let of t::RT2", List.of(other), run(backend, judge, ctx, "{|" + LETS.replace(
                "let runtime = t::RT.runtimeValue", "let runtime = t::RT2.runtimeValue")
                + "assertSize($testData.sqls, 2);}"));
        expect(wrong, "t::RT2", List.of(other), run(backend, judge, ctx, "{|" + LETS_45.replace("t::RT.runtimeValue",
                "t::RT2.runtimeValue") + "assertSize($testData.sqls, 2);}"));
        assertEquals(List.of(), wrong, what);
    }

    /**
     * A seed string ({@code generateSeedDataString}, B2d.3) read where it is read, as a generator's result is (the
     * user's ruling of 2026-11-11, "read where read", register S196): a database effect between its let and a read of
     * it -- directly, or through a let carrying it -- is refused by name, as upstream reads the seed at its let (the B2d.3
     * audit's M5, 2026-11-12: until then the seed string's statement ran after the effect, unguarded); an effect before
     * its let, or none, is not refused by this guard.
     */
    @ParameterizedTest
    @MethodSource("backendsAndJudges")
    void aSeedStringReadAcrossAnEffectIsRefusedByName(TestBackend backend, PlanningOptions.JudgeMode judge)
            throws Exception {
        ModelContext ctx = Compiler.compileModel(model(backend));
        String what = backend.name() + " " + judge;
        String prelude = "let runtime = t::RT.runtimeValue;\n"
                + "let conn = $runtime.connectionStores.connection->toOne()"
                + "->cast(@meta::external::store::relational::runtime::TestDatabaseConnection);\n";
        String let = "let s = meta::relational::testDataGeneration::generateSeedDataString({|t::Person.all()->project("
                + "[p|$p.name], ['n'])}, t::M, $runtime, ^meta::pure::runtime::ExecutionContext(), [], []);\n";
        String seed = prelude + let;
        String effect = "meta::relational::metamodel::execute::executeInDb('DELETE FROM BICYCLE', $conn);\n";
        StringBuilder lines = new StringBuilder();
        for (int i = 1; i <= 6; i++) {
            lines.append(i > 1 ? ",\n" : "").append("       meta::relational::testDataGeneration::createRowIdentifier("
                    + "['ID'], [").append(i).append("])");
        }
        String expected = "\nmeta::relational::testDataGeneration::createTableRowIdentifiers(t::DB, 'default',"
                + " 'PERSON', [\n" + lines + "\n  ])\n";
        String refused = "NotImplementedException: a database effect between the seed string's let"
                + " (generateSeedDataString) and a read of it: not planned (B2d.3: the seed string's statement runs"
                + " where it is read, upstream's at its let, and across the effect the two read different data)";
        List<String> wrong = new ArrayList<>();
        expect(wrong, "an effect, then the read", List.of(refused), run(backend, judge, ctx, "{|" + seed + effect
                + "assertEquals('" + pure(expected) + "', $s);}"));
        expect(wrong, "a carrier, an effect, then the read", List.of(refused), run(backend, judge, ctx, "{|" + seed
                + "let t = $s;\n" + effect + "assertEquals('" + pure(expected) + "', $t);}"));
        // no effect: the seed string, read where it is read
        expect(wrong, "no effect", List.of("assertEquals true"), run(backend, judge, ctx, "{|" + seed
                + "assertEquals('" + pure(expected) + "', $s);}"));
        // an effect before its let, none between: not this guard's (the harness's executeInDb is refused later, at
        // its own turn, by decision D2's rule for a connection whose text's language is unknown)
        List<String> before = run(backend, judge, ctx, "{|" + prelude + effect + let + "assertEquals('"
                + pure(expected) + "', $s);}");
        if (before.size() != 1 || !before.get(0).startsWith("NotImplementedException: executeInDb over a connection")) {
            wrong.add("an effect before the let -> " + before);
        }
        assertEquals(List.of(), wrong, what);
    }

    /**
     * The frame program's last statement after its asserts (B2d.2, BodyClass: upstream's loadAndTestExecution ends with
     * its frame's non-empty check): the body is planned whole before anything runs, so a last statement the program
     * road cannot plan refuses the body by name (never a pass); one it plans runs after the asserts, in statement
     * order -- a failing assert before it is the body's outcome, as upstream's.
     */
    @ParameterizedTest
    @MethodSource("backendsAndJudges")
    void theLastStatementRunsAfterTheAsserts(TestBackend backend, PlanningOptions.JudgeMode judge) throws Exception {
        ModelContext ctx = Compiler.compileModel(model(backend));
        String what = backend.name() + " " + judge;
        List<String> verdicts = run(backend, judge, ctx, "{|" + LETS + "t::assertTestData('" + pure(SLICE.replace(
                "2,ann", "2,anne")) + "', $testData.dataCsvString, t::DB);\n"
                + "t::loadAndTestExecution($query, [], $mapping, $runtime, $testData.dataCsvString, t::DB);}");
        assertEquals(1, verdicts.size(), what + ": " + verdicts);
        assertTrue(verdicts.get(0).startsWith("assert false"), what + ": " + verdicts);
    }

    /** A test-data generator's body planned whole (its comparison, its reload, the query over the slice) round-trips as
     *  JSON: the plan is data (B2d.2's audit, L5). */
    @ParameterizedTest
    @MethodSource("backendsAndJudges")
    void theGeneratorsPlanRoundTripsAsJson(TestBackend backend, PlanningOptions.JudgeMode judge) {
        ModelContext ctx = Compiler.compileModel(model(backend));
        com.legend.testexecutionplan.TestPlan plan = TestExecution.planBody(typed(ctx, "{|" + LETS
                + "t::assertTestData('" + pure(SLICE) + "', $testData.dataCsvString, t::DB);\n"
                + "t::loadAndTestExecution($query, [], $mapping, $runtime, $testData.dataCsvString, t::DB);}"), "t::RT",
                ExecuteOptions.NONE.withJudgeMode(judge));
        org.junit.jupiter.api.Assertions.assertNull(com.legend.testexecutionplan.TestPlanJson.roundTripDifference(plan),
                backend.name() + " " + judge);
    }

    /** The store, its data's tables as the harness makes them, and two runtimes on {@code backend}'s in-process
     *  database: the body's ({@code t::RT}) and another ({@code t::RT2}). */
    static String model(TestBackend backend) {
        return STORE + "\n###Connection\nRelationalDatabaseConnection t::C { store: t::DB; " + backend.connectionBody()
                + " }\n###Runtime\nRuntime t::RT { mappings: [t::M]; connections: [ t::DB: [ c0: t::C ] ]; }\n"
                + "Runtime t::RT2 { mappings: [t::M]; connections: [ t::DB: [ c1: t::C ] ]; }\n";
    }

    /** {@code body} run through the program entry on a fresh {@code backend} session holding {@link #CSV}: each verdict
     *  as {@code "<assert> <pass> <detail>"}, or the run's refusal as {@code "<exception>: <message>"}. */
    private static List<String> run(TestBackend backend, PlanningOptions.JudgeMode judge, ModelContext ctx,
            String body) throws Exception {
        return run(backend, judge, ctx, body, new ArrayList<>());
    }

    /** {@link #run(TestBackend, PlanningOptions.JudgeMode, ModelContext, String)}, each statement the run sent on the
     *  body's session recorded in {@code sent}. */
    private static List<String> run(TestBackend backend, PlanningOptions.JudgeMode judge, ModelContext ctx,
            String body, List<String> sent) throws Exception {
        return run(backend, judge, ctx, body, sent, com.legend.database.PlanStyle.PRODUCT);
    }

    /** {@link #run(TestBackend, PlanningOptions.JudgeMode, ModelContext, String, List)} planned in {@code style}. */
    private static List<String> run(TestBackend backend, PlanningOptions.JudgeMode judge, ModelContext ctx,
            String body, List<String> sent, com.legend.database.PlanStyle style) throws Exception {
        List<String> verdicts = new ArrayList<>();
        boolean[] failed = new boolean[1];
        try (Connection c = backend.open()) {
            try (Statement s = c.createStatement()) {
                for (String sql : CsvSeed.sqls(CSV, "t::DB", ctx, backend.dialect())) {
                    s.execute(sql);
                }
                s.execute("INSERT INTO PERSON (ID, NAME, MANAGERID) VALUES (6, '', 1)");
            }
            com.legend.testexecutionplan.AssertListener listener = new com.legend.testexecutionplan.AssertListener() {
                @Override
                public void verdict(String assertName, boolean pass, @com.legend.base.Nullable String detail) {
                    verdicts.add(assertName.substring(assertName.lastIndexOf(':') + 1) + " " + pass
                            + (pass ? "" : " " + detail));
                    failed[0] = !pass;
                }
            };
            try {
                TestExecution.runProgram(typed(ctx, body), "t::RT",
                        com.legend.exec.SessionSource.given(com.legend.testbackend.TestSessions.over(
                                com.legend.testbackend.RecordingConnection.wrap(c, sent))), listener, null,
                        ExecuteOptions.NONE.withJudgeMode(judge).withStyle(style));
            } catch (com.legend.error.NotImplementedException e) {
                verdicts.add("NotImplementedException: " + e.getMessage());
            } catch (AssertionError | RuntimeException e) {
                // a failed verdict ends the body (the listener told it); anything else is the test's failure
                if (!failed[0]) {
                    throw e;
                }
            }
        }
        return verdicts;
    }

    /** {@code body}, a lambda's text, typed as a test body. */
    static com.legend.TypedQuery typed(ModelContext ctx, String body) {
        com.legend.protocol.spec.ValueSpecification parsed = Compiler.parseQuery(body);
        List<com.legend.protocol.spec.ValueSpecification> statements =
                parsed instanceof LambdaFunction lf && lf.parameters().isEmpty() ? lf.body() : List.of(parsed);
        return Compiler.queryResolved(ctx, Compiler.resolveQuery(statements,
                new com.legend.model.ImportScope(List.of()), ctx));
    }

    /** {@code text} as a Pure string literal's body. */
    static String pure(String text) {
        return text.replace("\\", "\\\\").replace("'", "\\'").replace("\n", "\\n").replace("\r", "\\r");
    }

    private static LambdaFunction query(String text) {
        return (LambdaFunction) com.legend.compiler.NameResolver.resolveQuery(Compiler.parseQuery(text));
    }
}
