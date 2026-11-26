// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.json.Json;
import com.legend.server.LiteV1Api;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * LEGEND-ENGINE'S MEANING THROUGH EACH ROUTE THAT RUNS A QUERY, RUN (the user's ruling of 2026-11-20 ~07:55, "Delete the
 * settings, make engine only mode for engine for everything", claude/compat-program 25ede64f §1: the door always answers
 * legend-engine's meaning, with no setting). Each request is planned and run by the edge's own runner on the
 * connection's declared test data (H2): the answer is legend-engine's (the database's own SUBSTRING and position over
 * the arguments as given, or over legend-engine's corrected positions for {@code substr}). On a database where
 * legend-engine's answer is not yet checked, the request is refused by name while it is planned, before anything is
 * sent; a statement that is not store-bound keeps legend-pure's meaning, as legend-engine computes it on the host.
 * Where each call is placed is {@code EngineTablePlacementTest}'s; since the edition program's step 2 (2026-11-25) the
 * lowering's table decides it ({@link EngineTable}), and the query the runner is handed is the query as written.
 */
class PureMeaningEngineTest {

    /** {@link EngineTablePlacementTest#MODEL} with four rows declared on its H2 connection: 'abcdef' (SQL's
     *  {@code substring(1, 3)} is 'abc', Pure's 'bc'), 'xoxo' (SQL's {@code indexOf('o')} is 2, Pure's 1), 'abc' and
     *  'ab' (past the end of (1, 3) and (0, 3)). */
    static final String MODEL = EngineTablePlacementTest.MODEL.replace("specification: LocalH2 { };",
            "specification: LocalH2 { testDataSetupCSV: 'default\\nPERSON\\nID,NAME,CODE\\n1,abcdef,a\\n2,xoxo,b\\n3,abc,c\\n4,ab,d\\n'; };");

    private static final LiteV1Api.Runner EDGE = EdgeServer.runner(null);

    /** The edge's runner, counting the queries it is handed. */
    private static final class Recording implements LiteV1Api.Runner {
        final java.util.List<com.legend.TypedQuery> handed = new java.util.ArrayList<>();

        @Override
        public void run(com.legend.TypedQuery query, String runtime, com.legend.result.Output output)
                throws java.io.IOException {
            handed.add(query);
            EDGE.run(query, runtime, output);
        }
    }

    /** A runner that plans as the edge's does ({@link EdgeServer#runner}: planned in the door's style, then run) and
     *  must never get as far as running: a refusal is raised while the request is planned, before anything is sent. */
    private static final LiteV1Api.Runner PLANS_AND_NEVER_RUNS = (query, runtime, output) -> {
        query.executionPlan(runtime, java.util.Set.of(), PureText.PURE_V1);
        throw new AssertionError("a refused request is never run");
    };

    private static final String QUERY = "|" + EngineTablePlacementTest.TABLE + "->sort(~ID->ascending())"
            + "->extend(~[a: r|$r.NAME->toOne()->substring(1, 3), b: r|$r.NAME->toOne()->indexOf('o')])"
            + "->select(~[a, b])->from(t::RT)";

    private static String body(String lambdaText) {
        return body(MODEL, lambdaText);
    }

    private static String body(String modelText, String lambdaText) {
        String function = PureV1Routes.grammarToJsonLambda(lambdaText, true).json();
        Map<String, Object> model = new java.util.LinkedHashMap<>();
        model.put("_type", "text");
        model.put("code", modelText);
        return "{\"clientVersion\":\"vX_X_X\",\"function\":" + function + ",\"model\":" + Json.toCompact(model)
                + ",\"context\":{\"_type\":\"BaseExecutionContext\"}}";
    }

    private static LiteV1Api.Answer route(String path, String rawQuery, String body) {
        return PureV1Routes.route(path, rawQuery, body, EDGE, LiteV1Api.Models.COMPILED);
    }

    private static LiteV1Api.Answer execute(String query, Recording run) {
        return PureV1Routes.route("/api/pure/v1/execution/execute", null, body(query), run, LiteV1Api.Models.COMPILED);
    }

    /** The model on a database where a start of 0 differs (its plan read, never run here). */
    private static final String ZERO_START = EngineTablePlacementTest.MODEL.replace(
            "type: H2; specification: LocalH2 { }; auth: DefaultH2;", "type: DuckDB; specification: DuckDB { }; auth: Test;");

    /** Lite's own plan of {@code query} as written, legend-pure's meaning, in legend-pure's door's style: what this
     *  door's plan is not. */
    private static String puresPlan(String model, String query) {
        return com.legend.Compiler.query(com.legend.Compiler.compileModel(model), query)
                .plan("t::RT", PureText.UPSTREAM).sql().toLowerCase(java.util.Locale.ROOT);
    }

    @Test
    void executeAnswersLegendEnginesMeaning() {
        Recording run = new Recording();
        LiteV1Api.Answer a = execute(QUERY, run);
        assertEquals(200, a.status(), a.json());
        assertTrue(a.json().contains("[\"abc\",0]") && a.json().contains("[\"xox\",2]"), a.json());
        assertEquals(1, run.handed.size());
    }

    @Test
    void executeAsArrowAnswersLegendEnginesMeaning() {
        Recording run = new Recording();
        LiteV1Api.Answer a = PureV1Routes.route("/api/pure/v1/execution/execute", "serializationFormat=ARROW_IPC",
                body(QUERY), run, LiteV1Api.Models.COMPILED);
        assertEquals(200, a.status(), a.json());
        assertEquals("FormatNotSet", a.headers().get("x-legend-response-format"));
        assertEquals(1, run.handed.size());
    }

    /** generatePlan plans legend-engine's meaning: its SQL is not lite's own plan of the query as written. */
    @Test
    void generatePlanPlansLegendEnginesMeaning() {
        LiteV1Api.Answer a = route("/api/pure/v1/execution/generatePlan", null, body(QUERY));
        assertEquals(200, a.status(), a.json());
        assertTrue(a.json().contains("rootExecutionNode"), a.json());
        String sql = planSql(MODEL, QUERY).toLowerCase(java.util.Locale.ROOT);
        assertTrue(sql.contains("substr(\"t0\".name, 1, 3)") && sql.contains("locate('o', \"t0\".name)"), sql);
        assertFalse(puresPlan(MODEL, QUERY).contains("substr(\"t0\".name, 1, 3)"));
    }

    /** A graph fetch's qualified property: legend-engine computes it in the database (asked of legend-engine 4.145.0,
     *  E-M1: {@code deskPart()}, a {@code substring}, answers SQL's 'Cre' on H2 and DuckDB). The lowering asks the table
     *  for a graph tree's properties as for any call, so it answers the database's meaning: for 'abcdef',
     *  {@code substring(1, 2)} is 'ab' (legend-pure's 'b') and {@code indexOf('c')} is 3 (legend-pure's 2). Until the
     *  edition program's step 2 the door refused this by name (its rewrite could not reach a graph tree's properties). */
    @Test
    void aGraphFetchsQualifiedProperty_legendEnginesAnswer() {
        String graph = "|t::Person.all()->filter(p|$p.name == 'abcdef')->graphFetch(#{t::Person{name,initials,firstC}}#)"
                + "->serialize(#{t::Person{name,initials,firstC}}#)->from(t::M, t::RT)";
        LiteV1Api.Answer a = route("/api/pure/v1/execution/execute", null, body(graph));
        assertEquals(200, a.status(), a.json());
        assertTrue(a.json().contains("\"initials()\":\"ab\"") && a.json().contains("\"firstC()\":3"), a.json());
    }

    // ---- the class shapes lite serves (the compat owner's ~22:37, the reviewer's a9e827e2e): each asserts
    // legend-engine's probed answer (docs/records/compat-homework/a3-engine-probes/, legend-engine 4.145.0), run on H2;
    // on the second database the plan's SQL, the database's own call over the arguments as given.

    private static final String ONE = "t::Person.all()->filter(p|$p.name == 'abcdef')";

    /** cls.project.sub3: legend-engine's SQL is substring(desk, 1, 3), 'Cre'; here 'abc' (legend-pure's 'bc'). */
    @Test
    void aClassProjectsSubstring_legendEnginesAnswer() {
        LiteV1Api.Answer a = route("/api/pure/v1/execution/execute", null,
                body("|" + ONE + "->project([p|$p.name->substring(1, 3)], ['d'])->from(t::M, t::RT)"));
        assertEquals(200, a.status(), a.json());
        assertTrue(a.json().contains("[\"abc\"]"), a.json());
    }

    /** cls.filter.sub3: legend-engine filters in SQL with substring(desk, 1, 3) = 'Cre'; here 'abcdef' and 'abc'. */
    @Test
    void aClassFiltersSubstring_legendEnginesRows() {
        LiteV1Api.Answer a = route("/api/pure/v1/execution/execute", null,
                body("|t::Person.all()->filter(p|$p.name->substring(1, 3) == 'abc')->sortBy(p|$p.name)"
                        + "->project([p|$p.name], ['n'])->from(t::M, t::RT)"));
        assertEquals(200, a.status(), a.json());
        assertTrue(a.json().contains("[\"abc\"]") && a.json().contains("[\"abcdef\"]")
                && !a.json().contains("[\"ab\"]"), a.json());
    }

    /** cls.toOne.idx and deskAt(): legend-engine's position is 1-based, 3 for 'Credit'; here 3 for 'abcdef' (legend-pure's
     *  2). */
    @Test
    void aClassProjectsIndexOf_legendEnginesAnswer() {
        LiteV1Api.Answer a = route("/api/pure/v1/execution/execute", null,
                body("|" + ONE + "->project([p|$p.name->indexOf('c')], ['i'])->from(t::M, t::RT)"));
        assertEquals(200, a.status(), a.json());
        assertTrue(a.json().contains("[3]"), a.json());
    }

    /** On the second database (cls.toOne.sub0: legend-engine's substring(desk, 0, 2) is 'C' there) the plan holds its own
     *  substr over the arguments as given, which lite's own plan of the query as written does not (legend-pure's
     *  meaning). The 2-argument indexOf is the database's position function. */
    @Test
    void theClassShapesPlanTheDatabasesOwnCalls_whereAStartOfZeroDiffers() {
        String sub0 = "|" + ONE + "->project([p|$p.name->substring(0, 2)], ['d'])->from(t::M, t::RT)";
        String engine = planSql(ZERO_START, sub0).toLowerCase(java.util.Locale.ROOT);
        assertTrue(engine.matches("(?s).*substr\\([^,]*name, 0, 2\\).*"), engine);
        assertFalse(puresPlan(ZERO_START, sub0).matches("(?s).*substr\\([^,]*name, 0, 2\\).*"));
        String idx = planSql(ZERO_START, "|" + ONE + "->project([p|$p.name->indexOf('c')], ['i'])->from(t::M, t::RT)")
                .toLowerCase(java.util.Locale.ROOT);
        assertTrue(idx.contains("strpos(") || idx.contains("instr(") || idx.contains("position("), idx);
    }

    private static String planSql(String model, String query) {
        LiteV1Api.Answer a = route("/api/pure/v1/execution/generatePlan", null, body(model, query));
        assertEquals(200, a.status(), a.json());
        return Json.parseObject(a.json()).getObj("rootExecutionNode").getArr("executionNodes").items().stream()
                .map(n -> ((Json.Obj) n).getString("sqlQuery")).findFirst().orElseThrow();
    }

    // ---- the three test-data routes: the seed's query is the one whose meaning chooses the keys ------------------

    /** Under SQL's meaning {@code 'abcdef'->substring(1, 3)} is 'abc', so the row with ID 3 is the seed (under Pure's it
     *  is 'bc', and no row is). On a literal: the test-data scan reads a function applied to a class's property as a
     *  property hop ("scalar 'name' in MID position"), a gap of its own (ScanRelations' OPERATORS end a chain; substring
     *  is not one). */
    private static final String SEEDED = "|t::Person.all()->filter(p|$p.name == 'abcdef'->substring(1, 3))"
            + "->project([p|$p.name], ['n'])";

    private static String testDataBody(boolean identifiers) {
        return testDataBody(MODEL, SEEDED, identifiers);
    }

    private static String testDataBody(String modelText, String query, boolean identifiers) {
        String function = PureV1Routes.grammarToJsonLambda(query, true).json();
        Map<String, Object> model = new java.util.LinkedHashMap<>();
        model.put("_type", "text");
        model.put("code", modelText);
        String ids = identifiers
                ? "[{\"table\":{\"_type\":\"table\",\"table\":\"PERSON\",\"schema\":\"default\",\"database\":\"t::DB\"},"
                        + "\"rowIdentifiers\":[{\"columnValuePairs\":[{\"name\":\"ID\",\"value\":1}]}]}]"
                : "[]";
        return "{\"function\":" + function + ",\"mapping\":\"t::M\",\"runtime\":{\"_type\":\"runtimePointer\","
                + "\"runtime\":\"t::RT\"},\"model\":" + Json.toCompact(model) + ",\"tableRowIdentifiers\":" + ids + "}";
    }

    @Test
    void generateSeedDataChoosesLegendEnginesKeys() {
        LiteV1Api.Answer a = route("/api/pure/v1/execution/testDataGeneration/generateSeedData", null,
                testDataBody(false));
        assertEquals(200, a.status(), a.json());
        assertTrue(a.json().contains("\"value\":3"), a.json());
    }

    @Test
    void generateTestDataWithTheDefaultSeedAnswersLegendEnginesRows() {
        LiteV1Api.Answer a = route("/api/pure/v1/execution/testDataGeneration/generateTestData_WithDefaultSeed", null,
                testDataBody(false));
        assertEquals(200, a.status(), a.json());
        // the seeded row alone: 'abcdef'->substring(1, 3) is 'abc' in the database's own SUBSTRING
        assertTrue(a.json().contains("3,abc\n") && !a.json().contains("abcdef"), a.json());
    }

    @Test
    void generateTestDataWithASeedAnswersTheIdentifiersRows() {
        // the identifiers are given, so the rows do not depend on the query's calls
        LiteV1Api.Answer a = route("/api/pure/v1/execution/testDataGeneration/generateTestData_WithSeed", null,
                testDataBody(true));
        assertEquals(200, a.status(), a.json());
        assertTrue(a.json().contains("abcdef"), a.json());
    }

    /** The seed on the second database (the reviewer's E-S2): a 3-argument {@code substring} with a literal
     *  start of 0, which that database's literal-0 exemption never covered. The test asserts the SQL of the seed
     *  statement the route hands its runner, planned there, never run: it holds the database's own
     *  {@code substr('abcdef', 0, 3)} (whose answer is 'ab', as legend-engine's would be). A seed lowered in the
     *  product's style (plant W2') plans lite's own range-checked call instead. */
    @Test
    void theSeedIsLegendEngines_whereAStartOfZeroDiffers() {
        String seeded = "|t::Person.all()->filter(p|$p.name == 'abcdef'->substring(0, 3))->project([p|$p.name], ['n'])";
        java.util.List<String> sql = new java.util.ArrayList<>();
        LiteV1Api.Runner planOnly = (query, runtime, output) -> sql.add(query.plan(runtime, PureText.PURE_V1).sql());
        LiteV1Api.Answer a = PureV1Routes.route("/api/pure/v1/execution/testDataGeneration/generateSeedData", null,
                testDataBody(ZERO_START, seeded, false), planOnly, LiteV1Api.Models.COMPILED);
        assertEquals(1, sql.size(), a.json());
        assertTrue(sql.get(0).toLowerCase(java.util.Locale.ROOT).contains("substr('abcdef', 0, 3)"), sql.get(0));
    }

    // ---- substr, the corrected substring and a literal-0 start (2026-11-20) ----------------------------------------
    // legend-engine sends substr, and substring under CORRECT_SQL_SUBSTRING_INDEXING, as the database's SUBSTRING over
    // start + 1 and end - start with no range check (processSubstr), so in range it agrees with legend-pure and out of
    // range it answers the database's cut-short value ('FX'->substr(1, 3) is 'X' on H2 and DuckDB, asked of
    // legend-engine 4.145.0: docs/records/compat-homework/a3-engine-probes/substr/); its literal-0 substring is the
    // database's call as given. Lite's own raises out of range (S383). Here 'ab' is past the end of (1, 3) and (0, 3).

    private static final String CORRECTED = "->meta::pure::executionPlan::featureFlag::withFeatureFlags("
            + "meta::pure::executionPlan::features::Feature.CORRECT_SQL_SUBSTRING_INDEXING)";

    /** (1, 3) over 'abcdef', 'xoxo', 'abc' and 'ab' in legend-engine's SQL: 'ab' is cut short to 'b'. */
    private static final String PAST_THE_END = "{\"values\": [\"bc\"]},{\"values\": [\"ox\"]},"
            + "{\"values\": [\"bc\"]},{\"values\": [\"b\"]}";

    private static String cut(String call) {
        return "|" + EngineTablePlacementTest.TABLE + "->sort(~ID->ascending())->extend(~a: r|$r.NAME->toOne()->" + call
                + ")->select(~[a])->from(t::RT)";
    }

    /** legend-engine's answer for every row. */
    private static void legendEngines(String query, String rows) {
        LiteV1Api.Answer a = route("/api/pure/v1/execution/execute", null, body(query));
        assertEquals(200, a.status(), a.json());
        assertTrue(a.json().contains(rows), a.json());
    }

    @Test
    void substrPastTheEnd_legendEnginesCutShortValue() {
        legendEngines(cut("substr(1, 3)"), PAST_THE_END);
    }

    @Test
    void aCorrectedSubstringPastTheEnd_legendEnginesCutShortValue() {
        legendEngines(cut("substring(1, 3)") + CORRECTED, PAST_THE_END);
    }

    /** The reviewer's M-a1: a literal-0 start is legend-engine's too, in both forms; 'ab'->substring(0, 3) is 'ab', legend-engine's
     *  answer ('abcdef'->substring(0, 9) is 'abcdef' in legend-engine's SQL), where lite's own raises. */
    @Test
    void aLiteralZeroStartPastTheEnd_legendEnginesAnswer() {
        legendEngines(cut("substring(0, 3)"), "{\"values\": [\"abc\"]},{\"values\": [\"xox\"]},"
                + "{\"values\": [\"abc\"]},{\"values\": [\"ab\"]}");
        legendEngines(cut("substring(0)"), "{\"values\": [\"abcdef\"]},{\"values\": [\"xoxo\"]},"
                + "{\"values\": [\"abc\"]},{\"values\": [\"ab\"]}");
    }

    /** A computed index is legend-engine's too: substrAsGiven's positions are SQL arithmetic the database computes.
     *  substr(1, length + 1) runs one past every row's end: legend-engine's SUBSTRING(NAME, 2, length) answers the text
     *  after its first character. */
    @Test
    void aComputedSubstrIndex_legendEnginesAnswer() {
        legendEngines(cut("substr(1, $r.NAME->toOne()->length() + 1)"),
                "{\"values\": [\"bcdef\"]},{\"values\": [\"oxo\"]},{\"values\": [\"bc\"]},{\"values\": [\"b\"]}");
    }

    /** On the second database, the plan holds its own call over legend-engine's positions ('FX'->substr(1, 3) is
     *  substring('FX', 2, 2) in legend-engine's SQL), written as SQL arithmetic the database computes, with no range
     *  check; lite's own plan of the query as written holds its range check (S383). */
    @Test
    void substrPlansTheDatabasesOwnCallOverLegendEnginesPositions_whereAStartOfZeroDiffers() {
        String engine = planSql(ZERO_START, cut("substr(1, 3)")).toLowerCase(java.util.Locale.ROOT);
        assertTrue(engine.matches("(?s).*substr\\([^,]*name, \\(?1 \\+ 1\\)?, \\(?3 - 1\\)?\\).*")
                && !engine.contains("case"), engine);
        assertTrue(puresPlan(ZERO_START, cut("substr(1, 3)")).contains("case"));
    }

    // ---- toOne of an operand that is always empty (register S384) ---------------------------------------------------

    private static final String TO_ONE_OF_EMPTY = "|" + EngineTablePlacementTest.TABLE + "->sort(~ID->ascending())->limit(1)"
            + "->extend(~o: r|[]->cast(@Integer)->toOne())->select(~[o])->from(t::RT)";

    /** legend-engine's null (asked of legend-engine 4.145.0 on H2 and DuckDB: {@code select null as "o"}, the oracle's
     *  {@code raise.toOneOfEmpty}): the table's row lowers it with no check (trustOne's rule, legend-engine's
     *  processNoOp). Lite's own plan of the query as written raises legend-pure's size error. */
    @Test
    void aToOneOfEmpty_legendEnginesNull() {
        LiteV1Api.Answer a = route("/api/pure/v1/execution/execute", null, body(TO_ONE_OF_EMPTY));
        assertEquals(200, a.status(), a.json());
        assertTrue(a.json().contains("\"rows\" : [{\"values\": [null]}]"), a.json());
        // lite's own plan raises at a planned site (its marker, LG#n)
        assertTrue(puresPlan(MODEL, TO_ONE_OF_EMPTY).contains("lg#"), puresPlan(MODEL, TO_ONE_OF_EMPTY));
    }

    /** On a database where legend-engine's answer is not yet checked, refused by name, as every call a row answers is. */
    @Test
    void aToOneOfEmpty_onADatabaseNotYetChecked_isRefusedByName() {
        LiteV1Api.Answer a = PureV1Routes.route("/api/pure/v1/execution/execute", null,
                body(EngineTablePlacementTest.UNCHECKED, TO_ONE_OF_EMPTY), PLANS_AND_NEVER_RUNS,
                LiteV1Api.Models.COMPILED);
        assertEquals(500, a.status(), a.json());
        assertTrue(a.json().contains("toOne(empty)") && a.json().contains("is not yet checked on Postgres"), a.json());
    }

    // ---- a statement that is not store-bound keeps legend-pure's meaning ---------------------------------------------

    /** legend-engine computes a store-free let on the host, before the query, with legend-pure's meaning (H16's probe),
     *  so no row of the table applies in it: 'xabc'->substring(1, 4) is legend-pure's 'abc' (SQL's would be 'xab'), and
     *  'ab'->substring(1, 3) raises, as legend-pure's does (S383). */
    @Test
    void aStatementThatIsNotStoreBound_keepsLegendPuresMeaning_S383Included() {
        LiteV1Api.Answer pure = route("/api/pure/v1/execution/execute", null, body("{|let x = 'xabc'->substring(1, 4); "
                + EngineTablePlacementTest.TABLE + "->filter(r|$r.NAME == $x)->select(~[NAME])->from(t::RT);}"));
        assertEquals(200, pure.status(), pure.json());
        assertTrue(pure.json().contains("\"rows\" : [{\"values\": [\"abc\"]}]"), pure.json());
        LiteV1Api.Answer raised = route("/api/pure/v1/execution/execute", null, body("{|let x = 'ab'->substring(1, 3); "
                + EngineTablePlacementTest.TABLE + "->filter(r|$r.NAME == $x)->select(~[NAME])->from(t::RT);}"));
        assertEquals(500, raised.status(), raised.json());
        assertTrue(raised.json().contains("out of bounds"), raised.json());
    }

    // ---- refused by name -------------------------------------------------------------------------------------------

    /** In legend-engine's envelope, naming the call and the database, never the query's text. */
    @Test
    void aDatabaseNotYetCheckedIsRefusedByName_beforeItRuns() {
        LiteV1Api.Answer a = PureV1Routes.route("/api/pure/v1/execution/execute", null,
                body(EngineTablePlacementTest.UNCHECKED, QUERY), PLANS_AND_NEVER_RUNS, LiteV1Api.Models.COMPILED);
        assertEquals(500, a.status(), a.json());
        Json.Obj o = Json.parseObject(a.json());
        assertEquals("COMPILATION", o.getString("errorType"));
        String message = o.getString("message");
        assertTrue(message.contains("legend-engine's answer for substring/3 at 1:")
                && message.contains("is not yet checked on Postgres"), message);
        assertFalse(message.contains("NAME"), message);
    }

    /** A refused request sends nothing to the database, on every route whose answer a call a row answers chooses: the
     *  refusal is raised while the request is planned (the seed routes plan their seed themselves; execute's runner
     *  plans before it runs). The runner is the routes' one way to a session. */
    @Test
    void aRefusedRequestRunsNoStatement_onEveryRouteWhoseAnswerTheCallChooses() {
        java.util.concurrent.atomic.AtomicInteger runs = new java.util.concurrent.atomic.AtomicInteger();
        LiteV1Api.Runner counting = (query, runtime, output) -> {
            query.executionPlan(runtime, java.util.Set.of(), PureText.PURE_V1);
            runs.incrementAndGet();
        };
        String testData = testDataBody(EngineTablePlacementTest.UNCHECKED,
                "|t::Person.all()->filter(p|$p.name == 'abcdef'->substring(1, 3))->project([p|$p.name], ['n'])", false);
        Map<String, String> routes = new java.util.LinkedHashMap<>();
        routes.put("/api/pure/v1/execution/execute", body(EngineTablePlacementTest.UNCHECKED, QUERY));
        routes.put("/api/pure/v1/execution/generatePlan", body(EngineTablePlacementTest.UNCHECKED, QUERY));
        routes.put("/api/pure/v1/execution/testDataGeneration/generateSeedData", testData);
        routes.put("/api/pure/v1/execution/testDataGeneration/generateTestData_WithDefaultSeed", testData);
        for (Map.Entry<String, String> route : routes.entrySet()) {
            for (String format : java.util.Arrays.asList(null, "serializationFormat=ARROW_IPC")) {
                LiteV1Api.Answer a = PureV1Routes.route(route.getKey(), format, route.getValue(), counting,
                        LiteV1Api.Models.COMPILED);
                assertEquals(500, a.status(), route.getKey() + " " + a.json());
                assertTrue(a.json().contains("is not yet checked on Postgres"), route.getKey() + " " + a.json());
            }
        }
        assertEquals(0, runs.get(), "a refused request reached a run");
        // the partner: the same counting runner IS reached by a query no row answers, so the zero above is the refusal's
        PureV1Routes.route("/api/pure/v1/execution/execute", null, body(EngineTablePlacementTest.UNCHECKED,
                "|" + EngineTablePlacementTest.TABLE + "->select(~[NAME])->from(t::RT)"), counting,
                LiteV1Api.Models.COMPILED);
        assertEquals(1, runs.get(), "a query no row answers runs");
    }

    /** The routes that run no query plan nothing: compile types every body (the constraint and the functions call
     *  substring) on a database not yet checked, and is answered as before. */
    @Test
    void theRoutesThatRunNoQueryAreNotRefused() {
        Map<String, Object> model = new java.util.LinkedHashMap<>();
        model.put("_type", "text");
        model.put("code", EngineTablePlacementTest.UNCHECKED);
        LiteV1Api.Answer a = PureV1Routes.route("/api/pure/v1/compilation/compile", null, Json.toCompact(model),
                PLANS_AND_NEVER_RUNS, LiteV1Api.Models.COMPILED);
        assertEquals(200, a.status(), a.json());
    }

    /** A function value read out of a collection: the lowering asks the table for its body as for any call, so it is
     *  answered (until the edition program's step 2 the door refused it by name: its rewrite could not follow it). */
    @Test
    void aFunctionValueReadOutOfACollection_legendEnginesAnswer() {
        LiteV1Api.Answer a = route("/api/pure/v1/execution/execute", null, body("|" + EngineTablePlacementTest.TABLE
                + "->sort(~ID->ascending())->extend(~i: r|[{s: String[1]|$s->indexOf('o')}]->at(0)->eval($r.NAME->toOne()))"
                + "->select(~[i])->from(t::RT)"));
        assertEquals(200, a.status(), a.json());
        // 'abcdef', 'xoxo', 'abc', 'ab': the database's 1-based position, 0 on a miss
        assertTrue(a.json().contains("{\"values\": [0]},{\"values\": [2]},{\"values\": [0]},{\"values\": [0]}"), a.json());
    }
}
