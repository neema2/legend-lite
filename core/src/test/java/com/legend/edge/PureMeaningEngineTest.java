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
 * {@code LEGEND_LITE_PURE_MEANING=engine} THROUGH EACH ROUTE THE DETECTOR COVERS, RUN (A3's engine rewrite; the user's
 * ruling of ~20:05; the compat owner's ask of ~21:10: a flagged request runs on each route, so nothing downstream
 * re-resolves the rewritten native by name). Each request is planned and run by the edge's own runner on the
 * connection's declared test data (H2): the answer is legend-engine's (the database's own SUBSTRING and position over
 * the arguments as given), and the runner is handed the rewritten query (the natives found by FunctionId, the String
 * {@code substring} and {@code indexOf} gone from it).
 *
 * <p>Until the core switch (A3 piece 2) is under this tree, lite's own {@code substring} and {@code indexOf} still mean
 * SQL's, so {@code warn} answers the same as {@code engine} here; the both-ways halves (warn answers legend-pure's) join
 * these tests on the tree that carries piece 2.
 */
class PureMeaningEngineTest {

    /** {@link PureMeaningCheckTest#MODEL} with three rows declared on its H2 connection: 'abcdef' (SQL's
     *  {@code substring(1, 3)} is 'abc', Pure's 'bc'), 'xoxo' (SQL's {@code indexOf('o')} is 2, Pure's 1) and 'abc'. */
    static final String MODEL = PureMeaningCheckTest.MODEL.replace("specification: LocalH2 { };",
            "specification: LocalH2 { testDataSetupCSV: 'default\\nPERSON\\nID,NAME,CODE\\n1,abcdef,a\\n2,xoxo,b\\n3,abc,c\\n'; };");

    private static final LiteV1Api.Runner EDGE = EdgeServer.runner(null);

    /** The edge's runner, recording each query it is handed: what the route planned and ran. */
    private static final class Recording implements LiteV1Api.Runner {
        final java.util.List<com.legend.TypedQuery> handed = new java.util.ArrayList<>();

        @Override
        public void run(com.legend.TypedQuery query, String runtime, com.legend.result.Output output)
                throws java.io.IOException {
            handed.add(query);
            EDGE.run(query, runtime, output);
        }
    }

    /** The natives the query's typed body calls, by FunctionId: the rewritten calls are the core's, and no String
     *  {@code substring} or 2-argument {@code indexOf} is left. */
    private static void assertRewritten(com.legend.TypedQuery q) {
        java.util.List<com.legend.model.FunctionId> ids = new java.util.ArrayList<>();
        for (com.legend.compiler.spec.typed.TypedSpec s : q.body()) {
            natives(s, ids);
        }
        assertTrue(ids.stream().anyMatch(com.legend.builtin.Pure.AT_LEGEND_LITE_SUBSTRING_AS_GIVEN::contains), ids.toString());
        assertTrue(ids.stream().noneMatch(com.legend.builtin.Pure.AT_STRING_SUBSTRING::contains), ids.toString());
        assertTrue(ids.stream().noneMatch(com.legend.model.FunctionId.of(
                com.legend.builtin.Pure.INDEX_OF__STRING_1__STRING_1)::equals), ids.toString());
    }

    private static void natives(com.legend.compiler.spec.typed.TypedSpec n, java.util.List<com.legend.model.FunctionId> ids) {
        if (n instanceof com.legend.compiler.spec.typed.TypedNativeCall nc) {
            ids.add(nc.callee().id());
        }
        for (com.legend.compiler.spec.typed.TypedSpec c : n.children()) {
            natives(c, ids);
        }
    }

    private static final String QUERY = "|" + PureMeaningCheckTest.TABLE + "->sort(~ID->ascending())"
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

    private static LiteV1Api.Answer route(String path, String rawQuery, String body, PureMeaningCheck.Mode mode) {
        return PureV1Routes.route(path, rawQuery, body, EDGE, LiteV1Api.Models.COMPILED, mode);
    }

    @Test
    void executeAnswersLegendEnginesMeaning_runningTheRewrittenQuery() {
        Recording run = new Recording();
        LiteV1Api.Answer engine = PureV1Routes.route("/api/pure/v1/execution/execute", null, body(QUERY), run,
                LiteV1Api.Models.COMPILED, PureMeaningCheck.Mode.ENGINE);
        assertEquals(200, engine.status(), engine.json());
        assertEquals("engine substring/3=1, indexOf/2=1", engine.headers().get(PureMeaningCheck.HEADER));
        assertTrue(engine.json().contains("[\"abc\",0]") && engine.json().contains("[\"xox\",2]"), engine.json());
        assertEquals(1, run.handed.size());
        assertRewritten(run.handed.get(0));
    }

    @Test
    void executeAsArrowAnswersUnderEngine_runningTheRewrittenQuery() {
        Recording run = new Recording();
        LiteV1Api.Answer a = PureV1Routes.route("/api/pure/v1/execution/execute", "serializationFormat=ARROW_IPC",
                body(QUERY), run, LiteV1Api.Models.COMPILED, PureMeaningCheck.Mode.ENGINE);
        assertEquals(200, a.status(), a.json());
        assertEquals("engine substring/3=1, indexOf/2=1", a.headers().get(PureMeaningCheck.HEADER));
        assertEquals("FormatNotSet", a.headers().get("x-legend-response-format"));
        assertEquals(1, run.handed.size());
        assertRewritten(run.handed.get(0));
    }

    @Test
    void generatePlanPlansTheRewrittenQueryUnderEngine() {
        LiteV1Api.Answer engine = route("/api/pure/v1/execution/generatePlan", null, body(QUERY),
                PureMeaningCheck.Mode.ENGINE);
        assertEquals(200, engine.status(), engine.json());
        assertEquals("engine substring/3=1, indexOf/2=1", engine.headers().get(PureMeaningCheck.HEADER));
        assertTrue(engine.json().contains("rootExecutionNode"), engine.json());
    }

    // ---- the three test-data routes: the seed's query is the one whose meaning chooses the keys ------------------

    /** Under SQL's meaning {@code 'abcdef'->substring(1, 3)} is 'abc', so the row with ID 3 is the seed (under Pure's
     *  it is 'bc', and no row is, once piece 2 is under this tree). On a literal: the test-data scan reads a function
     *  applied to a class's property as a property hop ("scalar 'name' in MID position"), on every setting, a gap of its
     *  own (ScanRelations' OPERATORS end a chain; substring is not one). */
    private static final String SEEDED = "|t::Person.all()->filter(p|$p.name == 'abcdef'->substring(1, 3))"
            + "->project([p|$p.name], ['n'])";

    private static String testDataBody(boolean identifiers) {
        String function = PureV1Routes.grammarToJsonLambda(SEEDED, true).json();
        Map<String, Object> model = new java.util.LinkedHashMap<>();
        model.put("_type", "text");
        model.put("code", MODEL);
        String ids = identifiers
                ? "[{\"table\":{\"_type\":\"table\",\"table\":\"PERSON\",\"schema\":\"default\",\"database\":\"t::DB\"},"
                        + "\"rowIdentifiers\":[{\"columnValuePairs\":[{\"name\":\"ID\",\"value\":1}]}]}]"
                : "[]";
        return "{\"function\":" + function + ",\"mapping\":\"t::M\",\"runtime\":{\"_type\":\"runtimePointer\","
                + "\"runtime\":\"t::RT\"},\"model\":" + Json.toCompact(model) + ",\"tableRowIdentifiers\":" + ids + "}";
    }

    @Test
    void generateSeedDataChoosesLegendEnginesKeysUnderEngine() {
        String path = "/api/pure/v1/execution/testDataGeneration/generateSeedData";
        LiteV1Api.Answer engine = route(path, null, testDataBody(false), PureMeaningCheck.Mode.ENGINE);
        assertEquals(200, engine.status(), engine.json());
        assertEquals("engine substring/3=1", engine.headers().get(PureMeaningCheck.HEADER));
        assertTrue(engine.json().contains("\"value\":3"), engine.json());
    }

    @Test
    void generateTestDataWithTheDefaultSeedRunsUnderEngine() {
        String path = "/api/pure/v1/execution/testDataGeneration/generateTestData_WithDefaultSeed";
        LiteV1Api.Answer engine = route(path, null, testDataBody(false), PureMeaningCheck.Mode.ENGINE);
        assertEquals(200, engine.status(), engine.json());
        assertEquals("engine substring/3=1", engine.headers().get(PureMeaningCheck.HEADER));
        // the seeded row alone: 'abcdef'->substring(1, 3) is 'abc' in the database's own SUBSTRING
        assertTrue(engine.json().contains("3,abc\n") && !engine.json().contains("abcdef"), engine.json());
    }

    @Test
    void generateTestDataWithASeedRunsUnderEngine() {
        // the identifiers are given, so the rows do not depend on the query's meaning; the route runs and says engine
        String path = "/api/pure/v1/execution/testDataGeneration/generateTestData_WithSeed";
        LiteV1Api.Answer engine = route(path, null, testDataBody(true), PureMeaningCheck.Mode.ENGINE);
        assertEquals(200, engine.status(), engine.json());
        assertEquals("engine substring/3=1", engine.headers().get(PureMeaningCheck.HEADER));
        assertTrue(engine.json().contains("abcdef"), engine.json());
    }

    // ---- refused by name -------------------------------------------------------------------------------------------

    @Test
    void aDatabaseNotMeasuredIsRefusedByNameUnderEngine_beforeItRuns() {
        String unmeasured = PureMeaningCheckTest.MODEL.replace("type: H2; specification: LocalH2 { }; auth: DefaultH2;",
                "type: Postgres; specification: Static { name: 'x'; host: 'h'; port: 5432; };"
                        + " auth: UserNamePassword { baseVaultReference: 'v'; userNameVaultReference: 'u';"
                        + " passwordVaultReference: 'p'; };");
        LiteV1Api.Runner never = (query, runtime, output) -> {
            throw new AssertionError("a refused request is never run");
        };
        LiteV1Api.Answer a = PureV1Routes.route("/api/pure/v1/execution/execute", null, body(unmeasured, QUERY), never,
                LiteV1Api.Models.COMPILED, PureMeaningCheck.Mode.ENGINE);
        assertEquals(500, a.status(), a.json());
        assertTrue(a.json().contains("not measured on Postgres"), a.json());
        assertEquals("substring/3=1, indexOf/2=1", a.headers().get(PureMeaningCheck.HEADER));
    }
}
