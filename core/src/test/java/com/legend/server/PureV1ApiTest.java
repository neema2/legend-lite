package com.legend.server;

import com.legend.json.Json;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * legend-lite's {@code pure/v1} answers against legend-engine 4.145.0's OWN answers to the
 * same requests, committed under {@code upstream-api/}
 * (docs/UPSTREAM_ENDPOINTS_DESIGN_2026_09_27.md, U3).
 *
 * <p>E1 is byte-exact. E5 and E9 are compared as whole JSON trees after the RECORDED
 * differences are applied to the engine's side, each named once, below; anything else that
 * differs fails. The SQL text is not compared (two compilers spell SQL two ways): both
 * queries run on H2 over the model's own setup data and must return the same rows.
 */
class PureV1ApiTest {

    /** A runner for a route that must not run anything. */
    private static final PureV1Api.Runner NO_RUN = (model, lambda, runtime, rows) -> {
        throw new AssertionError("nothing is run here");
    };

    /** E8 through the server's driver, as {@code LegendHttpServer} runs it. */
    private static PureV1Api.Answer execute(String body) {
        return PureV1Api.execute(body, new QueryService()::executeUpstream);
    }

    /**
     * RECORDED DIFFERENCE 1, the type vocabulary: legend-engine types a relation's
     * columns with precise primitives; legend-lite's typer has none (the untangle's
     * compiler work). The engine's names map to lite's; a Varchar's size goes with it.
     */
    private static final Map<String, String> VOCABULARY = Map.of(
            "meta::pure::precisePrimitives::Varchar", "String",
            "meta::pure::precisePrimitives::Int", "Integer",
            "meta::pure::precisePrimitives::BigInt", "Integer",
            "meta::pure::precisePrimitives::Double", "Float",
            "meta::pure::precisePrimitives::Numeric", "Decimal",
            "meta::pure::precisePrimitives::Timestamp", "DateTime");

    private final String model = resource("upstream-api/trades-h2.pure");
    private final String query = resource("upstream-api/e1-groupby-sort.pure");
    private final String lambda = resource("upstream-api/e1-groupby-sort.json");

    PureV1ApiTest() throws IOException {
    }

    @Test
    void e1_grammarToJsonLambda_isByteExact() {
        PureV1Api.Answer a = PureV1Api.grammarToJsonLambda(query, true);
        assertEquals(200, a.status(), a.json());
        assertEquals(Json.toCompact(Json.parse(lambda)), Json.toCompact(Json.parse(a.json())));
    }

    @Test
    void e1_textThatIsNotALambda_isWrappedAsTheEngineWrapsIt() throws IOException {
        // the wrapper spans the TOKENS: a trailing comment and blank line do not extend it
        PureV1Api.Answer a = PureV1Api.grammarToJsonLambda(
                rawResource("upstream-api/e1-unprefixed-trailing-comment.pure"), true);
        assertEquals(200, a.status(), a.json());
        assertEquals(Json.toCompact(Json.parse(resource("upstream-api/e1-unprefixed-trailing-comment.json"))),
                Json.toCompact(Json.parse(a.json())));
    }

    @Test
    void e1_withoutSourceInformation_carriesNone() {
        PureV1Api.Answer a = PureV1Api.grammarToJsonLambda(query, false);
        assertFalse(a.json().contains("sourceInformation"), a.json());
    }

    @Test
    void e1_aParseErrorIsTheEnginesErrorShape() {
        PureV1Api.Answer a = PureV1Api.grammarToJsonLambda("1 +", true);
        assertEquals(400, a.status());
        Json.Obj o = Json.parseObject(a.json());
        assertEquals("error", o.getString("status"));
        assertEquals("PARSER", o.getString("errorType"));
    }

    @Test
    void aMalformedRequestIsAnswered500WithNoErrorType_asTheEngineAnswersIt() {
        // measured, 4.145.0: {"function": 1} -> 500, {code, message: "InvalidTypeIdException: ...",
        // status, trace}, no errorType
        PureV1Api.Answer a = PureV1Api.generatePlan("{\"function\": 1}");
        assertEquals(500, a.status(), a.json());
        Json.Obj o = Json.parseObject(a.json());
        assertEquals("error", o.getString("status"));
        assertFalse(o.has("errorType"), a.json());
    }

    @Test
    void generatePlansCompileErrorIs500_lambdaRelationTypesIs400_asTheEngineAnswersThem() {
        String bad = PureV1Api.grammarToJsonLambda(
                "|#>{trades::h2::DB.TRADES_SCHEMA.TRADES}#->select(~[nope])->from(trades::h2::RT)", true).json();
        PureV1Api.Answer plan = PureV1Api.generatePlan("{\"clientVersion\":\"vX_X_X\",\"function\":" + bad
                + ",\"model\":" + textModel() + ",\"context\":{\"_type\":\"BaseExecutionContext\"}}");
        assertEquals(500, plan.status(), plan.json());
        assertEquals("COMPILATION", Json.parseObject(plan.json()).getString("errorType"));
        PureV1Api.Answer type = PureV1Api.lambdaRelationType("{\"model\":" + textModel() + ",\"lambda\":" + bad + "}");
        assertEquals(400, type.status(), type.json());
        assertEquals("COMPILATION", Json.parseObject(type.json()).getString("errorType"));
    }

    @Test
    void e5_lambdaRelationType_isTheEnginesAnswer() throws IOException {
        String request = "{\"model\":" + textModel() + ",\"lambda\":" + lambda + "}";
        PureV1Api.Answer a = PureV1Api.lambdaRelationType(request);
        assertEquals(200, a.status(), a.json());
        Object engine = inLitesVocabulary(Json.parse(resource("upstream-api/e5-groupby-sort.json")));
        assertEquals(Json.toCompact(engine), Json.toCompact(Json.parse(a.json())));
    }

    @Test
    void e9_generatePlan_isTheEnginesPlan_andItsSqlGivesTheEnginesRows() throws Exception {
        String request = "{\"clientVersion\":\"vX_X_X\",\"function\":" + lambda
                + ",\"model\":" + textModel()
                + ",\"context\":{\"_type\":\"BaseExecutionContext\"}}";
        PureV1Api.Answer a = PureV1Api.generatePlan(request);
        assertEquals(200, a.status(), a.json());
        Json.Obj lite = Json.parseObject(a.json());
        Json.Obj engine = Json.parseObject(resource("upstream-api/e9-groupby-sort.json"));

        // the SQL: the same rows, on H2, over the model's own setup data
        Json.Obj liteSql = sqlNode(lite);
        Json.Obj engineSql = sqlNode(engine);
        List<String> setup = engineSql.getObj("connection").getObj("datasourceSpecification")
                .getStringArray("testDataSetupSqls");
        assertEquals(rows(setup, engineSql.getString("sqlQuery")),
                rows(setup, liteSql.getString("sqlQuery")));

        // everything else: the whole plan, the recorded differences applied
        assertEquals(Json.toCompact(comparable(engine)), Json.toCompact(comparable(lite)));
    }

    @Test
    void e8_execute_isTheEnginesResult_rowForRow() throws Exception {
        String request = "{\"clientVersion\":\"vX_X_X\",\"function\":" + lambda
                + ",\"model\":" + textModel()
                + ",\"context\":{\"_type\":\"BaseExecutionContext\"}}";
        PureV1Api.Answer a = execute(request);
        assertEquals(200, a.status(), a.json());
        String engineText = resource("upstream-api/e8-groupby-sort.json");

        // the layout: the engine's streaming serializer's separators, byte for byte
        assertEquals(layout(engineText), layout(a.json()));

        Json.Obj lite = Json.parseObject(a.json());
        Json.Obj engine = Json.parseObject(engineText);
        assertEquals(Json.toCompact(inLitesVocabulary(engine.getObj("builder"))),
                Json.toCompact(lite.getObj("builder")));
        assertEquals(engine.getObj("result").getStringArray("columns"),
                lite.getObj("result").getStringArray("columns"));
        assertEquals(values(engine), values(lite));

        // the activity's SQL: the rows it answers on H2, as the plan's
        List<String> setup = sqlNode(Json.parseObject(resource("upstream-api/e9-groupby-sort.json")))
                .getObj("connection").getObj("datasourceSpecification")
                .getStringArray("testDataSetupSqls");
        assertEquals(rows(setup, activitySql(engine)), rows(setup, activitySql(lite)));
    }

    /** {@code parameterValues}: a lambda's parameters bound to the request's values (Query app G1). */
    @Test
    void e8_execute_bindsParameterValues_scalarAndList() {
        String lam = PureV1Api.grammarToJsonLambda("{region:String[1], minQty:Integer[1], books:String[*]|"
                + "#>{trades::h2::DB.TRADES_SCHEMA.TRADES}#"
                + "->filter(r|(($r.region == $region) && ($r.qty > $minQty)) && $r.book->in($books))"
                + "->select(~[region, book, qty])->from(trades::h2::RT)}", false).json();
        PureV1Api.Answer a = execute("{\"function\":" + lam + ",\"model\":" + textModel()
                + ",\"parameterValues\":["
                + "{\"name\":\"region\",\"value\":{\"_type\":\"string\",\"value\":\"EMEA\"}},"
                + "{\"name\":\"minQty\",\"value\":{\"_type\":\"integer\",\"value\":5}},"
                + "{\"name\":\"books\",\"value\":{\"_type\":\"collection\",\"multiplicity\":{\"lowerBound\":1,\"upperBound\":1},"
                + "\"values\":[{\"_type\":\"string\",\"value\":\"Alpha\"}]}}]}");
        assertEquals(200, a.status(), a.json());
        List<Json.Node> rows = Json.parseObject(a.json()).getObj("result").getArr("rows").items();
        assertFalse(rows.isEmpty(), a.json());
        for (Json.Node r : rows) {
            List<Json.Node> v = ((Json.Obj) r).getArr("values").items();
            assertEquals("EMEA", ((Json.Str) v.get(0)).value());
            assertEquals("Alpha", ((Json.Str) v.get(1)).value());
            assertTrue(((Json.Num) v.get(2)).longValue() > 5, a.json());
        }
    }

    @Test
    void e8_execute_aParameterWithNoValue_isRefused_aStrayValueIgnored_asTheEngineAnswers() {
        String lam = PureV1Api.grammarToJsonLambda("{region:String[1]|#>{trades::h2::DB.TRADES_SCHEMA.TRADES}#"
                + "->filter(r|$r.region == $region)->from(trades::h2::RT)}", false).json();
        PureV1Api.Answer missing = execute("{\"function\":" + lam + ",\"model\":" + textModel() + "}");
        assertEquals(500, missing.status(), missing.json());
        assertTrue(Json.parseObject(missing.json()).getString("message").contains("Missing external parameter(s): region:String[1]"),
                missing.json());
        PureV1Api.Answer stray = execute("{\"function\":" + lam + ",\"model\":" + textModel()
                + ",\"parameterValues\":[{\"name\":\"region\",\"value\":{\"_type\":\"string\",\"value\":\"EMEA\"}},"
                + "{\"name\":\"nope\",\"value\":{\"_type\":\"integer\",\"value\":1}}]}");
        // measured, 4.145.0: a value for no parameter of the lambda is ignored
        assertEquals(200, stray.status(), stray.json());
    }

    /**
     * E8 in upstream's Arrow format, its plan half ({@code arrowPlan}): the schema metadata is the engine's answer
     * to the same request (recorded with its bytes, {@code e8-arrow-groupby-sort.json}), recorded difference 1
     * applied to the builder; the activity's fields are the engine's (no {@code _type}); the SQL gives the engine's
     * rows on H2.
     */
    @Test
    void e8_arrowPlan_isTheEnginesMetadata_andItsSqlGivesTheEnginesRows() throws Exception {
        String request = "{\"clientVersion\":\"vX_X_X\",\"function\":" + lambda
                + ",\"model\":" + textModel()
                + ",\"context\":{\"_type\":\"BaseExecutionContext\"}}";
        PureV1Api.Answer a = PureV1Api.arrowPlan(request, List.of(model));
        assertEquals(200, a.status(), a.json());
        Json.Obj lite = Json.parseObject(a.json());
        Json.Obj liteMeta = lite.getObj("metadata");
        Json.Obj engineMeta = Json.parseObject(resource("upstream-api/e8-arrow-groupby-sort.json")).getObj("metadata");
        assertEquals(new ArrayList<>(engineMeta.fields().keySet()), new ArrayList<>(liteMeta.fields().keySet()));
        assertEquals(Json.toCompact(inLitesVocabulary(Json.parse(engineMeta.getString("legend.builder")))),
                liteMeta.getString("legend.builder"));
        assertEquals(engineMeta.getString("legend.columns"), liteMeta.getString("legend.columns"));
        Json.Obj engineActivity = (Json.Obj) ((Json.Arr) Json.parse(engineMeta.getString("legend.activities"))).items().get(0);
        Json.Obj liteActivity = (Json.Obj) ((Json.Arr) Json.parse(liteMeta.getString("legend.activities"))).items().get(0);
        assertEquals(new ArrayList<>(engineActivity.fields().keySet()), new ArrayList<>(liteActivity.fields().keySet()));
        assertEquals(lite.getString("sql"), liteActivity.getString("sql"));
        List<String> setup = sqlNode(Json.parseObject(resource("upstream-api/e9-groupby-sort.json")))
                .getObj("connection").getObj("datasourceSpecification")
                .getStringArray("testDataSetupSqls");
        assertEquals(rows(setup, engineActivity.getString("sql")), rows(setup, lite.getString("sql")));

        // the JSON answer's builder is the same, from the same writer
        assertEquals(Json.toCompact(Json.parseObject(execute(request).json()).getObj("builder")),
                liteMeta.getString("legend.builder"));
    }

    /** The Arrow plan half runs only the models its host serves; a graph fetch has no Arrow answer. */
    @Test
    void e8_arrowPlan_refusesAModelItsHostDoesNotServe_andAGraphFetch() throws IOException {
        String request = "{\"function\":" + lambda + ",\"model\":" + textModel() + "}";
        PureV1Api.Answer other = PureV1Api.arrowPlan(request, List.of(model + "\n"));
        assertEquals(500, other.status(), other.json());
        assertTrue(Json.parseObject(other.json()).getString("message").contains("the models it serves"), other.json());

        String trading = resource("upstream-api/query-app/trading.pure");
        String fetch = PureV1Api.grammarToJsonLambda("|demo::trading::Trade.all()->graphFetch(#{demo::trading::Trade{tradeId}}#)"
                + "->serialize(#{demo::trading::Trade{tradeId}}#)->from(demo::trading::TradingMapping, demo::trading::H2Runtime)", false).json();
        PureV1Api.Answer graph = PureV1Api.arrowPlan("{\"function\":" + fetch + ",\"model\":"
                + Json.toCompact(Map.of("_type", "text", "code", trading)) + "}", List.of(trading));
        assertEquals(500, graph.status(), graph.json());
        assertTrue(Json.parseObject(graph.json()).getString("message").contains("graph fetch"), graph.json());
    }

    /** route: each path answers as its endpoint does; any other is 404 in the engine's shape; execute runs through the runner. */
    @Test
    void route_answersEachPathAsItsEndpoint() {
        assertEquals(PureV1Api.grammarToJsonLambda(query, false),
                PureV1Api.route("/api/pure/v1/grammar/grammarToJson/lambda", "returnSourceInformation=false", query, NO_RUN));
        assertEquals(PureV1Api.grammarToJsonLambda(query, true),
                PureV1Api.route("/api/pure/v1/grammar/grammarToJson/lambda", null, query, NO_RUN));
        assertEquals(PureV1Api.jsonToGrammarLambda(lambda, "STANDARD"),
                PureV1Api.route("/api/pure/v1/grammar/jsonToGrammar/lambda", "renderStyle=STANDARD", lambda, NO_RUN));
        String typed = "{\"model\":" + textModel() + ",\"lambda\":" + lambda + "}";
        assertEquals(PureV1Api.lambdaRelationType(typed),
                PureV1Api.route("/api/pure/v1/compilation/lambdaRelationType", null, typed, NO_RUN));

        PureV1Api.Answer missing = PureV1Api.route("/api/pure/v1/nope", null, "", NO_RUN);
        assertEquals(404, missing.status());
        assertEquals("{\"code\":-1,\"message\":\"no such legend-engine API in legend-lite: /api/pure/v1/nope\",\"status\":\"error\"}",
                missing.json());

        String[] ran = new String[1];
        PureV1Api.Answer refused = PureV1Api.route("/api/pure/v1/execution/execute", null,
                "{\"function\":" + lambda + ",\"model\":" + textModel() + "}", (m, l, runtime, rows) -> {
                    ran[0] = runtime;
                    throw new IllegalArgumentException("not here");
                });
        assertEquals("trades::h2::RT", ran[0]);
        assertEquals(500, refused.status(), refused.json());
        assertEquals("IllegalArgumentException: not here", Json.parseObject(refused.json()).getString("message"));
    }

    /** A host's own refusal (its database refusing, say) is a database's refusal in the engine's shape. */
    @Test
    void refused_isTheEnginesErrorShape() {
        PureV1Api.Answer a = PureV1Api.refused("ConversionException: no");
        assertEquals(500, a.status());
        assertEquals("{\"code\":-1,\"message\":\"ConversionException: no\",\"status\":\"error\"}", a.json());
    }

    /**
     * G2: a graph fetch through execute is the engine's JSON result -- its recorded answers for
     * the Query app's demo model, compared as JSON trees: one object bare, none as [], checked
     * results with their defects, nesting across to-one and to-many, derived properties.
     */
    @Test
    void e8_execute_graphFetch_isTheEnginesJsonResult() throws IOException {
        String trading = resource("upstream-api/query-app/trading.pure");
        String modelContext = Json.toCompact(Map.of("_type", "text", "code", trading));
        Json.Arr cases = (Json.Arr) Json.parse(resource("upstream-api/query-app/graph-fetch-execute.json"));
        for (Json.Node n : cases.items()) {
            Json.Obj c = (Json.Obj) n;
            String q = c.getString("query");
            String lam = PureV1Api.grammarToJsonLambda(q, false).json();
            PureV1Api.Answer a = execute("{\"function\":" + lam + ",\"model\":" + modelContext + "}");
            assertEquals(200, a.status(), q + " -> " + a.json());
            assertEquals(Json.toCompact(c.get("engine")), Json.toCompact(Json.parse(a.json())), q);
        }
    }

    /** An enumeration column executes as the engine answers it: typed String in the TDS builder (measured, 4.145.0). */
    @Test
    void e8_execute_anEnumerationColumn_isAStringInTheBuilder() throws IOException {
        String trading = resource("upstream-api/query-app/trading.pure");
        String lam = PureV1Api.grammarToJsonLambda("|demo::trading::Firm.all()->project(~[r:x|$x.region, n:x|$x.legalName])"
                + "->from(demo::trading::TradingMapping, demo::trading::H2Runtime)", false).json();
        PureV1Api.Answer a = execute("{\"function\":" + lam + ",\"model\":"
                + Json.toCompact(Map.of("_type", "text", "code", trading)) + "}");
        assertEquals(200, a.status(), a.json());
        Json.Obj col = (Json.Obj) Json.parseObject(a.json()).getObj("builder").getArr("columns").items().get(0);
        assertEquals("String", col.getString("type"));
        assertEquals("AMER", ((Json.Str) ((Json.Obj) Json.parseObject(a.json()).getObj("result").getArr("rows").items().get(0))
                .getArr("values").items().get(0)).value());
    }

    /** C1: a model compiles whole -- elements and every body -- or answers its first failure, 400. */
    @Test
    void c1_compile_okOrTheFirstFailure_asTheEngineAnswers() {
        PureV1Api.Answer ok = PureV1Api.compile(textModel());
        assertEquals(200, ok.status(), ok.json());
        assertEquals("OK", Json.parseObject(ok.json()).getString("message"));
        assertTrue(Json.parseObject(ok.json()).getArr("defects").items().isEmpty(), ok.json());

        // a function body that does not type: the engine answers 400 COMPILATION (measured, 4.145.0)
        String bad = model + "\n###Pure\nfunction trades::h2::broken(): Any[*] { #>{trades::h2::DB.TRADES_SCHEMA.TRADES}#->select(~[nope]) }\n";
        PureV1Api.Answer refused = PureV1Api.compile(Json.toCompact(Map.of("_type", "text", "code", bad)));
        assertEquals(400, refused.status(), refused.json());
        Json.Obj o = Json.parseObject(refused.json());
        assertEquals("COMPILATION", o.getString("errorType"));
        assertTrue(o.getString("message").contains("trades::h2::broken"), refused.json());
    }

    /** E6: a lambda's result type, named as the engine names it (measured, 4.145.0). */
    @Test
    void e6_lambdaReturnType_namesTheTypeAsTheEngineDoes() {
        Map<String, String> expected = new LinkedHashMap<>();
        expected.put("|#>{trades::h2::DB.TRADES_SCHEMA.TRADES}#->select(~[region])", "meta::pure::metamodel::relation::Relation");
        expected.put("|1 + 2", "Integer");
        expected.put("|'a'", "String");
        expected.put("|%2024-01-01", "StrictDate");
        for (Map.Entry<String, String> e : expected.entrySet()) {
            String lam = PureV1Api.grammarToJsonLambda(e.getKey(), false).json();
            PureV1Api.Answer a = PureV1Api.lambdaReturnType("{\"model\":" + textModel() + ",\"lambda\":" + lam + "}");
            assertEquals(200, a.status(), a.json());
            assertEquals(e.getValue(), Json.parseObject(a.json()).getString("returnType"), e.getKey());
        }
        String unbound = PureV1Api.grammarToJsonLambda("|$x.nope", false).json();
        PureV1Api.Answer refused = PureV1Api.lambdaReturnType("{\"model\":" + textModel() + ",\"lambda\":" + unbound + "}");
        assertEquals(400, refused.status(), refused.json());
        assertEquals("COMPILATION", Json.parseObject(refused.json()).getString("errorType"));
    }

    /**
     * A query WITH parameters types as its body, the parameters in scope at their declared types --
     * as the engine answers (and as upstream's Query asks while a query is edited, before any value
     * exists). Before 2026-10-01 lite answered the lambda's own function type
     * ({@code LambdaFunction<{Integer[1] -> Relation<...>[1]}>}) and a one-"column" relation type.
     */
    @Test
    void e5e6_aQueryWithParameters_typesAsItsBody() {
        String select = "#>{trades::h2::DB.TRADES_SCHEMA.TRADES}#->select(~[region])";
        Map<String, String> expected = new LinkedHashMap<>();
        expected.put("{n: Integer[1]|$n + 2}", "Integer");
        expected.put("{r: String[1]|" + select + "->filter(x|$x.region == $r)}", "meta::pure::metamodel::relation::Relation");
        for (Map.Entry<String, String> e : expected.entrySet()) {
            String lam = PureV1Api.grammarToJsonLambda(e.getKey(), false).json();
            PureV1Api.Answer a = PureV1Api.lambdaReturnType("{\"model\":" + textModel() + ",\"lambda\":" + lam + "}");
            assertEquals(200, a.status(), a.json());
            assertEquals(e.getValue(), Json.parseObject(a.json()).getString("returnType"), e.getKey());
        }
        String withParameter = PureV1Api.grammarToJsonLambda("{r: String[1]|" + select + "->filter(x|$x.region == $r)}", false).json();
        String without = PureV1Api.grammarToJsonLambda("|" + select, false).json();
        PureV1Api.Answer typed = PureV1Api.lambdaRelationType("{\"model\":" + textModel() + ",\"lambda\":" + withParameter + "}");
        assertEquals(200, typed.status(), typed.json());
        assertEquals(PureV1Api.lambdaRelationType("{\"model\":" + textModel() + ",\"lambda\":" + without + "}").json(), typed.json(),
                "the same columns as the query without its parameter");
    }

    // ---------------------------------------------------------------------

    /** The result's text with the per-run trace id, the SQL, the values and the type names
     *  (recorded difference 1, compared below) blanked. */
    private static String layout(String result) {
        return result.replaceAll("\"type\":\"[^\"]*\"", "\"type\":\"\"")
                .replaceAll("executionTraceID\\\\\" : \\\\\"[0-9a-f-]+", "executionTraceID")
                .replaceAll("\"sql\":\"(?:[^\"\\\\]|\\\\.)*\"", "\"sql\":\"\"")
                .replaceAll("\\{\"values\": \\[[^\\]]*\\]\\}", "{\"values\": []}");
    }

    private static String activitySql(Json.Obj result) {
        return ((Json.Obj) result.getArr("activities").items().get(0)).getString("sql");
    }

    /** Each row's values, a number compared by its value (the engine writes 528, H2's JSON 528.0). */
    private static List<List<Object>> values(Json.Obj result) {
        List<List<Object>> out = new ArrayList<>();
        for (Json.Node r : result.getObj("result").getArr("rows").items()) {
            List<Object> row = new ArrayList<>();
            for (Json.Node v : ((Json.Obj) r).getArr("values").items()) {
                row.add(v instanceof Json.Num n ? (Object) Double.valueOf(n.doubleValue()) : Json.toCompact(v));
            }
            out.add(row);
        }
        return out;
    }

    private String textModel() {
        return Json.toCompact(Map.of("_type", "text", "code", model));
    }

    private static Json.Obj sqlNode(Json.Obj plan) {
        return (Json.Obj) plan.getObj("rootExecutionNode").getArr("executionNodes").items().get(0);
    }

    /** A plan with its SQL text blanked and the recorded differences applied. */
    private static Object comparable(Json.Obj plan) {
        return rewrite(plan, null);
    }

    private static Object inLitesVocabulary(Json.Node n) {
        return rewrite(n, null);
    }

    /**
     * The recorded differences, applied structurally: the vocabulary (difference 1);
     * RECORDED DIFFERENCE 2, a plan's {@code resultColumns} carry no physical type in
     * lite (its typer has no physical provenance); the SQL text (compared by rows).
     */
    private static Object rewrite(Json.Node n, String key) {
        if (n instanceof Json.Obj o) {
            Map<String, Object> out = new LinkedHashMap<>();
            // a Varchar's size rides its generic type's typeVariableValues; lite's String
            // has none (a Numeric's precision and scale stay: lite's Decimal carries them)
            boolean sizedVarchar = o.getObjOr("rawType", null) instanceof Json.Obj raw
                    && "meta::pure::precisePrimitives::Varchar".equals(raw.getStringOr("fullPath", ""));
            for (Map.Entry<String, Json.Node> e : o.fields().entrySet()) {
                String k = e.getKey();
                Json.Node v = e.getValue();
                if (k.equals("sqlQuery")) {
                    out.put(k, "<compared by rows>");
                } else if (k.equals("dataType") && "resultColumns".equals(key)) {
                    out.put(k, "");
                } else if ((k.equals("fullPath") || k.equals("type")) && v instanceof Json.Str s
                        && VOCABULARY.containsKey(s.value())) {
                    out.put(k, VOCABULARY.get(s.value()));
                } else if (k.equals("typeVariableValues") && sizedVarchar) {
                    out.put(k, List.of());
                } else {
                    out.put(k, rewrite(v, k));
                }
            }
            return out;
        }
        if (n instanceof Json.Arr a) {
            List<Object> out = new ArrayList<>();
            for (Json.Node x : a.items()) {
                out.add(rewrite(x, key));
            }
            return out;
        }
        return n;
    }

    private static List<List<Object>> rows(List<String> setup, String sql) throws SQLException {
        // legend-engine's own H2 settings (its NON_KEYWORDS list, MODE=LEGACY): the setup
        // data names a `year` column, reserved in a bare H2 2.x
        try (Connection c = DriverManager.getConnection(
                "jdbc:h2:mem:" + com.legend.exec.H2Settings.SETTINGS);
                Statement st = c.createStatement()) {
            for (String s : setup) {
                st.execute(s);
            }
            List<List<Object>> out = new ArrayList<>();
            try (ResultSet rs = st.executeQuery(sql)) {
                int n = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    List<Object> row = new ArrayList<>();
                    for (int i = 1; i <= n; i++) {
                        Object v = rs.getObject(i);
                        row.add(v instanceof Number num ? num.doubleValue() : v);
                    }
                    out.add(row);
                }
            }
            return out;
        }
    }

    private static String resource(String name) throws IOException {
        return rawResource(name).strip();
    }

    private static String rawResource(String name) throws IOException {
        try (InputStream in = PureV1ApiTest.class.getClassLoader().getResourceAsStream(name)) {
            if (in == null) {
                throw new IOException("missing test resource " + name);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    // ---- E4: jsonToGrammar/lambda (byte parity with upstream's printer: ComposerParityTest) ----

    @Test
    void e4_jsonToGrammarLambda_printsWhatE1Parsed_plainText_prettyByDefault() {
        String json = PureV1Api.grammarToJsonLambda(query, false).json();
        PureV1Api.Answer standard = PureV1Api.jsonToGrammarLambda(json, "STANDARD");
        assertEquals(200, standard.status(), standard.json());
        assertEquals("text/plain", standard.contentType());
        // the print parses back to the same lambda
        assertEquals(Json.toCompact(Json.parse(json)),
                Json.toCompact(Json.parse(PureV1Api.grammarToJsonLambda(standard.json(), false).json())));
        PureV1Api.Answer pretty = PureV1Api.jsonToGrammarLambda(json, null);
        assertEquals(PureV1Api.jsonToGrammarLambda(json, "PRETTY").json(), pretty.json(), "PRETTY is the default");
        assertTrue(pretty.json().contains("\n"), pretty.json());
    }

    // ---- jsonToGrammar/model (byte parity with upstream's printer, both styles: ModelComposerParityTest) ----

    private static final Json.Config DEEP = new Json.Config(4096);

    /** A model whose function body prints differently in the two styles. */
    private static final String STYLED_MODEL = """
            Class demo::Person
            {
              name: String[1];
            }

            function demo::adults(people: demo::Person[*]): String[*]
            {
              $people->filter(p|$p.name->startsWith('A'))->map(p|$p.name)
            }
            """;

    @Test
    void jsonToGrammarModel_printsWhatE2Parsed_inEitherStyle_plainText_prettyByDefault() {
        String json = PureV1Api.grammarToJsonModel(STYLED_MODEL, false).json();
        PureV1Api.Answer standard = PureV1Api.jsonToGrammarModel(json, "STANDARD");
        PureV1Api.Answer pretty = PureV1Api.jsonToGrammarModel(json, null);
        assertEquals(200, standard.status(), standard.json());
        assertEquals(200, pretty.status(), pretty.json());
        assertEquals("text/plain", standard.contentType());
        assertEquals(PureV1Api.jsonToGrammarModel(json, "PRETTY").json(), pretty.json(), "PRETTY is the default");
        assertFalse(standard.json().equals(pretty.json()), "the function body prints across lines in PRETTY");
        // each print parses back to the same model
        for (PureV1Api.Answer printed : List.of(standard, pretty)) {
            assertEquals(Json.toCompact(Json.parse(json, DEEP)),
                    Json.toCompact(Json.parse(PureV1Api.grammarToJsonModel(printed.json(), false).json(), DEEP)));
        }
        assertEquals(pretty, PureV1Api.route("/api/pure/v1/grammar/jsonToGrammar/model", null, json, NO_RUN));
    }

    @Test
    void jsonToGrammarModel_refusesAModelContextItDoesNotRead_byName() {
        PureV1Api.Answer text = PureV1Api.jsonToGrammarModel("{\"_type\":\"text\",\"code\":\"Class a::B{}\"}", "PRETTY");
        assertEquals(500, text.status(), text.json());
        assertTrue(Json.parseObject(text.json()).getString("message").contains("text"), text.json());
    }

    @Test
    void e4_batch_answersEachKey_andAnUnservedStyleIsRefused() {
        String a = PureV1Api.grammarToJsonLambda("|1 + 2", false).json();
        String b = PureV1Api.grammarToJsonLambda("x: String[1]|$x->toUpper()", false).json();
        PureV1Api.Answer batch = PureV1Api.jsonToGrammarLambdaBatch("{\"a\":" + a + ",\"b\":" + b + "}", "STANDARD");
        assertEquals(200, batch.status(), batch.json());
        assertEquals("{\"a\":\"|1 + 2\",\"b\":\"x: String[1]|$x->toUpper()\"}", batch.json());
        PureV1Api.Answer html = PureV1Api.jsonToGrammarLambda(a, "PRETTY_HTML");
        assertEquals(500, html.status());
        assertTrue(html.json().contains("PRETTY_HTML"), html.json());
    }

    @Test
    void aQueryDeeperThanAConfigFile_isReadAndPrinted() {
        // 80 chained calls nest ~160 JSON levels: past the 64 a configuration file is held to
        String text = "|'a'" + "->toUpper()".repeat(80);
        String json = PureV1Api.grammarToJsonLambda(text, false).json();
        PureV1Api.Answer printed = PureV1Api.jsonToGrammarLambda(json, "STANDARD");
        assertEquals(200, printed.status(), printed.json());
        assertEquals(Json.toCompact(Json.parse(json, new Json.Config(4096))), Json.toCompact(Json.parse(
                PureV1Api.grammarToJsonLambda(printed.json(), false).json(), new Json.Config(4096))));
    }

    @Test
    void olderProtocolShapes_areBroughtCurrent_asUpstreamReadsThem() {
        // a Result variable with no type argument is Result<Any|1..*>; ^Pair(...) is pair(...),
        // named by its full path as upstream's converter names it (so upstream prints it so too)
        String old = """
                {"_type":"lambda","parameters":[{"_type":"var","name":"res","multiplicity":{"lowerBound":1,"upperBound":1},
                  "genericType":{"rawType":{"_type":"packageableType","fullPath":"meta::pure::mapping::Result"},"typeArguments":[],"multiplicityArguments":[],"typeVariableValues":[]}}],
                 "body":[{"_type":"func","function":"new","parameters":[
                   {"_type":"packageableElementPtr","fullPath":"meta::pure::functions::collection::Pair"},
                   {"_type":"string","value":""},
                   {"_type":"collection","values":[
                     {"_type":"keyExpression","key":{"_type":"string","value":"first"},"expression":{"_type":"integer","value":1}},
                     {"_type":"keyExpression","key":{"_type":"string","value":"second"},"expression":{"_type":"string","value":"a"}}]}]}]}
                """;
        PureV1Api.Answer a = PureV1Api.jsonToGrammarLambda(old, "STANDARD");
        assertEquals(200, a.status(), a.json());
        assertEquals("res: meta::pure::mapping::Result<meta::pure::metamodel::type::Any|1..*>[1]"
                + "|1->meta::pure::functions::collection::pair('a')", a.json());
    }
}
