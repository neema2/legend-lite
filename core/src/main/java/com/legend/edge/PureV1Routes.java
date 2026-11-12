// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.json.Json;
import com.legend.parser.PmcdParser;
import com.legend.parser.SpecParser;
import com.legend.plan.PlanSupportFunctions;
import com.legend.plan.QueryPlan;
import com.legend.plan.UpstreamRelationType;
import com.legend.protocol.ModelComposer;
import com.legend.protocol.ProtocolReader;
import com.legend.protocol.PureComposer;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.server.ModelInput;
import com.legend.server.PureV1Api;
import com.legend.server.PureV1Api.Answer;
import com.legend.server.PureV1Api.Models;
import com.legend.server.PureV1Api.Runner;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.legend.server.PureV1Api.answer;
import static com.legend.server.PureV1Api.boundParameters;
import static com.legend.server.PureV1Api.error;
import static com.legend.server.PureV1Api.facts;
import static com.legend.server.PureV1Api.request;
import static com.legend.server.PureV1Api.runtimeOf;

/**
 * legend-engine's {@code pure/v1} API, served EXACTLY, at the edge (phase D's D1.0b, the orchestrator's ruling of
 * 2026-11-12: "move pure/v1's upstream routes, TestDataRoutes and the server's composition root to the edge"):
 * upstream's paths, request and answer shapes, and its texts -- each call planned through the edge's dialects
 * ({@link PureText#DIALECTS}, so its SQL and its values' texts are upstream's) and run by the {@link Runner} its host
 * hands it ({@link EdgeServer}'s, the edge's dialects too). Until 2026-11-12 these routes were
 * {@code server.PureV1Api}'s, beside lite's own; lite's own routes ({@code /api/lite/v1}, the hosts' plan halves) stay
 * the product's there (G5: the edge depends on the product, never the reverse). Each call is a pure function of its
 * request, so it is tested without HTTP.
 *
 * <ul>
 *   <li>E1 {@code grammar/grammarToJson/lambda}: Pure text to lambda JSON ({@link SpecParser#lambdaJson},
 *       byte-exact).</li>
 *   <li>E2 {@code grammar/grammarToJson/model}: model text to PMCD JSON
 *       ({@link PmcdParser#parseDocument(String, boolean)}, byte-exact).</li>
 *   <li>E4 {@code grammar/jsonToGrammar/lambda} (and {@code /batch}): lambda JSON to Pure
 *       text ({@link PureComposer}, byte parity with upstream's printer).</li>
 *   <li>{@code grammar/jsonToGrammar/model}: model JSON to Pure text ({@link ModelComposer}, byte parity with
 *       upstream's printer in both styles).</li>
 *   <li>C1 {@code compilation/compile}, E6 {@code compilation/lambdaReturnType}, E5
 *       {@code compilation/lambdaRelationType} ({@link UpstreamRelationType}).</li>
 *   <li>E8 {@code execution/execute}: the query run on its runtime's connection, answered in the engine's TDS JSON;
 *       in upstream's Arrow format ({@code ARROW_IPC}).</li>
 *   <li>E9 {@code execution/generatePlan}: the relational TDS execution plan.</li>
 *   <li>the test-data generation routes ({@link TestDataRoutes}).</li>
 * </ul>
 */
public final class PureV1Routes {

    private PureV1Routes() {
    }

    /** The path prefix these routes answer under. */
    public static final String PREFIX = "/api/pure/v1/";

    /** {@code path}'s answer for {@code body}; execute's run is {@code runner}'s; a model given as text compiled by
     *  {@code models}. A path that is none of upstream's is answered 404, as upstream answers it. */
    public static Answer route(String path, @com.legend.base.Nullable String rawQuery, String body, Runner runner,
            Models models) {
        boolean sourceInformation = rawQuery == null || !rawQuery.contains("returnSourceInformation=false");
        return switch (path) {
            case "/api/pure/v1/grammar/grammarToJson/lambda" -> grammarToJsonLambda(body, sourceInformation);
            case "/api/pure/v1/grammar/grammarToJson/model" -> grammarToJsonModel(body, sourceInformation);
            case "/api/pure/v1/grammar/jsonToGrammar/lambda" ->
                    jsonToGrammarLambda(body, queryParam(rawQuery, "renderStyle"));
            case "/api/pure/v1/grammar/jsonToGrammar/lambda/batch" ->
                    jsonToGrammarLambdaBatch(body, queryParam(rawQuery, "renderStyle"));
            case "/api/pure/v1/grammar/jsonToGrammar/model" -> jsonToGrammarModel(body, queryParam(rawQuery, "renderStyle"));
            case "/api/pure/v1/compilation/lambdaRelationType" -> lambdaRelationType(body, models);
            case "/api/pure/v1/compilation/compile" -> compile(body);
            case "/api/pure/v1/compilation/lambdaReturnType" -> lambdaReturnType(body, models);
            case "/api/pure/v1/execution/generatePlan" -> generatePlan(body, models);
            case "/api/pure/v1/execution/execute" -> "ARROW_IPC".equals(queryParam(rawQuery, "serializationFormat"))
                    ? executeArrow(body, runner, models) : execute(body, runner, models);
            case "/api/pure/v1/execution/testDataGeneration/generateSeedData" ->
                    TestDataRoutes.seedData(body, runner, models);
            case "/api/pure/v1/execution/testDataGeneration/generateTestData_WithSeed" ->
                    TestDataRoutes.withSeed(body, runner, models);
            case "/api/pure/v1/execution/testDataGeneration/generateTestData_WithDefaultSeed" ->
                    TestDataRoutes.withDefaultSeed(body, runner, models);
            case "/api/pure/v1/testData/generation/DONOTUSE_generateTestData" -> TestDataRoutes.census(body, models);
            default -> error(404, null, "no such legend-engine API in legend-lite: " + path);
        };
    }

    /** {@link #route(String, String, String, Runner, Models)}, a model given as text compiled per call. */
    public static Answer route(String path, @com.legend.base.Nullable String rawQuery, String body, Runner runner) {
        return route(path, rawQuery, body, runner, Models.COMPILED);
    }

    /** One query parameter's (decoded) value, or null. */
    private static @com.legend.base.Nullable String queryParam(@com.legend.base.Nullable String rawQuery, String name) {
        if (rawQuery == null) {
            return null;
        }
        for (String pair : rawQuery.split("&")) {
            int eq = pair.indexOf('=');
            String k = eq < 0 ? pair : pair.substring(0, eq);
            if (k.equals(name)) {
                return eq < 0 ? "" : java.net.URLDecoder.decode(pair.substring(eq + 1), java.nio.charset.StandardCharsets.UTF_8);
            }
        }
        return null;
    }

    /** E1: a lambda's text to its protocol JSON. Text without a leading {@code |} is
     *  wrapped in a parameterless lambda spanning the whole text, as the engine does. */
    public static Answer grammarToJsonLambda(String text, boolean returnSourceInformation) {
        return answer(400, "PARSER", () -> SpecParser.lambdaJson(text, returnSourceInformation));
    }

    /** E2: a model's text to its PMCD JSON. */
    public static Answer grammarToJsonModel(String text, boolean returnSourceInformation) {
        return answer(400, "PARSER", () -> PmcdParser.parseDocument(text, returnSourceInformation));
    }

    /**
     * E4 {@code grammar/jsonToGrammar/lambda}: a lambda's protocol JSON to its Pure text, as
     * upstream prints it ({@link PureComposer}; byte parity pinned by ComposerParityTest).
     * {@code text/plain}; {@code renderStyle} is {@code PRETTY} (upstream's default) or
     * {@code STANDARD}.
     */
    public static Answer jsonToGrammarLambda(String body, @com.legend.base.Nullable String renderStyle) {
        Answer a = answer(500, null, () -> PureComposer.lambda(body, style(renderStyle)));
        return a.status() == 200 ? new Answer(200, a.json(), "text/plain") : a;
    }

    /**
     * {@code grammar/jsonToGrammar/model}: a model's protocol JSON ({@code PureModelContextData},
     * {@code {"_type":"data","elements":[...]}}, with or without its section index) as Pure text, as legend-engine
     * prints it ({@link ModelComposer}; byte parity in both styles pinned by ModelComposerParityTest).
     * {@code text/plain}; {@code renderStyle} as for a lambda. Recorded difference: the engine also takes a model
     * context of another kind (a pointer it loads from an SDLC, a text it parses); lite reads
     * {@code PureModelContextData} only and refuses another {@code _type} by name. An element kind lite cannot print
     * is refused by name, never printed approximately.
     */
    public static Answer jsonToGrammarModel(String body, @com.legend.base.Nullable String renderStyle) {
        Answer a = answer(500, null, () -> ModelComposer.model(body, style(renderStyle)));
        return a.status() == 200 ? new Answer(200, a.json(), "text/plain") : a;
    }

    /** E4 {@code grammar/jsonToGrammar/lambda/batch}: {@code {key: lambda}} to {@code {key: text}}. */
    public static Answer jsonToGrammarLambdaBatch(String body, @com.legend.base.Nullable String renderStyle) {
        return answer(500, null, () -> {
            PureComposer.Style style = style(renderStyle);
            Map<String, Object> out = new LinkedHashMap<>();
            for (Map.Entry<String, Json.Node> e : request(body).fields().entrySet()) {
                if (!(e.getValue() instanceof Json.Obj lambda)) {
                    throw new IllegalArgumentException("the batch entry '" + e.getKey() + "' is not a lambda");
                }
                out.put(e.getKey(), PureComposer.lambda(lambda, style));
            }
            return Json.toCompact(out);
        });
    }

    /**
     * C1 {@code compilation/compile}: a model context compiled whole -- its elements, then every
     * body in it (functions, derived properties, service queries) -- as legend-engine compiles
     * a model before answering ({@code Compiler.compileErrors}, which {@link #compileEveryError} answers whole).
     * {@code {"message":"OK","defects":[]}}, or the first failure in the engine's error shape, 400 (measured,
     * 4.145.0, 2026-09-30). Recorded differences: lite reports no {@code defects} (the engine's are warnings, e.g. a
     * service without a title), and its refusal carries the element in the message, not a
     * {@code sourceInformation}.
     */
    public static Answer compile(String body) {
        List<String> errors = new ArrayList<>();
        Answer a = answer(400, "COMPILATION", () -> {
            errors.addAll(ModelInput.of(request(body)).compileErrors());
            return "{\"message\":\"OK\",\"defects\":[]}";
        });
        // a body's message names its element ("in function '...'")
        return errors.isEmpty() ? a : error(400, "COMPILATION", errors.get(0));
    }

    /**
     * E6 {@code compilation/lambdaReturnType}: {@code {model, lambda}} to {@code {"returnType":
     * path}} -- the result's type as legend-engine names it (measured, 4.145.0, 2026-09-30): a
     * relation is {@code meta::pure::metamodel::relation::Relation}, a class or enumeration its
     * path, a primitive its name; a failure is 400 COMPILATION.
     */
    public static Answer lambdaReturnType(String body) {
        return lambdaReturnType(body, Models.COMPILED);
    }

    /** {@link #lambdaReturnType(String)}, a model given as text compiled by {@code models}. */
    public static Answer lambdaReturnType(String body, Models models) {
        return answer(400, "COMPILATION", () -> {
            Json.Obj request = request(body);
            ModelInput model = ModelInput.of(request.getObj("model"));
            LambdaFunction lambda = ProtocolReader.lambda(request.getObj("lambda"));
            com.legend.compiler.element.type.Type t = model.with(models,
                    ctx -> com.legend.Compiler.query(ctx, lambda).resultType().type());
            String path = com.legend.compiler.element.type.Type.schemaView(t) != null
                    ? "meta::pure::metamodel::relation::Relation"
                    : UpstreamRelationType.typePath(t);
            return Json.toCompact(Map.of("returnType", path));
        });
    }

    /**
     * The {@code renderStyle} query parameter, PRETTY unless asked. Recorded differences: legend-engine's
     * {@code PRETTY_HTML}, which lite does not print, is refused in the engine's error shape, 500; and a value that is no
     * style at all is refused so too, where the engine's JAX-RS answers 404 before its resource runs (found by leg 4's
     * audit, 2026-10-09).
     */
    private static PureComposer.Style style(@com.legend.base.Nullable String renderStyle) {
        if (renderStyle == null || renderStyle.isEmpty() || "PRETTY".equals(renderStyle)) {
            return PureComposer.Style.PRETTY;
        }
        if ("STANDARD".equals(renderStyle)) {
            return PureComposer.Style.STANDARD;
        }
        throw new IllegalArgumentException("renderStyle '" + renderStyle
                + "' is not served by legend-lite (PRETTY or STANDARD)");
    }

    /** E5: {@code {model, lambda}} to the query's {@code RelationType}. */
    public static Answer lambdaRelationType(String body) {
        return lambdaRelationType(body, Models.COMPILED);
    }

    /** {@link #lambdaRelationType(String)}, a model given as text compiled by {@code models}. */
    public static Answer lambdaRelationType(String body, Models models) {
        return answer(400, "COMPILATION", () -> {
            Json.Obj request = request(body);
            ModelInput model = ModelInput.of(request.getObj("model"));
            LambdaFunction lambda = ProtocolReader.lambda(request.getObj("lambda"));
            return model.with(models, ctx -> Json.toCompact(UpstreamRelationType.of(
                    com.legend.Compiler.query(ctx, lambda).resultType())));
        });
    }

    /** E9: an {@code ExecuteInput} to the relational TDS {@code SingleExecutionPlan}. */
    public static Answer generatePlan(String body) {
        return generatePlan(body, Models.COMPILED);
    }

    /** {@link #generatePlan(String)}, a model given as text compiled by {@code models}. */
    public static Answer generatePlan(String body, Models models) {
        // the engine answers generatePlan's and execute's compile errors 500 (measured)
        return answer(500, "COMPILATION", () -> {
            Json.Obj request = request(body);
            ModelInput model = ModelInput.of(request.getObj("model"));
            LambdaFunction lambda = ProtocolReader.lambda(request.getObj("function"));
            // compiled and typed once: the target it names and its plan come off the one typed query
            return model.with(models, ctx -> {
                com.legend.TypedQuery query = com.legend.Compiler.query(ctx, lambda);
                String runtime = runtimeOf(request, query);
                // a value the program wrote and folds (a String or Boolean literal, a model's DDL text): upstream's own
                // constant node (M5, the user's ruling); a diagnostic's (print) stays refused below, as upstream refuses
                // it (S128); a value the database computes (|1+1) is a statement, below
                if (query.executionPlan(runtime, java.util.Set.of(), PureText.DIALECTS).root() instanceof com.legend.executionplan.ExecutionPlan.Constant constant
                        && !query.answersADiagnostic()) {
                    return Json.toCompact(constantPlan(constant));
                }
                com.legend.Compiler.Target target = query.target();
                QueryPlan plan = query.plan(runtime, PureText.DIALECTS);
                // upstream's plan and nothing beside it (M5, the user's ruling of 2026-11-11: "yes, take out of
                // compatibility plan"): legend-engine's SingleExecutionPlan reader fails on an unknown field, and
                // lite's own plan -- what runs a data: payload, a file: path, a pivot's keys -- reaches a lite host by
                // lite's own routes (the planner's text plan's `plan`, arrowPlan's and jsonPlan's `plan`, the planner
                // protocol's executionPlan)
                return Json.toCompact(executionPlan(plan, connectionOf(model.records(), runtime, target.store())));
            });
        });
    }

    /**
     * E8: an {@code ExecuteInput} run on its runtime's connection by {@code runner} -- established first, as
     * legend-engine does on acquisition -- answered in the engine's TDS result shape, or its JSON result for a graph
     * fetch. The database computes every value; the engine's writer spells each typed cell as the engine's tokens
     * ({@link com.legend.result.PureV1Writer}).
     */
    public static Answer execute(String body, Runner runner) {
        return execute(body, runner, Models.COMPILED);
    }

    /** {@link #execute(String, Runner)}, a model given as text compiled by {@code models}. */
    public static Answer execute(String body, Runner runner, Models models) {
        return answer(500, "COMPILATION", () -> {
            Json.Obj request = request(body);
            LambdaFunction lambda = ProtocolReader.lambda(
                    boundParameters(request.getObj("function"), request.getArrOr("parameterValues", null)));
            // the model compiled ONCE (leg 8 step 3; a host that holds models answers over the one it holds for the
            // text), and the query typed once, here: the runner plans and runs it
            return ModelInput.of(request.getObj("model")).with(models, model -> {
                com.legend.TypedQuery query = com.legend.Compiler.query(model, lambda);
                String runtime = runtimeOf(request, query);
                java.io.StringWriter out = new java.io.StringWriter();
                try {
                    runner.run(query, runtime, new com.legend.result.Output.Engine(out, facts(query)));
                } catch (java.io.IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
                return out.toString();
            });
        });
    }

    /**
     * E8 in upstream's Arrow format ({@code ?serializationFormat=ARROW_IPC}; the result boundary program's track A,
     * 2026-10-29): the relation's rows as ONE Arrow IPC stream written by lite's own writer
     * ({@code result/ArrowWriter}: the schema from the plan's columns, each batch a record batch, the plan's facts as
     * each field's metadata), its schema carrying the metadata upstream's answer carries ({@code legend.builder},
     * {@code legend.activities}, {@code legend.columns}: {@link #arrowPlan}'s), the stream wrapped as one zstd frame
     * (raw blocks, our own: {@code arrowipc/ZstdFrame}) under upstream's headers (Content-Type application/json,
     * x-legend-response-format FormatNotSet; measured 2026-10-08). A graph fetch under it is refused by name (upstream's
     * answer unprobed; D57 (7)); a value's answer is unprobed. The columns' Arrow types are the plan's slots', not
     * upstream's JDBC-driven ones (register S125, pending the user's ruling).
     */
    public static Answer executeArrow(String body, Runner runner, Models models) {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        Answer run = answer(500, "COMPILATION", () -> {
            Json.Obj request = request(body);
            LambdaFunction lambda = ProtocolReader.lambda(
                    boundParameters(request.getObj("function"), request.getArrOr("parameterValues", null)));
            return ModelInput.of(request.getObj("model")).with(models, model -> {
                com.legend.TypedQuery query = com.legend.Compiler.query(model, lambda);
                String runtime = runtimeOf(request, query);
                try {
                    runner.run(query, runtime, new com.legend.result.Output.Arrow(out, facts(query)));
                } catch (java.io.IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
                return "";
            });
        });
        if (run.status() != 200) {
            return run;
        }
        return Answer.bytes(200, com.legend.arrowipc.ZstdFrame.of(out.toByteArray()), "application/json",
                Map.of("x-legend-response-format", "FormatNotSet"));
    }

    /**
     * The plan legend-engine generates for a relational TDS query: a
     * {@code relationalTdsInstantiation} root over one {@code sql} node. Key order is the
     * engine's ({@code _type} first, then alphabetical).
     */
    static Map<String, Object> executionPlan(QueryPlan plan, Map<String, Object> connection) {
        List<Map<String, Object>> tdsColumns = new ArrayList<>();
        List<Map<String, Object>> resultColumns = new ArrayList<>();
        List<com.legend.compiler.element.type.Type.Column> typed = UpstreamRelationType.columns(plan.rootType());
        for (int i = 0; i < typed.size(); i++) {
            var c = typed.get(i);
            Map<String, Object> col = new LinkedHashMap<>();
            col.put("enumMapping", Map.of());
            col.put("name", c.name());
            // the builder's relational type is the engine's spelling of the PURE type (measured, 4.145.0: a
            // VARCHAR(32) store column's is VARCHAR(1024)); the physical type rides resultColumns below
            col.put("relationalType", UpstreamRelationType.relationalSpelling(c.type()));
            col.put("type", UpstreamRelationType.tdsTypePath(c.type()));
            tdsColumns.add(col);
            Map<String, Object> rc = new LinkedHashMap<>();
            // the engine spells a PASS-THROUGH column's physical type here, a computed one's as "": the plan's
            // store fact (docs/PLAN_PATH_PROGRAM_2026_10_17.md §3), by provenance over the lowered query
            com.legend.sql.SqlDdl.ColumnType store = i < plan.columns().size() ? plan.columns().get(i).store() : null;
            rc.put("dataType", store == null ? "" : com.legend.executionplan.ColumnFacts.store(store));
            rc.put("label", "\"" + c.name() + "\"");
            resultColumns.add(rc);
        }
        Map<String, Object> anyType = new LinkedHashMap<>();
        anyType.put("_type", "dataType");
        anyType.put("dataType", "meta::pure::metamodel::type::Any");
        Map<String, Object> sql = new LinkedHashMap<>();
        sql.put("_type", "sql");
        sql.put("authDependent", false);
        sql.put("connection", connection);
        sql.put("executionNodes", List.of());
        sql.put("isMutationSQL", false);
        if (plan.plan().root() instanceof com.legend.executionplan.ExecutionPlan.Keyed) {
            // a pivot whose columns the data names: its result columns known only once it runs, as legend-engine marks
            // its own dynamic pivot's node (relationalMappingExecution.pure:236-249, written only when true:
            // SQLExecutionNode.java:34-35); the statement shown is the keyed plan's body lite runs (M5, the user: "yes
            // we should show lite"; register S110)
            sql.put("isResultColumnsDynamic", true);
        }
        sql.put("resultColumns", resultColumns);
        sql.put("resultType", anyType);
        sql.put("sqlComment", "-- \"executionTraceID\" : \"${execID}\"");
        sql.put("sqlQuery", plan.sql());
        Map<String, Object> tds = new LinkedHashMap<>();
        tds.put("_type", "tds");
        tds.put("tdsColumns", tdsColumns);
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("_type", "relationalTdsInstantiation");
        root.put("authDependent", false);
        root.put("executionNodes", List.of(sql));
        root.put("resultType", tds);
        Map<String, Object> serializer = new LinkedHashMap<>();
        serializer.put("name", "pure");
        serializer.put("version", "vX_X_X");
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("_type", "simple");
        out.put("authDependent", false);
        out.put("rootExecutionNode", root);
        out.put("serializer", serializer);
        out.put("templateFunctions", PlanSupportFunctions.relationalPlanSupportFunctions(null));
        return out;
    }

    /**
     * A VALUE THE PROGRAM WROTE as generatePlan answers it (M5, the user's ruling of 2026-11-11: "Shouldn't that route to
     * database unless boolean or string constant fold?"): a String or Boolean literal, or a model's DDL text -- folded,
     * nothing sent -- as legend-engine's own {@code ConstantExecutionNode}, written as its protocol writes one (4.145.0):
     * <ul>
     *   <li>the node: {@code _type} {@code constant}, its result type, and its {@code values} by {@code transformAny}
     *       with {@code PureOne} (protocol vX_X_X {@code transfers/executionPlan.pure:70-75});</li>
     *   <li>a value: one value its primitive, any other number a collection of the transfer's multiplicity
     *       ({@code transfers/valueSpecification.pure:276-662});</li>
     *   <li>{@code authDependent} and {@code executionNodes} as {@code ExecutionNode}'s Java writes them, the server
     *       writing every object's fields in alphabetical order with nulls dropped ({@code ObjectMapperFactory});</li>
     *   <li>no template function: a plan's are its nodes' support functions
     *       ({@code executionPlan_generation.pure:217}).</li>
     * </ul>
     * legend-engine's {@code PlanExecutor} runs such a plan ({@code ExecutionNodeExecutor.visit(ConstantExecutionNode)}: a
     * {@code ConstantResult}); legend-engine itself plans such a lambda as a {@code pureExp} node whose Java it generates
     * (register S199).
     */
    static Map<String, Object> constantPlan(com.legend.executionplan.ExecutionPlan.Constant constant) {
        com.legend.executionplan.ExecutionPlan.Column column = constant.columns().get(0);
        List<Object> values = new ArrayList<>();
        for (List<String> row : constant.cells()) {
            values.add(constantValue(column.type(), row.get(0)));
        }
        Object value;
        if (values.size() == 1) {
            value = values.get(0);
        } else {
            Map<String, Object> pureOne = new LinkedHashMap<>();
            pureOne.put("lowerBound", 1);
            pureOne.put("upperBound", 1);
            Map<String, Object> collection = new LinkedHashMap<>();
            collection.put("_type", "collection");
            collection.put("multiplicity", pureOne);
            collection.put("values", values);
            value = collection;
        }
        Map<String, Object> resultType = new LinkedHashMap<>();
        resultType.put("_type", "dataType");
        resultType.put("dataType", column.type());
        Map<String, Object> node = new LinkedHashMap<>();
        node.put("_type", "constant");
        node.put("authDependent", false);
        node.put("executionNodes", List.of());
        node.put("resultType", resultType);
        node.put("values", value);
        Map<String, Object> serializer = new LinkedHashMap<>();
        serializer.put("name", "pure");
        serializer.put("version", "vX_X_X");
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("_type", "simple");
        out.put("authDependent", false);
        out.put("rootExecutionNode", node);
        out.put("serializer", serializer);
        out.put("templateFunctions", List.of());
        return out;
    }

    /** One folded value as legend-engine's protocol writes it: a String's {@code CString}, a Boolean's {@code CBoolean}
     *  ({@code transformAny}'s arms); lite folds no other kind, so any other is refused by name, never guessed. */
    private static Map<String, Object> constantValue(String type, @com.legend.base.Nullable String text) {
        if (text == null) {
            throw new IllegalStateException("a folded constant of no value: lite folds a literal or a model's text");
        }
        Map<String, Object> v = new LinkedHashMap<>();
        switch (type) {
            case "String" -> {
                v.put("_type", "string");
                v.put("value", text);
            }
            case "Boolean" -> {
                v.put("_type", "boolean");
                v.put("value", switch (text) {
                    case "true" -> true;
                    case "false" -> false;
                    default -> throw new IllegalStateException("a Boolean constant's text is true or false: " + text);
                });
            }
            default -> throw new IllegalStateException("a folded constant of type " + type
                    + ": lite folds String and Boolean values alone");
        }
        return v;
    }

    /**
     * The runtime's connection for {@code store}, as a plan carries it: the model's own
     * {@code connectionValue} (lite's byte-exact PMCD) without {@code databaseType} and
     * the source spans, with {@code postProcessors} and an EMPTY {@code element} --
     * legend-engine's plan shape, measured against 4.145.0 on 2026-09-27.
     */
    /** The connection fields a plan does not carry (measured, 4.145.0). */
    private static final java.util.Set<String> NOT_IN_A_PLAN =
            java.util.Set.of("databaseType", "sourceInformation", "elementSourceInformation");

    static Map<String, Object> connectionOf(com.legend.protocol.Protocol.PureModelContextData model, String runtime,
            @com.legend.base.Nullable String store) {
        // the records, as the parser or the model reader made them (leg 8 step 3: never the parser's JSON); the plan's
        // connection written from its record through the emitter
        com.legend.protocol.Protocol.PRuntime rt =
                elementAt(model, runtime, com.legend.protocol.Protocol.PRuntime.class, "runtime");
        String pointer = null;
        List<com.legend.protocol.Protocol.PStoreConnections> connections = rt.connections();
        if (store == null && connections.size() != 1) {
            throw new com.legend.error.NotImplementedException("generatePlan: a class query's "
                    + "store is chosen by its mapping, and a runtime of " + connections.size()
                    + " connections is unprobed");
        }
        for (com.legend.protocol.Protocol.PStoreConnections sc : connections) {
            if (store != null && !store.equals(sc.store().path())) {
                continue;
            }
            for (com.legend.protocol.Protocol.PIdentifiedConnection ic : sc.storeConnections()) {
                if (!(ic.connection() instanceof com.legend.protocol.Protocol.PConnectionPointer cp)) {
                    throw new com.legend.error.NotImplementedException(
                            "generatePlan: an embedded runtime connection is unprobed");
                }
                pointer = cp.connection();
                break;
            }
        }
        if (pointer == null) {
            throw new IllegalArgumentException("runtime " + runtime + " has no connection"
                    + (store == null ? "" : " for " + store));
        }
        com.legend.protocol.Protocol.PConnection connection =
                elementAt(model, pointer, com.legend.protocol.Protocol.PConnection.class, "connection");
        Json.Obj value = Json.parseObject(com.legend.protocol.ProtocolEmitter.emitElement(connection))
                .getObj("connectionValue");
        Map<String, Object> out = new java.util.TreeMap<>();
        for (Map.Entry<String, Json.Node> e : value.fields().entrySet()) {
            if (!NOT_IN_A_PLAN.contains(e.getKey())) {
                out.put(e.getKey(), withoutSourceInformation(e.getValue()));
            }
        }
        out.putIfAbsent("postProcessors", List.of());
        out.put("element", "");
        Map<String, Object> ordered = new LinkedHashMap<>();
        Object type = out.remove("_type");
        ordered.put("_type", type);
        ordered.putAll(out);
        return ordered;
    }

    /** The model's element of {@code kind} at {@code path} (the engine's {@code PackageableElement.getPath},
     *  {@link com.legend.protocol.Protocol#pathOf}), by its record; {@code what} names the kind if there is none. */
    private static <T extends com.legend.protocol.Protocol.Element> T elementAt(
            com.legend.protocol.Protocol.PureModelContextData model, String path, Class<T> kind, String what) {
        for (com.legend.protocol.Protocol.Element e : model.elements()) {
            if (kind.isInstance(e) && path.equals(com.legend.protocol.Protocol.pathOf(e))) {
                return kind.cast(e);
            }
        }
        throw new IllegalArgumentException("the model has no " + what + " " + path);
    }

    private static Object withoutSourceInformation(Json.Node n) {
        return com.legend.protocol.SourceInformation.strip(n);
    }
}
