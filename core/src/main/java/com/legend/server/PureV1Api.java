// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * legend-engine's {@code pure/v1} API, served by legend-lite EXACTLY
 * (docs/UPSTREAM_ENDPOINTS_DESIGN_2026_09_27.md; the user's ruling of 2026-09-27: lite's
 * client surface is upstream's APIs and nothing of its own). Each call is a pure function
 * of its request -- text in, a status and JSON out -- so it is tested without HTTP;
 * {@code LegendHttpServer} only carries it, through {@link #route}.
 *
 * <p>THE PLAN SIDE ({@code //core:pure_v1}): no database and no driver, so the compiler's
 * boundary answers with this same code -- Python's engine serves DataCube through it
 * (docs/DATACUBE_PYTHON_SHOW_DESIGN_2026_10_08.md). Execute's run is its host's, handed in
 * as a {@link Runner}: the server's driver; a host that runs SQL itself takes
 * {@link #arrowPlan}'s SQL and metadata.
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
 *   <li>E5 {@code compilation/lambdaRelationType}: a query's result columns as the
 *       compiler types them ({@link UpstreamRelationType}).</li>
 *   <li>E8 {@code execution/execute}: the query run on its runtime's connection, answered in
 *       the engine's TDS JSON; in upstream's Arrow format ({@code ARROW_IPC}), its plan half.</li>
 *   <li>E9 {@code execution/generatePlan}: the relational TDS execution plan.</li>
 * </ul>
 *
 * <p>The model travels as {@code PureModelContextText}; any other model context is refused
 * in upstream's error shape until the PMCD reader exists (recorded). Recorded differences
 * from legend-engine, each named in the parity test: lite's type names (no precise
 * primitives), and a plan's {@code resultColumns} carry no physical type.
 */
public final class PureV1Api {

    private PureV1Api() {
    }

    /** An answer: HTTP status, a body, and its media type ({@code application/json} unless said). */
    public record Answer(int status, String json, String contentType) {
        public Answer(int status, String json) {
            this(status, json, "application/json");
        }
    }

    /**
     * A request body's JSON. A query built as protocol JSON nests a few levels per chained
     * function, so the default limit (64, sized for configuration files) would refuse a long
     * cube query; 1024 still bounds a hostile body.
     */
    static final Json.Config REQUEST = new Json.Config(1024);

    /**
     * How a host runs an execute: the query planned and run on its runtime's connection, its rows written as
     * the database renders them ({@code [{column: value}, ...]}), and the plan that ran returned. legend-lite's
     * server hands in its driver ({@code QueryService.executeUpstream}).
     */
    @FunctionalInterface
    public interface Runner {
        QueryPlan run(String model, LambdaFunction lambda, String runtime, java.io.Writer rows);
    }

    /**
     * One call by its path ({@code /api/pure/v1/...}) and raw query string, as both of legend-lite's hosts
     * route it: its server ({@code LegendHttpServer}) and the compiler's boundary. An execute runs through
     * {@code runner}; a path this API does not serve is answered 404 in the engine's error shape.
     */
    public static Answer route(String path, @com.legend.base.Nullable String rawQuery, String body, Runner runner) {
        boolean sourceInformation = rawQuery == null || !rawQuery.contains("returnSourceInformation=false");
        return switch (path) {
            case "/api/pure/v1/grammar/grammarToJson/lambda" -> grammarToJsonLambda(body, sourceInformation);
            case "/api/pure/v1/grammar/grammarToJson/model" -> grammarToJsonModel(body, sourceInformation);
            case "/api/pure/v1/grammar/jsonToGrammar/lambda" ->
                    jsonToGrammarLambda(body, queryParam(rawQuery, "renderStyle"));
            case "/api/pure/v1/grammar/jsonToGrammar/lambda/batch" ->
                    jsonToGrammarLambdaBatch(body, queryParam(rawQuery, "renderStyle"));
            case "/api/pure/v1/grammar/jsonToGrammar/model" -> jsonToGrammarModel(body, queryParam(rawQuery, "renderStyle"));
            case "/api/pure/v1/compilation/lambdaRelationType" -> lambdaRelationType(body);
            case "/api/pure/v1/compilation/compile" -> compile(body);
            case "/api/pure/v1/compilation/lambdaReturnType" -> lambdaReturnType(body);
            case "/api/pure/v1/execution/generatePlan" -> generatePlan(body);
            case "/api/pure/v1/execution/execute" -> execute(body, runner);
            default -> error(404, null, "no such legend-engine API in legend-lite: " + path);
        };
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

    private static Json.Obj request(String body) {
        Json.Node n = Json.parse(body, REQUEST);
        if (n instanceof Json.Obj o) {
            return o;
        }
        throw new IllegalArgumentException("the request body is not a JSON object");
    }

    // ---------------------------------------------------------------------
    // E1 / E2: grammar to JSON
    // ---------------------------------------------------------------------

    /** E1: a lambda's text to its protocol JSON. Text without a leading {@code |} is
     *  wrapped in a parameterless lambda spanning the whole text, as the engine does. */
    public static Answer grammarToJsonLambda(String text, boolean returnSourceInformation) {
        return answer(400, "PARSER", () -> SpecParser.lambdaJson(text, returnSourceInformation));
    }

    /** E2: a model's text to its PMCD JSON. */
    public static Answer grammarToJsonModel(String text, boolean returnSourceInformation) {
        return answer(400, "PARSER", () -> PmcdParser.parseDocument(text, returnSourceInformation));
    }

    // ---------------------------------------------------------------------
    // E4: JSON to grammar
    // ---------------------------------------------------------------------

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

    // ---------------------------------------------------------------------
    // C1 / E6: compile, lambdaReturnType
    // ---------------------------------------------------------------------

    /**
     * C1 {@code compilation/compile}: a model context compiled whole -- its elements, then every
     * body in it (functions, derived properties, service queries) -- as legend-engine compiles
     * a model before answering. {@code {"message":"OK","defects":[]}}, or the first failure in
     * the engine's error shape, 400 (measured, 4.145.0, 2026-09-30). Recorded differences: lite
     * reports no {@code defects} (the engine's are warnings, e.g. a service without a title), and
     * its refusal carries the element in the message, not a {@code sourceInformation}.
     */
    public static Answer compile(String body) {
        String[] wall = new String[1];
        Answer a = answer(400, "COMPILATION", () -> {
            Map<String, String> walls = com.legend.Compiler.compileAllBodies(
                    com.legend.Compiler.compileModel(modelText(request(body))));
            if (!walls.isEmpty()) {
                // the wall's message names the element ("in function '...'"); its key is the
                // overload signature, which a person does not need
                wall[0] = walls.values().iterator().next();
                return "";
            }
            return "{\"message\":\"OK\",\"defects\":[]}";
        });
        return wall[0] != null ? error(400, "COMPILATION", wall[0]) : a;
    }

    /**
     * E6 {@code compilation/lambdaReturnType}: {@code {model, lambda}} to {@code {"returnType":
     * path}} -- the result's type as legend-engine names it (measured, 4.145.0, 2026-09-30): a
     * relation is {@code meta::pure::metamodel::relation::Relation}, a class or enumeration its
     * path, a primitive its name; a failure is 400 COMPILATION.
     */
    public static Answer lambdaReturnType(String body) {
        return answer(400, "COMPILATION", () -> {
            Json.Obj request = request(body);
            String model = modelText(request.getObj("model"));
            LambdaFunction lambda = ProtocolReader.lambda(request.getObj("lambda"));
            com.legend.compiler.element.type.Type t = com.legend.Compiler.query(com.legend.Compiler.compileModel(model), lambda).resultType().type();
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

    // ---------------------------------------------------------------------
    // E5: relation type
    // ---------------------------------------------------------------------

    /** E5: {@code {model, lambda}} to the query's {@code RelationType}. */
    public static Answer lambdaRelationType(String body) {
        return answer(400, "COMPILATION", () -> {
            Json.Obj request = request(body);
            String model = modelText(request.getObj("model"));
            LambdaFunction lambda = ProtocolReader.lambda(request.getObj("lambda"));
            return Json.toCompact(UpstreamRelationType.of(
                    com.legend.Compiler.query(com.legend.Compiler.compileModel(model), lambda).resultType()));
        });
    }

    // ---------------------------------------------------------------------
    // E9: generatePlan
    // ---------------------------------------------------------------------

    /** E9: an {@code ExecuteInput} to the relational TDS {@code SingleExecutionPlan}. */
    public static Answer generatePlan(String body) {
        // the engine answers generatePlan's and execute's compile errors 500 (measured)
        return answer(500, "COMPILATION", () -> {
            Json.Obj request = request(body);
            String model = modelText(request.getObj("model"));
            LambdaFunction lambda = ProtocolReader.lambda(request.getObj("function"));
            // compiled and typed once: the target it names and its plan come off the one typed query
            com.legend.TypedQuery query = com.legend.Compiler.query(com.legend.Compiler.compileModel(model), lambda);
            com.legend.Compiler.Target target = query.target();
            String runtime = runtimeOf(request, target);
            QueryPlan plan = query.plan(runtime);
            return Json.toCompact(executionPlan(plan,
                    connectionOf(model, runtime, target.store())));
        });
    }

    // ---------------------------------------------------------------------
    // E8: execute
    // ---------------------------------------------------------------------

    /**
     * E8: an {@code ExecuteInput} run on its runtime's connection by {@code runner} -- established
     * first, as legend-engine does on acquisition -- answered in the engine's TDS result shape. The
     * database renders every value; Java arranges the rows into {@code {"values": [...]}}.
     */
    public static Answer execute(String body, Runner runner) {
        return answer(500, "COMPILATION", () -> {
            Json.Obj request = request(body);
            String model = modelText(request.getObj("model"));
            LambdaFunction lambda = ProtocolReader.lambda(
                    boundParameters(request.getObj("function"), request.getArrOr("parameterValues", null)));
            String runtime = runtimeOf(request, com.legend.Compiler.query(com.legend.Compiler.compileModel(model), lambda).target());
            java.io.StringWriter rows = new java.io.StringWriter();
            QueryPlan plan = runner.run(model, lambda, runtime, rows);
            if (plan.shape() == com.legend.plan.ResultShape.GRAPH) {
                // a graph fetch: the engine's JSON result (measured, 4.145.0, 2026-09-30) -- the
                // objects' array, or the one object bare when there is exactly one (as Pure's
                // serialize writes a single-element collection)
                String values = rows.toString().isEmpty() ? "[]" : rows.toString();
                Json.Node parsed = Json.parse(values);
                if (parsed instanceof Json.Arr arr && arr.items().size() == 1) {
                    values = Json.toCompact(arr.items().get(0));
                }
                return "{\"builder\":{\"_type\":\"json\"},\"values\":" + values + "}";
            }
            return tdsResult(plan, rows.toString());
        });
    }

    /**
     * E8 in upstream's Arrow format ({@code ?serializationFormat=ARROW_IPC}), its plan half, for a host that runs
     * the SQL itself (Python's engine, over duckdb-python): the {@code ExecuteInput} read and planned as
     * {@link #execute} plans it, answered {@code {"sql", "metadata"}} -- the SQL to run, and the Arrow schema
     * metadata upstream's answer carries, each value JSON text as upstream writes it (measured against legend-engine
     * 4.145.0, 2026-10-08): {@code legend.builder}, the TDS builder as the JSON answer's; {@code legend.activities},
     * the activities WITHOUT their {@code _type}; {@code legend.columns}, the column names. The host writes the rows
     * as upstream does: an Arrow IPC stream with this metadata, compressed as one zstd frame. {@code models} are the
     * only models the host runs: a request over any other is refused.
     */
    public static Answer arrowPlan(String body, java.util.Collection<String> models) {
        return answer(500, "COMPILATION", () -> {
            Json.Obj request = request(body);
            String model = modelText(request.getObj("model"));
            if (!models.contains(model)) {
                // a host's models change (Python's: a Live frame whose columns changed is written a new model), so
                // the refusal says what to do
                throw new IllegalArgumentException("this engine runs queries over the models it serves only, "
                        + "and the request's model is not one of them: if the engine's model changed since it was "
                        + "read (a frame whose columns changed), read it again");
            }
            LambdaFunction lambda = ProtocolReader.lambda(
                    boundParameters(request.getObj("function"), request.getArrOr("parameterValues", null)));
            com.legend.TypedQuery query = com.legend.Compiler.query(com.legend.Compiler.compileModel(model), lambda);
            QueryPlan plan = query.plan(runtimeOf(request, query.target()));
            if (plan.shape() == com.legend.plan.ResultShape.GRAPH) {
                // upstream's Arrow answer is a relational result's (RelationalResultToArrowIPCSerializer)
                throw new IllegalArgumentException("ARROW_IPC answers a relation query; a graph fetch is answered in JSON");
            }
            Map<String, Object> metadata = new LinkedHashMap<>();
            metadata.put("legend.builder", Json.toCompact(tdsBuilder(plan)));
            metadata.put("legend.activities", Json.toCompact(List.of(activity(plan, false))));
            metadata.put("legend.columns", Json.toCompact(columnNames(plan)));
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("sql", plan.sql());
            out.put("metadata", metadata);
            return Json.toCompact(out);
        });
    }

    /**
     * A host's refusal of a call it could not finish -- its database refusing the SQL, say -- answered as
     * {@link #answer} answers a database's refusal: 500, the engine's error shape, no {@code errorType}.
     */
    public static Answer refused(String message) {
        return error(500, null, message);
    }

    /**
     * {@code ExecuteInput.parameterValues} ({@code [{name, value}]}, each value a protocol value
     * specification -- a literal, a collection, an enum value, a call such as {@code today()})
     * bound to the lambda's parameters: each becomes {@code let name = value;} ahead of the
     * body, in the lambda's declared order, and the lambda takes no parameters. The database
     * still computes every value; this only rearranges the query's shape. As legend-engine
     * 4.145.0 answers (measured 2026-09-30): a parameter with no value is refused, named with
     * its type and multiplicity; a value for no parameter of the lambda is ignored.
     */
    static Json.Obj boundParameters(Json.Obj function, @com.legend.base.Nullable Json.Arr values) {
        List<Json.Node> declared = function.has("parameters") ? function.getArr("parameters").items() : List.of();
        Map<String, Json.Node> byName = new LinkedHashMap<>();
        if (values != null) {
            for (Json.Node n : values.items()) {
                Json.Obj pv = (Json.Obj) n;
                byName.put(pv.getString("name"), pv.get("value"));
            }
        }
        if (declared.isEmpty() && byName.isEmpty()) {
            return function;
        }
        List<Json.Node> body = new ArrayList<>();
        List<String> missing = new ArrayList<>();
        for (Json.Node p : declared) {
            String name = ((Json.Obj) p).getString("name");
            Json.Node value = byName.remove(name);
            if (value == null) {
                missing.add(name + ":" + signature((Json.Obj) p));
                continue;
            }
            LinkedHashMap<String, Json.Node> let = new LinkedHashMap<>();
            let.put("_type", Json.str("func"));
            let.put("function", Json.str("letFunction"));
            LinkedHashMap<String, Json.Node> letName = new LinkedHashMap<>();
            letName.put("_type", Json.str("string"));
            letName.put("value", Json.str(name));
            let.put("parameters", new Json.Arr(List.of(new Json.Obj(letName), value)));
            body.add(new Json.Obj(let));
        }
        if (!missing.isEmpty()) {
            throw new IllegalArgumentException("Missing external parameter(s): " + String.join(", ", missing));
        }
        body.addAll(function.getArr("body").items());
        LinkedHashMap<String, Json.Node> out = new LinkedHashMap<>(function.fields());
        out.put("parameters", new Json.Arr(List.of()));
        out.put("body", new Json.Arr(body));
        return new Json.Obj(out);
    }

    /** A lambda parameter's {@code Type[m]}, as the engine's refusal spells it. */
    private static String signature(Json.Obj parameter) {
        Json.Obj raw = parameter.getObj("genericType").getObj("rawType");
        Json.Obj m = parameter.getObj("multiplicity");
        long lower = m.getLong("lowerBound");
        String mult = !m.has("upperBound") || m.get("upperBound") instanceof Json.Null
                ? (lower == 0 ? "*" : lower + "..*")
                : lower == m.getLong("upperBound") ? String.valueOf(lower) : lower + ".." + m.getLong("upperBound");
        return raw.getString("fullPath") + "[" + mult + "]";
    }

    /**
     * The engine's TDS result, byte for byte in its layout (its streaming serializer's
     * separators): {@code builder} (each column's Pure type and relational spelling),
     * the {@code relational} activity carrying the SQL, then the columns and rows.
     */
    static String tdsResult(QueryPlan plan, String wireRows) {
        List<String> names = columnNames(plan);
        StringBuilder out = new StringBuilder();
        out.append("{\"builder\": ").append(Json.toCompact(tdsBuilder(plan)))
                .append(", \"activities\": ").append(Json.toCompact(List.of(activity(plan, true))))
                .append(", \"result\" : {\"columns\" : ").append(Json.toCompact(names))
                .append(", \"rows\" : [");
        Json.Node wire = wireRows.isEmpty() ? new Json.Arr(List.of()) : Json.parse(wireRows);
        boolean first = true;
        for (Json.Node row : ((Json.Arr) wire).items()) {
            Json.Obj r = (Json.Obj) row;
            List<Object> values = new ArrayList<>();
            for (String n : names) {
                values.add(r.fields().get(n));
            }
            out.append(first ? "" : ",").append("{\"values\": ").append(Json.toCompact(values)).append('}');
            first = false;
        }
        return out.append("]}}").toString();
    }

    /** The TDS builder: each column's name, Pure type and relational spelling. */
    private static Map<String, Object> tdsBuilder(QueryPlan plan) {
        List<Map<String, Object>> columns = new ArrayList<>();
        for (var c : UpstreamRelationType.columns(plan.rootType())) {
            Map<String, Object> col = new LinkedHashMap<>();
            col.put("name", c.name());
            col.put("type", UpstreamRelationType.tdsTypePath(c.type()));
            col.put("relationalType", UpstreamRelationType.relationalSpelling(c.type()));
            columns.add(col);
        }
        Map<String, Object> builder = new LinkedHashMap<>();
        builder.put("_type", "tdsBuilder");
        builder.put("columns", columns);
        return builder;
    }

    /**
     * The relational activity, carrying the SQL: with its {@code _type} first, as the JSON answer writes it, or
     * without, as the Arrow answer's metadata does (measured, 4.145.0).
     */
    private static Map<String, Object> activity(QueryPlan plan, boolean typed) {
        Map<String, Object> activity = new LinkedHashMap<>();
        if (typed) {
            activity.put("_type", "relational");
        }
        activity.put("comment", "-- \"executionTraceID\" : \"" + java.util.UUID.randomUUID() + "\"");
        activity.put("sql", plan.sql());
        return activity;
    }

    private static List<String> columnNames(QueryPlan plan) {
        List<String> names = new ArrayList<>();
        for (var c : UpstreamRelationType.columns(plan.rootType())) {
            names.add(c.name());
        }
        return names;
    }

    /**
     * The plan legend-engine generates for a relational TDS query: a
     * {@code relationalTdsInstantiation} root over one {@code sql} node. Key order is the
     * engine's ({@code _type} first, then alphabetical).
     */
    static Map<String, Object> executionPlan(QueryPlan plan, Map<String, Object> connection) {
        List<Map<String, Object>> tdsColumns = new ArrayList<>();
        List<Map<String, Object>> resultColumns = new ArrayList<>();
        for (var c : UpstreamRelationType.columns(plan.rootType())) {
            Map<String, Object> col = new LinkedHashMap<>();
            col.put("enumMapping", Map.of());
            col.put("name", c.name());
            col.put("relationalType", UpstreamRelationType.relationalSpelling(c.type()));
            col.put("type", UpstreamRelationType.tdsTypePath(c.type()));
            tdsColumns.add(col);
            Map<String, Object> rc = new LinkedHashMap<>();
            // the engine spells a PASS-THROUGH column's physical type here, a computed
            // one's as "" -- lite's typer has no physical provenance (recorded)
            rc.put("dataType", "");
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
     * The runtime's connection for {@code store}, as a plan carries it: the model's own
     * {@code connectionValue} (lite's byte-exact PMCD) without {@code databaseType} and
     * the source spans, with {@code postProcessors} and an EMPTY {@code element} --
     * legend-engine's plan shape, measured against 4.145.0 on 2026-09-27.
     */
    /** The connection fields a plan does not carry (measured, 4.145.0). */
    private static final java.util.Set<String> NOT_IN_A_PLAN =
            java.util.Set.of("databaseType", "sourceInformation", "elementSourceInformation");

    static Map<String, Object> connectionOf(String model, String runtime,
            @com.legend.base.Nullable String store) {
        Json.Arr elements = Json.parseObject(PmcdParser.parseDocument(model)).getArr("elements");
        Json.Obj rt = element(elements, "runtime", runtime);
        String pointer = null;
        List<Json.Node> connections = rt.getObj("runtimeValue").getArr("connections").items();
        if (store == null && connections.size() != 1) {
            throw new com.legend.error.NotImplementedException("generatePlan: a class query's "
                    + "store is chosen by its mapping, and a runtime of " + connections.size()
                    + " connections is unprobed");
        }
        for (Json.Node c : connections) {
            Json.Obj sc = (Json.Obj) c;
            if (store != null && !store.equals(sc.getObj("store").getString("path"))) {
                continue;
            }
            for (Json.Node n : sc.getArr("storeConnections").items()) {
                Json.Obj conn = ((Json.Obj) n).getObj("connection");
                if (!"connectionPointer".equals(conn.getString("_type"))) {
                    throw new com.legend.error.NotImplementedException(
                            "generatePlan: an embedded runtime connection is unprobed");
                }
                pointer = conn.getString("connection");
                break;
            }
        }
        if (pointer == null) {
            throw new IllegalArgumentException("runtime " + runtime + " has no connection"
                    + (store == null ? "" : " for " + store));
        }
        Json.Obj value = element(elements, "connection", pointer).getObj("connectionValue");
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

    private static Json.Obj element(Json.Arr elements, String type, String path) {
        for (Json.Node n : elements.items()) {
            Json.Obj e = (Json.Obj) n;
            if (type.equals(e.getStringOr("_type", ""))
                    && path.equals(e.getStringOr("package", "") + "::" + e.getStringOr("name", ""))) {
                return e;
            }
        }
        throw new IllegalArgumentException("the model has no " + type + " " + path);
    }

    /** The runtime: the request's own, else the one the query's {@code ->from} binds. */
    private static String runtimeOf(Json.Obj request, com.legend.Compiler.Target target) {
        Json.Obj rt = request.getObjOr("runtime", null);
        if (rt != null && rt.has("runtime")) {
            return rt.getString("runtime");
        }
        if (target.runtime() == null) {
            throw new IllegalArgumentException(
                    "no runtime: the request names none and the query has no ->from(runtime)");
        }
        return target.runtime();
    }

    // ---------------------------------------------------------------------
    // Plumbing
    // ---------------------------------------------------------------------

    /** A {@code PureModelContextText}'s code; any other model context is refused. */
    private static String modelText(Json.Obj model) {
        String type = model.getStringOr("_type", "");
        if (!"text".equals(type)) {
            throw new IllegalArgumentException("a model of _type '" + type + "': legend-lite reads "
                    + "PureModelContextText ({\"_type\":\"text\",\"code\":...}); the PMCD reader is not built");
        }
        return model.getString("code");
    }

    private static Object withoutSourceInformation(Json.Node n) {
        return com.legend.protocol.SourceInformation.strip(n);
    }

    /**
     * A call's JSON, or its failure in the engine's error shape, with the engine's status
     * for each kind (measured against 4.145.0, 2026-09-27):
     * <ul>
     *   <li>the text does not parse or compile, or the construct is not implemented:
     *       {@code status} with {@code errorType} (grammar and lambdaRelationType answer
     *       400, generatePlan and execute 500);</li>
     *   <li>a malformed request, or the database refusing: 500, no {@code errorType}, the
     *       message naming the exception;</li>
     *   <li>anything else is a BUG: logged whole, answered 500 the same way -- never a
     *       dropped connection.</li>
     * </ul>
     */
    private static Answer answer(int status, @com.legend.base.Nullable String errorType, Supplier<String> call) {
        try {
            return new Answer(200, call.get());
        } catch (com.legend.error.LegendCompileException
                | com.legend.error.NotImplementedException
                | com.legend.sql.dialect.DialectCapability e) {
            return error(status, errorType, String.valueOf(e.getMessage()));
        } catch (IllegalArgumentException | com.legend.error.DataError e) {
            Throwable named = e instanceof com.legend.error.DataError && e.getCause() != null ? e.getCause() : e;
            return error(500, null, named.getClass().getSimpleName() + ": " + named.getMessage());
        } catch (RuntimeException | StackOverflowError e) {
            e.printStackTrace();
            return error(500, null, e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private static Answer error(int status, @com.legend.base.Nullable String errorType, String message) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("code", -1);
        if (errorType != null) {
            out.put("errorType", errorType);
        }
        out.put("message", message);
        out.put("status", "error");
        return new Answer(status, Json.toCompact(out));
    }
}
