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
 * {@code LegendHttpServer} only carries it, through {@link #route}. Two routes are legend-lite's own, beside them --
 * {@code /api/lite/v1/compilation/compile} (every error where legend-engine's answers the first: the user's decision
 * of 2026-10-10, docs/PROTOCOL_PROGRAM_2026_10_05.md leg 6) and the model routes {@code /api/lite/v1/model/...}
 * (DataCube's Create a Class…, an Association…, a Data Space…: {@link #modelChanges}) -- routed by the same dispatcher, so each host that
 * calls {@link #route} answers them (lite's server, the tab, Python's library).
 *
 * <p>THE PLAN SIDE ({@code //core:pure_v1}): no database and no driver, so the compiler's
 * boundary answers with this same code -- Python's engine serves DataCube through it
 * (docs/DATACUBE_PYTHON_SHOW_DESIGN_2026_10_08.md). Execute's run is its host's, handed in
 * as a {@link Runner}: the server's driver; a host that runs SQL itself takes
 * {@link #jsonPlan}'s plan record and hands its rows back for the answer.
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
 * primitives). A plan's {@code resultColumns} carry the store's declared type where a column reads one through,
 * as the engine's do (the plan's own fact, docs/PLAN_PATH_PROGRAM_2026_10_17.md §3; the recorded difference
 * closed 2026-10-17).
 */
public final class PureV1Api {

    private PureV1Api() {
    }

    /** An answer: HTTP status, a body, and its media type ({@code application/json} unless said). */
    public record Answer(int status, String json, String contentType, byte @com.legend.base.Nullable [] bytes,
            Map<String, String> headers) {
        public Answer {
            headers = java.util.Collections.unmodifiableMap(new LinkedHashMap<>(headers));
        }

        public Answer(int status, String json) {
            this(status, json, "application/json");
        }

        public Answer(int status, String json, String contentType) {
            this(status, json, contentType, null, Map.of());
        }

        /** A BINARY answer (the ARROW_IPC execute, track A, 2026-10-29): its bytes, and the headers upstream sends
         *  beside the media type; its {@code json} is empty. */
        public static Answer bytes(int status, byte[] bytes, String contentType, Map<String, String> headers) {
            return new Answer(status, "", contentType, bytes, headers);
        }

        /** The body as bytes: the binary body, else the text as UTF-8. */
        public byte[] body() {
            return bytes != null ? bytes : json.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        }
    }

    /**
     * A request body's JSON. A query built as protocol JSON nests a few levels per chained
     * function, so the default limit (64, sized for configuration files) would refuse a long
     * cube query; and a request may carry a whole model as data ({@code ModelInput}), which nests as deep as a
     * model's own JSON -- the bound {@code ModelReader} reads a model's text at (4096; 2026-10-21, the independent
     * audit: at 1024 a model compiled as text and was refused as data). It still bounds a hostile body.
     */
    static final Json.Config REQUEST = new Json.Config(4096);

    /**
     * How a host runs an execute: the query, typed once by the route, planned as typed rows and run on its runtime's
     * connection, its rows streamed to the sink {@code sinkOf} makes from the plan that runs -- made before any
     * statement runs, so an answer the engine's API has no form for is refused before a program's writes land.
     * legend-lite's server hands in its driver ({@code ConnectionResolver.RUNNER}).
     */
    @FunctionalInterface
    public interface Runner {
        void run(com.legend.TypedQuery query, String runtime,
                java.util.function.Function<com.legend.executionplan.ExecutionPlan, com.legend.result.ResultSink> sinkOf)
                throws java.io.IOException;
    }

    /**
     * legend-engine's {@code ARROW_IPC} answer's writer, which legend-lite's SERVER alone hands in (the user's ruling
     * D22, 2026-11-05, docs/HOST_EXECUTORS_PROGRAM_2026_11_02.md §1.2: the server is the drop-in replacement for
     * legend-engine, which runs only as a server, so a client in production reading Arrow gets upstream's answer from
     * it; Python and the tab, the hosts lite is embedded in, refuse the format by name and stay alike). Each makes the
     * writer of one answer onto {@code out}: under a fixed schema metadata, or one made from the columns the result
     * announces and the database's report of each (a pivot whose columns its data names).
     */
    public interface EngineArrow {
        com.legend.result.ResultSink fixed(java.io.OutputStream out, Map<String, String> metadata);

        com.legend.result.ResultSink dynamic(java.io.OutputStream out, java.util.function.BiFunction<
                List<com.legend.executionplan.ExecutionPlan.Column>, @com.legend.base.Nullable List<@com.legend.base.Nullable String>,
                Map<String, String>> metadata);
    }

    /**
     * How a host compiles a request's model given as text, for one use: by default compiled for it alone (a server holds
     * nothing: a request has no holder); a host that holds models answers over the one it holds for that text, compiled
     * no more ({@code HeldModels.withText}). The answer is the same either way -- a held model is a function of its
     * text alone -- only its time differs.
     */
    public interface Models {
        <T> T with(String text, java.util.function.Function<com.legend.compiler.element.ModelContext, T> use);

        /** Each use compiles its text. */
        Models COMPILED = new Models() {
            @Override
            public <T> T with(String text, java.util.function.Function<com.legend.compiler.element.ModelContext, T> use) {
                return use.apply(com.legend.Compiler.compileModel(text));
            }
        };
    }

    /**
     * One call by its path ({@code /api/pure/v1/...}) and raw query string, as both of legend-lite's hosts
     * route it: its server ({@code LegendHttpServer}) and the compiler's boundary. An execute runs through
     * {@code runner}; a path this API does not serve is answered 404 in the engine's error shape.
     */
    public static Answer route(String path, @com.legend.base.Nullable String rawQuery, String body, Runner runner) {
        return route(path, rawQuery, body, runner, Models.COMPILED, null);
    }

    /** {@link #route(String, String, String, Runner)}, a model given as text compiled by {@code models}. */
    public static Answer route(String path, @com.legend.base.Nullable String rawQuery, String body, Runner runner,
            Models models) {
        return route(path, rawQuery, body, runner, models, null);
    }

    /** {@link #route(String, String, String, Runner)}, a model given as text compiled by {@code models}; legend-engine's
     *  {@code ARROW_IPC} answer written by {@code engineArrow}, which the server alone hands in (D22: without one the
     *  format is refused by name). */
    public static Answer route(String path, @com.legend.base.Nullable String rawQuery, String body, Runner runner,
            Models models, @com.legend.base.Nullable EngineArrow engineArrow) {
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
                    ? executeArrow(body, runner, models, engineArrow) : execute(body, runner, models);
            case "/api/lite/v1/compilation/compile" -> compileEveryError(body);
            case "/api/lite/v1/execution/execute" -> executeLite(body, runner, models);
            case "/api/lite/v1/model/createClass" -> modelChanges(body, ModelChanges::createClass);
            case "/api/lite/v1/model/createAssociation" -> modelChanges(body, ModelChanges::createAssociation);
            case "/api/lite/v1/model/createDataSpace" -> modelChanges(body, ModelChanges::createDataSpace);
            default -> error(404, null, (path.startsWith("/api/lite/") ? "no such legend-lite API: "
                    : "no such legend-engine API in legend-lite: ") + path);
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
     * legend-lite's own {@code /api/lite/v1/compilation/compile} (the user's decision, 2026-10-10; legend-engine has
     * no such route, so a client that may be talking to it asks this first and, answered 404, asks
     * {@link #compile}): the model compiled as {@link #compile} compiles it, answered with EVERY error, 200 --
     * {@code {"errors":[{"message":...}, ...]}}, the list empty when it compiles ({@code Compiler.compileErrors}: the
     * first element error alone, else every body's). Each error is an object, so where it is can be added beside its
     * message later (leg 6b) without changing the shape. A request that cannot be read, or a failure that is not the
     * model's, is answered as {@link #compile} answers it.
     */
    public static Answer compileEveryError(String body) {
        return answer(400, "COMPILATION", () -> {
            List<Map<String, Object>> errors = new ArrayList<>();
            for (String message : ModelInput.of(request(body)).compileErrors()) {
                errors.add(Map.of("message", message));
            }
            return Json.toCompact(Map.of("errors", errors));
        });
    }

    /**
     * legend-lite's own model routes, {@code /api/lite/v1/model/createClass}, {@code createAssociation} and
     * {@code createDataSpace} (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §15.6a, the user's ruling of 2026-10-11: named
     * after Legend's actions; upstream has no HTTP route that does this): each one call to the model's writer
     * ({@link ModelChanges}), atomic, answered as the files to write in the SDLC's own change shape. The project and
     * each class's source are model contexts, read as {@link #compile} reads one; what cannot be made is answered as a
     * model that does not compile is, 400 COMPILATION.
     */
    public static Answer modelChanges(String body, java.util.function.Function<Json.Obj, Map<String, Object>> make) {
        return answer(400, "COMPILATION", () -> Json.toCompact(make.apply(request(body))));
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

    // ---------------------------------------------------------------------
    // E5: relation type
    // ---------------------------------------------------------------------

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

    // ---------------------------------------------------------------------
    // E9: generatePlan
    // ---------------------------------------------------------------------

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
                com.legend.Compiler.Target target = query.target();
                String runtime = runtimeOf(request, target);
                QueryPlan plan = query.plan(runtime);
                return Json.toCompact(executionPlan(plan,
                        connectionOf(model.records(), runtime, target.store())));
            });
        });
    }

    // ---------------------------------------------------------------------
    // E8: execute
    // ---------------------------------------------------------------------

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
                String runtime = runtimeOf(request, query.target());
                java.io.StringWriter out = new java.io.StringWriter();
                try {
                    runner.run(query, runtime, plan -> engineWriter(query, plan, out));
                } catch (java.io.IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
                return out.toString();
            });
        });
    }

    /**
     * E8 in upstream's Arrow format ({@code ?serializationFormat=ARROW_IPC}; the result boundary program's track A,
     * 2026-10-29; written as upstream writes it since R4 step 3, 2026-11-03): the relation's rows as ONE Arrow IPC
     * stream written by upstream's writer ({@code result/EngineArrowWriter}: each field typed from the database's
     * report of its column, upstream's JDBC rules, nullable, no field metadata; batches of 1,024 rows), its schema
     * carrying the metadata upstream's answer carries ({@code legend.builder}, {@code legend.activities},
     * {@code legend.columns}, in Java HashMap's order), the stream wrapped as one zstd frame (raw blocks, our own:
     * {@code arrowipc/ZstdFrame}) under upstream's headers (Content-Type application/json, x-legend-response-format
     * FormatNotSet; measured 2026-10-08). A graph fetch under it is refused by name (upstream's answer unprobed; D57
     * (7)); a value's answer is unprobed. Lite's own Arrow, in the plan's types, is {@link #executeLite}'s (D19).
     */
    public static Answer executeArrow(String body, Runner runner, Models models,
            @com.legend.base.Nullable EngineArrow engineArrow) {
        if (engineArrow == null) {
            // D22 (the user, 2026-11-05): the embedded hosts answer legend-engine's JSON and lite's own Arrow, not
            // legend-engine's Arrow format -- refused by name, in legend-engine's error shape, before anything compiles
            return answer(500, null, () -> {
                throw new IllegalArgumentException("legend-engine's ARROW_IPC answer is served by legend-lite's server"
                        + " alone, the drop-in replacement for legend-engine; this host answers execute as legend-engine's"
                        + " JSON, or as lite's own Arrow (/api/lite/v1/execution/execute)");
            });
        }
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        Answer run = answer(500, "COMPILATION", () -> {
            Json.Obj request = request(body);
            LambdaFunction lambda = ProtocolReader.lambda(
                    boundParameters(request.getObj("function"), request.getArrOr("parameterValues", null)));
            return ModelInput.of(request.getObj("model")).with(models, model -> {
                com.legend.TypedQuery query = com.legend.Compiler.query(model, lambda);
                String runtime = runtimeOf(request, query.target());
                try {
                    runner.run(query, runtime, plan -> arrowWriter(query, plan, out, engineArrow));
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

    /** The Arrow schema metadata key of lite's own answer: the plan record its rows are the answer of (D20). */
    public static final String PLAN_KEY = "legend.plan";

    /**
     * LITE'S OWN EXECUTE (docs/HOST_EXECUTORS_PROGRAM_2026_11_02.md D19, D20, 2026-11-03): the request legend-engine's
     * execute takes ({@code ExecuteInput}: the lambda, the model, its parameter values), so a client's two adapters
     * differ only in the route and the decode; the answer lite's own -- the rows as ONE Arrow IPC stream in the plan's
     * types (lite's {@code result/ArrowWriter}, every database alike), its schema's metadata the plan record
     * ({@link #PLAN_KEY}: {@code PlanJson}'s text, its columns' Pure types, multiplicities and facts), no zstd frame,
     * {@code application/vnd.apache.arrow.stream}. A refusal is answered as the execute route answers one.
     */
    public static Answer executeLite(String body, Runner runner, Models models) {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        Answer run = answer(500, "COMPILATION", () -> {
            Json.Obj request = request(body);
            LambdaFunction lambda = ProtocolReader.lambda(
                    boundParameters(request.getObj("function"), request.getArrOr("parameterValues", null)));
            return ModelInput.of(request.getObj("model")).with(models, model -> {
                com.legend.TypedQuery query = com.legend.Compiler.query(model, lambda);
                String runtime = runtimeOf(request, query.target());
                try {
                    runner.run(query, runtime, plan -> liteWriter(plan, out));
                } catch (java.io.IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
                return "";
            });
        });
        if (run.status() != 200) {
            return run;
        }
        return Answer.bytes(200, out.toByteArray(), com.legend.arrowipc.ArrowIpc.MEDIA_TYPE, Map.of());
    }

    /** Lite's own Arrow writer of {@code plan}'s answer onto {@code out}: the plan's types, the plan record in the
     *  schema's metadata ({@link #executeLite}). */
    public static com.legend.result.ResultSink liteWriter(com.legend.executionplan.ExecutionPlan plan,
            java.io.OutputStream out) {
        return new com.legend.result.ArrowWriter(out, Map.of(PLAN_KEY, com.legend.executionplan.PlanJson.write(plan)));
    }

    /** {@link #executeLite}'s answer for a host that ran the plan's statement on its own database ({@link #jsonPlan}'s
     *  plan record), its rows the Arrow that database answered, read in place: the same writer, the same answer. */
    public static Answer liteAnswer(String plan, java.nio.ByteBuffer arrow) {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        Answer run = answer(500, "COMPILATION", () -> {
            com.legend.executionplan.ExecutionPlan read = com.legend.executionplan.PlanJson.read(plan);
            com.legend.executionplan.ExecutionPlan.TdsResult result = hostResult(plan);
            try {
                com.legend.result.ArrowRows.stream(arrow, result.columns(), liteWriter(read, out));
            } catch (java.io.IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
            return "";
        });
        if (run.status() != 200) {
            return run;
        }
        return Answer.bytes(200, out.toByteArray(), com.legend.arrowipc.ArrowIpc.MEDIA_TYPE, Map.of());
    }

    /**
     * {@link #liteAnswer} for a KEYED plan (a pivot whose columns its data names, D18 D) a host ran: its keys rows (as
     * {@link #keyedAnswer} takes them) and the Arrow the database answered the body the core wrote for them with,
     * read in the body's order (each per-key column under the name the body writes it) and placed in the answer's
     * (KeyedRun's {@code place}) -- lite's own Arrow of the answer's columns, the plan record beside it.
     */
    public static Answer keyedLiteAnswer(String plan, String keys, java.nio.ByteBuffer arrow) {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        Answer run = answer(500, "COMPILATION", () -> {
            com.legend.executionplan.ExecutionPlan read = com.legend.executionplan.PlanJson.read(plan);
            com.legend.executionplan.ExecutionPlan.Keyed keyed = hostKeyed(plan);
            com.legend.executionplan.KeyedRun.Answer answer =
                    com.legend.executionplan.KeyedRun.answer(keyed, keyRows(keys));
            List<com.legend.executionplan.ExecutionPlan.Column> columns = answer.columns();
            com.legend.result.ResultSink writer = liteWriter(read, out);
            try {
                com.legend.result.ArrowRows.stream(arrow, answer.read(), new com.legend.result.ResultSink() {
                    @Override
                    public void columns(List<com.legend.executionplan.ExecutionPlan.Column> body)
                            throws java.io.IOException {
                        writer.columns(columns);
                    }

                    @Override
                    public void batch(com.legend.result.Batch batch) throws java.io.IOException {
                        com.legend.result.Batch.Builder placed = com.legend.result.Batch.builder(columns, batch.size());
                        for (int r = 0; r < batch.size(); r++) {
                            List<Object> cells = new ArrayList<>(java.util.Collections.nCopies(columns.size(), null));
                            for (int i = 0; i < columns.size(); i++) {
                                cells.set(answer.place().get(i), batch.get(r, i));
                            }
                            placed.add(cells);
                        }
                        writer.batch(placed.build());
                    }

                    @Override
                    public void end() throws java.io.IOException {
                        writer.end();
                    }
                });
            } catch (java.io.IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
            return "";
        });
        if (run.status() != 200) {
            return run;
        }
        return Answer.bytes(200, out.toByteArray(), com.legend.arrowipc.ArrowIpc.MEDIA_TYPE, Map.of());
    }

    /** The Arrow writer of {@code plan}'s answer onto {@code out}, its schema metadata the engine's for the columns
     *  the run announces ({@link #executeArrow}). */
    public static com.legend.result.ResultSink arrowWriter(com.legend.TypedQuery query,
            com.legend.executionplan.ExecutionPlan plan, java.io.OutputStream out, EngineArrow engineArrow) {
        return switch (plan.answerShape()) {
            case OBJECTS -> throw new IllegalArgumentException(
                    "ARROW_IPC answers a relation query; a graph fetch under it is refused (upstream's answer unprobed)");
            case RELATION -> {
                String sql = plan.answerSql().statement();
                java.util.function.Function<com.legend.result.PureV1Writer.Envelope, Map<String, String>> metadata =
                        e -> engineArrowMetadata(sql, e);
                List<com.legend.executionplan.ExecutionPlan.Column> stated = plan.answerColumns();
                // the JSON route's rule (engineWriter; the phase 3 audit, 2026-10-27): the envelope from the plan's
                // stated columns BEFORE any statement runs, so a column with no builder entry refuses before a write
                // program changes anything -- made from the plan's own facts, as every host's is (D18 F, the user
                // 2026-11-03); made from the announced columns only where the plan states none (a dynamic pivot's)
                // upstream's writer (R4 step 3, D17): each field typed from the database's report of its column
                yield stated == null ? engineArrow.dynamic(out,
                        (columns, reported) -> metadata.apply(dynamicEnvelope(sql, columns, reported)))
                        : engineArrow.fixed(out, metadata.apply(planEnvelope(sql, stated)));
            }
            case VALUE -> throw unprobed(query);
        };
    }

    /** The schema metadata legend-engine's Arrow answer carries, as upstream puts it ({@code legend.builder}, the
     *  JSON answer's; {@code legend.activities}, the activity without its {@code _type}; {@code legend.columns}, the
     *  column names), in upstream's insertion order (the writer lays it out in HashMap's). */
    private static Map<String, String> engineArrowMetadata(String sql, com.legend.result.PureV1Writer.Envelope e) {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("legend.builder", e.builder());
        m.put("legend.activities", Json.toCompact(List.of(activity(sql, false))));
        m.put("legend.columns", Json.toCompact(e.names()));
        return m;
    }

    /** A host's rows of a statement whose result columns are {@code columns}, streamed to a sink: as JSON text
     *  ({@link com.legend.result.HostRows}) or as Arrow ({@link com.legend.result.ArrowRows}). */
    @FunctionalInterface
    private interface HostRowsSource {
        void stream(List<com.legend.executionplan.ExecutionPlan.Column> columns, com.legend.result.ResultSink sink)
                throws java.io.IOException;
    }

    /** The one typed result a host's plan answers with ({@link #jsonPlan}'s plan record, handed back): read by the plan
     *  record's own reader (its format and version), its root that one result, its statement binding no values; any
     *  other plan is refused by name, its head shown. */
    private static com.legend.executionplan.ExecutionPlan.TdsResult hostResult(String plan) {
        com.legend.executionplan.ExecutionPlan read;
        try {
            read = com.legend.executionplan.PlanJson.read(plan);
        } catch (IllegalArgumentException e) {
            // the refusal names what it was reading and shows its head: a host's answer text that does not parse
            // is otherwise a line and a column with no text to look at (2026-10-26, the arm and Windows lanes)
            throw new IllegalArgumentException("the plan half's answer does not read as a plan (" + e.getMessage()
                    + "); it begins: " + head(plan), e);
        }
        if (!(read.root() instanceof com.legend.executionplan.ExecutionPlan.TdsResult result)
                || !result.sql().slots().isEmpty()) {
            throw new IllegalArgumentException("the plan is not one typed result over a statement of text: it is not "
                    + "jsonPlan's; it begins: " + head(plan));
        }
        return result;
    }

    /**
     * The engine's writer of {@code plan}'s answer onto {@code out}: a relation's TDS result, its envelope (the
     * builder, the relational activity carrying the statement that answered, the columns' names as the engine labels
     * them) made from the plan's own columns NOW, before any statement runs -- so a column the engine has no builder
     * entry for refuses the answer before a write program changes anything (the phase 3 audit, 2026-10-27) -- or, for
     * a dynamic pivot, whose columns its keys name, from the columns its run announces; or a graph fetch's JSON
     * result. A value's answer (a scalar, a collection) is refused by name: the engine's serialization of one is
     * unprobed.
     */
    public static com.legend.result.ResultSink engineWriter(com.legend.TypedQuery query,
            com.legend.executionplan.ExecutionPlan plan, java.io.Writer out) {
        return switch (plan.answerShape()) {
            case OBJECTS -> com.legend.result.PureV1Writer.objects(out);
            case RELATION -> {
                List<com.legend.executionplan.ExecutionPlan.Column> stated = plan.answerColumns();
                if (stated == null) {
                    // a dynamic result: its header the database's report, as legend-engine makes one (D18 F)
                    String sql = plan.answerSql().statement();
                    yield com.legend.result.PureV1Writer.dynamicRelation(out,
                            (columns, reported) -> dynamicEnvelope(sql, columns, reported));
                }
                // the plan's own facts make the header, as every host's answer is made (planEnvelope; D18 F, the user
                // 2026-11-03): one maker of legend-engine's header for a plan that states its columns
                com.legend.result.PureV1Writer.Envelope made = planEnvelope(plan.answerSql().statement(), stated);
                yield com.legend.result.PureV1Writer.relation(out, columns -> made);
            }
            case VALUE -> throw unprobed(query);
        };
    }

    /** The refusal of a value's answer (a scalar, a collection), whose serialization by the engine is unprobed. */
    private static com.legend.error.NotImplementedException unprobed(com.legend.TypedQuery query) {
        return new com.legend.error.NotImplementedException("execute: a "
                + com.legend.plan.ResultShape.of(query.resultType()) + " result's serialization is unprobed");
    }

    /**
     * A relation's envelope from the plan alone (docs/HOST_EXECUTORS_PROGRAM_2026_11_02.md R2: a host's writer reads
     * the plan, never the compiler): each column's builder entry by the plan's own facts -- its type by the one rule
     * ({@link com.legend.executionplan.ExecutionPlan.Column#plainType}) and whether it is an enumeration
     * ({@link com.legend.executionplan.ExecutionPlan.Column#enumeration}, made at planning from the compiler's type
     * {@link #envelope} reads), its spellings the same functions' -- the relational activity carrying {@code sql},
     * and the columns' names as the engine labels them. A column the plan does not say is an enumeration or not is
     * none the compiler typed, and is refused by name.
     */
    static com.legend.result.PureV1Writer.Envelope planEnvelope(String sql,
            List<com.legend.executionplan.ExecutionPlan.Column> columns) {
        List<Map<String, Object>> builderColumns = new ArrayList<>();
        List<String> names = new ArrayList<>();
        for (var c : columns) {
            String name = com.legend.result.CellText.presentedName(c.name());
            Boolean enumeration = c.enumeration();
            if (enumeration == null) {
                throw new IllegalStateException("column '" + c.name() + "' is none the compiler typed: the plan does not"
                        + " say whether it is an enumeration");
            }
            Map<String, Object> col = new LinkedHashMap<>();
            col.put("name", name);
            col.put("type", UpstreamRelationType.tdsTypePath(c.plainType(), enumeration));
            col.put("relationalType", UpstreamRelationType.relationalSpelling(c.plainType(), enumeration));
            builderColumns.add(col);
            names.add(name);
        }
        Map<String, Object> builder = new LinkedHashMap<>();
        builder.put("_type", "tdsBuilder");
        builder.put("columns", builderColumns);
        return new com.legend.result.PureV1Writer.Envelope(Json.toCompact(builder),
                Json.toCompact(List.of(activity(sql, true))), names);
    }

    /**
     * A DYNAMIC result's envelope (a pivot whose columns its data names), as legend-engine makes one
     * ({@code RelationalResult.buildTransformersAndBuilder} with {@code isResultColumnsDynamic}; D18 F, the user
     * 2026-11-03): EVERY column rebuilt from the database's report -- its name (the plan's presented name: legend-engine's
     * label is its own statement's alias for the column, and lite's body writes its own aliases, so the plan's name is
     * the one legend-engine's label spells), and the type
     * {@code ResultColumn.labelTypePair} reads from the type name the driver reported ({@link #reportedType}), with no
     * relational spelling ({@code TDSColumn(name, type)}: its {@code relationalType} null, dropped) -- the relational
     * activity carrying {@code sql}, and the columns' names as the engine labels them. A database that reports no type
     * names (MongoDB's documents; legend-engine answers no MongoDB pivot to copy) has the plan's own facts
     * ({@link #planEnvelope}; register S132).
     */
    static com.legend.result.PureV1Writer.Envelope dynamicEnvelope(String sql,
            List<com.legend.executionplan.ExecutionPlan.Column> columns, @com.legend.base.Nullable List<@com.legend.base.Nullable String> reported) {
        if (reported == null) {
            return planEnvelope(sql, columns);
        }
        if (reported.size() != columns.size()) {
            throw new IllegalStateException("the database reports " + reported.size() + " columns, and the answer has "
                    + columns.size());
        }
        List<Map<String, Object>> builderColumns = new ArrayList<>();
        List<String> names = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            String name = com.legend.result.CellText.presentedName(columns.get(i).name());
            Map<String, Object> col = new LinkedHashMap<>();
            col.put("name", name);
            col.put("type", reportedType(reported.get(i)));
            builderColumns.add(col);
            names.add(name);
        }
        Map<String, Object> builder = new LinkedHashMap<>();
        builder.put("_type", "tdsBuilder");
        builder.put("columns", builderColumns);
        return new com.legend.result.PureV1Writer.Envelope(Json.toCompact(builder),
                Json.toCompact(List.of(activity(sql, true))), names);
    }

    /**
     * The Pure type legend-engine gives a dynamic result's column by the type name its driver reported
     * ({@code ResultColumn.labelTypePair}, upper-cased, by its prefix): {@code VARCHAR}, {@code CHAR} a String;
     * {@code FLOAT}, {@code DOUBLE}, {@code DECIMAL}, {@code NUMERIC}, {@code REAL} a Float; {@code INTEGER},
     * {@code BIGINT}, {@code SMALLINT}, {@code TINYINT}, {@code HUGEINT} an Integer; {@code BIT} a Boolean;
     * {@code TIMESTAMP} a DateTime; {@code DATE} a StrictDate; any other -- and none reported -- a String, as
     * legend-engine's own last line answers it (so a DuckDB {@code BOOLEAN} is a String there, copied as it is).
     */
    static String reportedType(@com.legend.base.Nullable String reported) {
        if (reported == null) {
            return "String";
        }
        String type = reported.toUpperCase(java.util.Locale.ROOT);
        if (type.startsWith("VARCHAR") || type.startsWith("CHAR")) {
            return "String";
        }
        if (type.startsWith("FLOAT") || type.startsWith("DOUBLE") || type.startsWith("DECIMAL")
                || type.startsWith("NUMERIC") || type.startsWith("REAL")) {
            return "Float";
        }
        if (type.startsWith("INTEGER") || type.startsWith("BIGINT") || type.startsWith("SMALLINT")
                || type.startsWith("TINYINT") || type.startsWith("HUGEINT")) {
            return "Integer";
        }
        if (type.startsWith("BIT")) {
            return "Boolean";
        }
        if (type.startsWith("TIMESTAMP")) {
            return "DateTime";
        }
        if (type.startsWith("DATE")) {
            return "StrictDate";
        }
        return "String";
    }

    /**
     * E8's plan for a host that runs the SQL itself and answers in the engine's JSON (the tab: its DuckDB; Python's
     * engine; docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C1): the {@code ExecuteInput} read, bound and planned as
     * {@link #execute} plans it -- as typed rows -- and answered with the plan record itself (PlanJson's text: since
     * 2026-11-02 the one plan every host path runs, docs/HOST_EXECUTORS_PROGRAM_2026_11_02.md R2): its one typed
     * result, whose statement the host runs and whose columns' facts decode the rows and make the answer's envelope
     * ({@link #jsonAnswer}); or, for a pivot whose columns its data names, its keyed plan, which the host runs as the
     * JVM's runner does ({@link #keyedBody}, {@link #keyedAnswer}; D18 D). {@code models} are the only models the host
     * runs: a request over any other is refused. What the host cannot run is refused here, at planning, by name
     * ({@code HostPlans.query}: a program that writes, a statement that binds values other than a pivot's keys) -- and
     * so is a relation column legend-engine's builder has no entry for, before any statement runs: legend-lite's
     * server runs the plans a host does not.
     */
    public static Answer jsonPlan(String body, java.util.Collection<String> models) {
        return answer(500, "COMPILATION", () -> {
            Hosted hosted = hosted(body, models);
            com.legend.executionplan.ExecutionPlan plan = hosted.query().executionPlan(hosted.runtime());
            if (plan.answerShape() == com.legend.executionplan.ExecutionPlan.TdsResult.Shape.VALUE) {
                // as the server's execute refuses it (engineWriter): a value's answer is not one the engine was
                // measured on
                throw unprobed(hosted.query());
            }
            try {
                // one statement of text, or a pivot whose columns its data names, run as the JVM runs it (D18 D)
                com.legend.executionplan.HostPlans.query(plan);
            } catch (IllegalArgumentException refused) {
                throw new com.legend.error.NotImplementedException(String.valueOf(refused.getMessage()));
            }
            if (plan.root() instanceof com.legend.executionplan.ExecutionPlan.TdsResult result
                    && result.shape() == com.legend.executionplan.ExecutionPlan.TdsResult.Shape.RELATION) {
                // the envelope made now, as the answer will make it: a column with no builder entry refuses at planning
                planEnvelope(result.sql().statement(), result.columns());
            }
            return com.legend.executionplan.PlanJson.write(plan);
        });
    }

    /**
     * E8's answer for a host that ran {@link #jsonPlan}'s plan's statement on its own database: {@code plan} that plan
     * (the plan record), {@code rows} the host's rows (a JSON array of objects, each a result column's name and its
     * cell as the host's driver read it: {@link com.legend.result.HostRows}) -- decoded by the plan's columns and
     * written by the writer {@link #execute} answers with, under the envelope the plan's own facts make
     * ({@link #planEnvelope}), so the host's answer is the server's text. The database computed every value; the
     * writer spells them.
     */
    public static Answer jsonAnswer(String plan, String rows) {
        return jsonAnswer(plan, (columns, sink) -> com.legend.result.HostRows.stream(rows, columns, sink));
    }

    /** {@link #jsonAnswer(String, String)} from the host's rows as ARROW: the IPC stream its database answered the
     *  plan's statement with ({@link com.legend.result.ArrowRows}; docs/HOST_EXECUTORS_PROGRAM_2026_11_02.md R3),
     *  read in place. */
    public static Answer jsonAnswer(String plan, java.nio.ByteBuffer arrow) {
        return jsonAnswer(plan, (columns, sink) -> com.legend.result.ArrowRows.stream(arrow, columns, sink));
    }

    private static Answer jsonAnswer(String plan, HostRowsSource rows) {
        return answer(500, "COMPILATION", () -> {
            com.legend.executionplan.ExecutionPlan.TdsResult result = hostResult(plan);
            java.io.StringWriter out = new java.io.StringWriter();
            com.legend.result.ResultSink sink = switch (result.shape()) {
                case OBJECTS -> com.legend.result.PureV1Writer.objects(out);
                case RELATION -> {
                    com.legend.result.PureV1Writer.Envelope envelope =
                            planEnvelope(result.sql().statement(), result.columns());
                    yield com.legend.result.PureV1Writer.relation(out, announced -> envelope);
                }
                case VALUE -> throw new IllegalArgumentException("the plan answers a value, which jsonPlan refuses: it "
                        + "is not jsonPlan's");
            };
            try {
                rows.stream(result.columns(), sink);
            } catch (java.io.IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
            return out.toString();
        });
    }

    /** An {@code ExecuteInput} read and its parameters bound, for a host that runs only {@code models}: its query
     *  (typed once) and its runtime. */
    private record Hosted(com.legend.TypedQuery query, String runtime) {
    }

    private static Hosted hosted(String body, java.util.Collection<String> models) {
        Json.Obj request = request(body);
        String model = modelText(request.getObj("model"));
        if (!models.contains(model)) {
            // a host's models change (Python's: a Live frame whose columns changed is written a new model), so
            // the refusal says what to do; a project's model with Data rows (Query's or Studio's, served by Python)
            // the host runs in that project's own database, handing it in as one of `models` (C5)
            throw new IllegalArgumentException("this engine runs queries over the models it serves, and over a "
                    + "project's model with Data rows (in a database of its own); the request's model is neither: if "
                    + "the engine's model changed since it was read (a frame whose columns changed), read it again");
        }
        LambdaFunction lambda = ProtocolReader.lambda(
                boundParameters(request.getObj("function"), request.getArrOr("parameterValues", null)));
        com.legend.TypedQuery query = com.legend.Compiler.query(com.legend.Compiler.compileModel(model), lambda);
        return new Hosted(query, runtimeOf(request, query.target()));
    }

    /**
     * A host's refusal of a call it could not finish -- its database refusing the SQL, say -- answered as
     * {@link #answer} answers a database's refusal: 500, the engine's error shape, no {@code errorType}.
     */
    public static Answer refused(String message) {
        return error(500, null, message);
    }

    /**
     * A host's run of {@link #jsonPlan}'s plan refused by its database (D18 B, the user 2026-11-03; the twin of
     * {@link #jsonAnswer} for a run that answered no rows): answered as legend-engine answers a database's refusal at
     * run time ({@code Execute}'s catch, {@code ExceptionTool}, {@code ExceptionError}) and as legend-lite's server
     * answers its own ({@link #named}) -- 500, {@code {"code":-1,"message":"<class>: <the database's words>",
     * "status":"error","trace":...}}. {@code message}: the database's words as the host received them; the class: the
     * one legend-engine's JDBC driver for the plan's database throws ({@code Databases.jdbcFailureClass}); {@code trace}: the host's
     * own account of the failure, since no host carries legend-engine's stack (S129).
     */
    public static Answer databaseRefused(String plan, String message, String trace) {
        com.legend.executionplan.ExecutionPlan read = hostPlan(plan);
        String failure = com.legend.database.Databases.jdbcFailureClass(read.answerSql().target().database().type());
        return error(500, null, failure + ": " + message, trace);
    }

    /**
     * A pivot's body for a host that read its keys ({@link #jsonPlan}'s keyed plan, D18 D, the user 2026-11-03):
     * {@code keys} the keys statement's rows as the host read them (a JSON array of rows, each an array of the texts it
     * wrote: each key's text and the names it gives), answered {@code {"statement", "parameters"}} -- the body written
     * for those keys by the plan record's own steps ({@link com.legend.executionplan.KeyedRun#expand}), and the text each
     * of its placeholders binds, in order. The host runs it as the JVM's runner does, in the keys' snapshot.
     */
    public static Answer keyedBody(String plan, String keys) {
        return answer(500, "COMPILATION", () -> {
            com.legend.executionplan.ExecutionPlan.Keyed keyed = hostKeyed(plan);
            List<List<Object>> read = keyRows(keys);
            // the column cap refused before the body is written, as the runner refuses it
            com.legend.executionplan.KeyedRun.answer(keyed, read);
            com.legend.executionplan.KeyedRun.Expanded body = com.legend.executionplan.KeyedRun.expand(keyed.body(),
                    read, i -> {
                        throw new IllegalStateException("a host's pivot body has no type hole (HostPlans.query)");
                    });
            List<Object> parameters = new ArrayList<>();
            for (com.legend.executionplan.KeyedRun.Bound b : body.bound()) {
                switch (b) {
                    case com.legend.executionplan.KeyedRun.Bound.Key key -> parameters.add(key.text());
                    case com.legend.executionplan.KeyedRun.Bound.Slot slot -> throw new IllegalStateException(
                            "a host's pivot body binds its keys alone (HostPlans.query), and placeholder binds slot "
                                    + slot.index());
                }
            }
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("statement", body.statement());
            out.put("parameters", parameters);
            return Json.toCompact(out);
        });
    }

    /**
     * Execute's answer for a host that ran a pivot's keys and then {@link #keyedBody}'s body (D18 D): {@code keys} the
     * keys' rows as {@link #keyedBody} took them, {@code rows} the body's rows (a JSON array of objects, each a body
     * column's name and its cell as the host's driver read it: {@link com.legend.result.HostRows}), {@code reported}
     * each body column's type as the database reported it, in the body's order (a JSON array of type names: DuckDB's own,
     * a {@code DESCRIBE} of the body) -- the answer's columns the plan record's own steps make
     * ({@link com.legend.executionplan.KeyedRun#answer}), its header from the report as legend-engine makes a dynamic
     * result's ({@link #dynamicEnvelope}), its activity the plan's body, as the server's answer carries it.
     */
    public static Answer keyedAnswer(String plan, String keys, String rows, String reported) {
        return answer(500, "COMPILATION", () -> {
            com.legend.executionplan.ExecutionPlan.Keyed keyed = hostKeyed(plan);
            com.legend.executionplan.KeyedRun.Answer answer = com.legend.executionplan.KeyedRun.answer(keyed, keyRows(keys));
            int n = answer.columns().size();
            List<String> physical = new ArrayList<>();
            for (Json.Node t : ((Json.Arr) Json.parse(reported)).items()) {
                physical.add(((Json.Str) t).value());
            }
            // the physical columns' report read onto the body's columns (a tagged one's slots beside it)
            List<@com.legend.base.Nullable String> types = com.legend.result.RowDecode.logicalReport(answer.read(), physical);
            // each body column read by the name the body writes it, placed in the answer's order
            List<com.legend.executionplan.ExecutionPlan.Column> readInOrder =
                    new ArrayList<>(java.util.Collections.nCopies(n, null));
            List<@com.legend.base.Nullable String> reportedInOrder = new ArrayList<>(java.util.Collections.nCopies(n, (String) null));
            for (int i = 0; i < n; i++) {
                readInOrder.set(answer.place().get(i), answer.read().get(i));
                reportedInOrder.set(answer.place().get(i), types.get(i));
            }
            String sql = keyed.body().statement();
            java.io.StringWriter out = new java.io.StringWriter();
            com.legend.result.PureV1Writer writer = com.legend.result.PureV1Writer.dynamicRelation(out,
                    (columns, report) -> dynamicEnvelope(sql, columns, report));
            try {
                com.legend.result.HostRows.stream(rows, readInOrder, new com.legend.result.ResultSink() {
                    @Override
                    public void columns(List<com.legend.executionplan.ExecutionPlan.Column> columns)
                            throws java.io.IOException {
                        writer.columns(answer.columns(), reportedInOrder);
                    }

                    @Override
                    public void batch(com.legend.result.Batch batch) throws java.io.IOException {
                        writer.batch(batch);
                    }

                    @Override
                    public void end() throws java.io.IOException {
                        writer.end();
                    }
                });
            } catch (java.io.IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
            return out.toString();
        });
    }

    /** A keys statement's rows as a host hands them back: a JSON array of rows, each an array of texts. */
    private static List<List<Object>> keyRows(String keys) {
        List<List<Object>> rows = new ArrayList<>();
        for (Json.Node row : ((Json.Arr) Json.parse(keys)).items()) {
            List<Object> cells = new ArrayList<>();
            for (Json.Node cell : ((Json.Arr) row).items()) {
                if (!(cell instanceof Json.Str text)) {
                    throw new IllegalArgumentException("a keys row's cell is the text the keys statement wrote, not "
                            + Json.toCompact(cell));
                }
                cells.add(text.value());
            }
            rows.add(cells);
        }
        return rows;
    }

    /** A host's plan ({@link #jsonPlan}'s, handed back), read by the plan record's own reader, its head shown when it
     *  does not read. */
    private static com.legend.executionplan.ExecutionPlan hostPlan(String plan) {
        try {
            return com.legend.executionplan.PlanJson.read(plan);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("the plan half's answer does not read as a plan (" + e.getMessage()
                    + "); it begins: " + head(plan), e);
        }
    }

    /** {@link #jsonPlan}'s keyed plan, handed back: one a host answers ({@code HostPlans.query}). */
    private static com.legend.executionplan.ExecutionPlan.Keyed hostKeyed(String plan) {
        com.legend.executionplan.ExecutionPlan read = com.legend.executionplan.HostPlans.query(hostPlan(plan));
        if (!(read.root() instanceof com.legend.executionplan.ExecutionPlan.Keyed keyed)) {
            throw new IllegalArgumentException("the plan is not a pivot whose columns its data names: it is not "
                    + "jsonPlan's keyed plan; it begins: " + head(plan));
        }
        return keyed;
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

    /** The first 200 characters of {@code text}, for a refusal that shows what it read. */
    private static String head(String text) {
        return text.length() <= 200 ? text : text.substring(0, 200) + "...";
    }

    /**
     * The relational activity, carrying the SQL: with its {@code _type} first, as the JSON answer writes it, or
     * without, as the Arrow answer's metadata does (measured, 4.145.0).
     */
    private static Map<String, Object> activity(String sql, boolean typed) {
        Map<String, Object> activity = new LinkedHashMap<>();
        if (typed) {
            activity.put("_type", "relational");
        }
        activity.put("comment", "-- \"executionTraceID\" : \"" + java.util.UUID.randomUUID() + "\"");
        activity.put("sql", sql);
        return activity;
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
            String store = i < plan.columns().size() ? plan.columns().get(i).store() : null;
            rc.put("dataType", store == null ? "" : store);
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

    /** A {@code PureModelContextText}'s code, for the routes that read a model's text itself (the host and the
     *  model-change routes); any other model context is refused there. The routes that compile a model read its
     *  data too ({@code ModelInput}; 2026-10-21, the independent audit: this said no reader was built). */
    static String modelText(Json.Obj model) {
        String type = model.getStringOr("_type", "");
        if (!"text".equals(type)) {
            throw new IllegalArgumentException("a model of _type '" + type + "': this route reads a model's text "
                    + "(PureModelContextText, {\"_type\":\"text\",\"code\":...})");
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
        } catch (RuntimeException | StackOverflowError e) {
            if (refusesTheText(e)) {
                return error(status, errorType, String.valueOf(e.getMessage()));
            }
            if (!(e instanceof IllegalArgumentException || e instanceof com.legend.error.DataError)) {
                e.printStackTrace();
            }
            // legend-engine's ExceptionError: the failure named, and, for a database's refusal at run time, its stack
            // (lite's own: S129; D18 B) -- a planning answer stays one text on every build
            return error(500, null, named(e), databaseRefusal(e) ? trace(e) : null);
        }
    }

    /**
     * A failure as the routes name it, for a host that answers one outside a route: the boundary's own operations,
     * which the tab and Python fold ({@code planner.Folded}), so that their callers read one vocabulary for every call
     * (docs/PROTOCOL_PROGRAM_2026_10_05.md, leg 6, with the DataCube + Python line). {@code kind} is legend-engine's
     * errorType for a refusal of the text -- {@code PARSER} when it does not parse, {@code COMPILATION} when it does
     * not compile or uses a construct not implemented -- else the status the server answers, {@code "500"};
     * {@code message} is as {@link #answer} writes it.
     */
    public record Refusal(String kind, String message) {
    }

    /** {@code e} as a {@link Refusal}. */
    public static Refusal refusal(Throwable e) {
        if (refusesTheText(e)) {
            return new Refusal(e instanceof com.legend.parser.ParseException ? "PARSER" : "COMPILATION",
                    String.valueOf(e.getMessage()));
        }
        return new Refusal("500", named(e));
    }

    /** The text does not parse or compile, or a construct is not implemented: answered with an errorType. */
    private static boolean refusesTheText(Throwable e) {
        return e instanceof com.legend.error.LegendCompileException
                || e instanceof com.legend.error.NotImplementedException
                || e instanceof com.legend.sql.dialect.DialectCapability;
    }

    /**
     * Any other failure, as legend-engine names one ({@code ExceptionError}: {@code ExceptionUtils.getRootCauseMessage},
     * D18 B, the user 2026-11-03): a database's refusal (a {@link com.legend.error.DataError} anywhere in its causes) by
     * its ROOT cause, the driver's own failure all the way down (on H2 a malformed number's
     * {@code NumberFormatException}, where one level up names {@code JdbcSQLDataException}); any other failure -- lite's
     * own, which legend-engine has no answer for -- by itself, so its words keep their context. Its class's short name
     * ({@link #shortClassName}), a colon, and its message (none an empty one).
     */
    private static String named(Throwable e) {
        Throwable n = databaseRefusal(e) ? causes(e).get(causes(e).size() - 1) : e;
        return shortClassName(n) + ": " + (n.getMessage() == null ? "" : n.getMessage());
    }

    /** Whether {@code e} is a database's refusal at run time: a {@link com.legend.error.DataError} among its causes. */
    private static boolean databaseRefusal(Throwable e) {
        return causes(e).stream().anyMatch(t -> t instanceof com.legend.error.DataError);
    }

    /** {@code e} and its causes, in order, each once ({@code ExceptionUtils.getThrowableList}). */
    private static List<Throwable> causes(Throwable e) {
        List<Throwable> causes = new ArrayList<>();
        for (Throwable t = e; t != null && !causes.contains(t); t = t.getCause()) {
            causes.add(t);
        }
        return causes;
    }

    /** {@code t}'s class as legend-engine names it ({@code ClassUtils.getShortClassName}): its package dropped, a nested
     *  class joined to its outer one by a dot ({@code Outer.Inner}). */
    private static String shortClassName(Throwable t) {
        String name = t.getClass().getName();
        return name.substring(name.lastIndexOf('.') + 1).replace('$', '.');
    }

    /** {@code e}'s stack as Java prints it: legend-engine's {@code trace} ({@code ExceptionError}), here lite's own
     *  (S129). */
    private static String trace(Throwable e) {
        java.io.StringWriter out = new java.io.StringWriter();
        e.printStackTrace(new java.io.PrintWriter(out));
        return out.toString();
    }

    private static Answer error(int status, @com.legend.base.Nullable String errorType, String message) {
        return error(status, errorType, message, null);
    }

    /** legend-engine's error envelope ({@code ExceptionError}, its keys sorted and its nulls dropped:
     *  {@code ObjectMapperFactory}). */
    private static Answer error(int status, @com.legend.base.Nullable String errorType, String message,
            @com.legend.base.Nullable String trace) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("code", -1);
        if (errorType != null) {
            out.put("errorType", errorType);
        }
        out.put("message", message);
        out.put("status", "error");
        if (trace != null) {
            out.put("trace", trace);
        }
        return new Answer(status, Json.toCompact(out));
    }
}
