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
        return route(path, rawQuery, body, runner, Models.COMPILED);
    }

    /** {@link #route(String, String, String, Runner)}, a model given as text compiled by {@code models}. */
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
            case "/api/lite/v1/compilation/compile" -> compileEveryError(body);
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
                String runtime = runtimeOf(request, query.target());
                try {
                    runner.run(query, runtime, plan -> arrowWriter(query, plan, out));
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

    /** The Arrow writer of {@code plan}'s answer onto {@code out}, its schema metadata the engine's for the columns
     *  the run announces ({@link #executeArrow}). */
    public static com.legend.result.ResultSink arrowWriter(com.legend.TypedQuery query,
            com.legend.executionplan.ExecutionPlan plan, java.io.OutputStream out) {
        return switch (plan.answerShape()) {
            case OBJECTS -> throw new IllegalArgumentException(
                    "ARROW_IPC answers a relation query; a graph fetch under it is refused (upstream's answer unprobed)");
            case RELATION -> {
                java.util.function.Function<List<com.legend.executionplan.ExecutionPlan.Column>, Map<String, String>>
                        metadata = columns -> {
                            com.legend.result.PureV1Writer.Envelope e = envelope(query.resultType(),
                                    plan.answerSql().statement(), columns);
                            Map<String, String> m = new LinkedHashMap<>();
                            m.put("legend.builder", e.builder());
                            m.put("legend.activities",
                                    Json.toCompact(List.of(activity(plan.answerSql().statement(), false))));
                            m.put("legend.columns", Json.toCompact(e.names()));
                            return m;
                        };
                List<com.legend.executionplan.ExecutionPlan.Column> stated = plan.answerColumns();
                // the JSON route's rule (engineWriter; the phase 3 audit, 2026-10-27): the envelope from the plan's
                // stated columns BEFORE any statement runs, so a column with no builder entry refuses before a write
                // program changes anything; made from the announced columns only where the plan states none (a
                // dynamic pivot's)
                yield stated == null ? new com.legend.result.ArrowWriter(out, metadata)
                        : new com.legend.result.ArrowWriter(out, metadata.apply(stated));
            }
            case VALUE -> throw unprobed(query);
        };
    }

    /**
     * Execute's answer in upstream's Arrow format for a host that ran {@link #jsonPlan}'s {@code run} statement
     * itself (track A, 2026-10-29; the twin of {@link #jsonAnswer}): the plan half's answer and the host's rows (a
     * JSON array of objects, each a column's name and its cell as the host's driver read it), decoded by the plan's
     * columns and written by lite's Arrow writer under the plan's envelope, as one zstd frame. A graph fetch's plan
     * is refused: ARROW_IPC answers relations.
     */
    public static Answer arrowAnswer(String plan, String rows) {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        Answer run = answer(500, "COMPILATION", () -> {
            Json.Obj planned;
            try {
                planned = Json.parseObject(plan);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("the plan half's answer does not parse (" + e.getMessage()
                        + "); it begins: " + head(plan), e);
            }
            if (!(planned.has("shape") && planned.get("shape") instanceof Json.Str shape
                    && planned.has("resultColumns") && planned.get("resultColumns") instanceof Json.Arr resultColumns
                    && planned.has("builder") && planned.get("builder") instanceof Json.Obj builder)) {
                throw new IllegalArgumentException("the plan answers no shape, result columns or builder: it is not "
                        + "jsonPlan's");
            }
            if (!shape.value().equals("RELATION")) {
                throw new IllegalArgumentException("ARROW_IPC answers a relation query; a graph fetch is answered in JSON");
            }
            if (!(planned.has("columns") && planned.get("columns") instanceof Json.Arr names
                    && planned.has("activities") && planned.get("activities") instanceof Json.Arr activities
                    && planned.has("sql") && planned.get("sql") instanceof Json.Str sql)) {
                throw new IllegalArgumentException("the plan answers no columns, activities or sql: it is not jsonPlan's");
            }
            List<com.legend.executionplan.ExecutionPlan.Column> columns =
                    com.legend.executionplan.PlanJson.readColumns(Json.toCompact(resultColumns));
            Map<String, String> metadata = new LinkedHashMap<>();
            metadata.put("legend.builder", Json.toCompact(builder));
            metadata.put("legend.activities", Json.toCompact(List.of(activity(sql.value(), false))));
            metadata.put("legend.columns", Json.toCompact(names));
            try {
                com.legend.result.HostRows.stream(rows, columns, new com.legend.result.ArrowWriter(out, metadata));
            } catch (java.io.IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
            return "";
        });
        if (run.status() != 200) {
            return run;
        }
        return Answer.bytes(200, com.legend.arrowipc.ZstdFrame.of(out.toByteArray()), "application/json",
                Map.of("x-legend-response-format", "FormatNotSet"));
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
                    yield com.legend.result.PureV1Writer.relation(out,
                            columns -> envelope(query.resultType(), plan.answerSql().statement(), columns));
                }
                com.legend.result.PureV1Writer.Envelope made = envelope(query.resultType(),
                        plan.answerSql().statement(), stated);
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

    /** A relation's envelope for the columns the run announced: each column's builder entry by the type the compiler
     *  gave it (a column only the data names -- a dynamic pivot's -- by its plan type, a primitive), the relational
     *  activity carrying {@code sql}, and the columns' names as the engine labels them. */
    private static com.legend.result.PureV1Writer.Envelope envelope(com.legend.compiler.element.type.ExprType rootType,
            String sql, List<com.legend.executionplan.ExecutionPlan.Column> columns) {
        Map<String, com.legend.compiler.element.type.Type> declared = new LinkedHashMap<>();
        for (var c : UpstreamRelationType.columns(rootType)) {
            declared.put(c.name(), c.type());
        }
        List<Map<String, Object>> builderColumns = new ArrayList<>();
        List<String> names = new ArrayList<>();
        for (var c : columns) {
            // a column's type is the compiler's, under its name as the plan spells it or as a cast presents it (a
            // pivot's generated name quoted); a column only the data names -- a dynamic pivot's generated one -- has
            // none, and is typed by its plan type, a primitive; any other column the compiler did not type is refused
            String name = com.legend.result.CellText.presentedName(c.name());
            com.legend.compiler.element.type.Type t = declared.containsKey(c.name()) ? declared.get(c.name())
                    : declared.containsKey(name) ? declared.get(name)
                    : c.name().contains(com.legend.compiler.element.type.Type.RelationType.PIVOT_SEPARATOR)
                            // (the plan names a primitive by its short name, the table by its path: every dynamic
                            // pivot's engine answer was refused until 2026-10-27, found by the PCT read as the engine's
                            // adapter reads it, the result boundary program's stage 1, step 3)
                            ? com.legend.compiler.element.type.Type.Primitive.findByFqn(
                                    "meta::pure::metamodel::type::" + c.type()).orElseThrow(() ->
                                    new IllegalStateException("the generated column '" + c.name() + "' of type "
                                            + c.type() + " is no primitive"))
                            : null;
            if (t == null) {
                throw new IllegalStateException("column '" + c.name() + "' is none the compiler typed: "
                        + declared.keySet());
            }
            builderColumns.add(builderColumn(name, t));
            names.add(name);
        }
        Map<String, Object> builder = new LinkedHashMap<>();
        builder.put("_type", "tdsBuilder");
        builder.put("columns", builderColumns);
        return new com.legend.result.PureV1Writer.Envelope(Json.toCompact(builder),
                Json.toCompact(List.of(activity(sql, true))), names);
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
            Hosted hosted = hosted(body, models);
            QueryPlan plan = hosted.query().plan(hosted.runtime());
            if (plan.shape() != com.legend.plan.ResultShape.TABULAR && plan.shape() != com.legend.plan.ResultShape.GRAPH) {
                throw unprobed(hosted.query());
            }
            if (plan.shape() == com.legend.plan.ResultShape.GRAPH) {
                // upstream's Arrow answer is a relational result's (RelationalResultToArrowIPCSerializer)
                throw new IllegalArgumentException("ARROW_IPC answers a relation query; a graph fetch is answered in JSON");
            }
            Map<String, Object> metadata = new LinkedHashMap<>();
            metadata.put("legend.builder", Json.toCompact(tdsBuilder(plan)));
            metadata.put("legend.activities", Json.toCompact(List.of(activity(plan.sql(), false))));
            metadata.put("legend.columns", Json.toCompact(columnNames(plan)));
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("sql", plan.sql());
            out.put("metadata", metadata);
            return Json.toCompact(out);
        });
    }

    /**
     * E8's plan half for a host that runs the SQL itself and answers in the engine's JSON (the tab: its DuckDB;
     * Python's engine; docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C1): the {@code ExecuteInput} read, bound and planned
     * as {@link #execute} plans it -- as typed rows -- and answered with what {@link #jsonAnswer} needs to write the
     * answer the server writes from the host's rows: {@code run}, the one statement to run (also {@code sql}); the
     * answer's {@code shape} ({@code RELATION}, or {@code OBJECTS} for a graph fetch); {@code resultColumns}, the
     * statement's result columns with their type facts, by which the rows decode; and a relation's envelope (the TDS
     * builder, the activities and the columns' names as the JSON answer writes them), or a graph fetch's builder
     * {@code {"_type": "json"}}. {@code models} are the only models the host runs, as {@link #arrowPlan}'s. A plan
     * the host cannot run as one statement of text -- a pivot whose columns the data names (its keys are read first),
     * a program that writes, a statement that binds values -- is refused by name: legend-lite's server runs it.
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
            if (!(plan.root() instanceof com.legend.executionplan.ExecutionPlan.TdsResult result)) {
                throw new com.legend.error.NotImplementedException("a host runs one statement, and this query's plan "
                        + "is not one (" + plan.root().getClass().getSimpleName() + ": a pivot whose columns the data "
                        + "names, or a program that writes); legend-lite's server runs it");
            }
            if (!result.sql().slots().isEmpty()) {
                throw new com.legend.error.NotImplementedException("a host runs a statement's text, and this one binds "
                        + "values (" + result.sql().slots().size() + "); legend-lite's server runs it");
            }
            String statement = result.sql().statement();
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("sql", statement);
            out.put("run", statement);
            out.put("shape", result.shape().name());
            out.put("resultColumns", Json.parse(com.legend.executionplan.PlanJson.writeColumns(result.columns())));
            if (result.shape() == com.legend.executionplan.ExecutionPlan.TdsResult.Shape.OBJECTS) {
                out.put("builder", Map.of("_type", "json"));
            } else {
                com.legend.result.PureV1Writer.Envelope e = envelope(hosted.query().resultType(), statement,
                        result.columns());
                out.put("builder", Json.parse(e.builder()));
                out.put("activities", Json.parse(e.activities()));
                out.put("columns", e.names());
            }
            return Json.toCompact(out);
        });
    }

    /**
     * E8's answer for a host that ran {@link #jsonPlan}'s {@code run} statement on its own database: {@code plan} that
     * plan's answer, {@code rows} the host's rows (a JSON array of objects, each a result column's name and its cell
     * as the host's driver read it: {@link com.legend.result.HostRows}) -- decoded by the plan's result columns and
     * written by the writer {@link #execute} answers with, so the host's answer is the server's text. The database
     * computed every value; the writer spells them.
     */
    public static Answer jsonAnswer(String plan, String rows) {
        return answer(500, "COMPILATION", () -> {
            // jsonPlan's own answer, handed back: read by kind, never cast (the tab's WebAssembly has no ClassCastException)
            Json.Obj planned;
            try {
                planned = Json.parseObject(plan);
            } catch (IllegalArgumentException e) {
                // the refusal names what it was reading and shows its head: a host's answer text that does not parse
                // is otherwise a line and a column with no text to look at (2026-10-26, the arm and Windows lanes)
                throw new IllegalArgumentException("the plan half's answer does not parse (" + e.getMessage()
                        + "); it begins: " + head(plan), e);
            }
            if (!(planned.has("shape") && planned.get("shape") instanceof Json.Str shape
                    && planned.has("resultColumns") && planned.get("resultColumns") instanceof Json.Arr resultColumns
                    && planned.has("builder") && planned.get("builder") instanceof Json.Obj builder)) {
                throw new IllegalArgumentException("the plan answers no shape, result columns or builder: it is not "
                        + "jsonPlan's");
            }
            List<com.legend.executionplan.ExecutionPlan.Column> columns =
                    com.legend.executionplan.PlanJson.readColumns(Json.toCompact(resultColumns));
            java.io.StringWriter out = new java.io.StringWriter();
            com.legend.result.ResultSink sink;
            switch (shape.value()) {
                case "OBJECTS" -> sink = com.legend.result.PureV1Writer.objects(out);
                case "RELATION" -> {
                    if (!(planned.has("columns") && planned.get("columns") instanceof Json.Arr names
                            && planned.has("activities") && planned.get("activities") instanceof Json.Arr activities)) {
                        throw new IllegalArgumentException("the plan answers no columns or activities: it is not "
                                + "jsonPlan's");
                    }
                    List<String> labels = new ArrayList<>();
                    for (Json.Node n : names.items()) {
                        if (!(n instanceof Json.Str name)) {
                            throw new IllegalArgumentException("the plan's columns are not names: it is not jsonPlan's");
                        }
                        labels.add(name.value());
                    }
                    com.legend.result.PureV1Writer.Envelope envelope = new com.legend.result.PureV1Writer.Envelope(
                            Json.toCompact(builder), Json.toCompact(activities), labels);
                    sink = com.legend.result.PureV1Writer.relation(out, announced -> envelope);
                }
                default -> throw new IllegalArgumentException("the plan's shape '" + shape.value()
                        + "' is none jsonPlan answers");
            }
            try {
                com.legend.result.HostRows.stream(rows, columns, sink);
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

    /** The TDS builder: each column's name, Pure type and relational spelling. */
    private static Map<String, Object> tdsBuilder(QueryPlan plan) {
        List<Map<String, Object>> columns = new ArrayList<>();
        for (var c : UpstreamRelationType.columns(plan.rootType())) {
            columns.add(builderColumn(c.name(), c.type()));
        }
        Map<String, Object> builder = new LinkedHashMap<>();
        builder.put("_type", "tdsBuilder");
        builder.put("columns", columns);
        return builder;
    }

    /** One column of the TDS builder: its name, its Pure type as the builder writes it, its relational spelling. */
    private static Map<String, Object> builderColumn(String name, com.legend.compiler.element.type.Type type) {
        Map<String, Object> col = new LinkedHashMap<>();
        col.put("name", name);
        col.put("type", UpstreamRelationType.tdsTypePath(type));
        col.put("relationalType", UpstreamRelationType.relationalSpelling(type));
        return col;
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
            return error(500, null, named(e));
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

    /** Any other failure, as the server writes it: its class's simple name and its message (a database's refusal, its cause's). */
    private static String named(Throwable e) {
        Throwable n = e instanceof com.legend.error.DataError && e.getCause() != null ? e.getCause() : e;
        return n.getClass().getSimpleName() + ": " + n.getMessage();
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
