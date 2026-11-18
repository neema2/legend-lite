// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;


import com.legend.json.Json;
import com.legend.protocol.ProtocolReader;
import com.legend.protocol.spec.LambdaFunction;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * legend-lite's OWN API, v1 (lite's API v1 spec, docs/records/lite-api-v1-2026-11-17.md; the user's go of 2026-11-17):
 * the server binding's router, one POST per operation at {@code /api/lite/v1/<operation>} ({@link #route}) -- the
 * model's and the query's operations ({@link LiteV1Planning}), the model actions ({@link ModelChanges}: DataCube's
 * Create a Class…, an Association…, a Data Space…) and {@code #TS}'s two ({@link #tsItemOf}, {@link #tsStatement}),
 * every binding's one road ({@link #operate}) --
 * and lite's ONE error shape, {@code {kind, message, where?, raise?}} ({@link Kind}, {@link Refusal}, {@link #refusal}:
 * the classifier every binding reads). legend-engine's {@code pure/v1} routes and their envelope are the edge's
 * ({@code com.legend.edge.PureV1Routes}, {@code com.legend.edge.EngineEnvelope}), which depends on this kit -- its
 * {@link Answer}, {@link #request}, {@link Runner} and {@link Models} -- never the reverse (G5). Each call is a pure
 * function of its request -- text in, a status and JSON out -- so it is tested without HTTP.
 *
 * <p>THE PLAN SIDE ({@code //core:lite_v1}): no database and no driver; a query's run is the host's, handed in.
 */
public final class LiteV1Api {

    private LiteV1Api() {
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
     * cube query; and a request may carry a whole model as data (legend-engine's {@code PureModelContextData}, read at
     * the edge by {@code com.legend.edge.ModelInput}: J40.ModelInput, moved 2026-11-18), which nests as deep as a
     * model's own JSON -- the bound {@code ModelReader} reads a model's text at (4096; 2026-10-21, the independent
     * audit: at 1024 a model compiled as text and was refused as data). It still bounds a hostile body.
     */
    static final Json.Config REQUEST = new Json.Config(4096);

    /**
     * How a host runs an execute: the query this API compiled, planned and run by the host's door (legend-lite's
     * server hands in its product door, {@code Execution.runOnProjectDatabases} under its file fence:
     * {@code ConnectionResolver.runner(sources)}, its startup settings' folders; {@code ConnectionResolver.RUNNER} is the
     * library's, any file),
     * its rows written in the output this API names ({@code Output.Rows} with the edge's receiver, {@code edge.EngineAnswers}: the engine's
     * formats, their envelope's facts read from the query before the run; the writer is made by the result library from
     * the plan before any statement runs, so an answer the engine's API has no form for is refused before a program's
     * writes land; until 2026-11-09 a factory the run called back). This API compiles and writes; it plans
     * and runs nothing of an execute itself (slice 5, 2026-11-09: its own plan-then-run is gone -- the user's question,
     * "why can't v1 call the same door directly?"; this library ships in the WebAssembly and native planners too, which
     * have no database, so the door is handed in).
     */
    @FunctionalInterface
    public interface Runner {
        void run(com.legend.TypedQuery query, String runtime, com.legend.result.Output output)
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

    /** How a binding plans a query on a runtime: the plan its own run runs -- the server's
     *  {@code TypedQuery::projectPlan} (its project databases), the tab's {@code TypedQuery::executionPlan}. */
    @FunctionalInterface
    public interface Plans {
        com.legend.executionplan.ExecutionPlan plan(com.legend.TypedQuery query, String runtime);
    }

    /**
     * ONE OPERATION OF LITE'S API BY ITS PATH ({@code /api/lite/v1/<operation>}, lite's API v1 spec §2; the user's go of
     * 2026-11-17), as the server routes it: {@code query/run} through {@code runner} ({@link LiteV1Planning#queryRun}),
     * every other operation through {@link #operate}, its failure answered as lite's refusal ({@link #answer}). The
     * server compiles a model by {@code models} and plans on its project databases ({@code TypedQuery::projectPlan}).
     */
    public static Answer route(String path, String body, LiteV1Planning.Runner runner, Models models) {
        return switch (path) {
            case "/api/lite/v1/query/run" -> LiteV1Planning.queryRun(body, runner, models);
            default -> answer(() -> operate(path, body, models, com.legend.TypedQuery::projectPlan));
        };
    }

    /**
     * THE ONE ROAD OF EVERY BINDING (spec §5: each binding a thin layer over the same product operation): an operation
     * by its path, its request's JSON in, its answer's JSON out, or the failure {@link #refusal} classifies -- the
     * server's route ({@link #route}) and the browser's exports (planner.Boundary, with its held models and
     * {@code TypedQuery::executionPlan}) call this one method. Each request is closed ({@link #read}); a model given as
     * text is compiled by {@code models}; a query is planned by {@code plans}. A path that names no operation is refused
     * by name, kind {@code request}; {@code query/run} is a host's (it runs), never this. The paths are written out, one
     * case each, as the server's surface census reads them ({@code PlannerSurfaceTest}).
     */
    public static String operate(String path, String request, Models models, Plans plans) {
        String body = request;
        return switch (path) {
            case "/api/lite/v1/model/check" -> {
                Json.Obj in = read(body, "a model/check request", java.util.Set.of(), "model");
                yield LiteV1Planning.modelCheck(text(in, "model"));
            }
            case "/api/lite/v1/model/elements" -> {
                Json.Obj in = read(body, "a model/elements request", java.util.Set.of(), "model");
                yield LiteV1Planning.modelElements(text(in, "model"));
            }
            case "/api/lite/v1/query/parse" -> {
                Json.Obj in = read(body, "a query/parse request", java.util.Set.of(), "text");
                yield LiteV1Planning.queryParse(text(in, "text"));
            }
            case "/api/lite/v1/query/print" -> {
                Json.Obj in = read(body, "a query/print request", java.util.Set.of("style"), "query");
                yield LiteV1Planning.queryPrint(object(in, "query"), in.has("style") ? text(in, "style") : "pretty");
            }
            case "/api/lite/v1/query/type" -> {
                Json.Obj in = read(body, "a query/type request", java.util.Set.of(), "model", "query");
                LambdaFunction query = ProtocolReader.lambda(object(in, "query"));
                yield models.with(text(in, "model"), ctx -> LiteV1Planning.queryType(ctx, query));
            }
            case "/api/lite/v1/query/plan" -> {
                Json.Obj in = read(body, "a query/plan request", java.util.Set.of(), "model", "query", "runtime");
                LambdaFunction query = ProtocolReader.lambda(object(in, "query"));
                String runtime = text(in, "runtime");
                yield models.with(text(in, "model"), ctx -> LiteV1Planning.queryPlan(ctx, query, runtime, plans));
            }
            case "/api/lite/v1/model/createClass" ->
                    Json.toCompact(ModelChanges.createClass(requestObject(body, "a model action's request")));
            case "/api/lite/v1/model/createAssociation" ->
                    Json.toCompact(ModelChanges.createAssociation(requestObject(body, "a model action's request")));
            case "/api/lite/v1/model/createDataSpace" ->
                    Json.toCompact(ModelChanges.createDataSpace(requestObject(body, "a model action's request")));
            case "/api/lite/v1/ts/item" -> {
                Json.Obj in = read(body, "a ts/item request", java.util.Set.of(), "text");
                yield tsItemOf(text(in, "text"));
            }
            case "/api/lite/v1/ts/statement" -> {
                Json.Obj in = read(body, "a ts/statement request", java.util.Set.of(), "model", "query");
                LambdaFunction query = ProtocolReader.lambda(object(in, "query"));
                yield models.with(text(in, "model"), ctx -> tsStatement(ctx, query));
            }
            // every other path, in lite's own words (B7, the orchestrator's ruling (3) of 2026-11-17: the product names
            // nothing engine-shaped, not even its paths' prefix; with the edge mounted, the edge serves legend-engine's)
            default -> throw new MalformedRequest("no such legend-lite API: " + path);
        };
    }

    /** A request body's JSON object, else an {@code IllegalArgumentException}: the edge's reading of legend-engine's
     *  requests (its envelope names the exception, byte for byte); lite's own read through {@link #read}. */
    public static Json.Obj request(String body) {
        Json.Node n = Json.parse(body, REQUEST);
        if (n instanceof Json.Obj o) {
            return o;
        }
        throw new IllegalArgumentException("the request body is not a JSON object");
    }

    /** {@code body} as lite's request, a JSON object, else refused by name, kind {@code request}. */
    static Json.Obj requestObject(String body, String what) {
        try {
            return request(body);
        } catch (IllegalArgumentException e) {
            throw new MalformedRequest(what + " is not a JSON object: " + e.getMessage(), e);
        }
    }

    /** {@code ts/statement}'s answer over a model compiled ({@code {"statement": text}} or {@code {"refused": why}}; D12,
     *  Show #TS; upstream has no #TS): a query of {@code #TS} over grids -- the lambda's protocol JSON, each grid bound
     *  by {@code let} and the island reading it -- as the one statement a person would write, each grid a flat source
     *  named by WITH ({@link com.legend.protocol.ts.TsShow}), refused where a grid's relation has no flat form yet. The
     *  model's compiler answers the type a calculated column's {@code +} reads (A6, 2026-11-03). The route's own Java,
     *  which the tab's export asks too (planner.Boundary.jsonToTs; the edge audit's H2, 2026-11-17). */
    public static String tsStatement(com.legend.compiler.element.ModelContext ctx, LambdaFunction lambda) {
        // the island's text read through the parse door, which asks the run's wall (C5-J's last round, 2026-11-15)
        com.legend.protocol.ts.TsShow.Shown shown = com.legend.protocol.ts.TsShow.of(
                lambda, tsTypes(ctx), com.legend.door.Parse::tsIsland);
        return Json.toCompact(shown.statement() != null ? Map.of("statement", shown.statement())
                : Map.of("refused", java.util.Objects.requireNonNull(shown.refused())));
    }

    /** {@code ts/item}'s answer for one item's {@code text} (A3 step 2, docs/TS_A3_ONE_READER_PLAN_2026_11_03.md;
     *  upstream has no #TS): one #TS item as a formula box holds a series, read by core's parser -- the one reader
     *  ({@link com.legend.protocol.ts.TsParser#item}, through the parse door {@code door.Parse.tsItem}):
     *  {@code {"text": canonical, "item": {alias, expr}}}, the item's tree as data ({@link TsTree}), or
     *  {@code {"error": {message, line, column}}} where the text is no item -- the box's answer, never a failed
     *  request. The route's own Java, which the tab's export asks too (planner.Boundary.tsItem; the edge audit's H2). */
    public static String tsItemOf(String text) {
        Map<String, Object> out = new LinkedHashMap<>();
        try {
            // read through the parse door, which asks the run's wall (C5-J's last round, 2026-11-15)
            com.legend.protocol.ts.TsAst.Line line = com.legend.door.Parse.tsItem(text);
            // both made before either is answered: an item whose tree is refused (a parameter, $A) is an error alone,
            // never a text with no tree (item 11b, 2026-11-07: the tile read the text and found no tree)
            String printed = com.legend.protocol.ts.TsPrinter.print(line);
            Map<String, Object> tree = TsTree.of(line);
            out.put("text", printed);
            out.put("item", tree);
        } catch (com.legend.protocol.sql.SqlTranslationException e) {
            Map<String, Object> error = new LinkedHashMap<>();
            error.put("message", e.getMessage());
            error.put("line", e.line());
            error.put("column", e.column());
            out.put("error", error);
        }
        return Json.toCompact(out);
    }

    /**
     * Show #TS's types over {@code ctx} ({@link com.legend.protocol.ts.TsShow.Types}): a column of a query's relation
     * typed as the compiler types it, read off the query's result -- the compiler's answer, never the printer's.
     */
    public static com.legend.protocol.ts.TsShow.Types tsTypes(com.legend.compiler.element.ModelContext ctx) {
        return (query, column) -> {
            for (com.legend.compiler.element.type.Type.Column c : com.legend.plan.PureSpelling.relationColumns(com.legend.Compiler
                    .query(ctx, query).resultType())) {
                if (c.name().equals(column)) {
                    return com.legend.plan.PureSpelling.typePath(c.type());
                }
            }
            throw new IllegalStateException("the relation has no column " + column);
        };
    }

    // ---------------------------------------------------------------------
    // Plumbing
    // ---------------------------------------------------------------------

    /** A {@code PureModelContextText}'s code, for the routes that read a model's text itself (the host and the
     *  model-change routes); any other model context is refused there. legend-engine's routes that compile a model read
     *  its data too ({@code com.legend.edge.ModelInput}, the edge's since 2026-11-18, J40.ModelInput; 2026-10-21, the
     *  independent audit: this said no reader was built). */
    public static String modelText(Json.Obj model) {
        String type = model.getStringOr("_type", "");
        if (!"text".equals(type)) {
            throw new IllegalArgumentException("a model of _type '" + type + "': this route reads a model's text "
                    + "(PureModelContextText, {\"_type\":\"text\",\"code\":...})");
        }
        return model.getString("code");
    }


    /**
     * LITE'S ERROR KINDS (lite's API v1 spec §3, the audit's J18, 2026-11-17): every refusal, on every binding, is one of
     * these, written {@code {kind, message, where?, raise?}} ({@link Refusal}). Over HTTP the status follows the kind:
     * 400 for parse, compile, unsupported, request and parameters; 422 for data; 500 for internal. There is no
     * {@code PARSER}/{@code COMPILATION}, no {@code code: -1}, no {@code status: "error"} and no {@code errorType} in the
     * product: legend-engine's envelope is the edge's ({@code com.legend.edge.EngineEnvelope}).
     */
    public enum Kind {
        /** The text does not parse. */
        PARSE(400, "parse"),
        /** The model or the query does not type or compile. */
        COMPILE(400, "compile"),
        /** A construct lite refuses by name (not yet, or by a ruling). */
        UNSUPPORTED(400, "unsupported"),
        /** A run's raise: division by zero, an index out of bounds, ... (its kind in {@link Refusal#raise}). */
        DATA(422, "data"),
        /** A run's parameter values: one missing, or one its declared type does not take (the caller's values). */
        PARAMETERS(400, "parameters"),
        /** The request itself is malformed: a key it does not read or lacks, a value of the wrong kind. */
        REQUEST(400, "request"),
        /** A saved query or saved page the request names does not exist (the user, 2026-11-17 ~23:31; spec §3). */
        NOT_FOUND(404, "notFound"),
        /** Anything else: a defect, said as one. */
        INTERNAL(500, "internal");

        private final int status;
        private final String wire;

        // each kind's word explicit (2026-11-17, notFound: lower camel, which no spelling of the constant's name gives)
        Kind(int status, String wire) {
            this.status = status;
            this.wire = wire;
        }

        /** The kind as every binding writes it: {@code parse}, {@code compile}, ..., {@code notFound}. */
        public String wire() {
            return wire;
        }

        /** The HTTP status a refusal of this kind is answered with. */
        public int status() {
            return status;
        }
    }

    /**
     * A refusal in lite's one error shape: its {@link Kind}, lite's own words, where it happened (a position, or a site)
     * where there is one, and for a {@code data} refusal alone the raise's kind, in lower camel as every wire name is
     * ({@code divideByZero}, {@code indexOutOfBounds}, ...).
     */
    public record Refusal(Kind kind, String message, @com.legend.base.Nullable String where,
            @com.legend.base.Nullable String raise) {

        /** {@code {kind, message, where?, raise?}}. */
        public String json() {
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("kind", kind.wire());
            out.put("message", message);
            if (where != null) {
                out.put("where", where);
            }
            if (raise != null) {
                out.put("raise", raise);
            }
            return Json.toCompact(out);
        }

        /** The refusal answered over HTTP: the kind's status, its JSON. */
        public Answer answer() {
            return new Answer(kind.status(), json());
        }
    }

    /** A request lite cannot read: not a JSON object, a key it does not read or one it lacks, a value of the wrong kind
     *  (kind {@code request}). */
    public static final class MalformedRequest extends IllegalArgumentException {
        public MalformedRequest(String message) {
            super(message);
        }

        public MalformedRequest(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /** A call's JSON, 200, or its failure as lite's refusal ({@link #refusal}); a defect is logged whole and answered as
     *  one, never a dropped connection. */
    public static Answer answer(Supplier<String> call) {
        try {
            return new Answer(200, call.get());
        } catch (RuntimeException | StackOverflowError e) {
            Refusal r = refusal(e);
            if (r.kind() == Kind.INTERNAL) {
                e.printStackTrace();
            }
            return r.answer();
        }
    }

    /** A refusal of {@code kind} in {@code message}'s words, answered. */
    public static Answer refuse(Kind kind, String message) {
        return new Refusal(kind, message, null, null).answer();
    }

    /**
     * {@code e} as lite's refusal -- THE one classifier, which every binding reads (the server's routes, and the browser
     * and Python bindings' folds): the text does not parse ({@code parse}); the model or the query does not compile
     * ({@code compile}, where its position is known); a construct refused by name ({@code unsupported}); a run's raise
     * ({@code data}, its kind in {@code raise}); a run's parameter values ({@code parameters}); a malformed request
     * ({@code request}); anything else a defect ({@code internal}, its class named).
     */
    public static Refusal refusal(Throwable e) {
        if (e instanceof com.legend.parser.ParseException p) {
            return new Refusal(Kind.PARSE, String.valueOf(p.getMessage()), p.position(), null);
        }
        if (e instanceof com.legend.error.LegendCompileException c) {
            return new Refusal(c.phase() == com.legend.error.LegendCompileException.Phase.PARSE ? Kind.PARSE
                    : Kind.COMPILE, String.valueOf(c.getMessage()), c.position(), null);
        }
        if (e instanceof com.legend.error.NotImplementedException || e instanceof com.legend.sql.DialectCapability) {
            return new Refusal(Kind.UNSUPPORTED, String.valueOf(e.getMessage()), null, null);
        }
        if (e instanceof com.legend.error.DataError d) {
            return dataRefusal(d);
        }
        if (e instanceof com.legend.error.MissingParameters || e instanceof com.legend.error.InvalidParameters) {
            return new Refusal(Kind.PARAMETERS, String.valueOf(e.getMessage()), null, null);
        }
        if (e instanceof MalformedRequest) {
            return new Refusal(Kind.REQUEST, String.valueOf(e.getMessage()), null, null);
        }
        return new Refusal(Kind.INTERNAL, e.getClass().getSimpleName() + ": " + e.getMessage(), null, null);
    }

    /**
     * A run's failure as lite's refusal (spec §3; PR #118 fix round, 2026-11-18, N3, ruled by the orchestrator pending the
     * user): a raise ({@link com.legend.error.ErrorKind#isRaise}) is {@code data}, its kind in {@code raise} and its
     * site's where in {@code where}; the database refusing the work in its own words ({@code DATABASE_ERROR}), a load's
     * file larger than it admits ({@code FILE_TOO_LARGE}, its path in the message) and an error of no kind are
     * {@code data} with no {@code raise}; a connection that could not open or was lost ({@code CONNECTION_ERROR}) is
     * the environment, not the caller's query: {@code internal}.
     */
    private static Refusal dataRefusal(com.legend.error.DataError d) {
        String message = String.valueOf(d.getMessage());
        com.legend.error.ErrorKind kind = d.kind();
        if (kind == com.legend.error.ErrorKind.CONNECTION_ERROR) {
            return new Refusal(Kind.INTERNAL, message, null, null);
        }
        if (kind != null && kind.isRaise()) {
            return new Refusal(Kind.DATA, message, d.where(), lowerCamel(kind.name()));
        }
        return new Refusal(Kind.DATA, message, null, null);
    }

    /** {@code DIVIDE_BY_ZERO} as {@code divideByZero}: a wire name. */
    private static String lowerCamel(String constant) {
        StringBuilder out = new StringBuilder();
        boolean up = false;
        for (char c : constant.toCharArray()) {
            if (c == '_') {
                up = true;
            } else {
                out.append(up ? c : Character.toLowerCase(c));
                up = false;
            }
        }
        return out.toString();
    }

    /**
     * {@code body} as lite's CLOSED request: a JSON object holding every one of {@code keys} and, where given,
     * {@code optional}'s, and no other key -- anything else refused by name ({@link MalformedRequest}).
     */
    public static Json.Obj read(String body, String what, java.util.Set<String> optional, String... keys) {
        Json.Obj request = requestObject(body, what);
        java.util.Set<String> wanted = java.util.Set.of(keys);
        String holds = keys.length == 0 && optional.isEmpty() ? "none" : String.join(", ", keys) + (optional.isEmpty()
                ? "" : "; optional " + String.join(", ", new java.util.TreeSet<>(optional)));
        for (String key : request.fields().keySet()) {
            if (!wanted.contains(key) && !optional.contains(key)) {
                throw new MalformedRequest(what + " has no key " + key + " (it holds " + holds + ")");
            }
        }
        for (String key : keys) {
            if (!request.fields().containsKey(key)) {
                throw new MalformedRequest(what + " names no " + key + " (it holds " + holds + ")");
            }
        }
        return request;
    }

    /** {@code request}'s {@code key}, a string, else refused by name. */
    public static String text(Json.Obj request, String key) {
        if (request.fields().get(key) instanceof Json.Str s) {
            return s.value();
        }
        throw new MalformedRequest("the request's " + key + " is not a string");
    }

    /** {@code request}'s {@code key}, a JSON object, else refused by name. */
    public static Json.Obj object(Json.Obj request, String key) {
        if (request.fields().get(key) instanceof Json.Obj o) {
            return o;
        }
        throw new MalformedRequest("the request's " + key + " is not a JSON object");
    }
}
