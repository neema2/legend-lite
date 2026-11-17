// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;


import com.legend.json.Json;
import com.legend.parser.PmcdParser;
import com.legend.parser.SpecParser;
import com.legend.plan.PlanSupportFunctions;
import com.legend.plan.QueryPlan;
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
 * legend-lite's OWN API (phase D's D1.0b, the orchestrator's ruling of 2026-11-12: lite's own routes stay the
 * product's, legend-engine's {@code pure/v1} routes are the edge's, {@code com.legend.edge.PureV1Routes}): the
 * {@code /api/lite/v1} routes -- every compile error ({@link #compileEveryError}; the user's decision of 2026-10-10) and
 * the model routes ({@link #modelChanges}: DataCube's Create a Class…, an Association…, a Data Space…) -- and the hosts'
 * plan halves, which the tab and Python call through the compiler's boundary ({@link #jsonPlan}, {@link #arrowPlan},
 * {@link #jsonAnswer}, {@link #arrowAnswer}). Its answer, request and error shapes ({@link Answer}, {@link #answer},
 * {@link #error}: the same JSON as upstream's, owned by lite) and the host's runner ({@link Runner}) are the kit both
 * route modules use; the edge's depends on this one, never the reverse (G5). Each call is a pure function of its
 * request -- text in, a status and JSON out -- so it is tested without HTTP.
 *
 * <p>THE PLAN SIDE ({@code //core:pure_v1}): no database and no driver, so the compiler's boundary answers with this
 * same code.
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
     * cube query; and a request may carry a whole model as data ({@code ModelInput}), which nests as deep as a
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

    /**
     * One call by its path ({@code /api/pure/v1/...}) and raw query string, as both of legend-lite's hosts
     * route it: its server ({@code LegendHttpServer}) and the compiler's boundary. An execute runs through
     * {@code runner}; a path this API does not serve is answered 404 in lite's error shape ({@link #error}).
     */
    public static Answer route(String path, @com.legend.base.Nullable String rawQuery, String body, Runner runner) {
        return route(path, rawQuery, body, runner, Models.COMPILED);
    }

    /** {@link #route(String, String, String, Runner)}, a model given as text compiled by {@code models}. */
    public static Answer route(String path, @com.legend.base.Nullable String rawQuery, String body, Runner runner,
            Models models) {
        return switch (path) {
            case "/api/lite/v1/compilation/compile" -> compileEveryError(body);
            case "/api/lite/v1/grammar/jsonToTs/lambda" -> jsonToTsLambda(body, models);
            case "/api/lite/v1/grammar/tsItem" -> tsItem(body);
            case "/api/lite/v1/model/createClass" -> modelChanges(body, ModelChanges::createClass);
            case "/api/lite/v1/model/createAssociation" -> modelChanges(body, ModelChanges::createAssociation);
            case "/api/lite/v1/model/createDataSpace" -> modelChanges(body, ModelChanges::createDataSpace);
            // every other path, in lite's own words (B7, the orchestrator's ruling (3) of 2026-11-17: the product names
            // nothing engine-shaped, not even its paths' prefix; with the edge mounted, the edge serves legend-engine's)
            default -> error(404, null, "no such legend-lite API: " + path);
        };
    }


    public static Json.Obj request(String body) {
        Json.Node n = Json.parse(body, REQUEST);
        if (n instanceof Json.Obj o) {
            return o;
        }
        throw new IllegalArgumentException("the request body is not a JSON object");
    }

    /**
     * legend-lite's own {@code /api/lite/v1/compilation/compile} (the user's decision, 2026-10-10; legend-engine has
     * no such route, so a client that may be talking to it asks this first and, answered 404, asks
     * {@code com.legend.edge.PureV1Routes#compile}): the model compiled as {@code com.legend.edge.PureV1Routes#compile} compiles it, answered with EVERY error, 200 --
     * {@code {"errors":[{"message":...}, ...]}}, the list empty when it compiles ({@code Compiler.compileErrors}: the
     * first element error alone, else every body's). Each error is an object, so where it is can be added beside its
     * message later (leg 6b) without changing the shape. A request that cannot be read, or a failure that is not the
     * model's, is answered as {@code com.legend.edge.PureV1Routes#compile} answers it.
     */
    public static Answer compileEveryError(String body) {
        return answer(400, RefusalKind.COMPILE.text(), () -> {
            List<Map<String, Object>> errors = new ArrayList<>();
            for (String message : ModelInput.of(request(body)).compileErrors()) {
                errors.add(Map.of("message", message));
            }
            return Json.toCompact(Map.of("errors", errors));
        });
    }

    /**
     * {@code /api/lite/v1/grammar/jsonToTs/lambda} (D12, Show #TS; upstream has no #TS): {@code {model, lambda}}, a
     * query of {@code #TS} over grids -- the lambda's protocol JSON, each grid bound by {@code let} and the island
     * reading it -- as the one statement a person would write, each grid a flat source named by WITH ({@link TsShow}):
     * {@code {"statement": text}}, or {@code {"refused": why}} where a grid's relation has no flat form yet. The model
     * is the query's, as {@link #lambdaRelationType} reads one: its compiler answers the type a calculated column's
     * {@code +} reads (A6, 2026-11-03).
     */
    public static Answer jsonToTsLambda(String body, Models models) {
        return answer(400, RefusalKind.COMPILE.text(), () -> {
            Json.Obj request = request(body);
            // lite's own route (upstream has no #TS): closed, a key it does not read refused by name (the final audit of
            // 9af16fbe58's J4, 2026-11-14); its model is upstream's PureModelContext, read as upstream reads it
            ModelChanges.only(request, "a jsonToTs/lambda request", "model", "lambda");
            ModelInput model = ModelInput.of(request.getObj("model"));
            LambdaFunction lambda = ProtocolReader.lambda(request.getObj("lambda"));
            // the island's text read through the parse door, which asks the run's wall (C5-J's last round, 2026-11-15)
            com.legend.protocol.ts.TsShow.Shown shown = model.with(models, ctx -> com.legend.protocol.ts.TsShow.of(
                    lambda, tsTypes(ctx), com.legend.door.Parse::tsIsland));
            return Json.toCompact(shown.statement() != null ? Map.of("statement", shown.statement())
                    : Map.of("refused", java.util.Objects.requireNonNull(shown.refused())));
        });
    }

    /**
     * {@code /api/lite/v1/grammar/tsItem} (A3 step 2, docs/TS_A3_ONE_READER_PLAN_2026_11_03.md; upstream has no #TS):
     * {@code {text}}, one #TS item as a formula box holds a series, read by core's parser -- the one reader
     * ({@link com.legend.protocol.ts.TsParser#item}, through the parse door {@code door.Parse.tsItem}):
     * {@code {"text": canonical, "item": {alias, expr}}}, the item's tree as data ({@link TsTree}), or
     * {@code {"error": {message, line, column}}} where the text is no item. An error is the box's answer, never a failed request: 200 either way.
     */
    public static Answer tsItem(String body) {
        return answer(400, RefusalKind.COMPILE.text(), () -> {
            Json.Obj request = request(body);
            // lite's own route (upstream has no #TS): closed (the final audit of 9af16fbe58's J4, 2026-11-14)
            ModelChanges.only(request, "a tsItem request", "text");
            String text = request.getString("text");
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
        });
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

    /**
     * legend-lite's own model routes, {@code /api/lite/v1/model/createClass}, {@code createAssociation} and
     * {@code createDataSpace} (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §15.6a, the user's ruling of 2026-10-11: named
     * after Legend's actions; upstream has no HTTP route that does this): each one call to the model's writer
     * ({@link ModelChanges}), atomic, answered as the files to write in the SDLC's own change shape. The project and
     * each class's source are model contexts, read as {@code com.legend.edge.PureV1Routes#compile} reads one; what cannot be made is answered as a
     * model that does not compile is, 400 COMPILATION.
     */
    public static Answer modelChanges(String body, java.util.function.Function<Json.Obj, Map<String, Object>> make) {
        return answer(400, RefusalKind.COMPILE.text(), () -> Json.toCompact(make.apply(request(body))));
    }

    /**
     * A host's refusal of a call it could not finish -- its database refusing the SQL, say -- answered as
     * {@link #answer} answers a database's refusal: 500, lite's error shape ({@link #error}), no {@code errorType}.
     */
    public static Answer refused(String message) {
        return error(500, null, message);
    }

    /** The first 200 characters of {@code text}, for a refusal that shows what it read. */
    public static String head(String text) {
        return text.length() <= 200 ? text : text.substring(0, 200) + "...";
    }

    /** The runtime: the request's own, else the one the query's {@code ->from}s bind -- every door's one rule
     *  ({@link com.legend.TypedQuery#runtime}: several refused by name; the checkpoint tenets audit's 1, 2026-11-10:
     *  until then this door kept the first from()'s and ran the rest on it). */
    public static String runtimeOf(Json.Obj request, com.legend.TypedQuery query) {
        Json.Obj rt = request.getObjOr("runtime", null);
        if (rt != null && !rt.has("runtime")) {
            // a runtime written out in full (upstream's ExecuteInput.runtime: an engineRuntime, a legacyRuntime) is
            // refused by name, never dropped for the query's own ->from runtime (register S301, the user's ruling of
            // 2026-11-12: "It can also go to follow up"; until then this door read it as no runtime at all)
            throw new com.legend.error.NotImplementedException("a runtime written out in the request ("
                    + rt.getStringOr("_type", "?") + "): not planned; name a runtime (runtimePointer), register S301");
        }
        String runtime = query.runtime(rt != null ? rt.getString("runtime") : null);
        if (runtime == null) {
            throw new IllegalArgumentException(
                    "no runtime: the request names none and the query has no ->from(runtime)");
        }
        return runtime;
    }

    // ---------------------------------------------------------------------
    // Plumbing
    // ---------------------------------------------------------------------

    /** A {@code PureModelContextText}'s code, for the routes that read a model's text itself (the host and the
     *  model-change routes); any other model context is refused there. The routes that compile a model read its
     *  data too ({@code ModelInput}; 2026-10-21, the independent audit: this said no reader was built). */
    public static String modelText(Json.Obj model) {
        String type = model.getStringOr("_type", "");
        if (!"text".equals(type)) {
            throw new IllegalArgumentException("a model of _type '" + type + "': this route reads a model's text "
                    + "(PureModelContextText, {\"_type\":\"text\",\"code\":...})");
        }
        return model.getString("code");
    }


    /**
     * A call's JSON, or its failure in lite's error shape (the JSON legend-engine's clients read too), with the status
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
    public static Answer answer(int status, @com.legend.base.Nullable String errorType, Supplier<String> call) {
        return answer(status, errorType, call, e -> String.valueOf(e.getMessage()));
    }

    /** {@link #answer(int, String, Supplier)} with a refusal of the text worded by {@code words}: the product's own
     *  message, or the compatibility edge's upstream sentence of a compile error's kind (D1.6, W7 of
     *  docs/records/d-plan-2026-11-16.md: {@code edge.PureMessages.compile}). */
    public static Answer answer(int status, @com.legend.base.Nullable String errorType, Supplier<String> call,
            java.util.function.Function<Throwable, String> words) {
        return answer(status, errorType, call, words, t -> t.getClass().getSimpleName());
    }

    /** {@link #answer}, an uncaught error's type named by {@code typeName} (the compatibility edge's: upstream's name of
     *  it; the P2 checkpoint audit's M3/A-M4, 2026-11-16). */
    public static Answer answer(int status, @com.legend.base.Nullable String errorType, Supplier<String> call,
            java.util.function.Function<Throwable, String> words, java.util.function.Function<Throwable, String> typeName) {
        return answer(status, errorType, call, words, typeName, e -> false);
    }

    /** {@link #answer}, the refusals {@code uncaught} names answered as an uncaught error instead (the compatibility
     *  edge's: a kind upstream raises at run, not at compile; the P2 checkpoint audit's A-S2, 2026-11-16). */
    public static Answer answer(int status, @com.legend.base.Nullable String errorType, Supplier<String> call,
            java.util.function.Function<Throwable, String> words, java.util.function.Function<Throwable, String> typeName,
            java.util.function.Predicate<Throwable> uncaught) {
        try {
            return new Answer(200, call.get());
        } catch (RuntimeException | StackOverflowError e) {
            if (refusesTheText(e) && !uncaught.test(e)) {
                return error(status, errorType, words.apply(e));
            }
            if (!(e instanceof IllegalArgumentException || e instanceof com.legend.error.DataError)) {
                e.printStackTrace();
            }
            Throwable n = e instanceof com.legend.error.DataError && e.getCause() != null ? e.getCause() : e;
            return error(500, null, typeName.apply(n) + ": " + words.apply(n));
        }
    }

    /**
     * A failure as the routes name it, for a host that answers one outside a route: the boundary's own operations,
     * which the tab and Python fold ({@code planner.Folded}), so that their callers read one vocabulary for every call
     * (docs/PROTOCOL_PROGRAM_2026_10_05.md, leg 6, with the DataCube + Python line). {@code kind} is lite's own
     * ({@link RefusalKind}); {@code message} is as {@link #answer} writes it.
     */
    public record Refusal(RefusalKind kind, String message) {
    }

    /**
     * LITE'S REFUSAL KINDS (B7 of the separation census, 2026-11-17, D's split by content): the product's own names, each
     * written as its {@link #text} -- the bytes a host's caller has read since the protocol program's leg 6, kept
     * identical (the orchestrator's condition: {@code Folded.failure}'s bytes do not move; pinned by
     * {@code BoundaryLiteExportsTest}). legend-engine's errorType words are the edge's own spellings (its routes'),
     * mapped there, not shared.
     */
    public enum RefusalKind {
        /** The text does not parse. */
        PARSE("PARSER"),
        /** The text does not compile, or uses a construct not implemented. */
        COMPILE("COMPILATION"),
        /** Any other failure: the status the server answers. */
        FAILURE("500");

        private final String text;

        RefusalKind(String text) {
            this.text = text;
        }

        /** The kind as a host's caller reads it. */
        public String text() {
            return text;
        }
    }

    /** {@code e} as a {@link Refusal}. */
    public static Refusal refusal(Throwable e) {
        if (refusesTheText(e)) {
            return new Refusal(e instanceof com.legend.parser.ParseException ? RefusalKind.PARSE : RefusalKind.COMPILE,
                    String.valueOf(e.getMessage()));
        }
        return new Refusal(RefusalKind.FAILURE, named(e));
    }

    /** The text does not parse or compile, or a construct is not implemented: answered with an errorType. */
    private static boolean refusesTheText(Throwable e) {
        return e instanceof com.legend.error.LegendCompileException
                || e instanceof com.legend.error.NotImplementedException
                || e instanceof com.legend.sql.DialectCapability;
    }

    /** Any other failure, as the server writes it: its class's simple name and its message (a database's refusal, its cause's). */
    private static String named(Throwable e) {
        Throwable n = e instanceof com.legend.error.DataError && e.getCause() != null ? e.getCause() : e;
        return n.getClass().getSimpleName() + ": " + n.getMessage();
    }

    public static Answer error(int status, @com.legend.base.Nullable String errorType, String message) {
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
