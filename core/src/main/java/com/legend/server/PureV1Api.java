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
     * How a host runs an execute: the query this API compiled, planned and run by the host's door (legend-lite's
     * server hands in its product door, {@code Execution.runOnProjectDatabases} under its file fence:
     * {@code ConnectionResolver.runner(sources)}, its startup settings' folders; {@code ConnectionResolver.RUNNER} is the
     * library's, any file),
     * its rows written in the output this API names ({@code Output.Engine} or {@code Output.Arrow}: the engine's
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
     * {@code runner}; a path this API does not serve is answered 404 in the engine's error shape.
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
            // legend-engine's routes are the edge's (com.legend.edge.PureV1Routes, D1.0b, 2026-11-12)
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
        return answer(400, "COMPILATION", () -> {
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
        return answer(400, "COMPILATION", () -> {
            Json.Obj request = request(body);
            ModelInput model = ModelInput.of(request.getObj("model"));
            LambdaFunction lambda = ProtocolReader.lambda(request.getObj("lambda"));
            com.legend.protocol.ts.TsShow.Shown shown = model.with(models, ctx -> com.legend.protocol.ts.TsShow.of(
                    lambda, tsTypes(ctx)));
            return Json.toCompact(shown.statement() != null ? Map.of("statement", shown.statement())
                    : Map.of("refused", java.util.Objects.requireNonNull(shown.refused())));
        });
    }

    /**
     * {@code /api/lite/v1/grammar/tsItem} (A3 step 2, docs/TS_A3_ONE_READER_PLAN_2026_11_03.md; upstream has no #TS):
     * {@code {text}}, one #TS item as a formula box holds a series, read by core's parser -- the one reader
     * ({@link com.legend.protocol.ts.TsParser#item}): {@code {"text": canonical, "item": {alias, expr}}}, the item's
     * tree as data ({@link TsTree}), or {@code {"error": {message, line, column}}} where the text
     * is no item. An error is the box's answer, never a failed request: 200 either way.
     */
    public static Answer tsItem(String body) {
        return answer(400, "COMPILATION", () -> {
            String text = request(body).getString("text");
            Map<String, Object> out = new LinkedHashMap<>();
            try {
                com.legend.protocol.ts.TsAst.Line line = com.legend.protocol.ts.TsParser.item(text);
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
            for (com.legend.compiler.element.type.Type.Column c : UpstreamRelationType.columns(com.legend.Compiler
                    .query(ctx, query).resultType())) {
                if (c.name().equals(column)) {
                    return UpstreamRelationType.typePath(c.type());
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
        return answer(400, "COMPILATION", () -> Json.toCompact(make.apply(request(body))));
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
            metadata.put("legend.activities", Json.toCompact(List.of(com.legend.result.PureV1Writer.activity(
                    sql.value(), false, hostExecution(planned)))));
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

    /** The refusal of a value's answer (a scalar, a collection), whose serialization by the engine is unprobed. */
    private static com.legend.error.NotImplementedException unprobed(com.legend.TypedQuery query) {
        return new com.legend.error.NotImplementedException(unprobedMessage(query));
    }

    private static String unprobedMessage(com.legend.TypedQuery query) {
        return "execute: a " + com.legend.plan.ResultShape.of(query.resultType()) + " result's serialization is unprobed";
    }

    /**
     * What the engine's envelope knows of {@code query}, read from it before the run as data
     * ({@link com.legend.result.PureV1Writer.Facts}; 2026-11-09, the user's question "why do we have any callbacks?": a
     * writer factory the run called back read the compiled query until then): each column the compiler typed, its
     * builder spelling or the engine's refusal of it; each primitive's, for a dynamic pivot's generated column; the
     * refusal of a value's answer.
     */
    public static com.legend.result.PureV1Writer.Facts facts(com.legend.TypedQuery query) {
        Map<String, com.legend.result.PureV1Writer.ColumnType> declared = new LinkedHashMap<>();
        for (var c : UpstreamRelationType.columns(query.resultType())) {
            declared.put(c.name(), columnType(c.type()));
        }
        return new com.legend.result.PureV1Writer.Facts(declared, PRIMITIVES,
                com.legend.compiler.element.type.Type.RelationType.PIVOT_SEPARATOR, unprobedMessage(query));
    }

    /** Each primitive's builder spelling, by the plan's short name of it (the type a dynamic pivot's generated column
     *  carries): a constant of the platform. */
    private static final Map<String, com.legend.result.PureV1Writer.ColumnType> PRIMITIVES = primitives();

    private static Map<String, com.legend.result.PureV1Writer.ColumnType> primitives() {
        String prefix = "meta::pure::metamodel::type::";
        Map<String, com.legend.result.PureV1Writer.ColumnType> out = new LinkedHashMap<>();
        for (var e : com.legend.compiler.element.type.Type.Primitive.byFqn().entrySet()) {
            if (e.getKey().startsWith(prefix)) {
                out.put(e.getKey().substring(prefix.length()), columnType(e.getValue()));
            }
        }
        return java.util.Collections.unmodifiableMap(out);
    }

    /** {@code type}'s builder spelling, or the engine's refusal of it (it has no relational spelling of the type). */
    private static com.legend.result.PureV1Writer.ColumnType columnType(com.legend.compiler.element.type.Type type) {
        String tds = UpstreamRelationType.tdsTypePath(type);
        String relational;
        try {
            relational = UpstreamRelationType.relationalSpelling(type);
        } catch (com.legend.error.NotImplementedException refused) {
            // the refusal kept as the column's data, raised where the column is answered (as the envelope raised it)
            return com.legend.result.PureV1Writer.ColumnType.refused(
                    java.util.Objects.requireNonNull(refused.getMessage(), "the refusal names its type"));
        }
        return com.legend.result.PureV1Writer.ColumnType.spelled(tds, relational);
    }

    /**
     * E8 in upstream's Arrow format ({@code ?serializationFormat=ARROW_IPC}), its plan half, for a host that runs
     * the SQL itself (Python's engine, over duckdb-python): the {@code ExecuteInput} read and planned as
     * {@code com.legend.edge.PureV1Routes#execute} plans it, answered {@code {"sql", "metadata"}} -- the SQL to run, and the Arrow schema
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
            String refused = plan.beyondItsText();
            if (refused != null) {
                throw new com.legend.error.NotImplementedException("a host runs a plan's statement itself, and this"
                        + " query's plan does not run as its text: " + refused + "; legend-lite's server runs it");
            }
            if (plan.shape() != com.legend.plan.ResultShape.TABULAR && plan.shape() != com.legend.plan.ResultShape.GRAPH) {
                throw unprobed(hosted.query());
            }
            if (plan.shape() == com.legend.plan.ResultShape.GRAPH) {
                // upstream's Arrow answer is a relational result's (RelationalResultToArrowIPCSerializer)
                throw new IllegalArgumentException("ARROW_IPC answers a relation query; a graph fetch is answered in JSON");
            }
            Map<String, Object> metadata = new LinkedHashMap<>();
            metadata.put("legend.builder", Json.toCompact(tdsBuilder(plan)));
            // the host's execution: it keeps no trace of its own, so its id is minted with the plan half it runs
            metadata.put("legend.activities", Json.toCompact(List.of(com.legend.result.PureV1Writer.activity(
                    plan.sql(), false, java.util.UUID.randomUUID()))));
            metadata.put("legend.columns", Json.toCompact(columnNames(plan)));
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("sql", plan.sql());
            out.put("metadata", metadata);
            if (!plan.textAlone()) {
                // what runs the text: the execution plan it renders from (B3, 2026-11-11), its target's loads
                out.put("plan", Json.parse(com.legend.executionplan.PlanJson.write(plan.plan())));
            }
            return Json.toCompact(out);
        });
    }

    /**
     * E8's plan half for a host that runs the SQL itself and answers in the engine's JSON (the tab: its DuckDB;
     * Python's engine; docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C1): the {@code ExecuteInput} read, bound and planned
     * as {@code com.legend.edge.PureV1Routes#execute} plans it -- as typed rows -- and answered with what {@link #jsonAnswer} needs to write the
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
                // as the server's execute refuses it (Output.Engine): a value's answer is not one the engine was
                // measured on
                throw unprobed(hosted.query());
            }
            if (!(plan.root() instanceof com.legend.executionplan.ExecutionPlan.TdsResult result)) {
                throw new com.legend.error.NotImplementedException("a host runs one statement, and this query's plan "
                        + "is not one (" + plan.root().getClass().getSimpleName() + ": a pivot whose columns the data "
                        + "names, or a program that writes); legend-lite's server runs it");
            }
            // a data: payload is the plan's own value, which the host binds from the plan carried below (the tenets
            // audit's M3, 2026-11-11); any other value -- a parameter, a key's cell, a file's path -- a host does not bind
            long others = result.sql().slots().stream().filter(sl -> !(sl.binding()
                    instanceof com.legend.executionplan.ExecutionPlan.Binding.Payload)).count();
            if (others > 0) {
                throw new com.legend.error.NotImplementedException("a host runs a statement's text, and this one binds "
                        + "values (" + others + "); legend-lite's server runs it");
            }
            if (!result.sql().target().loads().isEmpty()) {
                // the statement reads what its target's loads make (a connection's test data, a project's Data), and a
                // host runs the statement alone until B3 makes the hosts run a
                // plan's loads (the A checkpoint's finding of B3's prep, 2026-11-10: until then the host's database
                // failed on a table no load made, a catalog error, not a refusal by name: §3 rule 5)
                throw new com.legend.error.NotImplementedException("a host runs one statement, and this query's"
                        + " statement reads what its target loads first (" + result.sql().target().loads().size()
                        + " load(s): test data, a project's Data or a data: source), which a host does not run yet;"
                        + " legend-lite's server runs it");
            }
            String statement = result.sql().statement();
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("sql", statement);
            out.put("run", statement);
            if (!result.sql().slots().isEmpty()) {
                // what the host binds to `run`'s `?`s: the plan, its payloads its statement's content by digest
                out.put("plan", Json.parse(com.legend.executionplan.PlanJson.write(plan)));
            }
            out.put("shape", result.shape().name());
            out.put("resultColumns", Json.parse(com.legend.executionplan.PlanJson.writeColumns(result.columns())));
            if (result.shape() == com.legend.executionplan.ExecutionPlan.TdsResult.Shape.OBJECTS) {
                out.put("builder", Map.of("_type", "json"));
            } else {
                com.legend.result.PureV1Writer.Envelope e = com.legend.result.PureV1Writer.envelope(
                        facts(hosted.query()), statement, result.columns());
                // the host's execution: it keeps no trace of its own, so its id is minted with the plan half it runs,
                // and the answer the server writes from its rows (jsonAnswer, arrowAnswer) reports the same id
                java.util.UUID execution = java.util.UUID.randomUUID();
                out.put("execution", execution.toString());
                out.put("builder", Json.parse(e.builder()));
                out.put("activities", Json.parse(e.activities(execution)));
                out.put("columns", e.names());
            }
            return Json.toCompact(out);
        });
    }

    /**
     * E8's answer for a host that ran {@link #jsonPlan}'s {@code run} statement on its own database: {@code plan} that
     * plan's answer, {@code rows} the host's rows (a JSON array of objects, each a result column's name and its cell
     * as the host's driver read it: {@link com.legend.result.HostRows}) -- decoded by the plan's result columns and
     * written by the writer {@code com.legend.edge.PureV1Routes#execute} answers with, so the host's answer is the server's text. The database
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
                    if (!(planned.has("sql") && planned.get("sql") instanceof Json.Str sql)) {
                        throw new IllegalArgumentException("the plan answers no sql: it is not jsonPlan's");
                    }
                    com.legend.result.PureV1Writer.Envelope envelope = new com.legend.result.PureV1Writer.Envelope(
                            Json.toCompact(builder), sql.value(), labels);
                    // the execution jsonPlan named, its door's id, carried by the plan the host ran
                    sink = com.legend.result.PureV1Writer.relation(out, announced -> envelope, hostExecution(planned));
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

    /** The host's execution, as {@link #jsonPlan} minted it and handed it back in its answer: what the answer's activity
     *  reports (C1's second half, 2026-11-12). A plan half without it is refused: it is not jsonPlan's. */
    private static java.util.UUID hostExecution(Json.Obj planned) {
        if (!(planned.has("execution") && planned.get("execution") instanceof Json.Str id)) {
            throw new IllegalArgumentException("the plan answers no execution: it is not jsonPlan's");
        }
        return java.util.UUID.fromString(id.value());
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
        return new Hosted(query, runtimeOf(request, query));
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
    public static Json.Obj boundParameters(Json.Obj function, @com.legend.base.Nullable Json.Arr values) {
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

    private static List<String> columnNames(QueryPlan plan) {
        List<String> names = new ArrayList<>();
        for (var c : UpstreamRelationType.columns(plan.rootType())) {
            names.add(c.name());
        }
        return names;
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
    static String modelText(Json.Obj model) {
        String type = model.getStringOr("_type", "");
        if (!"text".equals(type)) {
            throw new IllegalArgumentException("a model of _type '" + type + "': this route reads a model's text "
                    + "(PureModelContextText, {\"_type\":\"text\",\"code\":...})");
        }
        return model.getString("code");
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
    public static Answer answer(int status, @com.legend.base.Nullable String errorType, Supplier<String> call) {
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
