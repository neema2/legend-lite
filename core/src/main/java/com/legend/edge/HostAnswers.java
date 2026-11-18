// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.json.Json;
import com.legend.plan.QueryPlan;
import com.legend.protocol.ProtocolReader;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.server.LiteV1Api.Answer;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.legend.edge.ExecuteInputs.boundParameters;
import static com.legend.edge.EngineEnvelope.error;
import static com.legend.server.LiteV1Api.modelText;
import static com.legend.server.LiteV1Api.request;
import static com.legend.edge.ExecuteInputs.runtimeOf;

/**
 * LEGEND-ENGINE'S EXECUTE, ITS PLAN AND ANSWER HALVES FOR A HOST (W8 of docs/records/d-plan-2026-11-16.md, D1.8,
 * 2026-11-16; D-1's host halves, moved from {@code server.LiteV1Api}): the plan half a host's runner runs
 * ({@link #arrowPlan}, {@link #jsonPlan}) and the answer it writes from the host's rows ({@link #arrowAnswer},
 * {@link #jsonAnswer}), in legend-engine's forms -- the compatibility edge's, as {@link PureV1Writer} is. The tab and
 * Python reach them through the boundary ({@code wasm/planner/Boundary}).
 */
public final class HostAnswers {

    /** pure/v1's answer of a host half, its errors in upstream's words and type names (the P2 checkpoint audit's M3 and
     *  A-M2, 2026-11-16: until then lite's, LiteV1Api.answer's default). */
    private static com.legend.server.LiteV1Api.Answer answer(int status, @com.legend.base.Nullable String errorType,
            java.util.function.Supplier<String> call) {
        return EngineEnvelope.answer(status, errorType, call, PureMessages::compile, PureMessages::typeName,
                PureMessages::uncaught);
    }

    private HostAnswers() {
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
            metadata.put("legend.activities", Json.toCompact(List.of(PureV1Writer.activity(
                    sql.value(), false, hostExecution(planned)))));
            metadata.put("legend.columns", Json.toCompact(names));
            try {
                com.legend.result.HostRows.stream(rows, columns, new com.legend.result.ArrowWriter(out,
                        // legend-pure's date text, as the server's twin writes it (EngineAnswers.arrow; the P2 checkpoint
                        // audit's A-M3, 2026-11-16: this host route wrote the product's S312 text)
                        cs -> metadata, Presenter.LEGEND_PURE));
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
     * ({@link PureV1Writer.Facts}; 2026-11-09, the user's question "why do we have any callbacks?": a
     * writer factory the run called back read the compiled query until then): each column the compiler typed, its
     * builder spelling or the engine's refusal of it; each primitive's, for a dynamic pivot's generated column; the
     * refusal of a value's answer.
     */
    public static PureV1Writer.Facts facts(com.legend.TypedQuery query) {
        Map<String, PureV1Writer.ColumnType> declared = new LinkedHashMap<>();
        for (var c : com.legend.plan.PureSpelling.relationColumns(query.resultType())) {
            declared.put(c.name(), columnType(c.type()));
        }
        return new PureV1Writer.Facts(declared, PRIMITIVES,
                com.legend.compiler.element.type.Type.RelationType.PIVOT_SEPARATOR, unprobedMessage(query));
    }

    /** Each primitive's builder spelling, by the plan's short name of it (the type a dynamic pivot's generated column
     *  carries): a constant of the platform. */
    private static final Map<String, PureV1Writer.ColumnType> PRIMITIVES = primitives();

    private static Map<String, PureV1Writer.ColumnType> primitives() {
        String prefix = "meta::pure::metamodel::type::";
        Map<String, PureV1Writer.ColumnType> out = new LinkedHashMap<>();
        for (var e : com.legend.compiler.element.type.Type.Primitive.byFqn().entrySet()) {
            if (e.getKey().startsWith(prefix)) {
                out.put(e.getKey().substring(prefix.length()), columnType(e.getValue()));
            }
        }
        return java.util.Collections.unmodifiableMap(out);
    }

    /** {@code type}'s builder spelling, or the engine's refusal of it (it has no relational spelling of the type). */
    private static PureV1Writer.ColumnType columnType(com.legend.compiler.element.type.Type type) {
        String tds = UpstreamRelationType.tdsTypePath(type);
        String relational;
        try {
            relational = UpstreamRelationType.relationalSpelling(type);
        } catch (com.legend.error.NotImplementedException refused) {
            // the refusal kept as the column's data, raised where the column is answered (as the envelope raised it)
            return PureV1Writer.ColumnType.refused(
                    java.util.Objects.requireNonNull(refused.getMessage(), "the refusal names its type"));
        }
        return PureV1Writer.ColumnType.spelled(tds, relational);
    }

    /**
     * E8 in upstream's Arrow format ({@code ?serializationFormat=ARROW_IPC}), its plan half, for a host that runs
     * the SQL itself (Python's engine, over duckdb-python): the {@code ExecuteInput} read and planned as
     * {@code com.legend.edge.PureV1Routes#execute} plans it, answered {@code {"sql", "metadata"}} -- the SQL to run, and the Arrow schema
     * metadata upstream's answer carries, each value JSON text as upstream writes it (measured against legend-engine
     * 4.145.0, 2026-10-08): {@code legend.builder}, the TDS builder as the JSON answer's; {@code legend.activities},
     * the activities WITHOUT their {@code _type}; {@code legend.columns}, the column names. The host writes the rows
     * as upstream does: an Arrow IPC stream with this metadata, compressed as one zstd frame. {@code models} are the
     * only models the host runs: a request over any other is refused. {@code style}: the plan's, pure/v1's the
     * compatibility edge's ({@code com.legend.edge.PureText.UPSTREAM}, Pure's sentence for a raise: the user's ruling of
     * 2026-11-10; the D part 1 audit's M2, 2026-11-15), handed by the host's door, which the product never names (G5).
     */
    public static Answer arrowPlan(String body, java.util.Collection<String> models,
            com.legend.database.PlanStyle style) {
        return answer(500, "COMPILATION", () -> {
            Hosted hosted = hosted(body, models);
            QueryPlan plan = hosted.query().plan(hosted.runtime(), style);
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
            metadata.put("legend.activities", Json.toCompact(List.of(PureV1Writer.activity(
                    plan.sql(), false, java.util.UUID.randomUUID()))));
            metadata.put("legend.columns", Json.toCompact(columnNames(plan)));
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("sql", plan.sql());
            // the raises the text can send, by kind, to their sentences (PlanJson.raises; phase D's option (D), 2026-11-15)
            out.put("raises", com.legend.executionplan.PlanJson.raises(plan.plan()));
            // the raise sites its text declares (v56): a token it raises read through them (phase D's Phase 2)
            out.put("sites", com.legend.executionplan.PlanJson.sitesOf(plan.plan(), plan.sql()));
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
     * a program that writes, a statement that binds values -- is refused by name: legend-lite's server runs it. {@code style}: the plan's, as {@link #arrowPlan}'s.
     */
    public static Answer jsonPlan(String body, java.util.Collection<String> models,
            com.legend.database.PlanStyle style) {
        return answer(500, "COMPILATION", () -> {
            Hosted hosted = hosted(body, models);
            com.legend.executionplan.ExecutionPlan plan = hosted.query().executionPlan(hosted.runtime(),
                    java.util.Set.of(), style);
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
            // the raises `run` can send, by kind, to their sentences (PlanJson.raises; phase D's option (D), 2026-11-15)
            out.put("raises", com.legend.executionplan.PlanJson.raises(plan));
            // the raise sites `run` declares (v56): a token it raises read through them (phase D's Phase 2)
            out.put("sites", com.legend.executionplan.PlanJson.sitesOf(plan, statement));
            if (!result.sql().slots().isEmpty()) {
                // what the host binds to `run`'s `?`s: the plan, its payloads its statement's content by digest
                out.put("plan", Json.parse(com.legend.executionplan.PlanJson.write(plan)));
            }
            out.put("shape", result.shape().name());
            out.put("resultColumns", Json.parse(com.legend.executionplan.PlanJson.writeColumns(result.columns())));
            if (result.shape() == com.legend.executionplan.ExecutionPlan.TdsResult.Shape.OBJECTS) {
                out.put("builder", Map.of("_type", "json"));
            } else {
                PureV1Writer.Envelope e = PureV1Writer.envelope(
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
                case "OBJECTS" -> sink = PureV1Writer.objects(out);
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
                    PureV1Writer.Envelope envelope = new PureV1Writer.Envelope(
                            Json.toCompact(builder), sql.value(), labels);
                    // the execution jsonPlan named, its door's id, carried by the plan the host ran
                    sink = PureV1Writer.relation(out, announced -> envelope, hostExecution(planned));
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

    /** The TDS builder (moved from server.LiteV1Api, the P2 checkpoint audit's S5, 2026-11-16: legend-engine's
     *  {@code tdsBuilder}, the edge's alone): each column's name, Pure type and relational spelling. */
    private static Map<String, Object> tdsBuilder(QueryPlan plan) {
        List<Map<String, Object>> columns = new ArrayList<>();
        for (var c : com.legend.plan.PureSpelling.relationColumns(plan.rootType())) {
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

    /** The answer's column names, in order. */
    private static List<String> columnNames(QueryPlan plan) {
        List<String> names = new ArrayList<>();
        for (var c : com.legend.plan.PureSpelling.relationColumns(plan.rootType())) {
            names.add(c.name());
        }
        return names;
    }

    /** The first 200 characters of {@code text}, for a refusal that shows what it read. */
    private static String head(String text) {
        return text.length() <= 200 ? text : text.substring(0, 200) + "...";
    }
}
