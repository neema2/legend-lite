// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import com.legend.json.Json;
import com.legend.protocol.ProtocolReader;
import com.legend.protocol.PureComposer;
import com.legend.server.LiteV1Api.Answer;
import com.legend.server.LiteV1Api.Models;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * LITE'S OWN OPERATIONS ON THE MODEL AND THE QUERY (lite's API v1 spec §2, the user's go of 2026-11-17; the server
 * binding: {@code /api/lite/v1/<operation>}): {@code model/check}, {@code model/elements}, {@code query/parse},
 * {@code query/print}, {@code query/type}, {@code query/plan} and {@code query/run}. Each operation is a product function
 * here -- text and trees in, JSON out -- which every binding is a thin layer over: the server's routes
 * ({@link LiteV1Api#route}), the browser's planner exports and Python's native entries call the same functions. A model travels as its text, a
 * query as its tree (the V1 protocol's lambda JSON); every refusal is lite's one error shape ({@link LiteV1Api#refusal}).
 *
 * <p>Each request is a closed JSON object: every key it names required, its optional ones read where given, any other
 * refused by name (kind {@code request}).
 */
public final class LiteV1Planning {
    private LiteV1Planning() {}

    /** The prefix of every lite operation's route: {@code /api/lite/v1/<operation>} ({@link LiteV1Api#route}). */
    public static final String API = "/api/lite/v1/";

    /** The media type of {@code query/run}'s Arrow answer. */
    public static final String ARROW = "application/vnd.apache.arrow.stream";

    /**
     * How the server runs a query with its run's parameter values: the product's door ({@code
     * Execution.runOnProjectDatabases}, its values-taking overload), handed in by the server's mount line
     * ({@code ConnectionResolver.liteRunner}). {@code values}: each declared parameter's value as its Pure type's Java
     * value ({@code exec.PlanParameters}), read by {@link #values}. A host with no database (the browser's and Python's
     * planners) has none: its run is its own runner's.
     */
    @FunctionalInterface
    public interface Runner {
        void run(com.legend.TypedQuery query, String runtime, Map<String, ?> values, com.legend.result.Output output)
                throws java.io.IOException;
    }

    // ---- the operations, each a product function every binding calls ------------------------------------------------

    /** {@code model/check}: {@code {errors: [{kind, message, where?}]}}, every error the model has, empty when it
     *  compiles ({@code Compiler.compileErrors}: the first element error alone, else every body's). */
    public static String modelCheck(String model) {
        List<Json.Node> errors = com.legend.Compiler.compileErrors(model, e -> LiteV1Api.refusal(e).json()).stream()
                .map(Json::parse).toList();
        return Json.toCompact(Map.of("errors", errors));
    }

    /** {@code model/elements}: {@code {elements}}, the model's protocol elements, read through the parse door. */
    public static String modelElements(String model) {
        Json.Obj document = Json.parseObject(com.legend.door.Parse.documentJson(model, false));
        return Json.toCompact(Map.of("elements", document.get("elements")));
    }

    /** {@code query/parse}: {@code {query}}, the lambda's tree, its numbers as written. Text without a leading {@code |}
     *  reads as a lambda of no parameters. */
    public static String queryParse(String text) {
        // the product's grammar, its islands (#Cypher{...}#, #SQL, #TS ...) included, the level the compiler's to decide
        // (ParserBoundaryArchTest.dialectIsConfinedToTheNamedClasses); legend-engine's grammarToJson (the edge's) reads
        // legend-engine's
        return "{\"query\":" + com.legend.Compiler.parseQueryJson(text) + "}";
    }

    /** {@code query/print}: {@code {text}}, in the style {@code pretty} or {@code standard}; any other refused by
     *  name. */
    public static String queryPrint(Json.Obj query, String style) {
        PureComposer.Style s = spelled(PureComposer.Style.class, style, "a query prints in the style pretty or standard");
        return Json.toCompact(Map.of("text", PureComposer.lambda(query, s)));
    }

    /** {@code query/run}'s formats (lite's API v1 spec §4), by their spec words: {@code json} and {@code arrow}. */
    private enum Format { JSON, ARROW }

    /**
     * The constant of {@code words} whose name in lower case is {@code word}: lite's API v1 spells each choice so
     * ({@code pretty}, {@code arrow}), one table per choice and no switch over words. Any other word is refused by name,
     * kind request: {@code what}, then the word given.
     */
    private static <E extends Enum<E>> E spelled(Class<E> words, String word, String what) {
        for (E e : words.getEnumConstants()) {
            if (e.name().toLowerCase(java.util.Locale.ROOT).equals(word)) {
                return e;
            }
        }
        throw new LiteV1Api.MalformedRequest(what + ", not '" + word + "'");
    }

    /** {@code query/type}: {@code {columns: [{name, type, enumeration?}]}}, the query's result typed by the compiler,
     *  each column's Pure type name; an enumeration's column says so ({@code enumeration: true}), its type the
     *  enumeration's path. */
    public static String queryType(com.legend.compiler.element.ModelContext ctx,
            com.legend.protocol.spec.LambdaFunction query) {
        List<Map<String, Object>> columns = new java.util.ArrayList<>();
        for (com.legend.compiler.element.type.Type.Column c : com.legend.plan.PureSpelling.relationColumns(
                com.legend.Compiler.query(ctx, query).resultType())) {
            Map<String, Object> column = new java.util.LinkedHashMap<>();
            column.put("name", c.name());
            column.put("type", com.legend.plan.PureSpelling.typePath(c.type()));
            if (c.type() instanceof com.legend.compiler.element.type.Type.EnumType) {
                column.put("enumeration", true);
            }
            columns.add(column);
        }
        return Json.toCompact(Map.of("columns", columns));
    }

    /** {@code query/plan}: {@code {plan}}, lite's plan of the query on {@code runtime} -- data: its SQL, its parameters'
     *  slots and its columns ({@code PlanJson}) -- planned by {@code plans}, the plan the binding's run runs (the
     *  server's {@code query/run} its project plan). */
    public static String queryPlan(com.legend.compiler.element.ModelContext ctx,
            com.legend.protocol.spec.LambdaFunction query, String runtime, LiteV1Api.Plans plans) {
        return "{\"plan\":" + com.legend.executionplan.PlanJson.write(
                plans.plan(com.legend.Compiler.query(ctx, query), runtime)) + "}";
    }

    /**
     * {@code query/run}: {@code {model, query, runtime, params?, format?}}, the query planned and run by {@code runner},
     * its parameters' values read by their declared types ({@link #values}) and bound at run. {@code format} is
     * {@code json} (the default: {@code {shape, columns, rows}}, {@link Answered}) or {@code arrow} (one Arrow IPC
     * stream of typed columns, {@code result.ArrowWriter}); the caller chooses, never a guess.
     */
    public static Answer queryRun(String body, Runner runner, Models models) {
        java.io.ByteArrayOutputStream arrow = new java.io.ByteArrayOutputStream();
        boolean[] asArrow = {false};
        Answer answered = LiteV1Api.answer(() -> {
            Json.Obj request = LiteV1Api.read(body, "a query/run request", Set.of("params", "format"),
                    "model", "query", "runtime");
            Json.Obj queryJson = LiteV1Api.object(request, "query");
            com.legend.protocol.spec.LambdaFunction query = ProtocolReader.lambda(queryJson);
            String runtime = LiteV1Api.text(request, "runtime");
            Map<String, Object> values = values(queryJson,
                    request.has("params") ? LiteV1Api.object(request, "params") : new Json.Obj(new java.util.LinkedHashMap<>()));
            String format = request.has("format") ? LiteV1Api.text(request, "format") : "json";
            asArrow[0] = spelled(Format.class, format, "a query runs in the format json or arrow") == Format.ARROW;
            return models.with(LiteV1Api.text(request, "model"), ctx -> {
                com.legend.TypedQuery typed = com.legend.Compiler.query(ctx, query);
                Answered rows = new Answered();
                com.legend.result.ResultSink sink = asArrow[0] ? new ShapedArrow(arrow) : rows;
                try {
                    runner.run(typed, runtime, values, new com.legend.result.Output.Rows(sink));
                } catch (java.io.IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
                return asArrow[0] ? "" : rows.json();
            });
        });
        return answered.status() == 200 && asArrow[0]
                ? Answer.bytes(200, arrow.toByteArray(), ARROW, Map.of()) : answered;
    }

    /**
     * The run's parameter values ({@code params}, by name) read by the query's own declaration of each -- its type, the
     * declarations its plan carries -- as its Pure type's Java value, the form {@code exec.PlanParameters} checks and
     * binds: an Integer a {@code Long} (the 64-bit range), a Float a {@code Double}, a Decimal a {@code BigDecimal} (its
     * digits, as text or as a number), a Number any of those three, a Boolean a {@code Boolean}, a String a
     * {@code String}, a StrictDate a {@code LocalDate} (its text, {@code YYYY-MM-DD}), a DateTime a {@code LocalDateTime}
     * (its ISO 8601 text in UTC, {@code YYYY-MM-DD[T ]HH:MM[:SS[.fffffffff]][Z]}), a Date either, an enumeration's value
     * its name; a list its values; JSON null none. A value of a JSON kind its type is not is refused by name, kind
     * {@code parameters}; whether a value is one its parameter takes (an enumeration's names, a list's size) is the run's
     * ({@code PlanParameters.check}). A value for no declared parameter is ignored, as the run ignores one.
     */
    static Map<String, Object> values(Json.Obj query, Json.Obj params) {
        Map<String, Object> out = new java.util.LinkedHashMap<>();
        List<Json.Node> declared = query.has("parameters") ? query.getArr("parameters").items() : List.of();
        for (Json.Node n : declared) {
            Json.Obj p = (Json.Obj) n;
            String name = p.getString("name");
            Json.Node value = params.fields().get(name);
            if (value == null) {
                continue;
            }
            if (!p.has("genericType")) {
                throw new com.legend.error.InvalidParameters("parameter '" + name + "' declares no type: a query's"
                        + " parameters are typed ({" + name + ": String[1]|...})");
            }
            String type = p.getObj("genericType").getObj("rawType").getString("fullPath");
            out.put(name, value instanceof Json.Arr list
                    ? list.items().stream().map(v -> value(name, type, v)).toList()
                    : value(name, type, value));
        }
        return out;
    }

    private static final java.util.regex.Pattern DATE_TIME = java.util.regex.Pattern.compile(
            "(\\d{4})-(\\d{2})-(\\d{2})[T ](\\d{2}):(\\d{2})(?::(\\d{2})(?:\\.(\\d{1,9}))?)?Z?");

    /** One value of parameter {@code name}, of Pure type {@code type}, as its Java value ({@link #values}). */
    private static @com.legend.base.Nullable Object value(String name, String type, Json.Node v) {
        if (v instanceof Json.Null) {
            return null;
        }
        Object read = switch (type) {
            case "Integer" -> v instanceof Json.Num n ? integer(name, n) : null;
            case "Float" -> v instanceof Json.Num n ? (Object) n.doubleValue() : null;
            case "Decimal" -> decimal(v);
            case "Number" -> v instanceof Json.Num n ? (n.isInteger() ? (Object) n.longValue() : n.doubleValue()) : decimal(v);
            case "Boolean" -> v instanceof Json.Bool b ? (Object) b.value() : null;
            case "StrictDate" -> v instanceof Json.Str s ? date(name, s.value()) : null;
            case "DateTime" -> v instanceof Json.Str s ? dateTime(name, s.value()) : null;
            case "Date" -> v instanceof Json.Str s
                    ? (s.value().length() == 10 ? date(name, s.value()) : dateTime(name, s.value())) : null;
            // a String, and an enumeration's value by its name (its names are the plan's to check)
            default -> v instanceof Json.Str s ? s.value() : null;
        };
        if (read == null) {
            throw new com.legend.error.InvalidParameters("parameter '" + name + "' (" + type + "): given "
                    + Json.toCompact(v) + ", which is no " + type + "'s JSON value");
        }
        return read;
    }

    /** An Integer's value, exact in the 64-bit range (a {@code Long}); a whole number beyond it refused by name (the
     *  JSON reader keeps one exact, {@code Json.Num.ofDecimal}), a fraction no Integer's value. */
    private static @com.legend.base.Nullable Object integer(String name, Json.Num n) {
        if (n.isInteger()) {
            return n.longValue();
        }
        java.math.BigDecimal exact = n.decimalValue();
        if (exact != null && n.token() == null && exact.scale() <= 0) {
            throw new com.legend.error.InvalidParameters("parameter '" + name + "' (Integer): given "
                    + exact.toPlainString() + ", which is beyond the 64-bit range an Integer holds");
        }
        return null;
    }

    private static @com.legend.base.Nullable Object decimal(Json.Node v) {
        return switch (v) {
            case Json.Str s -> new java.math.BigDecimal(s.value());
            case Json.Num n -> n.decimalValue() != null ? n.decimalValue()
                    : new java.math.BigDecimal(n.token() != null ? n.token() : Json.toCompact(n));
            default -> null;
        };
    }

    private static java.time.LocalDate date(String name, String text) {
        try {
            return java.time.LocalDate.parse(text);
        } catch (java.time.format.DateTimeParseException e) {
            throw new com.legend.error.InvalidParameters("parameter '" + name + "': given \"" + text + "\", which is no"
                    + " StrictDate's text (YYYY-MM-DD)");
        }
    }

    private static java.time.LocalDateTime dateTime(String name, String text) {
        java.util.regex.Matcher m = DATE_TIME.matcher(text);
        if (!m.matches()) {
            throw new com.legend.error.InvalidParameters("parameter '" + name + "': given \"" + text + "\", which is no"
                    + " DateTime's text (its ISO 8601 text in UTC, YYYY-MM-DD[T ]HH:MM[:SS[.fffffffff]][Z])");
        }
        String fraction = m.group(7) == null ? "" : m.group(7);
        try {
            return java.time.LocalDateTime.of(Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2)),
                    Integer.parseInt(m.group(3)), Integer.parseInt(m.group(4)), Integer.parseInt(m.group(5)),
                    m.group(6) == null ? 0 : Integer.parseInt(m.group(6)),
                    fraction.isEmpty() ? 0 : Integer.parseInt((fraction + "000000000").substring(0, 9)));
        } catch (java.time.DateTimeException e) {
            throw new com.legend.error.InvalidParameters("parameter '" + name + "': given \"" + text + "\", which is no"
                    + " DateTime: " + e.getMessage());
        }
    }

    /**
     * {@code query/run}'s ARROW ANSWER (spec §4): one IPC stream of typed columns ({@code result.ArrowWriter}, the plan's
     * facts as each field's metadata), the answer's shape in the stream's metadata ({@code legend.shape}) -- what the JSON
     * answer says as {@code shape}. A receiver of the run's; it decides nothing.
     */
    static final class ShapedArrow implements com.legend.result.ResultSink {
        /** The stream's metadata, its shape once the run tells it. */
        private final Map<String, String> told = new java.util.LinkedHashMap<>();
        private final com.legend.result.ArrowWriter writer;

        ShapedArrow(java.io.OutputStream out) {
            this.writer = new com.legend.result.ArrowWriter(out, columns -> {
                if (!told.containsKey("legend.shape")) {
                    throw new IllegalStateException("the run told its columns before its answer's shape");
                }
                return told;
            }, com.legend.result.CellText.PRODUCT);
        }

        @Override
        public void answers(com.legend.executionplan.ExecutionPlan.TdsResult.Shape shape) {
            told.put("legend.shape", shape.name());
        }

        @Override
        public void columns(List<com.legend.executionplan.ExecutionPlan.Column> columns) throws java.io.IOException {
            writer.columns(columns);
        }

        @Override
        public void batch(com.legend.result.Batch batch) throws java.io.IOException {
            writer.batch(batch);
        }

        @Override
        public void end() throws java.io.IOException {
            writer.end();
        }
    }

    /**
     * A RUN'S ANSWER AS LITE'S TYPED ROWS (spec §4): {@code {shape, columns: [{name, type}], rows: [[cell]]}}, each cell
     * a typed value -- an Integer an exact JSON number, a Decimal its exact text, a Float a double (a non-finite one its
     * name), a date and a date-time their text (a DateTime ISO UTC with {@code Z}), a boolean, a text, a list or a
     * structure as its JSON, null -- written by {@code CellText}'s rule, a decimal as its text. A receiver of the run's
     * ({@code Output.Rows}); it decides nothing.
     */
    static final class Answered implements com.legend.result.ResultSink {
        /** The answer as the run tells it, its frame: its shape, its columns and, once the run ends, its rows. */
        private final Map<String, Object> answer = new java.util.LinkedHashMap<>();
        private final List<com.legend.executionplan.ExecutionPlan.Column> columns = new java.util.ArrayList<>();
        private final List<List<Json.Node>> rows = new java.util.ArrayList<>();

        @Override
        public void answers(com.legend.executionplan.ExecutionPlan.TdsResult.Shape shape) {
            answer.put("shape", shape.name());
        }

        @Override
        public void columns(List<com.legend.executionplan.ExecutionPlan.Column> answered) {
            columns.addAll(answered);
            answer.put("columns", answered.stream().map(c -> {
                Map<String, Object> column = new java.util.LinkedHashMap<>();
                column.put("name", c.name());
                column.put("type", c.type());
                return column;
            }).toList());
        }

        @Override
        public void batch(com.legend.result.Batch batch) {
            for (int r = 0; r < batch.size(); r++) {
                List<Json.Node> row = new java.util.ArrayList<>(columns.size());
                for (int i = 0; i < columns.size(); i++) {
                    Object cell = batch.get(r, i);
                    com.legend.executionplan.ExecutionPlan.Column c = columns.get(i);
                    row.add(cell instanceof java.math.BigDecimal d && !c.type().equals("Float")
                            ? Json.str(d.toPlainString()) : com.legend.result.CellText.pureJson(cell, c));
                }
                rows.add(row);
            }
        }

        @Override
        public void end() {
            answer.put("rows", rows);
        }

        /** The answer's JSON, once the run has ended; a run that told it less is refused by name. */
        String json() {
            for (String told : List.of("shape", "columns", "rows")) {
                if (!answer.containsKey(told)) {
                    throw new IllegalStateException("the run ended without telling its answer's " + told);
                }
            }
            return Json.toCompact(answer);
        }
    }
}
