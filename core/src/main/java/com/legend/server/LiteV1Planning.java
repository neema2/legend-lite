// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import com.legend.json.Json;
import com.legend.protocol.ProtocolReader;
import com.legend.protocol.PureComposer;
import com.legend.server.LiteV1Api.Answer;
import com.legend.server.LiteV1Api.Models;
import com.legend.server.LiteV1Api.RefusalKind;

import java.util.Map;
import java.util.Set;

/**
 * LITE'S OWN GRAMMAR AND TYPING ROUTES (the edge program's HOST stream, 2026-11-17; the orchestrator's Q1 ruling: "a NEW
 * server file ... mounted from LiteV1Api by ONE line ... each route calls the same product Java the tab's lite exports
 * call ... not the pure/v1 router. Typed routes only, no generic dispatch"): what a query app asks a server to read and
 * type a query by -- a model's text as its protocol JSON, a lambda's text as its JSON and back, and a lambda's relation
 * type over a model -- answered from the product's own code, the calls {@code planner.Boundary}'s lite exports make
 * ({@code modelElements}, {@code parseLambda}, {@code printLambda}, {@code lambdaRelationType}; the census's A1).
 * legend-engine's {@code pure/v1} routes for the same questions are the edge's ({@code com.legend.edge.PureV1Routes}).
 *
 * <p>Each request is a closed JSON object: every key it names required, any other refused by name. Each answer is lite's
 * JSON; a refusal is {@link LiteV1Api#answer}'s, its envelope and words LiteV1Api's.
 */
public final class LiteV1Planning {
    private LiteV1Planning() {}

    /** {@code {"text", "sourceInformation"}}: a model's text as its protocol JSON ({@code PureModelContextData}). */
    public static final String MODEL_ELEMENTS = "/api/lite/v1/grammar/modelElements";
    /** {@code {"text", "sourceInformation"}}: a lambda's text as its protocol JSON. */
    public static final String PARSE_LAMBDA = "/api/lite/v1/grammar/parseLambda";
    /** {@code {"lambda", "style"}}: a lambda's protocol JSON as its Pure text, {@code {"text"}}. */
    public static final String PRINT_LAMBDA = "/api/lite/v1/grammar/printLambda";
    /** {@code {"model", "lambda"}}: a lambda's relation type over a model given as text. */
    public static final String LAMBDA_RELATION_TYPE = "/api/lite/v1/compilation/lambdaRelationType";

    /** {@code path}'s answer for {@code body}, a model given as text compiled by {@code models}. LiteV1Api routes
     *  only these paths here (its one line); any other is refused by name, never answered by another route. */
    public static Answer route(String path, String body, Models models) {
        return switch (path) {
            case MODEL_ELEMENTS -> modelElements(body);
            case PARSE_LAMBDA -> parseLambda(body);
            case PRINT_LAMBDA -> printLambda(body);
            case LAMBDA_RELATION_TYPE -> lambdaRelationType(body, models);
            default -> throw new IllegalArgumentException("not one of lite's planning routes: " + path);
        };
    }

    /** {@value #MODEL_ELEMENTS}: through the parse door, as the tab's {@code modelElements} reads it. */
    public static Answer modelElements(String body) {
        return LiteV1Api.answer(400, RefusalKind.PARSE.text(), () -> {
            Json.Obj request = closed(body, "a modelElements request", "text", "sourceInformation");
            return com.legend.door.Parse.documentJson(request.getString("text"), request.getBool("sourceInformation"));
        });
    }

    /** {@value #PARSE_LAMBDA}: text without a leading {@code |} read as a parameterless lambda, as the tab's
     *  {@code parseLambda} reads it, through the parse door. */
    public static Answer parseLambda(String body) {
        return LiteV1Api.answer(400, RefusalKind.PARSE.text(), () -> {
            Json.Obj request = closed(body, "a parseLambda request", "text", "sourceInformation");
            return com.legend.door.Parse.lambdaJson(request.getString("text"), request.getBool("sourceInformation"));
        });
    }

    /** {@value #PRINT_LAMBDA}: {@code PRETTY} or {@code STANDARD}, any other style refused by name, as the tab's
     *  {@code printLambda} prints it. */
    public static Answer printLambda(String body) {
        return LiteV1Api.answer(400, RefusalKind.COMPILE.text(), () -> {
            Json.Obj request = closed(body, "a printLambda request", "lambda", "style");
            String style = request.getString("style");
            PureComposer.Style s = switch (style) {
                case "PRETTY" -> PureComposer.Style.PRETTY;
                case "STANDARD" -> PureComposer.Style.STANDARD;
                default -> throw new IllegalArgumentException("a lambda prints PRETTY or STANDARD, not '" + style + "'");
            };
            return Json.toCompact(Map.of("text", PureComposer.lambda(request.getObj("lambda"), s)));
        });
    }

    /** {@value #LAMBDA_RELATION_TYPE}: the lambda typed, compile-only, over the model, as the tab's
     *  {@code lambdaRelationType} types it. */
    public static Answer lambdaRelationType(String body, Models models) {
        return LiteV1Api.answer(400, RefusalKind.COMPILE.text(), () -> {
            Json.Obj request = closed(body, "a lambdaRelationType request", "model", "lambda");
            ModelInput model = ModelInput.of(request.getObj("model"));
            com.legend.protocol.spec.LambdaFunction lambda = ProtocolReader.lambda(request.getObj("lambda"));
            return model.with(models, ctx -> Json.toCompact(com.legend.plan.RelationTypeJson.of(
                    com.legend.Compiler.query(ctx, lambda).resultType())));
        });
    }

    // ---- a query's plan and its run (the edge program's H7, 2026-11-17) ------------------------------------------------

    /** Where lite's execution routes are mounted: their module's prefix ({@link #execution}). */
    public static final String EXECUTION = "/api/lite/v1/execution";
    /** {@code {"model", "lambda"}}, {@code "runtime"} optional: the lambda's plan over the model given as text -- lite's
     *  execution plan, its JSON ({@code PlanJson}), the plan the execute route runs ({@code TypedQuery.projectPlan}). */
    public static final String EXECUTION_PLAN = EXECUTION + "/plan";
    /** {@code {"model", "lambda", "parameters"}}, {@code "runtime"} optional: the lambda planned and run on the server's
     *  project databases, its parameters bound at run; answered {@code {"sql", "shape", "columns", "rows"}}
     *  ({@link Answered}). */
    public static final String EXECUTION_EXECUTE = EXECUTION + "/execute";

    /**
     * How the server runs a query WITH ITS RUN'S PARAMETER VALUES: the product's door ({@code
     * Execution.runOnProjectDatabases}, its values-taking overload), handed in by the server's mount line. A runner of its
     * own beside {@link LiteV1Api.Runner}, whose run takes no values (the orchestrator's placements of 2026-11-17: that
     * interface is not touched; values bound at RUN, the Q2 ruling). {@code values}: each declared parameter's value as
     * its Pure type's Java value ({@code exec.PlanParameters}), read by {@link #values}. The server's is
     * {@code ConnectionResolver.liteRunner}.
     */
    @FunctionalInterface
    public interface Runner {
        void run(com.legend.TypedQuery query, @com.legend.base.Nullable String runtime, Map<String, ?> values,
                com.legend.result.Output output) throws java.io.IOException;
    }

    /** A request under {@value #EXECUTION}, answered: the server mounts these routes as a module of their own
     *  ({@code LegendHttpServer}'s mount line; this library is the plan side's, which names no server). POST only,
     *  every model compiled for its request alone. */
    public static Answer execution(String method, String path, String body, Runner runner) {
        if (!"POST".equals(method)) {
            return LiteV1Api.error(405, null, "lite's execution routes take POST, not " + method);
        }
        return switch (path) {
            case EXECUTION_PLAN -> plan(body, Models.COMPILED);
            case EXECUTION_EXECUTE -> execute(body, Models.COMPILED, runner);
            default -> LiteV1Api.error(404, null, "no such legend-lite API: " + path);
        };
    }

    /** {@value #EXECUTION_PLAN}: the plan as the execute route makes it, planned and not run. */
    public static Answer plan(String body, Models models) {
        return LiteV1Api.answer(400, RefusalKind.COMPILE.text(), () -> {
            Json.Obj request = closed(body, "a plan request", Set.of("runtime"), "model", "lambda");
            ModelInput model = ModelInput.of(request.getObj("model"));
            com.legend.protocol.spec.LambdaFunction lambda = ProtocolReader.lambda(request.getObj("lambda"));
            String runtime = request.has("runtime") ? request.getString("runtime") : null;
            return model.with(models, ctx -> com.legend.executionplan.PlanJson.write(
                    com.legend.Compiler.query(ctx, lambda).projectPlan(runtime)));
        });
    }

    /** {@value #EXECUTION_EXECUTE}: the lambda run by {@code runner}, its parameters' values read by their declared types
     *  ({@link #values}) and bound at run, its answer lite's typed rows ({@link Answered}). */
    public static Answer execute(String body, Models models, Runner runner) {
        return LiteV1Api.answer(400, RefusalKind.COMPILE.text(), () -> {
            Json.Obj request = closed(body, "an execute request", Set.of("runtime"), "model", "lambda", "parameters");
            ModelInput model = ModelInput.of(request.getObj("model"));
            Json.Obj lambdaJson = request.getObj("lambda");
            com.legend.protocol.spec.LambdaFunction lambda = ProtocolReader.lambda(lambdaJson);
            Map<String, Object> values = values(lambdaJson, request.getObj("parameters"));
            String runtime = request.has("runtime") ? request.getString("runtime") : null;
            return model.with(models, ctx -> {
                com.legend.TypedQuery typed = com.legend.Compiler.query(ctx, lambda);
                Answered answered = new Answered();
                try {
                    runner.run(typed, runtime, values, new com.legend.result.Output.Rows(answered));
                } catch (java.io.IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
                return answered.json();
            });
        });
    }

    /**
     * The run's parameter values ({@code parameters}, by name) read by the lambda's own declaration of each -- its type
     * and multiplicity, the declarations its plan carries -- as its Pure type's Java value, the form {@code
     * exec.PlanParameters} checks and binds: an Integer a {@code Long}, a Float a {@code Double}, a Decimal a {@code
     * BigDecimal} (its digits, as text or as a number), a Number any of those three, a Boolean a {@code Boolean}, a String
     * a {@code String}, a StrictDate a {@code LocalDate} (its text, {@code YYYY-MM-DD}), a DateTime a {@code
     * LocalDateTime} (its ISO 8601 text in UTC, {@code YYYY-MM-DD[T ]HH:MM[:SS[.fffffffff]][Z]}), a Date either, an
     * enumeration's value its name; a list its values; JSON null none. A value of a JSON kind its type is not is refused
     * by name; whether a value is one its parameter takes (an Integer within a Long, an enumeration's names, a list's
     * size) is the run's ({@code PlanParameters.check}). A value for no declared parameter is ignored, as the run ignores
     * one (and as legend-engine's execute does).
     */
    static Map<String, Object> values(Json.Obj lambda, Json.Obj parameters) {
        Map<String, Object> out = new java.util.LinkedHashMap<>();
        java.util.List<Json.Node> declared = lambda.has("parameters") ? lambda.getArr("parameters").items()
                : java.util.List.of();
        for (Json.Node n : declared) {
            Json.Obj p = (Json.Obj) n;
            String name = p.getString("name");
            Json.Node value = parameters.fields().get(name);
            if (value == null) {
                continue;
            }
            if (!p.has("genericType")) {
                throw new IllegalArgumentException("parameter '" + name + "' declares no type: a query's parameters are"
                        + " typed ({" + name + ": String[1]|...})");
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
            case "Integer" -> v instanceof Json.Num n && n.isInteger() ? (Object) n.longValue() : null;
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
            throw new IllegalArgumentException("parameter '" + name + "' (" + type + "): given " + Json.toCompact(v)
                    + ", which is no " + type + "'s JSON value");
        }
        return read;
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
            throw new IllegalArgumentException("parameter '" + name + "': given \"" + text + "\", which is no"
                    + " StrictDate's text (YYYY-MM-DD)", e);
        }
    }

    private static java.time.LocalDateTime dateTime(String name, String text) {
        java.util.regex.Matcher m = DATE_TIME.matcher(text);
        if (!m.matches()) {
            throw new IllegalArgumentException("parameter '" + name + "': given \"" + text + "\", which is no DateTime's"
                    + " text (its ISO 8601 text in UTC, YYYY-MM-DD[T ]HH:MM[:SS[.fffffffff]][Z])");
        }
        String fraction = m.group(7) == null ? "" : m.group(7);
        return java.time.LocalDateTime.of(Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2)),
                Integer.parseInt(m.group(3)), Integer.parseInt(m.group(4)), Integer.parseInt(m.group(5)),
                m.group(6) == null ? 0 : Integer.parseInt(m.group(6)),
                fraction.isEmpty() ? 0 : Integer.parseInt((fraction + "000000000").substring(0, 9)));
    }

    /**
     * A RUN'S ANSWER AS LITE'S TYPED ROWS, the form the tab's run answers (engine-client's {@code LiteAnswer}): the
     * statement its plan answers with, the answer's shape, its columns' names and Pure types, and each row's cells in the
     * host-rows form ({@code result.HostRows}' cells: an integer's digits and a double's shortest digits as numbers -- a
     * non-finite one its name in a string -- a decimal, a date and a date-time as their text, a boolean, a text, a
     * structure as its JSON, null). A receiver of the run's ({@code Output.Rows}); it decides nothing, each cell written
     * by {@code CellText}'s rule, a decimal's as its text.
     */
    static final class Answered implements com.legend.result.ResultSink {
        /** The answer as the run tells it, its frame: its shape, its statement, its columns and, once the run ends,
         *  its rows. */
        private final Map<String, Object> answer = new java.util.LinkedHashMap<>();
        private final java.util.List<com.legend.executionplan.ExecutionPlan.Column> columns = new java.util.ArrayList<>();
        private final java.util.List<java.util.List<Json.Node>> rows = new java.util.ArrayList<>();

        @Override
        public void answers(com.legend.executionplan.ExecutionPlan.TdsResult.Shape shape) {
            answer.put("shape", shape.name());
        }

        @Override
        public void planned(com.legend.executionplan.ExecutionPlan plan) {
            // the statement its answer is: none for a value the plan wrote (a constant)
            answer.put("sql", plan.sendsTheAnswer() ? plan.answerSql().statement() : "");
        }

        @Override
        public void columns(java.util.List<com.legend.executionplan.ExecutionPlan.Column> answered) {
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
                java.util.List<Json.Node> row = new java.util.ArrayList<>(columns.size());
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
            for (String told : java.util.List.of("shape", "sql", "columns", "rows")) {
                if (!answer.containsKey(told)) {
                    throw new IllegalStateException("the run ended without telling its answer's " + told);
                }
            }
            return Json.toCompact(answer);
        }
    }

    /** {@code body} as a JSON object holding exactly {@code keys}: a key it lacks, or one it does not read, refused by
     *  name. */
    private static Json.Obj closed(String body, String what, String... keys) {
        return closed(body, what, Set.of(), keys);
    }

    /** {@link #closed(String, String, String...)}, {@code optional}'s keys read where given. */
    private static Json.Obj closed(String body, String what, Set<String> optional, String... keys) {
        Json.Obj request = LiteV1Api.request(body);
        Set<String> wanted = Set.of(keys);
        String holds = String.join(", ", keys) + (optional.isEmpty() ? "" : "; optional " + String.join(", ",
                new java.util.TreeSet<>(optional)));
        for (String key : request.fields().keySet()) {
            if (!wanted.contains(key) && !optional.contains(key)) {
                throw new IllegalArgumentException(what + " has no key " + key + " (it holds " + holds + ")");
            }
        }
        for (String key : keys) {
            if (!request.fields().containsKey(key)) {
                throw new IllegalArgumentException(what + " names no " + key + " (it holds " + holds + ")");
            }
        }
        return request;
    }
}
