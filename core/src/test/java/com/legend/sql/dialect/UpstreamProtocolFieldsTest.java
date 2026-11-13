// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.json.Json;
import com.legend.server.PureV1Api;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

/**
 * NO FIELD OF UPSTREAM'S PROTOCOL REACHES A STATEMENT AS TEXT (the orchestrator's ruling of 2026-11-13). The server's
 * route bodies -- pure/v1's {@code ExecuteInput} and the test-data generator's requests -- stay open, read as upstream
 * reads its own protocol: the keys a reader takes by name, any other passed over (PlannerSurfaceTest's
 * {@code UPSTREAM_PROTOCOL} row names each open door). What holds an open door safe is held here: each payload of the
 * injection fuzz ({@link InjectionFuzzTest#PAYLOADS}) is planted in each position of each body, as a key no reader
 * takes and as each value a reader does, on every route that plans a statement from such a body, and every statement
 * planned is read by its database's lexical rules ({@link InjectionFuzzTest#finding}).
 *
 * <ul>
 *   <li>A key no reader takes, and a field read by nothing, is passed over: the statements are the request's without
 *       it, and the answer carries no marker.</li>
 *   <li>A value a reader takes (a parameter's value, a runtime's name, a table's, a column's, a row's value) is
 *       refused by name, or reaches the statement as data: the marker inside a literal or a quoted identifier,
 *       nothing left open.</li>
 *   <li>A key inside a value specification (a function, a parameter's value) is the protocol reader's, which refuses
 *       a field no rule takes (register S29): refused, nothing planned.</li>
 * </ul>
 */
class UpstreamProtocolFieldsTest {

    private static final String MARK = InjectionFuzzTest.MARK;

    private static final String MODEL = """
            ###Pure
            Class model::Person { name: String[1]; }

            ###Relational
            Database store::DB ( Table PERSON ( ID INTEGER PRIMARY KEY, NAME VARCHAR(100) ) )

            ###Mapping
            Mapping model::M (
                model::Person: Relational {
                    ~mainTable [store::DB] PERSON
                    name: [store::DB] PERSON.NAME
                }
            )

            ###Connection
            RelationalDatabaseConnection store::Conn {
                type: DuckDB;
                specification: DuckDB { path: 'upstream-protocol.duckdb'; };
                auth: Test;
            }

            ###Runtime
            Runtime model::RT {
                mappings: [ model::M ];
                connections: [ store::DB: [ c: store::Conn ] ];
            }
            """;

    private static final InjectionFuzzTest.Rules RULES = InjectionFuzzTest.rules(DatabaseType.DuckDB);

    /** What a position's field is to its reader. */
    enum Mode {
        /** A key no reader takes, or a field read by nothing: passed over, as upstream passes it over. */
        PASSED_OVER,
        /** A value a reader takes: refused by name, or data in the statement. */
        VALUE,
        /** A key inside a value specification: the protocol reader's, refused by name. */
        CLOSED
    }

    /** A place in a request a payload is planted: its name, what it is to its reader, the planting, and the routes
     *  that read it (every route when empty; on any other the field is read by nothing, so passed over). */
    record Position(String name, Mode mode, BiConsumer<Json.Obj, String> plant, Set<String> readBy) {
        Mode on(String route) {
            return readBy.isEmpty() || readBy.contains(route) ? mode : Mode.PASSED_OVER;
        }
    }

    /** A route's answer: its status, its text, and every statement it planned. */
    record Outcome(int status, String json, List<String> statements) {
    }

    // ---- the requests

    private static String lambda(String text) {
        return PureV1Api.grammarToJsonLambda(text, false).json();
    }

    /** An ExecuteInput: a lambda with a String parameter, bound by name. */
    private static final String EXECUTE_INPUT = "{\"clientVersion\":\"vX_X_X\",\"function\":"
            + lambda("{name:String[1]|#>{store::DB.PERSON}#->filter(r|$r.NAME == $name)->select(~[ID, NAME])"
                    + "->from(model::RT)}")
            + ",\"model\":" + Json.toCompact(Map.of("_type", "text", "code", MODEL))
            + ",\"runtime\":{\"_type\":\"runtimePointer\",\"runtime\":\"model::RT\"}"
            + ",\"context\":{\"_type\":\"BaseExecutionContext\"}"
            + ",\"parameterValues\":[{\"name\":\"name\",\"value\":{\"_type\":\"string\",\"value\":\"x\"}}]}";

    /** A test-data request: an ExecuteInput over the mapping, and a row identified by its key. */
    private static final String TEST_DATA_INPUT = "{\"clientVersion\":\"vX_X_X\",\"function\":"
            + lambda("|model::Person.all()->project([p|$p.name], ['name'])")
            + ",\"model\":" + Json.toCompact(Map.of("_type", "text", "code", MODEL))
            + ",\"mapping\":\"model::M\""
            + ",\"runtime\":{\"_type\":\"runtimePointer\",\"runtime\":\"model::RT\"}"
            + ",\"context\":{\"_type\":\"BaseExecutionContext\"}"
            + ",\"tableRowIdentifiers\":[{\"table\":{\"_type\":\"table\",\"table\":\"PERSON\",\"schema\":\"default\","
            + "\"database\":\"store::DB\"},\"rowIdentifiers\":[{\"columnValuePairs\":[{\"name\":\"ID\",\"value\":1}]}]}]}";

    /** The object at {@code path} (a key, or an array's index) of {@code o}. */
    private static Json.Obj at(Json.Obj o, Object... path) {
        Json.Node n = o;
        for (Object step : path) {
            n = step instanceof Integer i ? ((Json.Arr) n).items().get(i) : ((Json.Obj) n).get((String) step);
        }
        return (Json.Obj) n;
    }

    /** A key no reader takes, named and valued by the payload, in the object at {@code path}. */
    private static BiConsumer<Json.Obj, String> extra(Object... path) {
        return (o, p) -> at(o, path).fields().put(p, Json.str(p));
    }

    /** The field {@code key} of the object at {@code path}, the payload. */
    private static BiConsumer<Json.Obj, String> set(String key, Object... path) {
        return (o, p) -> at(o, path).fields().put(key, Json.str(p));
    }

    private static Position position(String name, Mode mode, BiConsumer<Json.Obj, String> plant) {
        return new Position(name, mode, plant, Set.of());
    }

    /** {@link #position}, read by {@code readBy}'s routes alone. */
    private static Position readBy(Set<String> readBy, String name, Mode mode, BiConsumer<Json.Obj, String> plant) {
        return new Position(name, mode, plant, readBy);
    }

    /** The routes that read an ExecuteInput's parameterValues: the hosts' plan halves, which bind them (generatePlan
     *  reads none: its plan's parameters are the plan's own, as upstream's). */
    private static final Set<String> BINDS_VALUES = Set.of("executeJsonPlan", "executePlan");

    /** The route that reads a request's row identifiers. */
    private static final Set<String> READS_IDENTIFIERS = Set.of("generateTestData_WithSeed");

    /** The positions every ExecuteInput has. */
    private static List<Position> envelope() {
        return List.of(
                position("a key of the request", Mode.PASSED_OVER, extra()),
                position("a key of the model", Mode.PASSED_OVER, extra("model")),
                position("a key of the runtime's pointer", Mode.PASSED_OVER, extra("runtime")),
                position("a key of the context", Mode.PASSED_OVER, extra("context")),
                position("the client's version", Mode.PASSED_OVER, set("clientVersion")),
                position("the context's type", Mode.PASSED_OVER, set("_type", "context")),
                position("the runtime's name", Mode.VALUE, set("runtime", "runtime")),
                position("the model's type", Mode.VALUE, set("_type", "model")),
                position("the model's text", Mode.VALUE, set("code", "model")),
                position("a key of the function", Mode.CLOSED, extra("function")));
    }

    private static List<Position> executeInput() {
        List<Position> out = new ArrayList<>(envelope());
        out.add(position("a key of a parameter's value", Mode.PASSED_OVER, extra("parameterValues", 0)));
        out.add(position("a value for no parameter", Mode.PASSED_OVER, (o, p) -> {
            List<Json.Node> values = new ArrayList<>(((Json.Arr) o.get("parameterValues")).items());
            values.add(Json.parseObject(Json.toCompact(Map.of("name", p, "value", Map.of("_type", "string",
                    "value", p)))));
            o.fields().put("parameterValues", new Json.Arr(values));
        }));
        out.add(readBy(BINDS_VALUES, "a parameter's value", Mode.VALUE, set("value", "parameterValues", 0, "value")));
        out.add(readBy(BINDS_VALUES, "a parameter's name", Mode.VALUE, set("name", "parameterValues", 0)));
        out.add(readBy(BINDS_VALUES, "a key of a parameter's value specification", Mode.CLOSED,
                extra("parameterValues", 0, "value")));
        return out;
    }

    private static List<Position> testDataInput() {
        List<Position> out = new ArrayList<>(envelope());
        Object[] table = {"tableRowIdentifiers", 0, "table"};
        Object[] pair = {"tableRowIdentifiers", 0, "rowIdentifiers", 0, "columnValuePairs", 0};
        out.add(position("a key of a table's identifiers", Mode.PASSED_OVER, extra("tableRowIdentifiers", 0)));
        out.add(position("a key of a table", Mode.PASSED_OVER, extra(table)));
        out.add(position("a key of a row's identifier", Mode.PASSED_OVER,
                extra("tableRowIdentifiers", 0, "rowIdentifiers", 0)));
        out.add(position("a key of a column-value pair", Mode.PASSED_OVER, extra(pair)));
        out.add(position("the mapping", Mode.VALUE, set("mapping")));
        out.add(readBy(READS_IDENTIFIERS, "a table's name", Mode.VALUE, set("table", table)));
        out.add(readBy(READS_IDENTIFIERS, "a table's schema", Mode.VALUE, set("schema", table)));
        out.add(readBy(READS_IDENTIFIERS, "a table's database", Mode.VALUE, set("database", table)));
        out.add(readBy(READS_IDENTIFIERS, "a column-value pair's column", Mode.VALUE, set("name", pair)));
        out.add(readBy(READS_IDENTIFIERS, "a column-value pair's value", Mode.VALUE, set("value", pair)));
        return out;
    }

    // ---- the routes

    /** Every statement an answer carries, at any depth: an SQL node's {@code sqlQuery}, a plan half's {@code sql} and
     *  {@code run}, a plan's {@code statement}s, and the same inside a value that is JSON text. */
    static List<String> statements(String json) {
        List<String> out = new ArrayList<>();
        try {
            collect(Json.parse(json), null, out);
        } catch (RuntimeException notJson) {
            // a test-data answer is text
        }
        return out;
    }

    private static void collect(Json.Node node, String key, List<String> out) {
        switch (node) {
            case Json.Obj o -> o.fields().forEach((k, v) -> collect(v, k, out));
            case Json.Arr a -> a.items().forEach(n -> collect(n, key, out));
            case Json.Str s -> {
                if (List.of("sqlQuery", "sql", "run", "statement", "statements").contains(key)) {
                    out.add(s.value());
                } else if (s.value().startsWith("{") || s.value().startsWith("[")) {
                    try {
                        collect(Json.parse(s.value()), key, out);
                    } catch (RuntimeException notJson) {
                        // a text that is not JSON: no statement in it
                    }
                }
            }
            default -> {
            }
        }
    }

    /** A route by its pure/v1 path: its answer's statements, and those of any statement it hands its runner (the
     *  test-data routes run theirs: each is planned here and not run). */
    private static Function<String, Outcome> route(String path) {
        return body -> {
            List<String> ran = new ArrayList<>();
            PureV1Api.Answer a = PureV1Api.route("/api/pure/v1/" + path, null, body, (query, runtime, output) ->
                    ran.addAll(InjectionFuzzTest.statements(com.legend.executionplan.PlanJson.write(
                            query.executionPlan(runtime)))));
            List<String> all = new ArrayList<>(statements(a.json()));
            all.addAll(ran);
            return new Outcome(a.status(), a.json(), all);
        };
    }

    private static Function<String, Outcome> answer(Function<String, PureV1Api.Answer> door) {
        return body -> {
            PureV1Api.Answer a = door.apply(body);
            return new Outcome(a.status(), a.json(), statements(a.json()));
        };
    }

    /** The routes that plan a statement from an ExecuteInput: generatePlan, and the hosts' two plan halves
     *  (executePlan's Arrow, executeJsonPlan's JSON). */
    private static Map<String, Function<String, Outcome>> executeRoutes() {
        Map<String, Function<String, Outcome>> out = new LinkedHashMap<>();
        out.put("generatePlan", route("execution/generatePlan"));
        out.put("executeJsonPlan", answer(body -> PureV1Api.jsonPlan(body, List.of(MODEL))));
        out.put("executePlan", answer(body -> PureV1Api.arrowPlan(body, List.of(MODEL))));
        return out;
    }

    /** The test-data generator's routes that plan a statement. */
    private static Map<String, Function<String, Outcome>> testDataRoutes() {
        Map<String, Function<String, Outcome>> out = new LinkedHashMap<>();
        out.put("generateTestData_WithSeed", route("execution/testDataGeneration/generateTestData_WithSeed"));
        out.put("generateTestData_WithDefaultSeed",
                route("execution/testDataGeneration/generateTestData_WithDefaultSeed"));
        out.put("generateSeedData", route("execution/testDataGeneration/generateSeedData"));
        return out;
    }

    // ---- the check

    /** Every way a payload in {@code positions} of {@code base} changed a statement on {@code routes}, each named; the
     *  counts of plantings planned and refused in {@code counts}. */
    private static List<String> findings(String base, List<Position> positions,
            Map<String, Function<String, Outcome>> routes, Map<String, Integer> counts) {
        List<String> out = new ArrayList<>();
        routes.forEach((routeName, route) -> {
            Outcome baseline = route.apply(base);
            assertEquals(200, baseline.status(), routeName + ": " + baseline.json());
            assertTrue(!baseline.statements().isEmpty(), routeName + " planned no statement: " + baseline.json());
            for (Position position : positions) {
                for (String payload : InjectionFuzzTest.PAYLOADS) {
                    Json.Obj request = Json.parseObject(base);
                    position.plant().accept(request, payload);
                    Outcome o = route.apply(Json.toCompact(request));
                    String where = routeName + ", " + position.name() + ", " + payload.replace("\n", "\\n") + ": ";
                    counts.merge(o.status() == 200 ? "answered" : "refused", 1, Integer::sum);
                    switch (position.on(routeName)) {
                        case PASSED_OVER -> {
                            if (o.status() != 200 || !o.statements().equals(baseline.statements())) {
                                out.add(where + "not passed over: " + o.status() + " " + o.statements() + " "
                                        + head(o.json()));
                            } else if (o.json().contains(MARK)) {
                                out.add(where + "the answer carries the marker: " + head(o.json()));
                            }
                        }
                        case VALUE -> {
                            if (o.statements().stream().anyMatch(st -> st.contains(MARK))) {
                                counts.merge("carried as data", 1, Integer::sum);
                            }
                            for (String statement : o.statements()) {
                                String wrong = InjectionFuzzTest.finding(statement, RULES, payload.equals(MARK));
                                if (wrong != null) {
                                    out.add(where + wrong + ": " + statement.replace("\n", " "));
                                }
                            }
                        }
                        case CLOSED -> {
                            if (o.status() == 200 || !o.statements().isEmpty()
                                    || !o.json().contains("no reader rule takes")) {
                                out.add(where + "not refused by the protocol reader: " + o.status() + " "
                                        + head(o.json()));
                            }
                        }
                    }
                }
            }
        });
        return out;
    }

    private static String head(String text) {
        return text.length() <= 300 ? text : text.substring(0, 300) + "...";
    }

    @Test
    void noFieldOfAnExecuteInputReachesAStatementAsText() {
        Map<String, Integer> counts = new TreeMap<>();
        assertEquals(List.of(), findings(EXECUTE_INPUT, executeInput(), executeRoutes(), counts), counts.toString());
        assertTrue(counts.getOrDefault("carried as data", 0) > 0, "no value reached a statement: " + counts);
    }

    @Test
    void noFieldOfATestDataRequestReachesAStatementAsText() {
        Map<String, Integer> counts = new TreeMap<>();
        assertEquals(List.of(), findings(TEST_DATA_INPUT, testDataInput(), testDataRoutes(), counts),
                counts.toString());
        assertTrue(counts.getOrDefault("carried as data", 0) > 0, "no value reached a statement: " + counts);
    }

    /** The check bites: a planted route that writes a parameter's value into its statement as text, and its request's
     *  client version into a comment, and reads its body leniently all through -- the value named for what it opened
     *  (a string left open), the version as a field not passed over, and a key inside the function as one
     *  the protocol reader did not refuse. */
    @Test
    void theCheckNamesAPlantedRouteThatSplicesItsFields() {
        Function<String, Outcome> spliced = body -> {
            Json.Obj o = Json.parseObject(body);
            String value = at(o, "parameterValues", 0, "value").getString("value");
            return new Outcome(200, "{}", List.of("SELECT ID, NAME FROM PERSON WHERE NAME = '" + value + "' /* "
                    + o.getString("clientVersion") + " */"));
        };
        List<String> found = findings(EXECUTE_INPUT, executeInput(), Map.of("executeJsonPlan", spliced),
                new TreeMap<>());
        for (String expected : List.of("executeJsonPlan, a parameter's value, a'ZQXMARK: a string left open",
                "executeJsonPlan, the client's version, ZQXMARK: not passed over",
                "executeJsonPlan, a key of the function, ZQXMARK: not refused by the protocol reader")) {
            assertTrue(found.stream().anyMatch(f -> f.startsWith(expected)), expected + " in:\n"
                    + String.join("\n", found));
        }
    }
}
