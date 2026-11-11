// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.Compiler;
import com.legend.Execution;
import com.legend.exec.PlanSessions;
import com.legend.exec.ProductRun;
import com.legend.exec.SessionSource;
import com.legend.json.Json;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.result.Collector;
import com.legend.result.Output;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import com.legend.sql.dialect.RenderedStatement;
import com.legend.sql.dialect.SqlDialect;
import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackends;
import com.legend.testbackend.TestSessions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A {@code data:} PAYLOAD READ BACK ON EVERY DATABASE, BY THE FORM ITS DIALECT ANSWERS (the JSON pass, 2026-11-11: the
 * payload bound as one parameter the database splits, or the statement's own rows on H2; no session table anywhere):
 * through the product door, a payload whose elements hold a quote, a backslash, a line break, nested and typed values,
 * a string element and a null element -- each element read back as the JSON value it is, in the payload's order (a
 * number's text as its database prints it: DuckDB's {@code json_each} prints {@code 1.50} as {@code 1.5}, MySQL's JSON
 * likewise, SQLite a top-level real so, measured), its members read back typed; an empty array no row; and a JSON
 * connection's frame (records with their place) read back on each database by its dialect's own statement. The
 * in-process databases run here; Postgres's matrix test and the server matrix's test call {@link #assertReadsBack}.
 */
public class JsonPayloadFormsTest {

    /** The payload's JSON text: a quote, a backslash, a line break, nested and typed values; a string, a null, a whole
     *  number and a real element. */
    static final String JSON = "[{\"i\":1,\"t\":\"it's a\\\\b\",\"n\":1.50,\"o\":{\"k\":[1,\"x\"]},\"b\":true},"
            + "{\"i\":2,\"t\":\"x\\ny\",\"n\":null,\"o\":{}},\"s\",null,7,2.50]";

    /** String members' JSON text, each escape JSON has: a quote, a backslash, a line break, a tab, a backslash-u escape,
     *  an escaped backslash before a u, an escaped solidus; and the string "null" and a string whose own edges are
     *  quotes (the orchestrator's ask, 2026-11-11: H2 read a member's escapes back as written). */
    static final String STRINGS = "[{\"i\":1,\"t\":\"it's \\\"q\\\"\"},{\"i\":2,\"t\":\"a\\\\b\"},"
            + "{\"i\":3,\"t\":\"x\\ny\"},{\"i\":4,\"t\":\"tab\\there\"},"
            + "{\"i\":5,\"t\":\"\\u00e9t\\u00E9\"},{\"i\":6,\"t\":\"\\\\u0041\"},{\"i\":7,\"t\":\"a\\/b\"},"
            + "{\"i\":8,\"t\":\"null\"},{\"i\":9,\"t\":\"\\\"edge\\\"\"}]";

    /** {@link #STRINGS}' members as their values. */
    static final List<String> STRING_VALUES = List.of("it's \"q\"", "a\\b", "x\ny", "tab\there",
            "\u00e9t\u00e9", "\\u0041", "a/b", "null", "\"edge\"");

    /** The query's text for {@code json} inside a Pure string: a quote and a backslash escaped. */
    static String pure(String json) {
        return json.replace("\\", "\\\\").replace("'", "\\'");
    }

    static String source(String json) {
        return "|sourceUrl('data:application/json," + pure(json) + "')";
    }

    static List<DatabaseType> inProcess() {
        return TestBackends.inProcess().stream().map(b -> b.type()).distinct().toList();
    }

    @ParameterizedTest
    @MethodSource("inProcess")
    void aPayloadReadsBackOnEachInProcessDatabase(DatabaseType db) throws Exception {
        try (Connection c = TestBackends.of(db).open()) {
            assertReadsBack(db, c);
        }
    }

    /** Every check on {@code db}, over the session {@code c}. */
    public static void assertReadsBack(DatabaseType db, Connection c) throws Exception {
        String model = TestBackends.of(db).withStorelessRuntime("");
        // each element, as the JSON value it is, in the payload's order
        List<List<Object>> raw = run(c, model, source(JSON) + "->from(" + StorelessRuntime.RUNTIME + ")");
        List<Object> elements = new ArrayList<>();
        for (List<Object> row : raw) {
            assertTrue(row.get(0) instanceof String, db + ": an element is its JSON text, a JSON null too: " + row);
            elements.add(value(Json.parse((String) row.get(0))));
        }
        List<Object> expected = new ArrayList<>();
        for (Json.Node n : ((Json.Arr) Json.parse(JSON)).items()) {
            expected.add(value(n));
        }
        assertEquals(expected, elements, db + ": the elements, in order: " + raw);
        // the members, typed: an integer, a decimal, a boolean, a JSON null member
        List<List<Object>> typed = run(c, model, source(JSON) + "->extend(~[i: r|cast(get($r.data, 'i'), @Integer),"
                + " n: r|cast(get($r.data, 'n'), @Float), b: r|cast(get($r.data, 'b'), @Boolean)])"
                + "->filter(r|$r.i->isNotEmpty())->select(~[i, n, b])->sort(~i->ascending())->from("
                + StorelessRuntime.RUNTIME + ")");
        assertEquals(List.of(List.of("1", "1.5", "true"), List.of("2", "null", "null")), strings(typed),
                db + ": the members");
        // each string member read back exactly: unquoted and unescaped, as its value
        List<List<Object>> strings = run(c, model, source(STRINGS) + "->extend(~[i: r|cast(get($r.data, 'i'),"
                + " @Integer), t: r|cast(get($r.data, 't'), @String)])->select(~[i, t])->sort(~i->ascending())->from("
                + StorelessRuntime.RUNTIME + ")");
        List<Object> read = new ArrayList<>();
        for (List<Object> row : strings) {
            read.add(row.get(1));
        }
        assertEquals(STRING_VALUES, read, db + ": the string members");
        // an empty array: no row
        assertEquals(List.of(), run(c, model, source("[]") + "->from(" + StorelessRuntime.RUNTIME + ")"),
                db + ": an empty array");
        assertFrameReadsBack(db, c);
        assertModelOrder(db, c);
    }

    /**
     * A PAYLOAD THE DATABASE REFUSES is told under its planned label (the B checkpoint's tenets audit, LOW 4,
     * 2026-11-11: the label is the plan's, the runner writing nothing from the value it binds): a payload's element
     * that is no JSON, on each database that binds the payload, refused naming the payload's digest and size; a raise
     * inside such a statement (pure's own error: a division by zero) is told by its type, its text pure's exactly.
     */
    @ParameterizedTest
    @MethodSource("inProcess")
    void aPayloadsRefusalIsToldUnderItsPlannedLabel(DatabaseType db) throws Exception {
        String model = TestBackends.of(db).withStorelessRuntime("");
        try (Connection c = TestBackends.of(db).open()) {
            String bad = "[{\"t\":nope}]";
            if (!TestBackends.of(db).dialect().deliver(SqlDialect.DataPayload.alone(new SqlSource.SourceUrl(
                    "data:application/json," + bad, "t0", List.of(new OutputCol("data", SqlType.Scalar.JSON, true,
                            OutputCol.Origin.PHYSICAL)), SqlSource.SourceUrl.Form.ELEMENTS), List.of("{\"t\":nope}")))
                    .getClass().equals(SqlSource.SourceUrl.class)) {
                return;   // the payload is the statement's own rows: no slot binds it, the database's text as it is
            }
            String digest = com.legend.cache.Hash.ofUtf8(bad).hex();
            var refused = org.junit.jupiter.api.Assertions.assertThrows(com.legend.error.DataError.class,
                    () -> run(c, model, source(bad) + "->from(" + StorelessRuntime.RUNTIME + ")"));
            assertTrue(refused.getMessage().startsWith("the statement reading the data: payload "
                    + digest.substring(0, 16) + " (" + bad.length() + " characters) was refused: "),
                    db + ": " + refused.getMessage());
            var raised = org.junit.jupiter.api.Assertions.assertThrows(com.legend.error.DataError.class,
                    () -> run(c, model, source("[{\"a\":1}]") + "->extend(~[d: r|1 / (cast(get($r.data, 'a'),"
                            + " @Integer)->toOne() - 1)])->select(~[d])->from(" + StorelessRuntime.RUNTIME + ")"));
            assertTrue(!raised.getMessage().contains("was refused"), db + ": a raise is pure's own text: "
                    + raised.getMessage());
        }
    }

    /** A JSON connection's model on {@code db}'s runtime (the orchestrator's finding, 2026-11-11): its records out of
     *  key order, so an order the database chose on its own would show. */
    static String jsonModel(DatabaseType db) {
        return """
                Class m::Raw { id: Integer[1]; t: String[1]; }
                Class m::P { id: Integer[1]; t: String[1]; }
                ###Mapping
                Mapping m::MM ( m::P: Pure { ~src m::Raw id: $src.id, t: $src.t } )
                ###Relational
                Database m::Db ( )
                ###Connection
                RelationalDatabaseConnection m::Conn { store: m::Db; %s }
                ###Runtime
                Runtime m::RT { mappings: [m::MM]; connections: [ m::Db: [ env: m::Conn ], ModelStore: [ json: #{
                    JsonModelConnection { class: m::Raw; url: 'data:application/json,[{"id":3,"t":"c"},{"id":1,"t":"a"},
                    {"id":5,"t":"e"},{"id":2,"t":"b"},{"id":4,"t":"d"}]'; } }# ] ]; }
                """.formatted(TestBackends.of(db).connectionBody());
    }

    /**
     * The PAYLOAD'S ORDER where a query asks none (the orchestrator's finding of 2026-11-11, the Databricks lane): a
     * JSON connection's model-to-model query, unsorted, answers its records in the payload's order, as legend-engine's
     * JSON reader yields them (the array's order) -- its statement orders by each record's place, so no database's
     * own order decides it; a {@code take} the first records in that order, below the root too (a filter, a union's
     * arm, a grouping above it); a bare {@code sourceUrl} relation too.
     */
    static void assertModelOrder(DatabaseType db, Connection c) throws Exception {
        String model = jsonModel(db);
        com.legend.executionplan.ExecutionPlan plan = Compiler.query(Compiler.compileModel(model),
                "|m::P.all()->project(~[id, t])").executionPlan("m::RT");
        assertTrue(plan.answerSql().statement().contains("ORDER BY") && plan.answerSql().statement().contains(
                SqlSource.SourceUrl.PLACE), db + ": ordered by the place: " + plan.answerSql().statement());
        assertEquals(List.of(List.of("3", "c"), List.of("1", "a"), List.of("5", "e"), List.of("2", "b"),
                List.of("4", "d")), strings(runOn(c, model, "|m::P.all()->project(~[id, t])")), db + ": in order");
        assertEquals(List.of(List.of("3", "c"), List.of("1", "a")),
                strings(runOn(c, model, "|m::P.all()->project(~[id, t])->take(2)")), db + ": the first two");
        // a take, a slice, a drop or a first BELOW the root (the B checkpoint's tenets audit, M1, 2026-11-11): the
        // payload's first records, and the rows above in its order
        assertEquals(List.of(List.of("3", "c"), List.of("1", "a")), strings(runOn(c, model,
                "|m::P.all()->project(~[id, t])->take(2)->filter(r|$r.id < 5)")), db + ": take, then filter");
        assertEquals(List.of(List.of("3", "c"), List.of("1", "a")), strings(runOn(c, model,
                "|m::P.all()->take(2)->project(~[id, t])")), db + ": a class's take");
        assertEquals(List.of(List.of("1", "a"), List.of("5", "e")), strings(runOn(c, model,
                "|m::P.all()->project(~[id, t])->slice(1, 3)->filter(r|$r.id > 0)")), db + ": slice, then filter");
        assertEquals(List.of(List.of("2", "b"), List.of("4", "d")), strings(runOn(c, model,
                "|m::P.all()->project(~[id, t])->drop(3)->filter(r|$r.id > 0)")), db + ": drop, then filter");
        assertEquals(List.of(List.of("3")), strings(runOn(c, model, "|m::P.all()->first()->toOne().id")),
                db + ": the first");
        List<List<String>> arms = new ArrayList<>(strings(runOn(c, model, "|m::P.all()->project(~[id, t])->take(2)"
                + "->concatenate(m::P.all()->project(~[id, t])->drop(3))")));
        arms.sort(java.util.Comparator.comparing(row -> row.get(0)));
        assertEquals(List.of(List.of("1", "a"), List.of("2", "b"), List.of("3", "c"), List.of("4", "d")), arms,
                db + ": a take in a union's arm");
        assertEquals(List.of(List.of("a", "1"), List.of("c", "3"), List.of("e", "5")), strings(runOn(c, model,
                "|m::P.all()->project(~[id, t])->take(3)->groupBy(~[t], ~[c: x|$x.id : y|$y->sum()])"
                        + "->sort(~t->ascending())")), db + ": a take under a grouping");
        assertEquals(List.of(List.of("c"), List.of("a")), strings(runOn(c, model,
                "|m::P.all()->project(~[t])->distinct()->take(2)")), db + ": the first distinct rows");
        String relation = "[{\"a\":2},{\"a\":9},{\"a\":1}]";
        List<Object> bare = new ArrayList<>();
        for (List<Object> row : runOn(c, model, source(relation) + "->from(m::RT)")) {
            bare.add(value(Json.parse((String) row.get(0))));
        }
        List<Object> expected = new ArrayList<>();
        for (Json.Node n : ((Json.Arr) Json.parse(relation)).items()) {
            expected.add(value(n));
        }
        assertEquals(expected, bare, db + ": a bare relation in order");
    }

    private static List<List<Object>> runOn(Connection c, String model, String query) throws Exception {
        Collector collector = new Collector();
        Execution.run(Compiler.query(Compiler.compileModel(model), query), "m::RT",
                SessionSource.given(TestSessions.over(c)), PlanSessions.of(TestSessions.over(c)),
                ProductRun.Establish.DECLARED_SETUP, new Output.Rows(collector));
        return collector.collected().rows();
    }

    /**
     * A JSON connection's FRAME on {@code db}: records and each one's place from 0, read by the statement its dialect
     * writes for a {@code data:} source of records ({@link SqlDialect#deliver}) with its payload bound as the plan binds
     * it, or its rows inline -- the place the payload's order.
     */
    static void assertFrameReadsBack(DatabaseType db, Connection c) throws Exception {
        SqlDialect dialect = TestBackends.of(db).dialect();
        String records = "[{\"t\":\"it's\"},{\"t\":\"a\\nb\"},{\"t\":\"c\"}]";
        List<OutputCol> outputs = List.of(new OutputCol("data", SqlType.Scalar.JSON, true, OutputCol.Origin.DERIVED),
                new OutputCol(SqlSource.SourceUrl.PLACE, SqlType.Scalar.BIGINT, false, OutputCol.Origin.DERIVED));
        SqlSource.SourceUrl url = new SqlSource.SourceUrl("data:application/json," + records, "t0", outputs,
                SqlSource.SourceUrl.Form.RECORDS);
        List<String> cut = List.of("{\"t\":\"it's\"}", "{\"t\":\"a\\nb\"}", "{\"t\":\"c\"}");
        SqlSource read = dialect.deliver(SqlDialect.DataPayload.alone(url, cut));
        RenderedStatement statement = dialect.renderStatement(SqlSelect.starOf(read));
        Map<Long, Object> byPlace = new TreeMap<>();
        try (java.sql.PreparedStatement p = c.prepareStatement(statement.sql())) {
            int i = 1;
            for (RenderedStatement.Bind b : statement.binds()) {
                assertTrue(b.binding() instanceof RenderedStatement.Binding.Payload, db + ": " + b);
                p.setString(i++, ((RenderedStatement.Binding.Payload) b.binding()).json());
            }
            try (java.sql.ResultSet rs = p.executeQuery()) {
                while (rs.next()) {
                    byPlace.put(rs.getLong(SqlSource.SourceUrl.PLACE), value(Json.parse(rs.getString("data"))));
                }
            }
        }
        Map<Long, Object> expected = new TreeMap<>();
        for (int i = 0; i < cut.size(); i++) {
            expected.put((long) i, value(Json.parse(cut.get(i))));
        }
        assertEquals(expected, byPlace, db + ": the frame's records by their place: " + statement.sql());
    }

    /** A JSON value as a comparable Java value: an object's members by name, a number by its value (its text's scale
     *  aside), a null a marker. */
    static Object value(Json.Node n) {
        return switch (n) {
            case Json.Obj o -> {
                Map<String, Object> m = new TreeMap<>();
                o.fields().forEach((k, v) -> m.put(k, value(v)));
                yield m;
            }
            case Json.Arr a -> a.items().stream().map(JsonPayloadFormsTest::value).toList();
            case Json.Str s -> s.value();
            case Json.Num num -> num.token() != null ? new BigDecimal(num.token()).stripTrailingZeros()
                    : num.isInteger() ? BigDecimal.valueOf(num.longValue())
                    : BigDecimal.valueOf(num.doubleValue()).stripTrailingZeros();
            case Json.Bool b -> b.value();
            case Json.Null x -> "<null>";
        };
    }

    private static List<List<Object>> run(Connection c, String model, String query) throws Exception {
        Collector collector = new Collector();
        Execution.run(Compiler.query(Compiler.compileModel(model), query), StorelessRuntime.RUNTIME,
                SessionSource.given(TestSessions.over(c)), PlanSessions.of(TestSessions.over(c)),
                ProductRun.Establish.DECLARED_SETUP, new Output.Rows(collector));
        return collector.collected().rows();
    }

    private static List<List<String>> strings(List<List<Object>> rows) {
        List<List<String>> out = new ArrayList<>();
        for (List<Object> row : rows) {
            out.add(row.stream().map(String::valueOf).toList());
        }
        return out;
    }
}
