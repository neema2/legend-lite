// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.exec.PlanRunner;
import com.legend.exec.PlanSessions;
import com.legend.executionplan.ExecutionPlan;
import com.legend.executionplan.PlanJson;
import com.legend.sql.dialect.DuckDb;
import com.legend.testbackend.TestSessions;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

/**
 * A TABLE OUTPUT, AS ONE PLAN (the final audit's item 2, 2026-11-12; {@link OutputPlan}, through the hosts' boundary,
 * {@code Boundary.outputPlan}): each mode's plan -- the table's DDL where the Output makes it, then the write the planner
 * plans -- one writing sequence whose every statement carries DuckDB's transaction; a created table's columns the
 * dialect's mapping of the rows' relation type; each mode run on DuckDB, its count the write's; the types a created
 * table holds beside those today's {@code CREATE ... AS rows LIMIT 0} made, pinned; every refusal by name.
 */
class DuckDbOutputPlanTest {

    /** The box's one table, as its fixture makes it. */
    private static Connection withTrades() throws SQLException {
        Connection c = com.legend.exec.Connectors.of(ConnectionPlans.type(
                com.legend.model.ConnectionDefinition.DatabaseType.DuckDB)).openPrivate(null);
        try (Statement s = c.createStatement()) {
            s.execute("CREATE TABLE trades (id INTEGER, desk VARCHAR, amount DECIMAL(10,2), day DATE)");
            s.execute("INSERT INTO trades VALUES (1, 'FX', 10.50, DATE '2024-01-01'),"
                    + " (2, 'Rates', 20.25, DATE '2024-01-02'), (3, 'FX', 5.00, DATE '2024-01-03')");
        }
        return c;
    }

    /** {@code table}'s catalog rows, as the platform's catalog question answers them (what a host reads). */
    private static List<Map<String, Object>> catalog(Connection c, String table) throws SQLException {
        List<Map<String, Object>> out = new ArrayList<>();
        try (Statement s = c.createStatement();
                ResultSet r = s.executeQuery(planner.Boundary.catalogColumnsSql("main", table))) {
            while (r.next()) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("name", r.getString(1));
                m.put("dataType", r.getString(2));
                if (r.getString(3) != null) {
                    m.put("logicalType", r.getString(3));
                }
                if (r.getObject(4) != null) {
                    m.put("precision", r.getInt(4));
                }
                if (r.getObject(5) != null) {
                    m.put("scale", r.getInt(5));
                }
                m.put("notNull", r.getBoolean(6));
                out.add(m);
            }
        }
        return out;
    }

    private static Map<String, Object> request(Connection c, String source, String name, String mode, String rows,
            List<String> keys, boolean target) throws SQLException {
        Map<String, Object> table = new LinkedHashMap<>();
        table.put("table", source);
        table.put("convertible", true);
        table.put("columns", catalog(c, source));
        Map<String, Object> output = new LinkedHashMap<>();
        output.put("name", name);
        output.put("mode", mode);
        if (keys != null) {
            output.put("keys", keys);
        }
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("databaseType", "DuckDB");
        r.put("tables", List.of(table));
        r.put("output", output);
        r.put("rows", com.legend.parser.SpecParser.lambdaJson(rows, false));
        if (target) {
            r.put("target", catalog(c, name));
        }
        return r;
    }

    private static ExecutionPlan plan(Map<String, Object> request) {
        String json = planner.Boundary.outputPlan(com.legend.json.Json.toCompact(request));
        ExecutionPlan plan = PlanJson.read(json);
        assertEquals(json, PlanJson.write(plan), "the plan survives its JSON");
        return plan;
    }

    private static List<ExecutionPlan.Sql> sqls(ExecutionPlan plan) {
        List<ExecutionPlan.Sql> out = new ArrayList<>();
        for (ExecutionPlan.Node step : ((ExecutionPlan.Sequence) plan.root()).steps()) {
            out.add(switch (step) {
                case ExecutionPlan.Effect e -> e.sql();
                case ExecutionPlan.Update u -> u.sql();
                case ExecutionPlan.TdsResult t -> t.sql();
                default -> throw new AssertionError("an output's step: " + step);
            });
        }
        return out;
    }

    /** The count {@code plan} answers, run by Java's exec on {@code c}. */
    private static long run(Connection c, ExecutionPlan plan) throws Exception {
        ExecutionPlan.Target target = sqls(plan).get(0).target();
        PlanRunner.Rows rows = PlanRunner.rows(plan, Map.of(), PlanSessions.acquired(TestSessions.over(c), target),
                java.util.UUID.randomUUID());
        assertEquals(1, rows.rows().size(), String.valueOf(rows.rows()));
        return ((Number) rows.rows().get(0).get(0)).longValue();
    }

    private static List<List<String>> rows(Connection c, String sql) throws SQLException {
        List<List<String>> out = new ArrayList<>();
        try (Statement s = c.createStatement(); ResultSet r = s.executeQuery(sql)) {
            int n = r.getMetaData().getColumnCount();
            while (r.next()) {
                List<String> row = new ArrayList<>();
                for (int i = 1; i <= n; i++) {
                    row.add(r.getString(i));
                }
                out.add(row);
            }
        }
        return out;
    }

    private static final String FX = "|#>{local::DB.trades}#->filter(r|$r.desk == 'FX')";

    /** Each mode's plan: the DDL where the Output makes its table (a drop first where it overwrites), then the write
     *  the planner plans, its answer one count; every statement on one target carrying DuckDB's transaction. */
    @Test
    void eachModesPlanIsOneWritingSequenceInTheDatabasesTransaction() throws Exception {
        ExecutionPlan.Transaction duck = Targets.of(new ExecutionPlan.Database.Platform(
                com.legend.executionplan.DatabaseType.DuckDB, null), new DuckDb()).transaction();
        try (Connection c = withTrades()) {
            ExecutionPlan created = plan(request(c, "trades", "out", "create", FX, null, false));
            List<ExecutionPlan.Sql> made = sqls(created);
            // every name delimited, as the rows name it: the made table's columns are those the write reads
            assertEquals("Create Table \"out\"(\"id\" BIGINT NULL, \"desk\" VARCHAR(4096) NULL,"
                    + " \"amount\" DECIMAL(10, 2) NULL, \"day\" DATE NULL);", made.get(0).statement());
            ExecutionPlan overwritten = plan(request(c, "trades", "out", "overwrite", FX, null, false));
            List<String> remade = sqls(overwritten).subList(0, 2).stream().map(ExecutionPlan.Sql::statement).toList();
            assertTrue(remade.get(0).toUpperCase(java.util.Locale.ROOT).startsWith("DROP TABLE IF EXISTS \"OUT\""),
                    remade.get(0));
            assertEquals(made.get(0).statement(), remade.get(1));
            assertEquals(1, run(c, created) - 1, "the made table holds the box's rows");
            List<ExecutionPlan> written = List.of(created, overwritten,
                    plan(request(c, "trades", "out", "append", FX, null, true)),
                    plan(request(c, "trades", "out", "deleteAppend", FX, null, true)),
                    plan(request(c, "trades", "out", "merge", FX, List.of("id"), true)));
            for (ExecutionPlan p : written) {
                assertTrue(p.writes());
                long results = ((ExecutionPlan.Sequence) p.root()).steps().stream()
                        .filter(s -> s instanceof ExecutionPlan.TdsResult).count();
                assertEquals(1, results, "one answer: the write's count");
                ExecutionPlan.Target one = sqls(p).get(0).target();
                for (ExecutionPlan.Sql sql : sqls(p)) {
                    assertEquals(one, sql.target(), "every statement on the write's target");
                    assertEquals(duck, sql.target().transaction(), "DuckDB's transaction, on every statement");
                }
            }
            // what a mode that writes a table there does not make
            for (ExecutionPlan p : written.subList(2, 5)) {
                assertTrue(sqls(p).stream().noneMatch(s -> s.statement().startsWith("CREATE TABLE")
                        || s.statement().startsWith("DROP TABLE")), String.valueOf(sqls(p)));
            }
        }
    }

    /** A created table's columns are the dialect's mapping of the rows' relation type: an Integer a BIGINT, a String
     *  a VARCHAR, a Decimal its own DECIMAL, a StrictDate a DATE, a Float a DOUBLE, a Boolean a BOOLEAN, a DateTime a
     *  TIMESTAMP -- as the made table's catalog answers them. */
    @Test
    void aCreatedTablesColumnsAreTheDialectsMappingOfTheRowsRelationType() throws Exception {
        try (Connection c = withTrades()) {
            run(c, plan(request(c, "trades", "typed", "create", "|#>{local::DB.trades}#->extend(~[f: r|1.5, b: r|true,"
                    + " t: r|%2024-01-01T10:00:00])", null, false)));
            Map<String, String> types = new LinkedHashMap<>();
            for (Map<String, Object> col : catalog(c, "typed")) {
                types.put((String) col.get("name"), (String) col.get("dataType"));
            }
            assertEquals(Map.of("id", "BIGINT", "desk", "VARCHAR", "amount", "DECIMAL(10,2)", "day", "DATE",
                    "f", "DOUBLE", "b", "BOOLEAN", "t", "TIMESTAMP"), types);
        }
    }

    /** Each mode run on DuckDB, its count the write's: create writes the box's rows into a new table; append adds them;
     *  deleteAppend replaces the table's rows with them; merge matches them by their keys. */
    @Test
    void eachModeRunOnDuckDbAnswersItsCount() throws Exception {
        try (Connection c = withTrades()) {
            assertEquals(2, run(c, plan(request(c, "trades", "out", "create", FX, null, false))));
            assertEquals(List.of(List.of("1", "FX"), List.of("3", "FX")), rows(c, "SELECT id, desk FROM out ORDER BY id"));
            assertEquals(2, run(c, plan(request(c, "trades", "out", "append", FX, null, true))));
            assertEquals(List.of(List.of("4")), rows(c, "SELECT count(*) FROM out"));
            assertEquals(2, run(c, plan(request(c, "trades", "out", "deleteAppend", FX, null, true))));
            assertEquals(List.of(List.of("2")), rows(c, "SELECT count(*) FROM out"));
            assertEquals(3, run(c, plan(request(c, "trades", "out", "merge", "|#>{local::DB.trades}#", List.of("id"),
                    true))));
            assertEquals(List.of(List.of("1", "FX"), List.of("2", "Rates"), List.of("3", "FX")),
                    rows(c, "SELECT id, desk FROM out ORDER BY id"));
            assertEquals(3, run(c, plan(request(c, "trades", "out", "overwrite", "|#>{local::DB.trades}#", null,
                    false))));
            assertEquals(List.of(List.of("3")), rows(c, "SELECT count(*) FROM out"));
        }
    }

    /**
     * THE BEHAVIOUR CHANGE, PINNED (the orchestrator's ruling of 2026-11-12: widening accepted, any narrowing or
     * precision loss ruled before it lands): for every DuckDB type a box's source column can hold, and a sum, the type
     * a created table holds beside the type today's {@code CREATE ... AS <rows> LIMIT 0} made -- each pair where they
     * differ.
     */
    @Test
    void theCreatedTypesBesideTodaysCreate() throws Exception {
        try (Connection c = withTrades(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE every (v VARCHAR, b BOOLEAN, i8 TINYINT, i16 SMALLINT, i32 INTEGER, i64 BIGINT,"
                    + " u8 UTINYINT, u16 USMALLINT, u32 UINTEGER, u64 UBIGINT, i128 HUGEINT, f32 FLOAT, f64 DOUBLE,"
                    + " d DECIMAL(9,2), dt DATE, ts TIMESTAMP, tss TIMESTAMP_S, tsms TIMESTAMP_MS, tsns TIMESTAMP_NS,"
                    + " tstz TIMESTAMP WITH TIME ZONE, j JSON, st STRUCT(a INTEGER, b VARCHAR), l INTEGER[],"
                    + " m MAP(VARCHAR, INTEGER), tm TIME, u UUID, iv INTERVAL, e ENUM('a', 'b'))");
            Map<String, String> changed = new TreeMap<>();
            changed.putAll(changes(c, s, "every", "|#>{local::DB.every}#"));
            changed.putAll(changes(c, s, "trades", "|#>{local::DB.trades}#->groupBy(~[desk], ~[total: r|$r.id :"
                    + " y|$y->plus()])"));
            // measured 2026-11-12 on the DuckDB lite builds with; every other type is today's (VARCHAR, BOOLEAN,
            // BIGINT, DOUBLE, DECIMAL(9,2), DATE, TIMESTAMP, a JSON, STRUCT, LIST or MAP as JSON, a TIME, UUID, INTERVAL
            // or ENUM as VARCHAR). Widening, accepted: the smaller integers, FLOAT, the coarser timestamps, UBIGINT.
            // Narrowing or precision lost, put to the orchestrator before it lands: TIMESTAMP_NS (its nanoseconds),
            // HUGEINT and an integer sum's HUGEINT (their range beyond DECIMAL(38,0) and BIGINT: the database refuses a
            // value out of range); a zoned timestamp kept as its UTC wall time.
            assertEquals(Map.ofEntries(
                    Map.entry("every.f32", "FLOAT -> DOUBLE"),
                    Map.entry("every.i128", "HUGEINT -> DECIMAL(38,0)"),
                    Map.entry("every.i16", "SMALLINT -> BIGINT"),
                    Map.entry("every.i32", "INTEGER -> BIGINT"),
                    Map.entry("every.i8", "TINYINT -> BIGINT"),
                    Map.entry("every.tsms", "TIMESTAMP_MS -> TIMESTAMP"),
                    Map.entry("every.tsns", "TIMESTAMP_NS -> TIMESTAMP"),
                    Map.entry("every.tss", "TIMESTAMP_S -> TIMESTAMP"),
                    Map.entry("every.tstz", "TIMESTAMP WITH TIME ZONE -> TIMESTAMP"),
                    Map.entry("every.u16", "USMALLINT -> BIGINT"),
                    Map.entry("every.u32", "UINTEGER -> BIGINT"),
                    Map.entry("every.u64", "UBIGINT -> DECIMAL(20,0)"),
                    Map.entry("every.u8", "UTINYINT -> BIGINT"),
                    Map.entry("trades.total", "HUGEINT -> BIGINT")), changed, "the created types beside today's");
        }
    }

    /** {@code rows} over {@code source}: each column whose created type differs from today's, {@code today -> made}. */
    private static Map<String, String> changes(Connection c, Statement s, String source, String rows) throws Exception {
        Map<String, Object> request = request(c, source, "made_" + source, "create", rows, null, false);
        run(c, plan(request));
        // today's: the database's own CREATE of the rows' statement, LIMIT 0 (the host operation a query fed)
        com.legend.json.Json.Obj model = com.legend.json.Json.parseObject(planner.Boundary.tableModel(
                com.legend.json.Json.toCompact(Map.of("databaseType", "DuckDB", "tables", request.get("tables")))));
        String sql = com.legend.Compiler.query(com.legend.Compiler.compileModel(model.getString("model")),
                rows + "->limit(0)").plan(model.getString("runtime")).sql();
        s.execute("CREATE TABLE today_" + source + " AS " + sql);
        Map<String, String> today = new LinkedHashMap<>();
        for (Map<String, Object> col : catalog(c, "today_" + source)) {
            today.put(((String) col.get("name")).toLowerCase(java.util.Locale.ROOT), (String) col.get("dataType"));
        }
        Map<String, String> out = new LinkedHashMap<>();
        for (Map<String, Object> col : catalog(c, "made_" + source)) {
            String was = today.get(((String) col.get("name")).toLowerCase(java.util.Locale.ROOT));
            if (!col.get("dataType").equals(was)) {
                out.put(source + "." + col.get("name"), was + " -> " + col.get("dataType"));
            }
        }
        return out;
    }

    /** Every refusal by name: a statement, a shape or any other key; a mode not named; keys beside a mode not a
     *  merge's, and a merge with none; a target where the Output makes the table, and none where it writes one there;
     *  rows with a parameter or more than one body; an output named for one of the box's own tables. */
    @Test
    void eachRefusalIsByName() throws Exception {
        try (Connection c = withTrades()) {
            Map<String, Object> withSql = request(c, "trades", "out", "create", FX, null, false);
            withSql.put("sql", "SELECT 1");
            assertEquals("a table output has no key sql (names and fields only, never a value)", refused(withSql));
            Map<String, Object> withShape = request(c, "trades", "out", "create", FX, null, false);
            withShape.put("shape", "SELECT 1");
            assertEquals("a table output has no key shape (names and fields only, never a value)", refused(withShape));
            assertEquals("an output's mode is create, overwrite, append, deleteAppend or merge, not replace",
                    refused(request(c, "trades", "out", "replace", FX, null, false)));
            assertEquals("an output's keys are a merge's: append takes none",
                    refused(request(c, "trades", "trades_copy", "append", FX, List.of("id"), false)));
            assertEquals("a merge names its keys: the columns a row is matched by",
                    refused(request(c, "trades", "out", "merge", FX, List.of(), false)));
            Map<String, Object> createWithTarget = request(c, "trades", "out", "create", FX, null, false);
            createWithTarget.put("target", List.of());
            assertEquals("a created table's columns are its rows' relation type: the request names no target",
                    refused(createWithTarget));
            assertEquals("this page's database has no table 'out': write it with Create a new table first",
                    refused(request(c, "trades", "out", "append", FX, null, false)));
            assertEquals("an output's rows are one query of no parameter", refused(request(c, "trades", "out",
                    "create", "{n: Integer[1]|#>{local::DB.trades}#->filter(r|$r.id > $n)}", null, false)));
            assertEquals("an output's rows are one query of no parameter", refused(request(c, "trades", "out",
                    "create", "{|#>{local::DB.trades}#; #>{local::DB.trades}#;}", null, false)));
            assertEquals("'TRADES' is one of this page's own tables (a file's, a copy's): name the output's table"
                    + " another way", refused(request(c, "trades", "TRADES", "create", FX, null, false)));
        }
    }

    private static String refused(Map<String, Object> request) {
        return assertThrows(IllegalArgumentException.class, () -> plan(request)).getMessage();
    }
}
