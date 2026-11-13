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
            // the create carries the data: the database's own create of the rows -- the rows' statement, planned -- then
            // the made table's rows counted
            com.legend.json.Json.Obj sources = com.legend.json.Json.parseObject(planner.Boundary.tableModel(
                    com.legend.json.Json.toCompact(Map.of("databaseType", "DuckDB", "tables",
                            request(c, "trades", "out", "create", FX, null, false).get("tables")))));
            String rowsStatement = com.legend.Compiler.query(com.legend.Compiler.compileModel(
                    sources.getString("model")), FX).plan(sources.getString("runtime")).sql();
            assertEquals(2, made.size(), String.valueOf(made));
            assertEquals("CREATE TABLE \"out\" AS " + rowsStatement, made.get(0).statement());
            assertEquals("SELECT COUNT(*) AS value\nFROM \"out\" AS t0", made.get(1).statement());
            ExecutionPlan overwritten = plan(request(c, "trades", "out", "overwrite", FX, null, false));
            List<String> remade = sqls(overwritten).stream().map(ExecutionPlan.Sql::statement).toList();
            assertEquals(3, remade.size(), String.valueOf(remade));
            assertTrue(remade.get(0).toUpperCase(java.util.Locale.ROOT).startsWith("DROP TABLE IF EXISTS \"OUT\""),
                    remade.get(0));
            assertEquals(List.of(made.get(0).statement(), made.get(1).statement()), remade.subList(1, 3));
            assertEquals(2, run(c, created), "the made table holds the box's rows, counted");
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
            // a made table's answer is a write's: one row, one column value, the same facts
            List<ExecutionPlan.Column> writes = ((ExecutionPlan.TdsResult) ((ExecutionPlan.Sequence) written.get(2)
                    .root()).steps().stream().filter(x -> x instanceof ExecutionPlan.TdsResult).findFirst()
                    .orElseThrow()).columns();
            for (ExecutionPlan p : written.subList(0, 2)) {
                assertEquals(writes, ((ExecutionPlan.TdsResult) ((ExecutionPlan.Sequence) p.root()).steps().stream()
                        .filter(x -> x instanceof ExecutionPlan.TdsResult).findFirst().orElseThrow()).columns());
            }
            // what a mode that writes a table there does not make
            for (ExecutionPlan p : written.subList(2, 5)) {
                assertTrue(sqls(p).stream().noneMatch(s -> s.statement().startsWith("CREATE TABLE")
                        || s.statement().startsWith("DROP TABLE")), String.valueOf(sqls(p)));
            }
        }
    }

    /** A made table's columns are the database's types of its rows -- its own create of them: a passed-through
     *  INTEGER an INTEGER, a VARCHAR, a DECIMAL(10,2), a DATE, and computed ones as the database computes them -- every
     *  name as the rows name it. */
    @Test
    void aMadeTablesColumnsAreTheDatabasesTypesOfTheRows() throws Exception {
        try (Connection c = withTrades(); Statement s = c.createStatement()) {
            String rows = "|#>{local::DB.trades}#->extend(~[f: r|1.5, b: r|true, t: r|%2024-01-01T10:00:00])";
            assertEquals(Map.of(), changes(c, s, "trades", "typed", rows), "the database's types, as its own create");
            Map<String, String> types = new LinkedHashMap<>();
            for (Map<String, Object> col : catalog(c, "made_typed")) {
                types.put((String) col.get("name"), (String) col.get("dataType"));
            }
            assertEquals(List.of("id", "desk", "amount", "day", "f", "b", "t"), List.copyOf(types.keySet()));
            assertEquals(List.of("INTEGER", "VARCHAR", "DECIMAL(10,2)", "DATE"),
                    List.copyOf(types.values()).subList(0, 4));
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
     * NO TYPE CHANGES, PINNED (the orchestrator's ruling (e), 2026-11-12: the made table is the database's own create
     * of the rows, none of them): for every DuckDB type a box's source column can hold, and a sum, the type a made
     * table holds is the type today's {@code CREATE ... AS <rows> LIMIT 0} made -- so a regression to a type map fails.
     */
    @Test
    void theCreatedTypesBesideTodaysCreate() throws Exception {
        try (Connection c = withTrades(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE every (v VARCHAR, b BOOLEAN, i8 TINYINT, i16 SMALLINT, i32 INTEGER, i64 BIGINT,"
                    + " u8 UTINYINT, u16 USMALLINT, u32 UINTEGER, u64 UBIGINT, i128 HUGEINT, f32 FLOAT, f64 DOUBLE,"
                    + " d DECIMAL(9,2), dt DATE, ts TIMESTAMP, tss TIMESTAMP_S, tsms TIMESTAMP_MS, tsns TIMESTAMP_NS,"
                    + " tstz TIMESTAMP WITH TIME ZONE, j JSON, st STRUCT(a INTEGER, b VARCHAR), l INTEGER[],"
                    + " m MAP(VARCHAR, INTEGER), tm TIME, u UUID, iv INTERVAL, e ENUM('a', 'b'))");
            Map<String, String> changed = new java.util.TreeMap<>();
            changed.putAll(changes(c, s, "every", "every", "|#>{local::DB.every}#"));
            changed.putAll(changes(c, s, "trades", "sum", "|#>{local::DB.trades}#->groupBy(~[desk], ~[total: r|$r.id"
                    + " : y|$y->plus()])"));
            changed.putAll(changes(c, s, "trades", "dsum", DECIMAL_SUM));
            // measured 2026-11-12: until the ruling, a map of the rows' Pure types made 14 of them differ (the smaller
            // integers, FLOAT, the coarser and the nanosecond timestamps, a zoned one, UBIGINT, HUGEINT, a sum's HUGEINT)
            assertEquals(Map.of(), changed, "the made types beside today's");
        }
    }

    /** A sum of decimals: a plain Decimal, of no stated precision and scale. */
    private static final String DECIMAL_SUM = "|#>{local::DB.trades}#->groupBy(~[desk], ~[total: r|$r.amount :"
            + " y|$y->plus()])";

    /** A computed plain-Decimal column, which a declared column could not name, is made as the database types it (the
     *  orchestrator's proposal, 2026-11-12: the create carries the data, so no column is declared or refused). */
    @Test
    void aPlainDecimalSumIsMade() throws Exception {
        try (Connection c = withTrades()) {
            Map<String, Object> request = request(c, "trades", "totals", "create", DECIMAL_SUM, null, false);
            com.legend.json.Json.Obj sources = com.legend.json.Json.parseObject(planner.Boundary.tableModel(
                    com.legend.json.Json.toCompact(Map.of("databaseType", "DuckDB", "tables", request.get("tables")))));
            com.legend.compiler.element.type.Type total = com.legend.plan.UpstreamRelationType.columns(
                    com.legend.Compiler.query(com.legend.Compiler.compileModel(sources.getString("model")), DECIMAL_SUM)
                            .resultType()).get(1).type();
            assertTrue(!(total instanceof com.legend.compiler.element.type.Type.PrecisionDecimal)
                    && com.legend.plan.UpstreamRelationType.typePath(total).equals("Decimal"), String.valueOf(total));
            assertEquals(2, run(c, plan(request)));
            assertEquals(List.of(List.of("FX", "15.50"), List.of("Rates", "20.25")),
                    rows(c, "SELECT desk, total FROM totals ORDER BY desk"));
        }
    }

    /** {@code rows} over {@code source}, made as {@code made_<label>} beside today's {@code today_<label>}: each column
     *  whose made type differs from today's, {@code today -> made}. */
    private static Map<String, String> changes(Connection c, Statement s, String source, String label, String rows)
            throws Exception {
        Map<String, Object> request = request(c, source, "made_" + label, "create", rows, null, false);
        run(c, plan(request));
        // today's: the database's own CREATE of the rows' statement, LIMIT 0 (the host operation a query fed)
        com.legend.json.Json.Obj model = com.legend.json.Json.parseObject(planner.Boundary.tableModel(
                com.legend.json.Json.toCompact(Map.of("databaseType", "DuckDB", "tables", request.get("tables")))));
        String sql = com.legend.Compiler.query(com.legend.Compiler.compileModel(model.getString("model")),
                rows + "->limit(0)").plan(model.getString("runtime")).sql();
        s.execute("CREATE TABLE today_" + label + " AS " + sql);
        Map<String, String> today = new LinkedHashMap<>();
        for (Map<String, Object> col : catalog(c, "today_" + label)) {
            today.put(((String) col.get("name")).toLowerCase(java.util.Locale.ROOT), (String) col.get("dataType"));
        }
        Map<String, String> out = new LinkedHashMap<>();
        for (Map<String, Object> col : catalog(c, "made_" + label)) {
            String was = today.get(((String) col.get("name")).toLowerCase(java.util.Locale.ROOT));
            if (!col.get("dataType").equals(was)) {
                out.put(label + "." + col.get("name"), was + " -> " + col.get("dataType"));
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
