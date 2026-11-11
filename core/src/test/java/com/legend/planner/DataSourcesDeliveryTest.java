// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.database.Databases;
import com.legend.executionplan.ExecutionPlan;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import com.legend.sql.dialect.SqlDialect;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A {@code data:} payload ON THE LANES' DATABASES, as their probes measured it (the orchestrator's rulings of
 * 2026-11-11; register S190 withdrawn: every database has a delivery): Snowflake's and BigQuery's the statement's own
 * rows, Databricks' a session table whose literals escape a quote and a backslash with a backslash; and an EMPTY payload
 * on every database that writes its rows into the statement -- the relation's columns and no row.
 */
class DataSourcesDeliveryTest {

    /** A payload whose record holds a quote, a backslash and a line break. */
    private static final String PAYLOAD = "data:application/json,[{\"t\":\"it's a\\\\b\"},{\"t\":\"x\\ny\"}]";

    private static SqlQuery frame(String url) {
        List<OutputCol> outputs = List.of(new OutputCol("data", SqlType.Scalar.JSON, true, OutputCol.Origin.DERIVED),
                new OutputCol(SqlSource.SourceUrl.PLACE, SqlType.Scalar.BIGINT, false, OutputCol.Origin.DERIVED));
        return SqlSelect.starOf(new SqlSource.SourceUrl(url, "t0", outputs, SqlSource.SourceUrl.Form.RECORDS));
    }

    private static SqlDialect dialect(DatabaseType type) {
        return Databases.dialect(type);
    }

    /** A payload refused when it is cut (not one array, not records) leaves NOTHING recorded -- no make, fill or drop
     *  for the next take -- on a database that makes a session table: the cut comes ahead of the make and the release
     *  (the B2a audit's note; the second audit's S9, 2026-11-11: a make with no fill, and its drop twice at the next
     *  take). The next read of a good payload takes its own table's steps alone. */
    @Test
    void aRefusedPayloadRecordsNothing() {
        for (DatabaseType type : List.of(DatabaseType.DuckDB, DatabaseType.H2, DatabaseType.SQLite,
                DatabaseType.Postgres)) {
            DataSources.Tables tables = new DataSources.Tables(dialect(type));
            org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class,
                    () -> tables.read(frame("data:application/json,[{\"t\":1}, 7]")),
                    type + ": a record that is no object");
            DataSources.Tables.Made none = tables.take();
            assertTrue(none.steps().isEmpty() && none.release().isEmpty() && none.drops().isEmpty()
                    && none.content().isEmpty(), type + ": nothing recorded: " + none);
            tables.read(frame(PAYLOAD));
            DataSources.Tables.Made one = tables.take();
            assertEquals(1, one.drops().stream().filter(d -> d.toLowerCase(java.util.Locale.ROOT).contains("drop")).count(),
                    type + ": one table's drop: " + one.drops());
        }
    }

    /** One body's tables are made on ONE database, the first read's: a later read there -- a query's or a write's --
     *  takes the same table, and a read on another database is refused by name (the second audit's S6, 2026-11-11: only
     *  "system or not" was held, and a write's read held nothing). */
    @Test
    void theTablesKeepToTheirHomeDatabase() {
        DataSources.Tables tables = new DataSources.Tables(dialect(DatabaseType.DuckDB));
        ExecutionPlan.Target home = new ExecutionPlan.Target(new ExecutionPlan.Database.System(
                com.legend.executionplan.DatabaseType.DuckDB, "home"), List.of(), List.of());
        ExecutionPlan.Target other = new ExecutionPlan.Target(new ExecutionPlan.Database.System(
                com.legend.executionplan.DatabaseType.DuckDB, "other"), List.of(), List.of());
        tables.read(frame(PAYLOAD), home);
        tables.read(frame(PAYLOAD), home);
        assertEquals(1, tables.take().drops().size(), "one table, read twice at home");
        org.junit.jupiter.api.Assertions.assertThrows(com.legend.error.NotImplementedException.class,
                () -> tables.read(frame(PAYLOAD), other), "a query's read on another database");
        com.legend.sql.SqlDml write = new com.legend.sql.SqlDml.InsertSelect(null, "T", com.legend.sql.NameQuoting.BARE,
                List.of("data", SqlSource.SourceUrl.PLACE), java.util.Set.of(), frame(PAYLOAD));
        org.junit.jupiter.api.Assertions.assertThrows(com.legend.error.NotImplementedException.class,
                () -> tables.read(write, other), "a write's read on another database");
        tables.read(write, home);
    }

    /** Snowflake and BigQuery: the payload as ONE bound parameter the database splits (round 2's measurement, the
     *  user's ruling of 2026-11-11) -- no load, the payload never the statement's text, its one JSON array the bound
     *  value, a frame's place the split's index or offset. */
    @Test
    void snowflakeAndBigQueryBindThePayloadAsOneParameter() {
        String array = PAYLOAD.substring(PAYLOAD.indexOf(',') + 1);
        for (var expected : java.util.Map.of(
                DatabaseType.Snowflake, "SELECT legend_payload.value AS \"data\", CAST(legend_payload.index AS BIGINT)"
                        + " AS \"u_frame_ord__\" FROM TABLE(FLATTEN(INPUT => PARSE_JSON(?))) legend_payload",
                DatabaseType.BigQuery, "SELECT legend_payload AS `data`, legend_place AS `u_frame_ord__` FROM"
                        + " UNNEST(JSON_QUERY_ARRAY(PARSE_JSON(?))) AS legend_payload WITH OFFSET AS legend_place")
                .entrySet()) {
            SqlDialect dialect = dialect(expected.getKey());
            DataSources d = DataSources.of(frame(PAYLOAD), dialect);
            assertTrue(d.steps().isEmpty() && d.content().isEmpty() && d.release().isEmpty(), "no load: " + d);
            com.legend.sql.dialect.RenderedStatement written = dialect.renderStatement(d.query());
            assertTrue(written.sql().contains(expected.getValue()), expected.getKey() + ": " + written.sql());
            assertEquals(List.of(new com.legend.sql.dialect.RenderedStatement.Binding.Payload(array)),
                    written.binds().stream().map(com.legend.sql.dialect.RenderedStatement.Bind::binding).toList(),
                    expected.getKey() + ": the payload is the bound value");
            assertTrue(!written.sql().contains("it's"), expected.getKey() + ": never the text: " + written.sql());
            // an empty payload: the empty array bound, no row (measured)
            for (String url : List.of("data:application/json,[]", "data:application/json,")) {
                com.legend.sql.dialect.RenderedStatement empty = dialect.renderStatement(
                        DataSources.of(frame(url), dialect).query());
                assertEquals(List.of(new com.legend.sql.dialect.RenderedStatement.Binding.Payload("[]")),
                        empty.binds().stream().map(com.legend.sql.dialect.RenderedStatement.Bind::binding).toList(),
                        expected.getKey() + " " + url);
            }
        }
    }

    /** Databricks: the session table made, filled and dropped by name -- single-part, TEMPORARY on every statement,
     *  the elements a VARIANT -- and each element's literal escaping a quote and a backslash with a backslash
     *  ({@code ''} is two literals there, measured). */
    @Test
    void databricksFillsItsTemporaryTable() {
        SqlDialect databricks = dialect(DatabaseType.Databricks);
        DataSources d = DataSources.of(frame(PAYLOAD), databricks);
        List<String> statements = new ArrayList<>();
        for (ExecutionPlan.LoadStep step : d.steps()) {
            if (step instanceof ExecutionPlan.LoadStep.Statement s) {
                statements.add(s.sql());
            }
        }
        String table = statements.get(0).substring("DROP TEMPORARY TABLE IF EXISTS ".length());
        assertTrue(table.matches("legend_data_[0-9a-f]{16}_o"), table);
        assertEquals(List.of("DROP TEMPORARY TABLE IF EXISTS " + table, "CREATE TEMPORARY TABLE " + table
                + " (`data` VARIANT, `u_frame_ord__` INTEGER)"), statements);
        List<String> released = new ArrayList<>();
        for (ExecutionPlan.LoadStep step : d.release()) {
            released.add(((ExecutionPlan.LoadStep.Statement) step).sql());
        }
        assertEquals(List.of("DROP TEMPORARY TABLE IF EXISTS " + table), released);
        List<String> inserts = new ArrayList<>();
        for (String blob : d.content().values()) {
            inserts.addAll(com.legend.executionplan.DataRef.Statements.decode(blob));
        }
        assertEquals(1, inserts.size(), String.valueOf(inserts));
        assertTrue(inserts.get(0).contains("parse_json('{\"t\":\"it\\'s a\\\\\\\\b\"}')"), inserts.get(0));
        assertTrue(inserts.get(0).contains("parse_json('{\"t\":\"x\\\\ny\"}')"), inserts.get(0));
        assertTrue(databricks.render(d.query()).contains(table), "the query reads the table");
    }

    /** An empty payload on each database that writes the rows into the statement: its columns, no row -- one typed
     *  NULL row removed by WHERE FALSE (the lowering's empty TDS literal's shape); never refused, never a load. */
    @Test
    void anEmptyPayloadIsNoRowOnEveryInlineDatabase() {
        for (DatabaseType type : List.of(DatabaseType.Trino, DatabaseType.ClickHouse, DatabaseType.MongoDB)) {
            SqlDialect dialect = dialect(type);
            assertEquals(com.legend.sql.dialect.DataDeliveryDeclaredTest.Kind.INLINE,
                    com.legend.sql.dialect.DataDeliveryDeclaredTest.kindOf(dialect), type.name());
            for (String url : List.of("data:application/json,[]", "data:application/json,")) {
                DataSources d = DataSources.of(frame(url), dialect);
                assertTrue(d.steps().isEmpty() && d.content().isEmpty(), type + ": no load: " + d);
                String sql = dialect.render(d.query());
                assertTrue(type == DatabaseType.MongoDB ? sql.contains("$match") : sql.contains("WHERE FALSE")
                        || sql.contains("WHERE false") || sql.contains("WHERE 1 = 0"), type + ": " + sql);
            }
        }
    }
}
