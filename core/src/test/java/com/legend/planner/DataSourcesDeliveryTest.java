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

    /** Snowflake: VALUES of the texts (PARSE_JSON is refused inside VALUES), the select over it parsing them -- the
     *  lane's form (a), a quote doubled and a backslash escaped (measured: {@code 'back\\slash'}). */
    @Test
    void snowflakeParsesTheTextsOfAValues() {
        SqlDialect snowflake = dialect(DatabaseType.Snowflake);
        DataSources d = DataSources.of(frame(PAYLOAD), snowflake);
        String sql = snowflake.render(d.query());
        assertTrue(d.steps().isEmpty() && d.content().isEmpty(), "no load: " + d);
        assertTrue(sql.contains("(SELECT PARSE_JSON(column1) AS \"data\", CAST(column2 AS BIGINT) AS \"u_frame_ord__\" FROM VALUES"
                + " ('{\"t\":\"it''s a\\\\\\\\b\"}', 0), ('{\"t\":\"x\\\\ny\"}', 1)) AS \"t0\""), sql);
    }

    /** BigQuery: the rows as an array of typed structs, each text parsed; a quote is {@code \'} there ({@code ''} a
     *  syntax error), a backslash {@code \\}, a line break {@code \n}. */
    @Test
    void bigQueryUnnestsItsTypedRows() {
        SqlDialect bigQuery = dialect(DatabaseType.BigQuery);
        DataSources d = DataSources.of(frame(PAYLOAD), bigQuery);
        String sql = bigQuery.render(d.query());
        assertTrue(d.steps().isEmpty() && d.content().isEmpty(), "no load: " + d);
        assertTrue(sql.contains("UNNEST(ARRAY<STRUCT<"), sql);
        assertTrue(sql.contains("PARSE_JSON('{\"t\":\"it\\'s a\\\\\\\\b\"}')"), sql);
        assertTrue(sql.contains("PARSE_JSON('{\"t\":\"x\\\\ny\"}')"), sql);
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
        for (DatabaseType type : List.of(DatabaseType.Trino, DatabaseType.ClickHouse, DatabaseType.MongoDB,
                DatabaseType.Snowflake, DatabaseType.BigQuery)) {
            SqlDialect dialect = dialect(type);
            assertEquals(SqlDialect.DataDelivery.INLINE, dialect.dataDelivery(), type.name());
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
