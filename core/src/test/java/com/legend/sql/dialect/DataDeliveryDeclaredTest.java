// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlDdl;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * THE INLINE FORM'S BOUND (census M18, the user's ruling of 2026-11-11: "we need to make json work everywhere";
 * docs/DATA_DELIVERY_DESIGN_2026_11_10.md §6): how each database takes a {@code data:} payload is its dialect's
 * declaration ({@link SqlDialect#dataDelivery}), measured, and the payload written into the statement's own rows is
 * BOUNDED to the databases that hold no session table of a load's -- never a default, never a fallback from a session
 * table that failed. A database moves between the lists only with its measurement (a probe in
 * docs/data-delivery-homework/probes, or a lane's probe the orchestrator relays): a new INLINE one must show it holds no
 * session table a plan can make and drop safely (Snowflake, BigQuery: their lanes' probes of 2026-11-11). Every database
 * has a delivery: none refuses a {@code data:} payload (register S190 withdrawn, 2026-11-11).
 */
class DataDeliveryDeclaredTest {

    private static final List<SqlDdl.Column> COLUMNS = List.of(new SqlDdl.Column("data", false,
            new SqlDdl.ColumnType.Plain(SqlDdl.ColumnType.Kind.JSON), false, false));

    @Test
    void eachDatabasesDeliveryIsItsMeasuredOne() {
        Map<String, SqlDialect.DataDelivery> declared = new TreeMap<>();
        for (com.legend.model.ConnectionDefinition.DatabaseType type
                : com.legend.model.ConnectionDefinition.DatabaseType.values()) {
            SqlDialect dialect;
            try {
                dialect = com.legend.database.Databases.dialect(type);
            } catch (com.legend.error.NotImplementedException none) {
                continue;   // no dialect at all: no query reaches the database
            }
            declared.put(type.name(), dialect.dataDelivery());
        }
        assertEquals(Map.ofEntries(
                Map.entry("DuckDB", SqlDialect.DataDelivery.SESSION_TABLE),
                Map.entry("H2", SqlDialect.DataDelivery.SESSION_TABLE),
                Map.entry("SQLite", SqlDialect.DataDelivery.SESSION_TABLE),
                Map.entry("Postgres", SqlDialect.DataDelivery.SESSION_TABLE),
                Map.entry("MariaDB", SqlDialect.DataDelivery.SESSION_TABLE),
                Map.entry("MySQL", SqlDialect.DataDelivery.SESSION_TABLE),
                Map.entry("Trino", SqlDialect.DataDelivery.INLINE),
                Map.entry("ClickHouse", SqlDialect.DataDelivery.INLINE),
                Map.entry("MongoDB", SqlDialect.DataDelivery.INLINE),
                Map.entry("Snowflake", SqlDialect.DataDelivery.INLINE),
                Map.entry("Databricks", SqlDialect.DataDelivery.SESSION_TABLE),
                Map.entry("BigQuery", SqlDialect.DataDelivery.INLINE)), new java.util.HashMap<>(declared),
                "each database's data: delivery, as measured (a change is a measurement and this list's edit)");
    }

    /** An unread {@code data:} source -- one the planner did not turn into its delivery -- reaching any renderer is the
     *  planner's defect, refused by one type and message on every database (the second audit's S8, 2026-11-11:
     *  MongoDB's pipeline raised a capability refusal where the SQL dialects raise the defect). */
    @Test
    void anUnreadDataSourceIsOneRefusalOnEveryDatabase() {
        List<com.legend.sql.OutputCol> outputs = List.of(new com.legend.sql.OutputCol("data",
                com.legend.sql.SqlType.Scalar.JSON, true, com.legend.sql.OutputCol.Origin.DERIVED));
        com.legend.sql.SqlQuery unread = com.legend.sql.SqlSelect.starOf(new com.legend.sql.SqlSource.SourceUrl(
                "data:application/json,[1]", "t0", outputs, com.legend.sql.SqlSource.SourceUrl.Form.ELEMENTS));
        int rendered = 0;
        for (com.legend.model.ConnectionDefinition.DatabaseType type
                : com.legend.model.ConnectionDefinition.DatabaseType.values()) {
            SqlDialect dialect;
            try {
                dialect = com.legend.database.Databases.dialect(type);
            } catch (com.legend.error.NotImplementedException none) {
                continue;
            }
            IllegalStateException refused = assertThrows(IllegalStateException.class, () -> dialect.render(unread),
                    type + ": an unread data: source is the planner's defect");
            assertEquals(true, refused.getMessage().contains("reached a render unread"), type + ": " + refused);
            rendered++;
        }
        assertEquals(true, rendered >= 12, "every database's renderer asked: " + rendered);
    }

    @Test
    void aSessionTableIsSpelledExactlyWhereItIsDeclared() {
        for (com.legend.model.ConnectionDefinition.DatabaseType type
                : com.legend.model.ConnectionDefinition.DatabaseType.values()) {
            SqlDialect dialect;
            try {
                dialect = com.legend.database.Databases.dialect(type);
            } catch (com.legend.error.NotImplementedException none) {
                continue;
            }
            if (dialect.dataDelivery() == SqlDialect.DataDelivery.SESSION_TABLE) {
                SqlDialect.DataTable table = dialect.dataTable("legend_data_0123456789abcdef", COLUMNS);
                assertEquals(false, table.make().isEmpty() || table.release().isEmpty(), type + ": " + table);
            } else {
                assertThrows(com.legend.sql.DialectCapability.class,
                        () -> dialect.dataTable("legend_data_0123456789abcdef", COLUMNS),
                        type + " declares no session table, and spells none");
            }
        }
    }

    /** On a database that writes the payload as rows, a JSON connection's frame (its RECORDS form) is a row per record
     *  -- the record's JSON text parsed by the database, its place beside it from 0 -- with no load at all. */
    @Test
    void aFrameOnTrinoIsItsRecordsAsRowsWithTheirPlaces() {
        SqlDialect trino = com.legend.database.Databases.dialect(
                com.legend.model.ConnectionDefinition.DatabaseType.Trino);
        List<com.legend.sql.OutputCol> outputs = List.of(
                new com.legend.sql.OutputCol("data", com.legend.sql.SqlType.Scalar.JSON, true,
                        com.legend.sql.OutputCol.Origin.DERIVED),
                new com.legend.sql.OutputCol(com.legend.sql.SqlSource.SourceUrl.PLACE,
                        com.legend.sql.SqlType.Scalar.BIGINT, false, com.legend.sql.OutputCol.Origin.DERIVED));
        com.legend.sql.SqlQuery q = com.legend.sql.SqlSelect.starOf(new com.legend.sql.SqlSource.SourceUrl(
                "data:application/json,[{\"a\":1},{\"a\":\"it's\"}]", "t0", outputs,
                com.legend.sql.SqlSource.SourceUrl.Form.RECORDS));
        com.legend.planner.DataSources.Tables tables = new com.legend.planner.DataSources.Tables(trino);
        String sql = trino.render(tables.read(q));
        assertEquals(true, sql.contains("(VALUES (json_parse('{\"a\":1}'), BIGINT '0'), (json_parse('{\"a\":"
                + "\"it''s\"}'), BIGINT '1')) AS \"t0\"(\"data\", \"u_frame_ord__\")"), sql);
        assertEquals(false, sql.contains("legend_data_"), "no session table: " + sql);
    }
}
