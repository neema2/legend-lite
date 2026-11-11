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
 * THE PAYLOAD-IN-THE-STATEMENT FORMS' BOUND (census M18, the user's rulings of 2026-11-11: "we need to make json
 * work everywhere", and the delivery "delegated to dialect to decide"): how each database takes a {@code data:} payload
 * is its dialect's answer ({@link SqlDialect#deliver}), measured -- a session table, the statement's own rows, or the
 * payload as one bound parameter the database splits -- and the payload in the statement is BOUNDED to the databases
 * measured for it: never a default, never a fallback from a form that failed. A database moves between the lists only
 * with its measurement (a probe in docs/data-delivery-homework/probes, or a lane's probe the orchestrator relays).
 * Every database has a delivery: none refuses a {@code data:} payload (register S190 withdrawn, 2026-11-11).
 */
public class DataDeliveryDeclaredTest {

    private static final List<SqlDdl.Column> COLUMNS = List.of(new SqlDdl.Column("data", false,
            new SqlDdl.ColumnType.Plain(SqlDdl.ColumnType.Kind.JSON), false, false));

    /** How a database takes a payload, as its dialect's {@code deliver} answers a sample: a session TABLE, the
     *  statement's own ROWS, or the payload as one BOUND parameter (the source the dialect renders itself). */
    public enum Kind { SESSION_TABLE, INLINE, BOUND_PARAMETER }

    static final List<com.legend.sql.OutputCol> FRAME_OUTPUTS = List.of(new com.legend.sql.OutputCol("data",
            com.legend.sql.SqlType.Scalar.JSON, true, com.legend.sql.OutputCol.Origin.DERIVED),
            new com.legend.sql.OutputCol(com.legend.sql.SqlSource.SourceUrl.PLACE, com.legend.sql.SqlType.Scalar.BIGINT,
                    false, com.legend.sql.OutputCol.Origin.DERIVED));

    /** {@code dialect}'s answer for a two-record frame. */
    public static Kind kindOf(SqlDialect dialect) {
        com.legend.sql.SqlSource.SourceUrl u = new com.legend.sql.SqlSource.SourceUrl(
                "data:application/json,[{\"a\":1},{\"a\":2}]", "t0", FRAME_OUTPUTS,
                com.legend.sql.SqlSource.SourceUrl.Form.RECORDS);
        return switch (dialect.deliver(new SqlDialect.DataPayload(u, "legend_data_0123456789abcdef_o",
                List.of("{\"a\":1}", "{\"a\":2}")))) {
            case SqlDialect.Delivered.Table t -> Kind.SESSION_TABLE;
            case SqlDialect.Delivered.Read r -> r.source() instanceof com.legend.sql.SqlSource.SourceUrl
                    ? Kind.BOUND_PARAMETER : Kind.INLINE;
        };
    }

    @Test
    void eachDatabasesDeliveryIsItsMeasuredOne() {
        Map<String, Kind> declared = new TreeMap<>();
        for (com.legend.model.ConnectionDefinition.DatabaseType type
                : com.legend.model.ConnectionDefinition.DatabaseType.values()) {
            SqlDialect dialect;
            try {
                dialect = com.legend.database.Databases.dialect(type);
            } catch (com.legend.error.NotImplementedException none) {
                continue;   // no dialect at all: no query reaches the database
            }
            declared.put(type.name(), kindOf(dialect));
        }
        assertEquals(Map.ofEntries(
                Map.entry("DuckDB", Kind.SESSION_TABLE),
                Map.entry("H2", Kind.SESSION_TABLE),
                Map.entry("SQLite", Kind.SESSION_TABLE),
                Map.entry("Postgres", Kind.SESSION_TABLE),
                Map.entry("MariaDB", Kind.SESSION_TABLE),
                Map.entry("MySQL", Kind.SESSION_TABLE),
                Map.entry("Trino", Kind.INLINE),
                Map.entry("ClickHouse", Kind.INLINE),
                Map.entry("MongoDB", Kind.INLINE),
                // round 2's measurement, the user's ruling of 2026-11-11: the payload as one bound parameter
                Map.entry("Snowflake", Kind.BOUND_PARAMETER),
                Map.entry("Databricks", Kind.SESSION_TABLE),
                Map.entry("BigQuery", Kind.BOUND_PARAMETER)), new java.util.HashMap<>(declared),
                "each database's data: delivery, as measured (a change is a measurement and this list's edit)");
    }

    /** An unread {@code data:} source -- one the planner did not turn into its delivery -- reaching any renderer is the
     *  planner's defect, refused by one type and message on every database (the second audit's S8, 2026-11-11:
     *  MongoDB's pipeline raised a capability refusal where the SQL dialects raise the defect); a database whose
     *  delivery is the bound parameter renders it itself, the payload a bound value, never text. */
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
            if (kindOf(dialect) == Kind.BOUND_PARAMETER) {
                // a database that reads the payload itself: one placeholder, the payload its bound value
                RenderedStatement written = dialect.renderStatement(unread);
                assertEquals(List.of(new RenderedStatement.Binding.Payload("[1]")),
                        written.binds().stream().map(RenderedStatement.Bind::binding).toList(), type + ": " + written);
                assertEquals(false, written.sql().contains("[1]"), type + ": the payload is no text: " + written);
            } else {
                IllegalStateException refused = assertThrows(IllegalStateException.class,
                        () -> dialect.render(unread), type + ": an unread data: source is the planner's defect");
                assertEquals(true, refused.getMessage().contains("reached a render unread"), type + ": " + refused);
            }
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
            if (kindOf(dialect) == Kind.SESSION_TABLE) {
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
