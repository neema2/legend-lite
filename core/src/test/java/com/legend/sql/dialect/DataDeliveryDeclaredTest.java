// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * THE PAYLOAD-IN-THE-STATEMENT FORMS' BOUND (census M18, the user's rulings of 2026-11-11: "we need to make json
 * work everywhere", the delivery "delegated to dialect to decide", and the JSON pass's "move everything off temp
 * table"): how each database takes a {@code data:} payload is its dialect's answer ({@link SqlDialect#deliver}),
 * measured -- the payload as one bound parameter the database splits, or the statement's own rows -- and the payload's
 * rows in the statement are BOUNDED to the databases measured for them: never a default, never a fallback from a form
 * that failed. A database moves between the lists only with its measurement (a probe in
 * docs/data-delivery-homework/probes, or a lane's probe the orchestrator relays). Every database has a delivery: none
 * refuses a {@code data:} payload (register S190 withdrawn, 2026-11-11), and none makes a session table (the JSON
 * pass, 2026-11-11: the kind is gone).
 */
public class DataDeliveryDeclaredTest {

    /** How a database takes a payload, as its dialect's {@code deliver} answers a sample: the statement's own ROWS, or
     *  the payload as one BOUND parameter (the source the dialect renders itself). */
    public enum Kind { INLINE, BOUND_PARAMETER }

    static final List<com.legend.sql.OutputCol> FRAME_OUTPUTS = List.of(new com.legend.sql.OutputCol("data",
            com.legend.sql.SqlType.Scalar.JSON, true, com.legend.sql.OutputCol.Origin.DERIVED),
            new com.legend.sql.OutputCol(com.legend.sql.SqlSource.SourceUrl.PLACE, com.legend.sql.SqlType.Scalar.BIGINT,
                    false, com.legend.sql.OutputCol.Origin.DERIVED));

    /** {@code dialect}'s answer for a two-record frame. */
    public static Kind kindOf(SqlDialect dialect) {
        com.legend.sql.SqlSource.SourceUrl u = new com.legend.sql.SqlSource.SourceUrl(
                "data:application/json,[{\"a\":1},{\"a\":2}]", "t0", FRAME_OUTPUTS,
                com.legend.sql.SqlSource.SourceUrl.Form.RECORDS);
        return dialect.deliver(SqlDialect.DataPayload.alone(u, List.of("{\"a\":1}", "{\"a\":2}")))
                instanceof com.legend.sql.SqlSource.SourceUrl ? Kind.BOUND_PARAMETER : Kind.INLINE;
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
            // 2026-11-14 (main's JSON pass merged into the Deephaven branch, the user to rule: the desk roadmap's open
            // items): Deephaven, the one database with no delivery -- a server reads no rows a plan carries (its
            // DeephavenBatch sources are a server's tables; a plan's values uploaded first are DH5, unbuilt), so its
            // dialect refuses a data: payload by name rather than default to a form it cannot run
            if (type == com.legend.model.ConnectionDefinition.DatabaseType.Deephaven) {
                assertRefusedOnDeephaven(dialect);
                continue;
            }
            declared.put(type.name(), kindOf(dialect));
        }
        // the JSON pass (2026-11-11, the user's rulings): bound wherever the database splits one parameter (measured:
        // 100,000 records each), inline on H2 (an array parameter of more than 65,536 elements refused) and MongoDB
        // ($documents); Databricks inline only above its 1 MiB parameter limit (DataSourcesDeliveryTest)
        assertEquals(Map.ofEntries(
                Map.entry("DuckDB", Kind.BOUND_PARAMETER),
                Map.entry("H2", Kind.INLINE),
                Map.entry("SQLite", Kind.BOUND_PARAMETER),
                Map.entry("Postgres", Kind.BOUND_PARAMETER),
                Map.entry("MariaDB", Kind.BOUND_PARAMETER),
                Map.entry("MySQL", Kind.BOUND_PARAMETER),
                Map.entry("Trino", Kind.BOUND_PARAMETER),
                Map.entry("ClickHouse", Kind.BOUND_PARAMETER),
                Map.entry("MongoDB", Kind.INLINE),
                Map.entry("Snowflake", Kind.BOUND_PARAMETER),
                Map.entry("Databricks", Kind.BOUND_PARAMETER),
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
            if (type == com.legend.model.ConnectionDefinition.DatabaseType.Deephaven) {
                // 2026-11-14: no delivery (eachDatabasesDeliveryIsItsMeasuredOne); an unread source refused by name
                assertRefusedOnDeephaven(dialect);
                assertThrows(com.legend.sql.DialectCapability.class, () -> dialect.render(unread), type.name());
                rendered++;
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

    /** Deephaven's refusal of a payload, by name (2026-11-14: the one database with no delivery). */
    public static void assertRefusedOnDeephaven(SqlDialect dialect) {
        var refused = assertThrows(com.legend.sql.DialectCapability.class, () -> kindOf(dialect));
        assertEquals("a data payload reached Deephaven: a server reads no rows a plan carries", refused.getMessage());
    }

    /** On a database that writes the payload as rows (H2), a JSON connection's frame (its RECORDS form) is a row per
     *  record -- the record's JSON text parsed by the database, its place beside it from 0 -- with no load at all. */
    @Test
    void aFrameOnH2IsItsRecordsAsRowsWithTheirPlaces() {
        SqlDialect h2 = com.legend.database.Databases.dialect(com.legend.model.ConnectionDefinition.DatabaseType.H2);
        com.legend.sql.SqlQuery q = com.legend.sql.SqlSelect.starOf(new com.legend.sql.SqlSource.SourceUrl(
                "data:application/json,[{\"a\":1},{\"a\":\"it's\"}]", "t0", FRAME_OUTPUTS,
                com.legend.sql.SqlSource.SourceUrl.Form.RECORDS));
        String sql = h2.render(com.legend.planner.DataSources.read(q, h2));
        assertEquals(true, sql.contains("(VALUES (JSON '{\"a\":1}', 0), (JSON '{\"a\":\"it''s\"}', 1)) AS \"t0\"(\"data\","
                + " \"u_frame_ord__\")"), sql);
        assertEquals(false, sql.toLowerCase(java.util.Locale.ROOT).contains("legend_data_"), "no session table: " + sql);
    }
}
