// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * THE IDENTIFIER ESCAPING, MEASURED ON A LIVE CLOUD WAREHOUSE (the orchestrator's item 4, 2026-11-13): what
 * {@link IdentifierEscapingTest} reads by each database's lexical rules, asked of the database itself, for the two clouds
 * this sandbox cannot reach -- Snowflake and Databricks. Each name -- IdentifierEscapingTest's {@code a"b\c} and each
 * payload of the injection fuzz ({@link InjectionFuzzTest#PAYLOADS}: quotes of every kind, a backslash before one, a
 * comment's opening, a line break) -- is the alias of one literal column, rendered by the cloud's own dialect and run on
 * the account: the database must answer ONE column, labelled exactly the name. A statement the database refuses, a second
 * column, or another label is a finding, named.
 *
 * <p>A manual probe, never in a lane: {@code bazel test //core:identifier_escaping_cloud_probe} with
 * {@code LEGENDLITE_PCT_CLOUD_PROPERTIES} naming the cloud lanes' properties file (the account's coordinates and
 * credentials, as {@code testbackend.CloudBackend} reads them) and {@code LEGENDLITE_PROBE_CLOUD} the cloud
 * ({@code snowflake} or {@code databricks}). Without them it fails by name: it never skips (the skip census).
 */
@Tag("probe")
class IdentifierEscapingCloudProbeTest {

    /** The names probed: IdentifierEscapingTest's, then the injection fuzz's payloads. */
    static List<String> names() {
        List<String> out = new ArrayList<>();
        out.add("a\"b\\c");
        out.addAll(InjectionFuzzTest.PAYLOADS);
        return out;
    }

    /** The cloud {@code LEGENDLITE_PROBE_CLOUD} names. */
    static DatabaseType cloud() {
        String cloud = System.getenv("LEGENDLITE_PROBE_CLOUD");
        if (cloud == null) {
            throw new IllegalStateException("LEGENDLITE_PROBE_CLOUD names the cloud this probe asks (snowflake or"
                    + " databricks), and LEGENDLITE_PCT_CLOUD_PROPERTIES its account's properties file: neither is"
                    + " defaulted");
        }
        return switch (cloud) {
            case "snowflake" -> DatabaseType.Snowflake;
            case "databricks" -> DatabaseType.Databricks;
            default -> throw new IllegalStateException("LEGENDLITE_PROBE_CLOUD is snowflake or databricks, not " + cloud);
        };
    }

    @Test
    void eachNameIsOneColumnLabelledExactlyOnTheLiveDatabase() throws SQLException {
        DatabaseType type = cloud();
        SqlDialect dialect = com.legend.database.Databases.dialect(type);
        List<String> wrong = new ArrayList<>();
        int answered = 0;
        try (Connection c = com.legend.testbackend.TestBackends.of(type).open()) {
            for (String name : names()) {
                SqlSelect select = SqlSelect.starOf(new SqlSource.Dual()).withProjections(List.of(
                        new SqlSelect.Projection(new SqlExpr.IntLit(1), name, null)));
                String sql = dialect.render(select);
                String shown = name.replace("\n", "\\n");
                try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
                    ResultSetMetaData md = rs.getMetaData();
                    if (md.getColumnCount() != 1) {
                        wrong.add(shown + ": " + md.getColumnCount() + " columns answered by " + sql);
                    } else if (!name.equals(md.getColumnLabel(1))) {
                        wrong.add(shown + ": labelled '" + md.getColumnLabel(1).replace("\n", "\\n") + "' by " + sql);
                    } else {
                        answered++;
                    }
                } catch (SQLException refused) {
                    wrong.add(shown + ": the database refused " + sql.replace("\n", " ") + ": " + refused.getMessage());
                }
            }
        }
        System.out.println("[identifier escaping probe] " + type + ": " + answered + " of " + names().size()
                + " names answered exactly");
        assertEquals(List.of(), wrong, type + ": " + answered + " of " + names().size() + " answered exactly");
        assertTrue(answered > 0, "nothing answered");
    }
}
