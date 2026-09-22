// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.harness;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * THE ROW VERDICT, JUDGED IN THE DATABASE
 * (docs/REFEREE_IN_DATABASE_DESIGN_2026_09_07.md): the golden's rows,
 * produced by the H2 oracle, are TRANSFERRED into the DuckDB session as a
 * typed table — column types from the oracle's JDBC metadata, values by
 * JDBC type through the driver's appender, never spelled as text — and ONE
 * query decides whether that table and our query's rows are the same
 * multiset. No Java looks at a value. A golden column an enumeration
 * mapping encodes decodes by a JOIN to the mapping's pairs (model metadata
 * as a VALUES relation), not by a Java map over cells.
 *
 * <p>Declines (an {@link H2Verify.Unverifiable}) keep their historical
 * bucket texts: column arity, a JDBC type the transfer cannot carry.
 */
final class InDbVerdict {

    private InDbVerdict() {
    }

    private static final AtomicInteger TABLES = new AtomicInteger();
    private static final java.util.regex.Pattern ORDERED =
            java.util.regex.Pattern.compile("(?i)\\border\\s+by\\b");

    /** Null = the multisets agree; text = a REAL divergence (sampled rows,
     * for the message only). {@code valueFrame}: our side is a VALUE
     * collection (pure collections hold no empties), so a one-column
     * all-NULL row counts on neither side. */
    static @com.legend.base.Nullable String judge(Connection session, Statement oracle,
            String goldenSql, String ourSql,
            Map<Integer, Map<String, String>> enumDecode, boolean valueFrame)
            throws SQLException {
        String table = "__golden_" + TABLES.incrementAndGet();
        int n;
        List<Integer> jdbcTypes = new ArrayList<>();
        try (ResultSet rs = oracle.executeQuery(goldenSql)) {
            ResultSetMetaData md = rs.getMetaData();
            n = md.getColumnCount();
            StringBuilder ddl = new StringBuilder("CREATE TABLE " + table + " (");
            for (int i = 1; i <= n; i++) {
                int t = md.getColumnType(i);
                jdbcTypes.add(t);
                ddl.append(i > 1 ? ", " : "").append("c").append(i).append(' ')
                        .append(enumDecode.containsKey(i - 1) ? "VARCHAR"
                                : duckType(t, md.getPrecision(i), md.getScale(i)));
            }
            ddl.append(')');
            try (Statement st = session.createStatement()) {
                st.execute("DROP TABLE IF EXISTS " + table);
                st.execute(ddl.toString());
            }
            transfer(session, table, rs, n, jdbcTypes, enumDecode);
        } catch (SQLException e) {
            throw new H2Verify.Unverifiable("golden execution: " + e.getMessage(), e);
        }
        try {
            int ourArity = arity(session, ourSql);
            if (ourArity != n) {
                if (n == 1 && goldenSql.toLowerCase(java.util.Locale.ROOT)
                        .startsWith("select distinct")) {
                    throw new H2Verify.Unverifiable("population statement of a"
                            + " chained plan — no counterpart statement in our"
                            + " one-statement plan", null);
                }
                throw new H2Verify.Unverifiable("column arity differs: golden " + n
                        + " vs frame " + ourArity, null);
            }
            if (ORDERED.matcher(goldenSql).find()) {
                // an ordered golden judges as a multiset today (the keyed
                // positional rule is the follow-up) — counted, never silent
                H2Verify.verdict("ord-multiset");
            }
            String golden = goldenSelect(table, n, enumDecode, valueFrame);
            String ours = valueFrame && n == 1
                    ? "SELECT * FROM (" + ourSql + ") AS o WHERE o.* IS NOT NULL"
                    : "(" + ourSql + ")";
            String diff = "SELECT count(*) FROM ((SELECT * FROM (" + golden + ") AS g"
                    + " EXCEPT ALL SELECT * FROM " + ours + " AS oo) UNION ALL"
                    + " (SELECT * FROM " + ours + " AS oo2 EXCEPT ALL SELECT * FROM ("
                    + golden + ") AS g2)) AS d";
            try (Statement st = session.createStatement();
                    ResultSet rs = st.executeQuery(diff)) {
                rs.next();
                if (rs.getLong(1) == 0) {
                    return null;
                }
            }
            return "rows differ: golden-only " + sample(session, "SELECT * FROM ("
                    + golden + ") AS g EXCEPT ALL SELECT * FROM " + ours + " AS oo")
                    + " ours-only " + sample(session, "SELECT * FROM " + ours
                    + " AS oo EXCEPT ALL SELECT * FROM (" + golden + ") AS g");
        } finally {
            try (Statement st = session.createStatement()) {
                st.execute("DROP TABLE IF EXISTS " + table);
            }
        }
    }

    /** The golden side as a SELECT: enum-coded columns decode by a JOIN to
     * the mapping's pairs. */
    private static String goldenSelect(String table, int n,
            Map<Integer, Map<String, String>> enumDecode, boolean valueFrame) {
        StringBuilder cols = new StringBuilder();
        StringBuilder joins = new StringBuilder();
        for (int i = 1; i <= n; i++) {
            Map<String, String> dec = enumDecode.get(i - 1);
            cols.append(i > 1 ? ", " : "");
            if (dec == null || dec.isEmpty()) {
                cols.append("g.c").append(i);
                continue;
            }
            String alias = "d" + i;
            StringBuilder values = new StringBuilder();
            for (var e : dec.entrySet()) {
                values.append(values.length() > 0 ? ", " : "")
                        .append('(').append(lit(e.getKey())).append(", ")
                        .append(lit(e.getValue())).append(')');
            }
            joins.append(" LEFT JOIN (VALUES ").append(values).append(") AS ")
                    .append(alias).append("(src, name) ON g.c").append(i)
                    .append(" = ").append(alias).append(".src");
            cols.append("COALESCE(").append(alias).append(".name, g.c").append(i).append(')');
        }
        String where = valueFrame && n == 1 ? " WHERE g.c1 IS NOT NULL" : "";
        return "SELECT " + cols + " FROM " + table + " AS g" + joins + where;
    }

    private static String lit(String s) {
        return "'" + s.replace("'", "''") + "'";
    }

    private static int arity(Connection session, String ourSql) throws SQLException {
        try (Statement st = session.createStatement();
                ResultSet rs = st.executeQuery("SELECT * FROM (" + ourSql + ") AS a LIMIT 0")) {
            return rs.getMetaData().getColumnCount();
        }
    }

    private static String sample(Connection session, String sql) throws SQLException {
        List<String> rows = new ArrayList<>();
        try (Statement st = session.createStatement();
                ResultSet rs = st.executeQuery(sql + " LIMIT 3")) {
            int n = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder r = new StringBuilder();
                for (int i = 1; i <= n; i++) {
                    r.append(i > 1 ? "|" : "").append(rs.getObject(i));
                }
                rows.add(r.toString());
            }
        }
        return rows.toString();
    }

    /** The oracle's JDBC column type as a DuckDB column type — the ONE
     * mapping of the transfer. */
    private static String duckType(int jdbcType, int precision, int scale) {
        return switch (jdbcType) {
            case Types.TINYINT, Types.SMALLINT, Types.INTEGER -> "INTEGER";
            case Types.BIGINT -> "BIGINT";
            case Types.REAL, Types.FLOAT, Types.DOUBLE -> "DOUBLE";
            case Types.DECIMAL, Types.NUMERIC -> precision > 0 && precision <= 38
                    ? "DECIMAL(" + precision + ", " + Math.max(0, scale) + ")" : "DOUBLE";
            case Types.BOOLEAN, Types.BIT -> "BOOLEAN";
            case Types.DATE -> "DATE";
            case Types.TIME, Types.TIME_WITH_TIMEZONE -> "TIME";
            case Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE -> "TIMESTAMP";
            case Types.BINARY, Types.VARBINARY, Types.BLOB -> "BLOB";
            default -> "VARCHAR";
        };
    }

    /** Rows move by JDBC TYPE: the driver hands the value in the type's own
     * Java class and the appender takes it as that class — no text. */
    private static void transfer(Connection session, String table, ResultSet rs, int n,
            List<Integer> jdbcTypes, Map<Integer, Map<String, String>> enumDecode)
            throws SQLException {
        org.duckdb.DuckDBConnection duck = session.unwrap(org.duckdb.DuckDBConnection.class);
        try (org.duckdb.DuckDBAppender ap = duck.createAppender(table)) {
            while (rs.next()) {
                ap.beginRow();
                for (int i = 1; i <= n; i++) {
                    if (enumDecode.containsKey(i - 1)) {
                        String s = rs.getString(i);
                        if (s == null) {
                            ap.appendNull();
                        } else {
                            ap.append(s);
                        }
                        continue;
                    }
                    appendTyped(ap, rs, i, jdbcTypes.get(i - 1));
                }
                ap.endRow();
            }
            ap.flush();
        }
    }

    private static void appendTyped(org.duckdb.DuckDBAppender ap, ResultSet rs, int i,
            int jdbcType) throws SQLException {
        switch (jdbcType) {
            case Types.TINYINT, Types.SMALLINT, Types.INTEGER -> {
                int v = rs.getInt(i);
                if (rs.wasNull()) {
                    ap.appendNull();
                } else {
                    ap.append(v);
                }
            }
            case Types.BIGINT -> {
                long v = rs.getLong(i);
                if (rs.wasNull()) {
                    ap.appendNull();
                } else {
                    ap.append(v);
                }
            }
            case Types.REAL, Types.FLOAT, Types.DOUBLE -> {
                double v = rs.getDouble(i);
                if (rs.wasNull()) {
                    ap.appendNull();
                } else {
                    ap.append(v);
                }
            }
            case Types.DECIMAL, Types.NUMERIC -> {
                java.math.BigDecimal v = rs.getBigDecimal(i);
                if (v == null) {
                    ap.appendNull();
                } else {
                    ap.append(v);
                }
            }
            case Types.BOOLEAN, Types.BIT -> {
                boolean v = rs.getBoolean(i);
                if (rs.wasNull()) {
                    ap.appendNull();
                } else {
                    ap.append(v);
                }
            }
            case Types.DATE -> {
                java.time.LocalDate v = rs.getObject(i, java.time.LocalDate.class);
                if (v == null) {
                    ap.appendNull();
                } else {
                    ap.append(v);
                }
            }
            case Types.TIME, Types.TIME_WITH_TIMEZONE -> {
                java.time.LocalTime v = rs.getObject(i, java.time.LocalTime.class);
                if (v == null) {
                    ap.appendNull();
                } else {
                    ap.append(v);
                }
            }
            case Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE -> {
                java.time.LocalDateTime v = rs.getObject(i, java.time.LocalDateTime.class);
                if (v == null) {
                    ap.appendNull();
                } else {
                    ap.append(v);
                }
            }
            case Types.BINARY, Types.VARBINARY, Types.BLOB -> {
                byte[] v = rs.getBytes(i);
                if (v == null) {
                    ap.appendNull();
                } else {
                    ap.append(v);
                }
            }
            default -> {
                String v = rs.getString(i);
                if (v == null) {
                    ap.appendNull();
                } else {
                    ap.append(v);
                }
            }
        }
    }
}
