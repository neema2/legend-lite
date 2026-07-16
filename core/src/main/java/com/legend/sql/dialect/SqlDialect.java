package com.legend.sql.dialect;

import com.legend.sql.SqlQuery;

/**
 * The dialect seam: rendering (Phase J) AND value normalization (Phase K).
 * A backend's JDBC driver may hand back dialect-flavored Java objects
 * (SQLite: dates as Strings, booleans as ints); {@link #normalize} converts
 * to the canonical representation for a PURE type so the typed-result
 * contract holds on every backend.
 */
public interface SqlDialect {

    String render(SqlQuery query);

    /** JDBC cell value → canonical Java value for {@code type}. Default: identity. */
    default Object normalize(Object jdbcValue, com.legend.sql.SqlType type) {
        return jdbcValue;
    }

    /**
     * Adapt ONE raw caller-supplied SQL statement (the K-native
     * {@code executeInDb} boundary) to this backend. The engine's test
     * corpus writes H2-flavored DDL/DML — unquoted keyword column names,
     * {@code CURRENT_TIMESTAMP()} — that other backends reject; a dialect
     * rewrites ONLY what it must to execute the same semantics. Callers
     * split multi-statement blobs ({@link com.legend.sql.RawSql}) BEFORE
     * adapting — the recognizers anchor at statement start. Default:
     * identity.
     */
    default String adaptRawSql(String sql) {
        return sql;
    }
}
