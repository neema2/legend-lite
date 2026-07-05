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

    /** JDBC cell value → canonical Java value for {@code pureType}. Default: identity. */
    default Object normalize(Object jdbcValue, com.legend.compiler.element.type.Type pureType) {
        return jdbcValue;
    }
}
