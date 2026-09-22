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

    /** B6 (truthfulness burn) — SESSION SETUP IS DIALECT-OWNED as a
     * FACT: the statements a backend's session needs for the
     * platform's value contracts. The dialect DECIDES; the exec layer
     * EXECUTES (F1.3: java.sql never enters this package) — applied at
     * {@code Compiler.dialectOf}'s connection seam, idempotent by
     * contract. Default: none. */
    default java.util.List<String> sessionSetup() {
        return java.util.List.of();
    }

    /** JDBC cell value → canonical Java value for {@code type}. Default: identity. */
    default @com.legend.base.Nullable Object normalize(@com.legend.base.Nullable Object jdbcValue,
            com.legend.sql.@com.legend.base.Nullable SqlType type) {
        return jdbcValue;
    }

    /** True when corpus-authored raw H2 statements execute NATIVELY on
     * this dialect's session — the {@code RawSqlBoundary.h2ToDuckDb}
     * rewrite is a DUCKDB-TARGET adaptation and must be identity here
     * (H2_BACKEND.md §12 step 12). */
    /** Whether dynamic PIVOT needs the two-phase staticization
     * pre-pass ({@link com.legend.exec.DynamicPivot} — no native
     * dynamic pivot on this backend). */
    default boolean needsStaticPivot() {
        return false;
    }

    default boolean rawH2IsNative() {
        return false;
    }

    /** An effect segment's statements as ONE script (block-compiler stage 3): the
     *  target decides its transaction bracket INSIDE its dialect — DuckDB brackets
     *  the segment so a failure applies nothing (transactional DDL); H2 cannot roll
     *  DDL back and sends the statements bare. */
    default String script(java.util.List<String> statements) {
        return String.join(";\n", statements) + ";";
    }

    /** The statement that ABORTS a failed script's bracket, or null when the dialect
     *  brackets nothing — a DuckDB transaction a failing statement left open must be
     *  rolled back, or every later transaction on the connection is refused. */
    default @com.legend.base.Nullable String scriptAbort() {
        return null;
    }

    /** Which statement of a failed script the engine's message names (0-based), or
     *  empty when the engine does not say — H2 quotes the failing statement
     *  ({@code SQL statement: …}); DuckDB names values and columns only. */
    default java.util.OptionalInt failingStatement(String message, java.util.List<String> statements) {
        return java.util.OptionalInt.empty();
    }

    /** DDL rendered like a query (2026-09-16): the dialect spells the
     *  store's declared shape — its identifier rule, its type names.
     *  Retires the {@code Ddl.Flavor} enum and the {@code rawH2IsNative()}
     *  ternaries that chose it: a target is decided INSIDE its dialect. */
    String render(com.legend.sql.SqlDdl ddl);

}
