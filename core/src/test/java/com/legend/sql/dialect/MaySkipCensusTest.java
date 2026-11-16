// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.database.Databases;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE MAY-SKIP SET'S CENSUS (the PR1 re-review's A6 and B6, 2026-11-16): {@link SqlFn#MAY_SKIP_AN_ARGUMENT} is a hand
 * list of the functions some dialect renders with a branch, which may leave an argument unevaluated, so
 * {@code LetBinding.alwaysEvaluated} never descends into them (R6). A function outside the set whose rendering on any
 * dialect branches would let a raising repeat be bound out of an untaken arm. This renders every function on every
 * dialect, the dialects' helpers included (the whole call is rendered, not one arm), and fails where a rendering
 * branches ({@code CASE}, {@code IF(}, {@code IIF(}, {@code DECODE(}, {@code multiIf(}) for a function outside the set.
 */
class MaySkipCensusTest {

    private static final Pattern BRANCH = Pattern.compile("(?i)\\bCASE\\b|\\bIIF\\s*\\(|\\bIF\\s*\\(|\\bDECODE\\s*\\("
            + "|\\bmultiIf\\s*\\(");

    /** Argument types tried, each at arities 1 to 4: the first that renders is read. */
    private static final List<SqlType.Scalar> TYPES = List.of(SqlType.Scalar.INTEGER, SqlType.Scalar.VARCHAR,
            SqlType.Scalar.DOUBLE, SqlType.Scalar.BOOLEAN, SqlType.Scalar.DATE, SqlType.Scalar.TIMESTAMP);

    private static List<String> renderings(SqlDialect d, SqlFn fn) {
        List<String> out = new ArrayList<>();
        for (SqlType.Scalar t : TYPES) {
            for (int arity = 1; arity <= 4; arity++) {
                List<SqlExpr> args = new ArrayList<>();
                List<OutputCol> cols = new ArrayList<>();
                for (int i = 0; i < arity; i++) {
                    args.add(SqlExpr.Column.of("t", "c" + i, t, false, OutputCol.Origin.PHYSICAL));
                    cols.add(new OutputCol("c" + i, t, false));
                }
                try {
                    out.add(d.render(new SqlSelect(List.of(new SqlSelect.Projection(SqlExpr.Call.of(fn,
                            args.toArray(SqlExpr[]::new)), "x", null)), false,
                            new SqlSource.Table("t", "t", cols, false, Map.of(), com.legend.sql.NameQuoting.BARE),
                            null, List.of(), null, null, List.of(), null, null, List.of())));
                } catch (RuntimeException notThisShape) {
                    // an arity or type this function does not take: the next is tried
                }
            }
        }
        return out;
    }

    @Test
    void everyBranchingRenderingIsInTheMaySkipSet() {
        Map<String, TreeSet<String>> outside = new TreeMap<>();
        int rendered = 0;
        for (DatabaseType type : DatabaseType.values()) {
            SqlDialect d;
            try {
                d = Databases.dialect(type);
            } catch (RuntimeException noDialect) {
                continue;
            }
            for (SqlFn fn : SqlFn.values()) {
                for (String sql : renderings(d, fn)) {
                    rendered++;
                    if (!SqlFn.MAY_SKIP_AN_ARGUMENT.contains(fn) && BRANCH.matcher(sql).find()) {
                        outside.computeIfAbsent(fn.name(), k -> new TreeSet<>()).add(type.name());
                    }
                }
            }
        }
        assertTrue(rendered > 1000, "the census rendered " + rendered + " calls");
        Map<String, TreeSet<String>> known = new TreeMap<>();
        KNOWN.forEach((fn, dialects) -> known.put(fn, new TreeSet<>(List.of(dialects.split(",")))));
        assertEquals(known, outside, "a function whose rendering branches, outside SqlFn.MAY_SKIP_AN_ARGUMENT"
                + " (function -> the dialects that branch): add it to the set, or render it without a branch -- or one"
                + " that left: shrink KNOWN, with the date");
    }

    /**
     * The functions outside the set whose rendering branches today, as the census first found them (2026-11-16, the
     * PR1 re-review's A6). Most are null guards ({@code CASE WHEN x IS NULL THEN NULL ELSE ...}), but behind a
     * short-circuit OR, or in a helper, some do leave an argument unevaluated. Deciding each -- into the set, or
     * rendered without the branch -- moves LetBinding's product SQL, which is E-SQL's (the orchestrator, 2026-11-16).
     * SHRINK-ONLY: a new entry fails the build; a resolved one is deleted here with its date.
     */
    private static final Map<String, String> KNOWN = Map.ofEntries(
            Map.entry("ACOS", "DuckDB,H2,MariaDB,MySQL,Postgres,SQLite,Snowflake"),
            Map.entry("ASIN", "DuckDB,H2,MariaDB,MySQL,Postgres,SQLite,Snowflake"),
            Map.entry("CONCAT", "BigQuery,MariaDB,MySQL,SQLite"),
            Map.entry("CONCAT_JOIN", "BigQuery,MariaDB,MySQL,SQLite"),
            Map.entry("DAYNAME", "SQLite,Snowflake"),
            Map.entry("DECIMAL_TO_TEXT", "BigQuery,MariaDB,MongoDB,MySQL,SQLite,Snowflake"),
            Map.entry("DECODE_BASE64", "DuckDB,Postgres,SQLite"),
            Map.entry("ENCODE_BASE64", "BigQuery,SQLite"),
            Map.entry("EPOCH_MS", "SQLite"),
            Map.entry("EPOCH_SECONDS", "SQLite"),
            Map.entry("FLOAT_CANON", "BigQuery,ClickHouse,Databricks,DuckDB,H2,MariaDB,MongoDB,MySQL,Postgres,SQLite,"
                    + "Snowflake,Trino"),
            Map.entry("FLOAT_PRINT", "BigQuery,ClickHouse,Databricks,DuckDB,H2,MariaDB,MongoDB,MySQL,Postgres,SQLite,"
                    + "Snowflake,Trino"),
            Map.entry("HASH", "BigQuery,MariaDB,MySQL,SQLite"),
            Map.entry("JARO_WINKLER", "Databricks,MySQL,Postgres,SQLite,Trino"),
            Map.entry("LEFT", "BigQuery,MariaDB,MySQL,SQLite"),
            Map.entry("LENGTH", "BigQuery,MariaDB,MySQL,SQLite"),
            Map.entry("LEVENSHTEIN", "MySQL,Postgres,SQLite"),
            Map.entry("LIST_APPEND", "MariaDB,MySQL,SQLite"),
            Map.entry("LIST_AVG", "Snowflake"),
            Map.entry("LIST_FLATTEN", "MariaDB,MySQL,SQLite"),
            Map.entry("LIST_INIT", "BigQuery,MariaDB,MySQL,SQLite"),
            Map.entry("LIST_MEDIAN", "MySQL,Trino"),
            Map.entry("LIST_MODE", "Trino"),
            Map.entry("LIST_REVERSE", "MariaDB,MySQL,SQLite"),
            Map.entry("LIST_SLICE", "BigQuery,MariaDB,MySQL,SQLite"),
            Map.entry("LIST_SORT", "BigQuery,MariaDB,MySQL,SQLite"),
            Map.entry("LIST_SORT_DESC", "BigQuery,MariaDB,MySQL,SQLite"),
            Map.entry("LIST_TAIL", "BigQuery,MariaDB,MySQL,SQLite"),
            Map.entry("MD5", "BigQuery,SQLite"),
            Map.entry("MONTHNAME", "SQLite"),
            Map.entry("PARSE_DATE", "MariaDB,MySQL,SQLite"),
            Map.entry("PURE_SPLIT_PART", "ClickHouse,MariaDB,MySQL,SQLite,Snowflake"),
            Map.entry("RANGE_FN", "BigQuery,Databricks,MariaDB,MySQL,Postgres,SQLite,Trino"),
            Map.entry("REGEXP_EXTRACT_ALL", "ClickHouse"),
            Map.entry("ROUND", "H2,MariaDB,MySQL,Postgres,SQLite,Snowflake,Trino"),
            Map.entry("ROUND_HALF_UP", "BigQuery,SQLite"),
            Map.entry("SHA1", "BigQuery,SQLite"),
            Map.entry("SHA256", "BigQuery,SQLite"),
            Map.entry("TO_VARIANT", "MariaDB,MySQL,Postgres,SQLite"),
            Map.entry("UNNEST", "MySQL"),
            Map.entry("VARIANT_ELEMENTS", "DuckDB,MariaDB,MySQL,Postgres,SQLite"),
            Map.entry("VARIANT_GET", "BigQuery,ClickHouse,Databricks,MySQL,SQLite,Snowflake,Trino"));

    @Test
    void theCensusBites() {
        // a function the set does not hold whose rendering is a CASE: found
        assertTrue(BRANCH.matcher("SELECT CASE WHEN t.c0 IS NULL THEN NULL ELSE t.c0 END AS x").find());
        assertTrue(BRANCH.matcher("SELECT multiIf(a, b, c) AS x").find());
        assertTrue(!BRANCH.matcher("SELECT lower(t.c0) AS x").find());
    }
}
