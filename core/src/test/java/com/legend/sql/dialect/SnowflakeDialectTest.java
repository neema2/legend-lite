// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import com.legend.sql.SqlWith;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The Snowflake dialect's spellings no local database runs, pinned on the rendered text; each measured on the
 * Snowflake lane.
 */
class SnowflakeDialectTest {

    private static SqlSelect selectOne(SqlSource from) {
        return new SqlSelect(List.of(new SqlSelect.Projection(new SqlExpr.IntLit(1), "one", null)), false, from, null,
                List.of(), null, null, List.of(), null, null, List.of());
    }

    /**
     * A Float's text is its shortest digits that read back as the same double, as DuckDB's cast writes it (2026-10-21):
     * Snowflake's own cast writes ten significant digits (EXP(1) '2.718281828') and its formatter caps a double at
     * sixteen, and is not correctly rounded at seventeen, so the exact DECFLOAT's mantissa is rounded to 15, 16 and 17
     * digits, a tie to the even digit, each read back; the value is written once, carried by one-element TRANSFORMs
     * (Snowflake refuses a correlated subquery). Measured against Java's text on the lane.
     */
    @Test
    void aFloatsTextIsItsShortestDigitsThatReadBack() {
        SqlExpr x = SqlExpr.Column.of("t", "x", SqlType.Scalar.DOUBLE, true, OutputCol.Origin.PHYSICAL);
        String sql = new Snowflake().render(new SqlSelect(List.of(new SqlSelect.Projection(
                new SqlExpr.Cast(x, SqlType.Scalar.VARCHAR), "s", null)), false, new SqlSource.Table("t", "t",
                List.of(new OutputCol("x", SqlType.Scalar.DOUBLE, true)), false, Map.of(), com.legend.sql.NameQuoting.BARE),
                null, List.of(), null, null, List.of(), null, null, List.of()));
        // the value written once; its exact decimal by a power of two's scaling and an exact NUMBER, never its own cast
        // to a DECFLOAT, which rounds to 18 digits at run time
        assertTrue(sql.contains("CAST(GET(TRANSFORM(ARRAY_CONSTRUCT(OBJECT_CONSTRUCT_KEEP_NULL('v', \"t\".\"x\")), "), sql);
        assertTrue(sql.split("\"t\"\\.\"x\"", -1).length - 1 == 1, sql);
        assertTrue(sql.contains(" AS NUMBER(38, 28)) AS DECFLOAT) * POWER(CAST(2 AS DECFLOAT), -"), sql);
        assertTrue(sql.contains("AS DECFLOAT) AS NUMBER(38, 37)))"), sql);
        for (int places = 14; places <= 16; places++) {
            assertTrue(sql.contains("AS NUMBER(38, 37)), " + places + ", 'HALF_TO_EVEN')"), sql);
        }
        assertTrue(sql.contains(" BETWEEN -4 AND 15 THEN "), sql);
        assertFalse(sql.contains("'FM"), sql);
        assertFalse(sql.contains("(SELECT"), sql);
    }

    /**
     * Pure's literal spelling of a Float is spelled here, its shortest text read once per branch (2026-10-21): the
     * portable spelling reads the double's text some fifty times, and channel B's standard suite took 57 minutes on the
     * lane. A coalesce of a JSON value and a text parses the text and reads the JSON as a VARIANT (the zips).
     */
    @Test
    void aFloatsCanonIsSpelledHere_andACoalesceOfJsonAndTextIsOneType() {
        SqlExpr x = SqlExpr.Column.of("t", "x", SqlType.Scalar.DOUBLE, true, OutputCol.Origin.PHYSICAL);
        SqlExpr j = SqlExpr.Column.of("t", "j", SqlType.Scalar.JSON, true, OutputCol.Origin.PHYSICAL);
        String sql = new Snowflake().render(new SqlSelect(List.of(
                new SqlSelect.Projection(SqlExpr.Call.of(SqlFn.FLOAT_CANON, x), "c", null),
                new SqlSelect.Projection(SqlExpr.Call.of(SqlFn.COALESCE, j, new SqlExpr.StringLit("[]")), "z", null)),
                false, new SqlSource.Table("t", "t", List.of(new OutputCol("x", SqlType.Scalar.DOUBLE, true),
                        new OutputCol("j", SqlType.Scalar.JSON, true)), false, Map.of(), com.legend.sql.NameQuoting.BARE),
                null, List.of(), null, null, List.of(), null, null, List.of()));
        assertTrue(sql.contains(" AS DOUBLE) = 0 THEN '0.0'"), sql);
        assertTrue(sql.split("\"t\"\\.\"x\"", -1).length - 1 == 1, sql);
        assertTrue(sql.contains("RPAD("), sql);
        assertTrue(sql.length() < 12_000, sql.length() + " characters: " + sql);
        assertTrue(sql.contains("coalesce(TO_VARIANT(\"t\".\"j\"), PARSE_JSON('[]'))"), sql);
    }

    /**
     * A Float's print forms are spelled here, from its digits read once (2026-10-22): Pure's {@code toString} plain where
     * the exponent is from -4 to 15, legend-engine's text plain from -3 to 6 and with an {@code E} past it. The portable
     * spellings read the double's text nine times and over a thousand, each read this dialect's long text of it:
     * the PCT statement for {@code 1.5 + 2.25} was 48 KB and the served one 6 MB. A literal is no double here and
     * prints through the database's own text of it, as before.
     */
    @Test
    void aFloatsPrintFormsAreSpelledHere_theValueReadOnce() {
        SqlExpr x = SqlExpr.Column.of("t", "x", SqlType.Scalar.DOUBLE, true, OutputCol.Origin.PHYSICAL);
        for (SqlFn fn : List.of(SqlFn.FLOAT_PRINT)) {
            String sql = new Snowflake().render(new SqlSelect(List.of(new SqlSelect.Projection(
                    SqlExpr.Call.of(fn, x), "p", null)), false, new SqlSource.Table("t", "t",
                    List.of(new OutputCol("x", SqlType.Scalar.DOUBLE, true)), false, Map.of(),
                    com.legend.sql.NameQuoting.BARE), null, List.of(), null, null, List.of(), null, null, List.of()));
            assertTrue(sql.split("\"t\"\\.\"x\"", -1).length - 1 == 1, fn + ": " + sql);
            assertTrue(sql.split("AS NUMBER\\(38, 28\\)", -1).length - 1 == 1, fn + ": " + sql);
            assertTrue(sql.length() < 8_000, fn + ": " + sql.length() + " characters: " + sql);
            assertFalse(sql.contains("(SELECT"), fn + ": " + sql);
            assertTrue(sql.contains(" BETWEEN -4 AND 15 THEN "), sql);
            assertTrue(sql.contains("AS NUMBER(38, 0)) AS VARCHAR) || '.0'"), sql);
            assertTrue(sql.contains("AS NUMBER(38, 18)) AS VARCHAR), '0')"), sql);
            assertTrue(sql.contains("THEN 'NaN' WHEN "), sql);
        }
        String literal = new Snowflake().render(new SqlSelect(List.of(new SqlSelect.Projection(
                SqlExpr.Call.of(SqlFn.FLOAT_PRINT, new SqlExpr.FloatLit(1.5)), "p", null)), false,
                new SqlSource.Dual(), null, List.of(), null, null, List.of(), null, null, List.of()));
        assertFalse(literal.contains("NUMBER(38, 28)"), literal);
    }

    /**
     * A ranking window with no order is ordered by a constant (2026-10-21): Snowflake refuses ROW_NUMBER() OVER ()
     * ("requires ORDER BY in window specification"), which the judges' verdict numbers a side with -- 261 channel B
     * tests on the lane at 08ac296d. A value's type is its VARIANT's TYPEOF, a DATE's 'DATE' (SYSTEM$TYPEOF aborted
     * the judges' statement with an internal error, measured at a76cffee).
     */
    @Test
    void aRankingWindowWithNoOrderIsOrderedByAConstant_andAValuesTypeIsItsBaseName() {
        SqlExpr rn = new SqlExpr.WindowCall(new com.legend.sql.SqlAgg.RankingFn(com.legend.sql.SqlAgg.Fn.ROW_NUMBER,
                List.of()), List.of(), List.of(), null, com.legend.sql.SqlTyping.UNKNOWN);
        SqlExpr x = SqlExpr.Column.of("t", "d", SqlType.Scalar.DATE, true, OutputCol.Origin.PHYSICAL);
        SqlExpr isDate = new SqlExpr.Call(SqlFn.EQUAL, List.of(new SqlExpr.Call(SqlFn.TYPEOF, List.of(x)),
                new SqlExpr.TypeNameLit(SqlType.Scalar.DATE)));
        String sql = new Snowflake().render(new SqlSelect(List.of(new SqlSelect.Projection(rn, "n", null),
                new SqlSelect.Projection(isDate, "b", null)), false, new SqlSource.Table("t", "t",
                List.of(new OutputCol("d", SqlType.Scalar.DATE, true)), false, Map.of(), com.legend.sql.NameQuoting.BARE),
                null, List.of(), null, null, List.of(), null, null, List.of()));
        assertTrue(sql.contains("ROW_NUMBER() OVER (ORDER BY NULL)"), sql);
        assertTrue(sql.contains("TYPEOF(TO_VARIANT(\"t\".\"d\")) = 'DATE'"), sql);
    }

    /**
     * A projection that IS error() is raised in its column's type, as a CASE branch is (2026-10-21): a bare raise is
     * text, and the TDS text's date print of the column, TO_CHAR with a format, was refused before the statement ran
     * (testNewDateError's {@code date(2016, 13)}, on the lane at 6c8de4e).
     */
    @Test
    void aProjectedRaiseIsTypedAsItsColumn() {
        SqlExpr raise = new SqlExpr.Call(SqlFn.ERROR, List.of(new SqlExpr.StringLit("Invalid month: 13")));
        String sql = new Snowflake().render(new SqlSelect(List.of(new SqlSelect.Projection(raise, "value",
                new OutputCol("value", SqlType.Scalar.TIMESTAMP, false))), false, new SqlSource.Dual(), null,
                List.of(), null, null, List.of(), null, null, List.of()));
        assertTrue(sql.contains("('Invalid month: 13') || CHR(31) AS TIMESTAMP_NTZ) AS \"value\""), sql);
    }

    /**
     * A CTE's name is quoted at its head as at every reference (2026-10-21): written bare it folds to upper case, and
     * the database judge's verdict reads its CTEs as tables, which Snowflake quotes -- "Object '"__se"' does not
     * exist" on every channel B test of the lane at 6c8de4e.
     */
    @Test
    void aCteIsNamedAsItIsRead() {
        List<OutputCol> outs = List.of(new OutputCol("n", SqlType.Scalar.BIGINT, false));
        SqlSource asTable = new SqlSource.Table("__se", "e", outs, false, Map.of(), com.legend.sql.NameQuoting.BARE);
        SqlSource asCte = new SqlSource.Cte("__se", "a", outs);
        String sql = new Snowflake().render(new SqlWith(List.of(new SqlWith.Cte("__se", selectOne(new SqlSource.Dual()))),
                selectOne(new SqlSource.Join(asTable, asCte, SqlSource.Join.Kind.CROSS, null))));
        assertTrue(sql.startsWith("WITH \"__se\" AS ("), sql);
        assertTrue(sql.contains("\"__se\" AS \"e\""), sql);
        assertTrue(sql.contains("\"__se\" AS \"a\""), sql);
        assertFalse(sql.matches("(?s).*[^\"]__se[^\"].*"), sql);
    }

    /**
     * A continuous quantile over a window is interpolated as DuckDB and the engine do, over the partition's values
     * sorted in the quantile's order (2026-10-21): Snowflake's own window PERCENTILE_CONT computes another last bit
     * (testPercentile_Relation_Window: 1.7999999999999998 for 1.8, measured on the lane).
     */
    @Test
    void aWindowsContinuousQuantileIsTheReferencesInterpolation() {
        SqlExpr v = SqlExpr.Column.of("t", "v", SqlType.Scalar.DOUBLE, true, OutputCol.Origin.PHYSICAL);
        SqlExpr k = SqlExpr.Column.of("t", "k", SqlType.Scalar.BIGINT, true, OutputCol.Origin.PHYSICAL);
        SqlExpr q = new SqlExpr.WindowCall(new com.legend.sql.SqlAgg.Reducer(com.legend.sql.SqlAgg.Fn.QUANTILE_CONT,
                List.of(v, new SqlExpr.FloatLit(0.6)), false, List.of(new SqlSelect.SortKey(v, false, null, null))),
                List.of(k), List.of(), null, com.legend.sql.SqlTyping.UNKNOWN);
        String sql = new Snowflake().render(new SqlSelect(List.of(new SqlSelect.Projection(q, "p", null)), false,
                new SqlSource.Table("t", "t", List.of(new OutputCol("v", SqlType.Scalar.DOUBLE, true),
                        new OutputCol("k", SqlType.Scalar.BIGINT, true)), false, Map.of(), com.legend.sql.NameQuoting.BARE),
                null, List.of(), null, null, List.of(), null, null, List.of()));
        assertTrue(sql.contains("ARRAY_SORT(ARRAY_COMPACT(ARRAY_AGG(\"t\".\"v\") OVER (PARTITION BY \"t\".\"k\")), FALSE)"),
                sql);
        assertTrue(sql.contains(" - FLOOR("), sql);
        assertFalse(sql.contains("PERCENTILE_CONT"), sql);
    }
}
