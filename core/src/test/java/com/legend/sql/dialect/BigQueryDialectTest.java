// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The BigQuery dialect's spellings no local database runs, pinned on the rendered text; each measured on the BigQuery
 * lane (2026-10-21, the plan route's first run there against the merge-base).
 */
class BigQueryDialectTest {

    private static final SqlExpr X = SqlExpr.Column.of("t", "x", SqlType.Scalar.DOUBLE, true, OutputCol.Origin.PHYSICAL);

    private static String render(SqlExpr e, @com.legend.base.Nullable OutputCol out) {
        return new BigQuery().render(new SqlSelect(List.of(new SqlSelect.Projection(e, "v", out)), false,
                new SqlSource.Table("t", "t", List.of(new OutputCol("x", SqlType.Scalar.DOUBLE, true)), false, Map.of(),
                        com.legend.sql.NameQuoting.BARE), null, List.of(), null, null, List.of(), null, null, List.of()));
    }

    /** A Float's text is its shortest digits that read back as the same double, as DuckDB's cast writes it: BigQuery's
     *  own cast writes seventeen (pi() '3.1415926535897931') and FORMAT's %.17g is not correctly rounded. */
    @Test
    void aFloatsTextIsItsShortestDigitsThatReadBack() {
        String sql = render(new SqlExpr.Cast(X, SqlType.Scalar.VARCHAR), null);
        // the value bound once; its exact BIGNUMERIC rounded to 15, 16 and 17 significant digits, each read back
        assertTrue(sql.contains("FROM (SELECT `t`.`x` AS `__fv`)"), sql);
        assertTrue(sql.contains("SAFE_CAST(`__fv` AS BIGNUMERIC) AS `__fn`"), sql);
        assertTrue(sql.contains("WHEN SAFE_CAST(CAST(ROUND(`__fn`, 14 - `__fe`, 'ROUND_HALF_EVEN') AS STRING) AS FLOAT64) = `__fv`"), sql);
        assertTrue(sql.contains("ELSE ROUND(`__fn`, 16 - `__fe`, 'ROUND_HALF_EVEN') END"), sql);
        assertFalse(sql.contains("FORMAT('%.1"), sql);
        assertFalse(sql.contains("`t`.`x` AS STRING"), sql);
    }

    /** A Float's print form reads its text once (2026-10-22): the value and its text bound in a one-row derived table
     *  under a scalar subquery, as the canon's binding; the portable spelling reads the text nine times, each read its
     *  own subquery here. (legend-engine's print form, and its served statement of 321 KB for {@code 1.5 + 2.25}, went
     *  with the database-written wires, 2026-10-27.) */
    @Test
    void aFloatsPrintFormsReadItsTextOnce() {
        for (SqlFn fn : List.of(SqlFn.FLOAT_PRINT)) {
            String sql = render(SqlExpr.Call.of(fn, X), null);
            // the double's text once (its own BIGNUMERIC read; the print form's HUGEINT and DECIMAL casts are BIGNUMERIC
            // here too)
            assertTrue(sql.split("SAFE_CAST\\(`__fv` AS BIGNUMERIC\\)", -1).length - 1 == 1, fn + ": " + sql);
            assertTrue(sql.split("`t`\\.`x`", -1).length - 1 == 1, fn + ": " + sql);
            assertTrue(sql.contains("AS `__fp0`"), fn + ": " + sql);
            assertTrue(sql.length() < 4_000, fn + ": " + sql.length() + ": " + sql);
        }
    }

    /** A projected error() is raised in its column's type (a bare ERROR is INT64 here: testNewDateError); a DATETIME
     *  through text, since an IF of a DATETIME with an ERROR branch fails before it raises. */
    @Test
    void aProjectedRaiseIsTypedAsItsColumn() {
        SqlExpr raise = new SqlExpr.Call(SqlFn.ERROR, List.of(new SqlExpr.StringLit("Invalid month: 13")));
        String sql = render(raise, new OutputCol("v", SqlType.Scalar.TIMESTAMP, false));
        assertTrue(sql.contains("CAST(IF(TRUE, ERROR(chr(31) || ('Invalid month: 13') || chr(31)), CAST(NULL AS STRING))"
                + " AS DATETIME)"), sql);
        String date = render(raise, new OutputCol("v", SqlType.Scalar.DATE, false));
        assertTrue(date.contains("IF(TRUE, ERROR(chr(31) || ('Invalid month: 13') || chr(31)), CAST(NULL AS DATE))"), date);
    }

    /** An element of the NULL list is a NULL, never a subscript of a bare NULL, which is INT64 ([]->head()). */
    @Test
    void anElementOfTheNullListIsANull() {
        String sql = render(new SqlExpr.Call(SqlFn.LIST_GET, List.of(new SqlExpr.NullLit(), new SqlExpr.IntLit(1))), null);
        assertFalse(sql.contains("SAFE_OFFSET"), sql);
    }

    /** A text cast to a timestamp has its fraction cut to BigQuery's microseconds: the judges' grid decode writes nine
     *  digits ("Invalid datetime string", channel B's lead-adjust tests). */
    @Test
    void aTextsFractionIsCutToMicrosecondsForATimestamp() {
        SqlExpr text = SqlExpr.Column.of("t", "s", SqlType.Scalar.VARCHAR, true, OutputCol.Origin.PHYSICAL);
        String sql = render(new SqlExpr.Cast(text, SqlType.Scalar.TIMESTAMP), null);
        assertTrue(sql.contains("CAST(REGEXP_REPLACE(`t`.`s`, '(\\\\.[0-9]{6})[0-9]+', '\\\\1') AS DATETIME)"), sql);
    }

    /** A text operand beside a JSON one in a COALESCE is parsed (the zips of Pairs of Pairs). */
    @Test
    void aCoalescesTextBesideJsonIsParsed() {
        SqlExpr json = SqlExpr.Column.of("t", "j", SqlType.Scalar.JSON, true, OutputCol.Origin.PHYSICAL);
        String sql = render(new SqlExpr.Call(SqlFn.COALESCE, List.of(json, new SqlExpr.StringLit("[]"))), null);
        assertTrue(sql.contains("COALESCE(`t`.`j`, PARSE_JSON('[]'))"), sql);
    }
}
