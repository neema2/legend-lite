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

    /** The parse guards write no raw '?': BigQuery's JDBC driver (1.6.0) counts a '?' inside a string literal as a
     *  placeholder and refuses the statement before BigQuery sees it ("No value was set for parameter 1 of 5", round
     *  73); the grammar's literals go through stringLit, which writes a '?' as \x3f. */
    @Test
    void theParseGuardsWriteNoRawQuestionMark() {
        SqlExpr text = SqlExpr.Column.of("t", "s", SqlType.Scalar.VARCHAR, true, OutputCol.Origin.PHYSICAL);
        String f = render(new SqlExpr.Cast(text, SqlType.Scalar.DOUBLE, SqlExpr.Reason.USER), null);
        String i = render(SqlExpr.Call.of(com.legend.sql.SqlFn.PARSE_INT, text), null);
        for (String sql : List.of(f, i)) {
            assertTrue(sql.contains("REGEXP_CONTAINS("), sql);
            assertFalse(sql.contains("?"), sql);
        }
    }

    /** A Float's text is its shortest digits that read back as the same double, as DuckDB's cast writes it: BigQuery's
     *  own cast writes seventeen (pi() '3.1415926535897931') and FORMAT's %.17g is not correctly rounded. */
    @Test
    void aFloatsTextIsItsShortestDigitsThatReadBack() {
        String sql = render(new SqlExpr.Cast(X, SqlType.Scalar.VARCHAR, SqlExpr.Reason.USER), null);
        // the value bound once; its exact BIGNUMERIC rounded to 15, 16 and 17 significant digits, each read back
        assertTrue(sql.contains("FROM (SELECT `t`.`x` AS `__fv`)"), sql);
        assertTrue(sql.contains("SAFE_CAST(`__fv` AS BIGNUMERIC) AS `__fn`"), sql);
        assertTrue(sql.contains("WHEN SAFE_CAST(CAST(ROUND(`__fn`, 14 - `__fe`, 'ROUND_HALF_EVEN') AS STRING) AS FLOAT64) = `__fv`"), sql);
        assertTrue(sql.contains("ELSE ROUND(`__fn`, 16 - `__fe`, 'ROUND_HALF_EVEN') END"), sql);
        assertFalse(sql.contains("FORMAT('%.1"), sql);
        assertFalse(sql.contains("`t`.`x` AS STRING"), sql);
    }

    // (aFloatsPrintFormsReadItsTextOnce: gone 2026-11-26 with FLOAT_SHORTEST_TEXT and the readsLongTextOnce hook, the
    // edition program's step 8, slice 1 -- legend-pure's print of a Float is the compatibility edge's alone, composed
    // at lowering from the product's nodes, edge.EdgePureOnlyTexts)

    /** The product's Float text (D1.2, 2026-11-15; the user's ruling of 2026-11-10): {@code FORMAT}'s six decimals,
     *  trimmed, BigQuery's {@code nan} and {@code inf} by Pure's names; the value read once, no derived table. */
    @Test
    void aFloatsTextIsSixDecimalsTrimmed() {
        String sql = render(SqlExpr.Call.of(SqlFn.FLOAT_TO_TEXT, X), null);
        // a negative value that rounds to zero is 0.0 (round 56, 2026-11-15: its -0.000000 written 0.000000 first); a
        // whole value's .000000 marked .# before the trim and written 0 after (S311 amended, the user's ruling of
        // 2026-11-23, "I think we do it as 2.0"; B2a, 2026-11-24)
        assertTrue(sql.contains("REPLACE(REPLACE(REPLACE(RTRIM(REPLACE(REPLACE(FORMAT('%.6f', `t`.`x`), '-0.000000',"
                + " '0.000000'), '.000000', '.#'), '0'), '#', '0'), 'nan', 'NaN'),"
                + " 'inf', 'Infinity')"), sql);
        assertTrue(sql.split("`t`\\.`x`", -1).length - 1 == 1, sql);
        assertFalse(sql.contains("(SELECT"), sql);
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
        String sql = render(new SqlExpr.Cast(text, SqlType.Scalar.TIMESTAMP, SqlExpr.Reason.USER), null);
        assertTrue(sql.contains("CAST(REGEXP_REPLACE(`t`.`s`, '(\\\\.[0-9]{6})[0-9]+', '\\\\1') AS DATETIME)"), sql);
    }

    /** A text operand beside a JSON one in a COALESCE is parsed (the zips of Pairs of Pairs). */
    @Test
    void aCoalescesTextBesideJsonIsParsed() {
        SqlExpr json = SqlExpr.Column.of("t", "j", SqlType.Scalar.JSON, true, OutputCol.Origin.PHYSICAL);
        String sql = render(new SqlExpr.Call(SqlFn.COALESCE, List.of(json, new SqlExpr.StringLit("[]"))), null);
        assertTrue(sql.contains("COALESCE(`t`.`j`, PARSE_JSON('[]'))"), sql);
    }

    /** A date or date-time literal BigQuery does not hold, a year before 1 or after 9999, is refused at planning by
     *  name (S340): the database itself refuses it, with the driver's bare "BigQueryException during runQuery"
     *  (measured on the lane, 2026-11-13). Its ends, 0001-01-01 and 9999-12-31, are held. */
    @Test
    void aDateLiteralOutsideBigQuerysRangeIsRefusedByName() {
        for (SqlExpr lit : List.of(new SqlExpr.DateLit("-43-03-15"), new SqlExpr.TimestampLit("-43-03-15T10:30:00"),
                new SqlExpr.DateLit("0-01-01"), new SqlExpr.DateLit("10000-01-01"))) {
            com.legend.sql.DialectCapability refused = org.junit.jupiter.api.Assertions.assertThrows(
                    com.legend.sql.DialectCapability.class, () -> render(lit, null), lit.toString());
            assertTrue(refused.getMessage().contains("outside BigQuery's range: it holds dates from 0001-01-01 to"
                    + " 9999-12-31"), refused.getMessage());
        }
        assertTrue(render(new SqlExpr.DateLit("0001-01-01"), null).contains("DATE '0001-01-01'"));
        assertTrue(render(new SqlExpr.DateLit("9999-12-31"), null).contains("DATE '9999-12-31'"));
        assertTrue(render(new SqlExpr.TimestampLit("2024-01-02T10:30:00"), null).contains("DATETIME '2024-01-02 10:30:00'"));
    }
}
