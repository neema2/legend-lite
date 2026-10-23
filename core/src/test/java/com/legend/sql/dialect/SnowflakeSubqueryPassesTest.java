// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.NameQuoting;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Snowflake's subquery passes (the IQ scale program, docs/IQ_SCALE_PROGRAM_2026_10_22.md Q24): each rewrite Snowflake
 * evaluates where it refused the lowering's form ("Unsupported subquery type cannot be evaluated", the IQ parity lane
 * on a real account), and each shape an independent review (2026-10-23) found a rewrite changing the answer of, which
 * now stays as it is.
 */
class SnowflakeSubqueryPassesTest {

    private static final SqlType INT = SqlType.Scalar.INTEGER;

    private static SqlExpr.Column col(String table, String name) {
        return SqlExpr.Column.of(table, name, INT, true, OutputCol.Origin.PHYSICAL);
    }

    private static SqlSource table(String name, String alias, String... columns) {
        List<OutputCol> outs = java.util.Arrays.stream(columns).map(c -> new OutputCol(c, INT, true)).toList();
        return new SqlSource.Table(name, alias, outs, false, Map.of(), NameQuoting.BARE);
    }

    private static SqlSelect select(List<SqlSelect.Projection> items, SqlSource from, SqlExpr where) {
        return new SqlSelect(items, false, from, where, List.of(), null, null, List.of(), null, null, List.of());
    }

    private static SqlSelect.Projection item(SqlExpr e, String name) {
        return new SqlSelect.Projection(e, name, new OutputCol(name, INT, true));
    }

    private static SqlExpr and(SqlExpr... parts) {
        SqlExpr out = parts[0];
        for (int i = 1; i < parts.length; i++) {
            out = SqlExpr.Call.of(SqlFn.AND, out, parts[i]);
        }
        return out;
    }

    private static SqlExpr notNull(SqlExpr e) {
        return SqlExpr.Call.of(SqlFn.IS_NOT_NULL, e);
    }

    /** {@code SELECT t0.id FROM t t0 WHERE where}. */
    private static String filtered(SqlExpr where) {
        return render(select(List.of(item(col("t0", "id"), "id")), table("t", "t0", "id", "vc", "arr"), where));
    }

    private static String render(SqlQuery q) {
        return new Snowflake().render(q).replace('\n', ' ');
    }

    private static SqlSelect one(SqlSource from, SqlExpr where) {
        return select(List.of(new SqlSelect.Projection(new SqlExpr.IntLit(1), null, null)), from, where);
    }

    @Test
    void aCorrelatedCountThroughDerivedTablesIsOneSelect() {
        SqlSelect count = select(List.of(item(SqlAgg.Reducer.of(SqlAgg.Fn.COUNT), "c")), table("u", "t1", "id", "t_id"),
                SqlExpr.Call.of(SqlFn.EQUAL, col("t1", "t_id"), col("t0", "id")));
        SqlSelect mid = select(List.of(item(col("t2", "c"), "value")), new SqlSource.Subselect(count, "t2", null), null);
        SqlSelect top = select(List.of(item(col("t3", "value"), "value")), new SqlSource.Subselect(mid, "t3", null),
                null);
        String sql = render(select(List.of(item(col("t0", "id"), "id"), item(new SqlExpr.ScalarSubquery(top), "n")),
                table("t", "t0", "id"), null));
        assertTrue(sql.contains("(SELECT COUNT(*) AS \"value\" FROM \"u\" AS \"t1\" WHERE \"t1\".\"t_id\" = \"t0\".\"id\")"),
                sql);
    }

    /** {@code (SELECT x.k FROM (SELECT 'a' AS k, COUNT(*) AS c FROM u WHERE u.t_id = t.id) x)} is one row, 'a': read
     *  alone, k would be one row per u row. */
    @Test
    void aColumnBesideAnUngroupedAggregateIsNotFlattened() {
        SqlSelect agg = select(List.of(new SqlSelect.Projection(new SqlExpr.StringLit("a"), "k", null),
                item(SqlAgg.Reducer.of(SqlAgg.Fn.COUNT), "c")), table("u", "t1", "id", "t_id"),
                SqlExpr.Call.of(SqlFn.EQUAL, col("t1", "t_id"), col("t0", "id")));
        SqlSelect read = select(List.of(item(col("x", "k"), "value")), new SqlSource.Subselect(agg, "x", null), null);
        String sql = render(select(List.of(item(col("t0", "id"), "id"), item(new SqlExpr.ScalarSubquery(read), "r")),
                table("t", "t0", "id"), null));
        assertTrue(sql.contains("COUNT(*)"), sql);
        assertFalse(sql.contains("ANY_VALUE"), sql);
    }

    @Test
    void aCorrelatedReadOfNoAggregateIsItsRowsOneValue() {
        SqlSelect label = select(List.of(item(col("t1", "label"), "value")), table("u", "t1", "id", "label"),
                SqlExpr.Call.of(SqlFn.EQUAL, col("t1", "id"), col("t0", "id")));
        String sql = render(select(List.of(item(col("t0", "id"), "id"), item(new SqlExpr.ScalarSubquery(label), "l")),
                table("t", "t0", "id"), null));
        assertTrue(sql.contains("CASE WHEN COUNT(*) > 1 THEN"), sql);
        assertTrue(sql.contains("ANY_VALUE(\"t1\".\"label\")"), sql);
    }

    /** The island's {@code id NOT IN (SELECT t_id FROM u)}, two-valued, is Snowflake's {@code <> ALL} in a WHERE. */
    @Test
    void theIslandsNotInIsAnAll() {
        SqlExpr notIn = SqlExpr.Call.of(SqlFn.NOT, new SqlExpr.Exists(one(table("u", "t1", "t_id"),
                SqlExpr.Call.of(SqlFn.NOT, new SqlExpr.Group(and(notNull(col("t0", "id")), notNull(col("t1", "t_id")),
                        SqlExpr.Call.of(SqlFn.IS_DISTINCT_FROM, col("t0", "id"), col("t1", "t_id"))))))));
        String sql = filtered(notIn);
        assertTrue(sql.contains("WHERE \"t0\".\"id\" <> ALL (SELECT \"t1\".\"t_id\" FROM \"u\" AS \"t1\")"), sql);
        // under a NOT, where the ALL's unknown and the NOT EXISTS's false part ways, it is refused by name: Snowflake
        // evaluates the NOT EXISTS in a WHERE's condition alone
        DialectCapability refused = assertThrows(DialectCapability.class,
                () -> filtered(SqlExpr.Call.of(SqlFn.NOT, notIn)));
        assertTrue(refused.getMessage().contains("NOT IN or ALL"), refused.getMessage());
    }

    /** SQL Anywhere's correlated {@code (SELECT TOP 1 label FROM u WHERE u.t_id = t.id ORDER BY u.id DESC)}. */
    /** A correlated read of one group of a grouped derived table: its value, with no count, which Snowflake's
     *  internal error 370001 met over a grouped derived table (the IQ scale program's Q24, measured 2026-10-23). A
     *  WHERE that fixes no key keeps the count. */
    @Test
    void aCorrelatedReadOfOneGroupIsItsValue() {
        SqlSelect grouped = new SqlSelect(List.of(item(col("t1", "t_id"), "t_id"),
                item(SqlAgg.Reducer.of(SqlAgg.Fn.COUNT), "n")), false, table("u", "t1", "t_id"), null,
                List.of(col("t1", "t_id")), null, null, List.of(), null, null, List.of());
        SqlSource q = new SqlSource.Subselect(grouped, "t2", null);
        SqlSelect byKey = select(List.of(item(col("t2", "n"), "value")), q,
                and(notNull(col("t2", "t_id")), notNull(col("t0", "id")),
                        SqlExpr.Call.of(SqlFn.EQUAL, col("t2", "t_id"), col("t0", "id"))));
        String sql = render(select(List.of(item(col("t0", "id"), "id"), item(new SqlExpr.ScalarSubquery(byKey), "n")),
                table("t", "t0", "id"), null));
        assertTrue(sql.contains("ANY_VALUE(\"t2\".\"n\")") && !sql.contains("COUNT(*) > 1"), sql);
        SqlSelect byCount = select(List.of(item(col("t2", "t_id"), "value")), q,
                SqlExpr.Call.of(SqlFn.EQUAL, col("t2", "n"), col("t0", "id")));
        String counted = render(select(List.of(item(col("t0", "id"), "id"),
                item(new SqlExpr.ScalarSubquery(byCount), "k")), table("t", "t0", "id"), null));
        assertTrue(counted.contains("CASE WHEN COUNT(*) > 1 THEN"), counted);
    }

    @Test
    void aCorrelatedScalarSubqueryOfARowLimitIsRefused() {
        SqlSelect top = new SqlSelect(List.of(item(col("t1", "label"), "label")), false,
                table("u", "t1", "t_id", "label"), SqlExpr.Call.of(SqlFn.EQUAL, col("t1", "t_id"), col("t0", "id")),
                List.of(), null, null, List.of(new SqlSelect.SortKey(col("t1", "id"), false, null, null)), 1L, null,
                List.of());
        SqlSelect read = select(List.of(item(col("t2", "label"), "value")), new SqlSource.Subselect(top, "t2", null),
                null);
        DialectCapability refused = assertThrows(DialectCapability.class, () -> render(select(List.of(
                item(col("t0", "id"), "id"), item(new SqlExpr.ScalarSubquery(read), "l")), table("t", "t0", "id"),
                null)));
        assertTrue(refused.getMessage().contains("row limit"), refused.getMessage());
    }

    @Test
    void anExistsOverOneRowOfConstantsIsItsCondition() {
        SqlSource dummy = new SqlSource.Values(List.of(List.of(new SqlExpr.IntLit(0))), List.of("dummy_col"), "d",
                List.of(new OutputCol("dummy_col", INT, false)));
        String sql = filtered(new SqlExpr.Exists(one(dummy, SqlExpr.Call.of(SqlFn.EQUAL, col("t0", "vc"),
                new SqlExpr.StringLit("x")))));
        assertTrue(sql.contains("coalesce(\"t0\".\"vc\" = 'x', FALSE)"), sql);
        assertFalse(sql.contains("EXISTS"), sql);
    }

    /** sa_rowgenerator's explosion has none to many rows: an EXISTS over one is no constant (it was NOT TRUE). */
    @Test
    void anExistsOverAnExplosionIsNoConstant() {
        SqlType list = new SqlType.Array(INT);
        SqlSelect marker = select(List.of(new SqlSelect.Projection(SqlExpr.Call.of(SqlFn.UNNEST,
                SqlExpr.Column.of("t0", "arr", list, true, OutputCol.Origin.PHYSICAL)), "e", new OutputCol("e", INT,
                        true))), new SqlSource.Dual(), null);
        String sql;
        try {
            sql = filtered(SqlExpr.Call.of(SqlFn.NOT, new SqlExpr.Exists(marker)));
        } catch (DialectCapability refused) {
            return;
        }
        assertFalse(sql.contains("NOT TRUE"), sql);
    }

    @Test
    void aGuardOfAComparedColumnIsDroppedAndTheOuterRowsTestHoisted() {
        String sql = filtered(new SqlExpr.Exists(one(table("u", "t1", "id"), and(notNull(col("t1", "id")),
                notNull(col("t0", "id")), SqlExpr.Call.of(SqlFn.EQUAL, col("t1", "id"), col("t0", "id")),
                SqlExpr.Call.of(SqlFn.GREATER, col("t0", "vc"), new SqlExpr.IntLit(3))))));
        assertTrue(sql.contains("EXISTS (SELECT 1 FROM \"u\" AS \"t1\" WHERE \"t1\".\"id\" = \"t0\".\"id\")"), sql);
        assertTrue(sql.contains("coalesce(\"t0\".\"vc\" > 3, FALSE)"), sql);
        assertFalse(sql.contains("IS NOT NULL"), sql);
    }

    /** SQL Anywhere's TOP 1 START AT 2 in an EXISTS: a row past the first. */
    @Test
    void anExistsPastAnOffsetCountsItsRows() {
        SqlSelect limited = new SqlSelect(List.of(new SqlSelect.Projection(new SqlExpr.IntLit(1), null, null)), false,
                table("u", "t1", "t_id"), SqlExpr.Call.of(SqlFn.EQUAL, col("t1", "t_id"), col("t0", "id")), List.of(),
                null, null, List.of(), 1L, 1L, List.of());
        String sql = filtered(new SqlExpr.Exists(limited));
        assertTrue(sql.contains("(SELECT COUNT(*) FROM \"u\" AS \"t1\" WHERE \"t1\".\"t_id\" = \"t0\".\"id\") > 1"), sql);
    }

    @Test
    void aDoubleLiteralPastANumbersDigitsIsADouble() {
        String sql = filtered(SqlExpr.Call.of(SqlFn.GREATER, col("t0", "id"),
                new SqlExpr.DecimalLit(new BigDecimal("3.4028234663852886E38"))));
        assertTrue(sql.contains("CAST('3.4028234663852886E38' AS DOUBLE)"), sql);
    }

    /** A HUGEINT literal past 38 digits (10^38 to 2^127): no NUMBER holds it, and a DOUBLE would round it. */
    @Test
    void aHugeintLiteralPastANumbersDigitsIsRefused() {
        SqlExpr huge = new SqlExpr.DecimalLit(new BigDecimal("100000000000000000000000000000000000001"));
        DialectCapability refused = assertThrows(DialectCapability.class,
                () -> filtered(SqlExpr.Call.of(SqlFn.GREATER, col("t0", "id"), huge)));
        assertTrue(refused.getMessage().contains("38 digits"), refused.getMessage());
    }
}
