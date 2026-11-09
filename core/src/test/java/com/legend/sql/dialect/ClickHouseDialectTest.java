// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.DialectCapability;

import com.legend.sql.OutputCol;
import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The ClickHouse dialect's rewrites that PCT's inputs do not reach, pinned on the rendered text (PR #34's
 * independent review, 2026-10-13). Each shape was run on ClickHouse 25.8.2.29, the lanes' server.
 */
class ClickHouseDialectTest {

    private static final SqlType CITY = new SqlType.Struct(List.of(new SqlType.Struct.Field("city",
            SqlType.Scalar.VARCHAR)));
    private static final SqlType PERSON = new SqlType.Struct(List.of(
            new SqlType.Struct.Field("name", SqlType.Scalar.VARCHAR), new SqlType.Struct.Field("address", CITY)));

    private static SqlExpr.Column col(String table, String name, SqlType t) {
        return SqlExpr.Column.of(table, name, t, true, OutputCol.Origin.PHYSICAL);
    }

    private static SqlSource.Table table(String name, String alias, String col, SqlType t) {
        return new SqlSource.Table(name, alias, List.of(new OutputCol(col, t, true)), false, Map.of(), com.legend.sql.NameQuoting.BARE);
    }

    private static SqlSelect select(SqlSource from, @com.legend.base.Nullable SqlExpr where, SqlExpr... values) {
        List<SqlSelect.Projection> ps = new java.util.ArrayList<>();
        for (int i = 0; i < values.length; i++) {
            ps.add(new SqlSelect.Projection(values[i], "c" + i, null));
        }
        return new SqlSelect(ps, false, from, where, List.of(), null, null, List.of(), null, null, List.of());
    }

    private static String render(SqlQuery q) {
        return new ClickHouse().render(q);
    }

    @Test
    void aStructFieldIsItsVariantAndOnlyAFieldReadOfItUnwrapsIt() {
        SqlExpr p = col("t0", "p", PERSON);
        SqlExpr address = new SqlExpr.StructGet(p, "address", new com.legend.sql.TypeFact.Typed(CITY, true));
        SqlExpr city = new SqlExpr.StructGet(address, "city",
                new com.legend.sql.TypeFact.Typed(SqlType.Scalar.VARCHAR, true));
        String sql = render(select(table("people", "t0", "p", PERSON), null,
                new SqlExpr.Call(SqlFn.IS_NULL, List.of(address)), city));
        // the absent instance is the NULL Variant: its IS NULL reads the Variant, not a Tuple of NULLs (never NULL)
        assertTrue(sql.contains("tupleElement(\"t0\".\"p\", 'address') IS NULL"), sql);
        // a field of it reads the Tuple the Variant holds
        assertTrue(sql.contains("tupleElement(variantElement(tupleElement(\"t0\".\"p\", 'address'), "), sql);
    }

    @Test
    void aCorrelatedExistsRunsOnlyUnderANullRejectingFilter() {
        SqlExpr id = col("t0", "id", SqlType.Scalar.BIGINT);
        SqlExpr dept = col("e", "dept", SqlType.Scalar.BIGINT);
        SqlSource.Table e = table("employees", "e", "dept", SqlType.Scalar.BIGINT);
        SqlSource.Table d = table("departments", "t0", "id", SqlType.Scalar.BIGINT);
        SqlExpr matches = new SqlExpr.Call(SqlFn.EQUAL, List.of(dept, id));
        // e.dept = d.id: an outer row with a NULL id matches no row, which is what ClickHouse answers for it
        String sql = render(select(d, new SqlExpr.Exists(select(e, matches, new SqlExpr.IntLit(1))), id));
        assertTrue(sql.contains("allow_experimental_correlated_subqueries = 1"), sql);
        // ... OR d.id IS NULL keeps the NULL-id row, which ClickHouse 25.8 drops (measured): refused by name
        SqlExpr keepsNull = new SqlExpr.Call(SqlFn.OR, List.of(matches, new SqlExpr.Call(SqlFn.IS_NULL, List.of(id))));
        assertThrows(DialectCapability.class,
                () -> render(select(d, new SqlExpr.Exists(select(e, keepsNull, new SqlExpr.IntLit(1))), id)));
        // a correlated quantified comparison is checked the same way
        SqlExpr above = new SqlExpr.Quantified(id, SqlFn.GREATER, SqlExpr.Quantifier.ALL, select(e, matches, dept),
                new com.legend.sql.TypeFact.Typed(SqlType.Scalar.BOOLEAN, true));
        assertTrue(render(select(d, above, id)).contains("allow_experimental_correlated_subqueries = 1"));
        SqlExpr aboveKeepingNull = new SqlExpr.Quantified(id, SqlFn.GREATER, SqlExpr.Quantifier.ALL,
                select(e, keepsNull, dept), new com.legend.sql.TypeFact.Typed(SqlType.Scalar.BOOLEAN, true));
        assertThrows(DialectCapability.class, () -> render(select(d, aboveKeepingNull, id)));
        // every other statement turns the setting off (25.8's default is on), so no other correlated subquery runs
        assertTrue(render(select(d, null, id)).contains("allow_experimental_correlated_subqueries = 0"));
    }

    private static String fractionalRange(SqlAgg.Fn fn) {
        SqlExpr k = col("t0", "k", SqlType.Scalar.DOUBLE);
        SqlExpr v = col("t0", "v", SqlType.Scalar.DOUBLE);
        SqlExpr w = new SqlExpr.WindowCall(new SqlAgg.Reducer(fn, List.of(v), false, List.of()), List.of(),
                List.of(new SqlSelect.SortKey(k, true, null, null)),
                new SqlExpr.WindowCall.Frame(SqlExpr.WindowCall.Frame.Kind.RANGE,
                        SqlExpr.WindowCall.Frame.Bound.Preceding.of(0.5),
                        SqlExpr.WindowCall.Frame.Bound.Following.of(0.5)));
        return render(select(table("t", "t0", "k", SqlType.Scalar.DOUBLE), null, w));
    }

    @Test
    void aFractionalRangeFrameReducesWithTheNativeAggregate() {
        // COUNT an Int64, as the native path's
        assertTrue(fractionalRange(SqlAgg.Fn.COUNT).contains("toInt64(arrayReduce('count', "));
        // a sample statistic: its OrNull form, NULL under two values (ClickHouse's one-value answer is inf)
        String sd = fractionalRange(SqlAgg.Fn.STDDEV_SAMP);
        assertTrue(sd.contains("arrayReduce('stddevSampStableOrNull', ") && sd.contains("if(arrayReduce('count', "),
                sd);
        // the exact median, not ClickHouse's approximate one
        String median = fractionalRange(SqlAgg.Fn.MEDIAN);
        assertTrue(median.contains("arrayReduce('quantileExactInclusiveOrNull(0.5)', "), median);
        assertFalse(median.contains("'median'"), median);
        // anything else is refused by name
        assertThrows(DialectCapability.class, () -> fractionalRange(SqlAgg.Fn.MODE));
    }

    @Test
    void aNegationBracketsItsWholeOperand() {
        // ClickHouse reads `NOT (` as its function not(: an operand that only starts with a bracket would be cut short
        // (PCT round 21, 2026-11-05: `NOT (SELECT ...) = (SELECT ...)` refused, "failed at position ... (toInt64)")
        SqlSource.Table t = table("t", "t0", "k", SqlType.Scalar.BIGINT);
        SqlExpr k = col("t0", "k", SqlType.Scalar.BIGINT);
        SqlExpr one = new SqlExpr.ScalarSubquery(select(t, null, k),
                new com.legend.sql.TypeFact.Typed(SqlType.Scalar.BIGINT, true));
        SqlExpr two = new SqlExpr.ScalarSubquery(select(t, null, k),
                new com.legend.sql.TypeFact.Typed(SqlType.Scalar.BIGINT, true));
        String sql = render(select(t, SqlExpr.Call.of(SqlFn.NOT, SqlExpr.Call.of(SqlFn.EQUAL, one, two)), k));
        assertTrue(sql.contains("WHERE NOT ((SELECT \"t0\".\"k\" AS \"c0\" FROM \"t\" AS \"t0\")"
                + " = (SELECT \"t0\".\"k\" AS \"c0\" FROM \"t\" AS \"t0\"))\n"), sql);
    }
}
