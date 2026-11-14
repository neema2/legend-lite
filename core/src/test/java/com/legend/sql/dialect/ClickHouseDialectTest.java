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

import static org.junit.jupiter.api.Assertions.assertEquals;
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

    /**
     * A table of the database's own is legend-engine's ClickHouse table, {@code ENGINE = MergeTree ORDER BY tuple()}; a
     * keyed one sorts by its key (register S157, 2026-11-07: ClickHouse refuses the engine's text for a keyed table); a
     * session's TEMPORARY table names no engine.
     */
    @Test
    void aTableIsAMergeTreeOneSortedByItsKey() {
        com.legend.sql.SqlDdl.ColumnType text = new com.legend.sql.SqlDdl.ColumnType.Plain(
                com.legend.sql.SqlDdl.ColumnType.Kind.DATE);
        List<com.legend.sql.SqlDdl.Column> keyed = List.of(new com.legend.sql.SqlDdl.Column("d", false, text, false,
                true), new com.legend.sql.SqlDdl.Column("e", false, text, false, true),
                new com.legend.sql.SqlDdl.Column("v", false, text, false, false));
        List<com.legend.sql.SqlDdl.Column> loose = List.of(new com.legend.sql.SqlDdl.Column("v", false, text, false,
                false));
        ClickHouse ch = new ClickHouse();
        String k = ch.render(new com.legend.sql.SqlDdl.CreateTable(null, "t", com.legend.sql.NameQuoting.BARE, keyed));
        assertTrue(k.endsWith(") ENGINE = MergeTree ORDER BY (\"d\", \"e\");"), k);
        assertTrue(k.contains("PRIMARY KEY("), k);
        String l = ch.render(new com.legend.sql.SqlDdl.CreateTable(null, "t", com.legend.sql.NameQuoting.BARE, loose));
        assertTrue(l.endsWith(") ENGINE = MergeTree ORDER BY tuple();"), l);
        String temp = ch.render(new com.legend.sql.SqlDdl.CreateTable(null, "t", com.legend.sql.NameQuoting.BARE, loose,
                true));
        assertFalse(temp.contains("ENGINE"), temp);
    }

    @Test
    void aStagedTableIsDroppedAsTheSessionsOwn() {
        // the plain drop of a name the session holds no temporary table of drops the database's table of that name
        // (measured 2026-11-09 on 26.3 with clickhouse local; the final review of PR #81)
        ClickHouse ch = new ClickHouse();
        assertEquals("DROP TEMPORARY TABLE IF EXISTS \"__stage0__let0\";", ch.render(
                new com.legend.sql.SqlDdl.DropTable(null, "__stage0__let0", com.legend.sql.NameQuoting.BARE, true)));
        assertTrue(ch.render(new com.legend.sql.SqlDdl.DropTable(null, "t", com.legend.sql.NameQuoting.BARE))
                .startsWith("Drop table if exists "));
    }

    @Test
    void aComputedKeyIsGroupedByItsName() {
        // each raise is written with a number of its own, so a key holding one (a date's range guard) written twice
        // differed from itself and ClickHouse refused the select (measured 2026-11-09 on 26.3; the final review of PR
        // #81): a computed key the select names is grouped by that name, a plain column as it is
        SqlSource.Table t = table("t", "t0", "d", SqlType.Scalar.DATE);
        SqlExpr d = col("t0", "d", SqlType.Scalar.DATE);
        SqlExpr later = new SqlExpr.Call(SqlFn.ADD_INTERVAL, List.of(new SqlExpr.DatePartLit(com.legend.sql.DatePart.DAY),
                new SqlExpr.IntLit(1), d));
        SqlSelect grouped = new SqlSelect(List.of(new SqlSelect.Projection(later, "d2", null),
                new SqlSelect.Projection(d, "d", null)), false, t, null, List.of(later, d), null, null, List.of(), null,
                null, List.of());
        String sql = render(grouped);
        String keys = sql.substring(sql.indexOf("GROUP BY "));
        assertTrue(keys.startsWith("GROUP BY \"d2\", \"t0\".\"d\""), sql);
        assertEquals(1, sql.split("VALUE_NOT_HELD", -1).length - 1, "the guard written once: " + sql);
    }

    /** ClickHouse's {@code *} reads no ARRAY JOIN column, so a star over a lateral explosion names the exploded column
     *  after it; with the root's columns labeled (0276a473e) the star is each source but the explosion, so every column
     *  is named once (round 37, 2026-11-12: {@code testVariantMapColumn_values_LateralFlatten} answered 4 columns for
     *  its 3, the exploded one twice). */
    @Test
    void aStarOverAnArrayJoinNamesEachColumnOnce() {
        SqlType ints = new SqlType.Array(SqlType.Scalar.BIGINT);
        SqlSource.Table people = new SqlSource.Table("people", "t0", List.of(new OutputCol("id", SqlType.Scalar.BIGINT,
                true), new OutputCol("xs", ints, true)), false, Map.of(), com.legend.sql.NameQuoting.BARE);
        SqlSelect marker = new SqlSelect(List.of(new SqlSelect.Projection(new SqlExpr.Call(SqlFn.UNNEST,
                List.of(col("t0", "xs", ints))), "x", null)), false, new SqlSource.Dual(), null, List.of(), null, null,
                List.of(), null, null, List.of());
        for (SqlSource.Join.Kind kind : List.of(SqlSource.Join.Kind.CROSS_LATERAL, SqlSource.Join.Kind.LEFT_LATERAL)) {
            // a LEFT lateral carries ON TRUE (its join requires one); a CROSS lateral none
            SqlSource from = new SqlSource.Join(people, new SqlSource.Subselect(marker, "t1", null), kind,
                    kind == SqlSource.Join.Kind.LEFT_LATERAL ? new SqlExpr.BoolLit(true) : null);
            String sql = render(new SqlSelect(List.of(), false, from, null, List.of(), null, null, List.of(), null, null,
                    List.of()));
            String head = sql.substring(0, sql.indexOf("FROM"));
            assertTrue(sql.contains("ARRAY JOIN"), sql);
            assertTrue(head.contains("AS \"id\""), sql);
            assertTrue(head.contains("AS \"xs\""), sql);
            assertTrue(head.indexOf("AS \"x\"") >= 0 && head.indexOf("AS \"x\"") == head.lastIndexOf("AS \"x\""), sql);
            assertFalse(head.contains("*"), sql);
        }
    }

    /** A source joined after an explosion keeps its place: the select answers the join's order, as the plan declares
     *  it, since rows are read by position (the B close's audit, SHOULD-FIX 2, 2026-11-12: the exploded columns were
     *  written after every source's star). */
    @Test
    void aSourceAfterAnArrayJoinKeepsTheJoinsOrder() {
        SqlType ints = new SqlType.Array(SqlType.Scalar.BIGINT);
        SqlSource.Table people = new SqlSource.Table("people", "t0", List.of(new OutputCol("id", SqlType.Scalar.BIGINT,
                true), new OutputCol("xs", ints, true)), false, Map.of(), com.legend.sql.NameQuoting.BARE);
        SqlSource.Table towns = table("towns", "t2", "town", SqlType.Scalar.VARCHAR);
        SqlSelect marker = new SqlSelect(List.of(new SqlSelect.Projection(new SqlExpr.Call(SqlFn.UNNEST,
                List.of(col("t0", "xs", ints))), "x", null)), false, new SqlSource.Dual(), null, List.of(), null, null,
                List.of(), null, null, List.of());
        SqlSource exploded = new SqlSource.Join(people, new SqlSource.Subselect(marker, "t1", null),
                SqlSource.Join.Kind.CROSS_LATERAL, null);
        SqlSource from = new SqlSource.Join(exploded, towns, SqlSource.Join.Kind.CROSS, null);
        String sql = render(new SqlSelect(List.of(), false, from, null, List.of(), null, null, List.of(), null, null,
                List.of()));
        String head = sql.substring(0, sql.indexOf("FROM"));
        int id = head.indexOf("AS \"id\""), xs = head.indexOf("AS \"xs\""), x = head.indexOf("AS \"x\""),
                town = head.indexOf("AS \"town\"");
        assertTrue(id >= 0 && id < xs && xs < x && x < town, sql);
    }
}
