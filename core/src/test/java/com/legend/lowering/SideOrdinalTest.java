// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** A grid side's row ordinal is computed where its plan defines the order (SideOrdinal; channel B's reduce and pivot
 *  tests on MariaDB and Trino, 2026-10-20, whose databases kept neither a sorted CTE's order nor a literal's). */
class SideOrdinalTest {

    /** The platform's own engine's dialect: the spelling the assertions read. */
    private static final com.legend.sql.dialect.SqlDialect PLATFORM =
            com.legend.database.Databases.dialect(com.legend.database.Databases.PLATFORM);

    private static final OutputCol A = new OutputCol("a", SqlType.Scalar.INTEGER, false);

    private static SqlSource.Values literal() {
        return new SqlSource.Values(List.of(List.of(new SqlExpr.IntLit(2)), List.of(new SqlExpr.IntLit(1))),
                List.of("a"), "t", List.of(A));
    }

    @Test
    void aLiteralsRowsCarryTheirIndex() {
        SqlQuery q = SideOrdinal.withOrdinal(SqlSelect.starOf(literal()).withOutputs(List.of(A)));
        assertEquals(List.of("a", SideOrdinal.ORD), q.outputs().stream().map(OutputCol::name).toList());
        // 2026-11-12 (the total wall's B): alone, the side is a root, labeled (AnsiSqlRenderer#labeled)
        assertEquals("""
                SELECT t.a AS a, t.__ord AS __ord
                FROM (VALUES (2, 1), (1, 2)) AS t(a, __ord)""", PLATFORM.render(q));
    }

    @Test
    void aSortedSideIsNumberedByItsOwnKeysInsideItself() {
        // no select wrapped around the side: H2 re-parses each derived table per plan it weighs (2026-10-21)
        SqlSelect sorted = SqlSelect.starOf(literal()).withOutputs(List.of(A))
                .withOrderBy(List.of(new SqlSelect.SortKey(SqlExpr.Column.of("t", A), false, null, null)));
        String sql = PLATFORM.render(SideOrdinal.withOrdinal(sorted));
        // 2026-11-12 (the total wall's B): alone, the side is a root, labeled (AnsiSqlRenderer#labeled)
        assertEquals(true, sql.startsWith("SELECT t.a AS a, ROW_NUMBER() OVER (ORDER BY t.a DESC"), sql);
        assertEquals(false, sql.contains("__o."), sql);
    }

    @Test
    void aDistinctSortedSideIsNumberedOutsideIt() {
        // inside, the ordinal would count among its distinct columns
        SqlSelect sorted = SqlSelect.starOf(literal()).withOutputs(List.of(A)).withDistinct()
                .withOrderBy(List.of(new SqlSelect.SortKey(SqlExpr.Column.of("t", A), false, null, null)));
        String sql = PLATFORM.render(SideOrdinal.withOrdinal(sorted));
        assertEquals(1, sql.split("ROW_NUMBER\\(\\) OVER \\(ORDER BY __o\\.a DESC", -1).length - 1, sql);
    }

    @Test
    void aSideSortedByWhatItDoesNotOutputIsNumberedInsideItself() {
        // the scan order's key (ScanOrder): a table's row ids, which no output carries
        SqlSource.Table t = new SqlSource.Table("T", "t", List.of(A), false, java.util.Map.of(),
                com.legend.sql.NameQuoting.BARE);
        SqlSelect sorted = SqlSelect.starOf(t).withOutputs(List.of(A))
                .withOrderBy(List.of(new SqlSelect.SortKey(new SqlExpr.RowOrder("t"), true, null, null)));
        String sql = PLATFORM.render(SideOrdinal.withOrdinal(sorted));
        // 2026-11-12 (the total wall's B): alone, the side is a root, labeled (AnsiSqlRenderer#labeled)
        assertEquals(true, sql.startsWith("SELECT t.a AS a, ROW_NUMBER() OVER (ORDER BY t.rowid"), sql);
    }

    @Test
    void aSideWhoseOrderThePlanDoesNotDefineIsNumberedByArrivalInsideItself() {
        SqlSelect grouped = SqlSelect.starOf(literal()).withOutputs(List.of(A)).withGroupBy(
                List.of(SqlExpr.Column.of("t", A)));
        String sql = PLATFORM.render(SideOrdinal.withOrdinal(grouped));
        assertEquals(1, sql.split("ROW_NUMBER\\(\\) OVER \\(\\)", -1).length - 1, sql);
        assertEquals(false, sql.contains("__o."), sql);
    }

    private static final OutputCol C = new OutputCol("c", SqlType.Scalar.INTEGER, false);

    /** {@code concatenate(literal, literal)->extend(~c: 1)}: a star over the union, a column after it. */
    private static SqlSelect extendedUnion() {
        // each branch a projection of its literal (a union the database judge numbers by branch)
        SqlQuery left = SqlSelect.starOf(literal()).withProjections(List.of(
                new SqlSelect.Projection(SqlExpr.Column.of("t", A), "a", A)));
        SqlQuery right = SqlSelect.starOf(literal()).withProjections(List.of(
                new SqlSelect.Projection(SqlExpr.Column.of("t", A), "a", A)));
        com.legend.sql.SqlUnion u = new com.legend.sql.SqlUnion(List.of(left, right), true, List.of(A));
        return SqlSelect.starOf(new SqlSource.Subselect(u, "u", null)).withProjections(List.of(
                new SqlSelect.Projection(new SqlExpr.Star("u"), null, null),
                new SqlSelect.Projection(new SqlExpr.IntLit(1), "c", C)));
    }

    @Test
    void aStarOverANumberedUnionKeepsTheOrdinalLast() {
        // the final audit, 2026-11-06: the star read the union's ordinal in the middle of the outputs, and the verdict
        // canon reads a numbered side's ordinal by position -- it read c as the ordinal and __ord as a column
        SqlQuery q = SideOrdinal.withOrdinal(extendedUnion());
        assertEquals(List.of("a", "c", SideOrdinal.ORD), q.outputs().stream().map(OutputCol::name).toList());
        // the outer star spelled as the union's columns (no star left to read the ordinal in the middle), the
        // ordinal written once, last, read from the union numbered by branch
        SqlSelect s = (SqlSelect) q;
        assertEquals(false, s.projections().stream().anyMatch(p -> p.expr() instanceof SqlExpr.Star), PLATFORM.render(q));
        assertEquals(SideOrdinal.ORD, s.projections().get(s.projections().size() - 1).alias());
        assertEquals(true, PLATFORM.render(q).contains("__ob"), PLATFORM.render(q));
    }

    @Test
    void aUnionsBranchesOfDifferentShapesLineUpByPosition() {
        // a branch that is a star over a numbered union, and one of explicit columns: UNION ALL lines columns up by
        // position, so each branch's ordinal and branch index must sit where the other's do
        SqlSelect other = SqlSelect.starOf(literal()).withProjections(List.of(
                new SqlSelect.Projection(SqlExpr.Column.of("t", A), "a", A),
                new SqlSelect.Projection(new SqlExpr.IntLit(2), "c", C)));
        com.legend.sql.SqlUnion outer = new com.legend.sql.SqlUnion(List.of(extendedUnion(), other), true,
                List.of(A, C));
        com.legend.sql.SqlUnion numbered = firstUnion(SideOrdinal.withOrdinal(outer));
        List<String> first = numbered.branches().get(0).outputs().stream().map(OutputCol::name).toList();
        assertEquals(List.of("a", "c", SideOrdinal.ORD, "__ob"), first);
        for (SqlQuery b : numbered.branches()) {
            assertEquals(first, b.outputs().stream().map(OutputCol::name).toList());
        }
    }

    private static com.legend.sql.SqlUnion firstUnion(SqlQuery q) {
        if (q instanceof com.legend.sql.SqlUnion u) {
            return u;
        }
        if (q instanceof SqlSelect s && s.from() instanceof SqlSource.Subselect sub) {
            return firstUnion(sub.inner());
        }
        throw new AssertionError("no union in " + q);
    }
}
