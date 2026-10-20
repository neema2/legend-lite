// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSource;
import org.junit.jupiter.api.Test;

/**
 * DuckDB's outer join ON without the IS NOT NULL tests a comparison beside them already makes (2026-10-17, the IQ
 * island's TPC-H corpus lane): DuckDB plans a LEFT JOIN whose ON tests a preserved column IS NOT NULL as a nested loop,
 * 56 s against 0.6 s at TPC-H's scale 0.01. An inner join's ON, and a test no comparison makes, are kept.
 */
class DuckDbJoinConditionTest {

    private static final SqlExpr.Column A = new SqlExpr.Column("l", "a");
    private static final SqlExpr.Column B = new SqlExpr.Column("r", "b");

    private static SqlExpr guarded() {
        return SqlExpr.Call.of(SqlFn.AND, SqlExpr.Call.of(SqlFn.AND, SqlExpr.Call.of(SqlFn.IS_NOT_NULL, A),
                SqlExpr.Call.of(SqlFn.IS_NOT_NULL, B)), SqlExpr.Call.of(SqlFn.EQUAL, A, B));
    }

    @Test
    void anOuterJoinDropsTheNullTestsItsEqualityMakes() {
        assertEquals(SqlExpr.Call.of(SqlFn.EQUAL, A, B), new DuckDb().joinCondition(SqlSource.Join.Kind.LEFT,
                guarded()));
        assertEquals(SqlExpr.Call.of(SqlFn.EQUAL, A, B), new DuckDb().joinCondition(SqlSource.Join.Kind.FULL,
                guarded()));
    }

    @Test
    void anInnerJoinAndAnUnmadeTestAreKept() {
        SqlExpr on = guarded();
        assertSame(on, new DuckDb().joinCondition(SqlSource.Join.Kind.INNER, on));
        SqlExpr other = SqlExpr.Call.of(SqlFn.AND, SqlExpr.Call.of(SqlFn.IS_NOT_NULL, new SqlExpr.Column("l", "c")),
                SqlExpr.Call.of(SqlFn.EQUAL, A, B));
        assertSame(other, new DuckDb().joinCondition(SqlSource.Join.Kind.LEFT, other));
    }
}
