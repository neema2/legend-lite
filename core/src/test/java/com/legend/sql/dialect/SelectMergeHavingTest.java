// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.NameQuoting;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlRewriter;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * A FILTER OVER GROUPS IS THE GROUPS' HAVING (the DuckDB plan's step 3), never a raise added (E-v2's review, A-M1):
 * ANDed onto an inner HAVING, the outer filter may be evaluated on groups that HAVING drops, so it folds there only when
 * it raises nothing.
 */
class SelectMergeHavingTest {

    private static final OutputCol K = new OutputCol("k", SqlType.Scalar.VARCHAR, false);
    private static final SqlSource.Table T = new SqlSource.Table("T", "t0", List.of(K), false, Map.of(),
            NameQuoting.BARE);

    private static SqlSelect overGroups(SqlExpr innerHaving, SqlExpr outerWhere) {
        SqlSelect in = new SqlSelect(List.of(new SqlSelect.Projection(SqlExpr.Column.of("t0", K), "k", K)), false, T,
                null, List.of(SqlExpr.Column.of("t0", K)), innerHaving, null, List.of(), null, null, List.of(K));
        return new SqlSelect(List.of(new SqlSelect.Projection(SqlExpr.Column.derived("s", "k"), "k", K)), false,
                new SqlSource.Subselect(in, "s", null), outerWhere, List.of(), null, null, List.of(), null, null,
                List.of(K));
    }

    private static SqlSelect merged(SqlSelect s) {
        SqlRewriter merge = new DuckDb().passes().stream().filter(SelectMerge.class::isInstance).findFirst()
                .orElseThrow();
        return (SqlSelect) merge.rewrite(s);
    }

    private static SqlExpr countAboveOne() {
        return SqlExpr.Call.of(SqlFn.GREATER, SqlAgg.Reducer.of(SqlAgg.Fn.COUNT), new SqlExpr.IntLit(1));
    }

    @Test
    void aRaiseFreeFilterOverGroupsIsTheirHaving() {
        SqlExpr notX = SqlExpr.Call.of(SqlFn.NOT_EQUAL, SqlExpr.Column.derived("s", "k"), new SqlExpr.StringLit("x"));
        SqlSelect out = merged(overGroups(countAboveOne(), notX));
        assertInstanceOf(SqlSource.Table.class, out.from());
        assertNotNull(out.having());
    }

    @Test
    void aRaisingFilterIsNotAndedOntoAHaving() {
        SqlExpr numeric = SqlExpr.Call.of(SqlFn.GREATER, new SqlExpr.Cast(SqlExpr.Column.derived("s", "k"),
                SqlType.Scalar.INTEGER, SqlExpr.Reason.USER), new SqlExpr.IntLit(990));
        assertInstanceOf(SqlSource.Subselect.class, merged(overGroups(countAboveOne(), numeric)).from());
    }
}
