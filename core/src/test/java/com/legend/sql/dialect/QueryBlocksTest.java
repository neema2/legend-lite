// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import com.legend.sql.SqlWith;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * An alias names its source within its query block alone (2026-11-05): a statement composed of several lowered ones --
 * the database judge's fused verdict statement, each assert's rows counting its aliases from {@code t0} -- holds one
 * alias in two blocks, and each block's references spell their own source's names. Found on H2's corpus by the split
 * rung's tripwire: a statement-wide alias map resolved one block's {@code "t3"} to the other's, so a table column was
 * quoted as a projection's label ({@code Column "t3.bookId" not found}).
 */
class QueryBlocksTest {

    private static final OutputCol PHYSICAL = new OutputCol("bookId", SqlType.Scalar.INTEGER, true,
            OutputCol.Origin.PHYSICAL);
    private static final OutputCol DERIVED = new OutputCol("bookId", SqlType.Scalar.INTEGER, true,
            OutputCol.Origin.DERIVED);

    private static SqlSelect select(SqlExpr e, SqlSource from, OutputCol out) {
        return new SqlSelect(List.of(new SqlSelect.Projection(e, "v", null)), false, from, null, List.of(), null, null,
                List.of(), null, null, List.of(new OutputCol("v", SqlType.Scalar.INTEGER, true, OutputCol.Origin.DERIVED)));
    }

    /** {@code SELECT "t3".bookId AS "v" FROM Trade AS "t3"}: the alias a table, its column the DDL's bare name. */
    private static SqlQuery overTheTable() {
        SqlSource.Table trade = new SqlSource.Table("Trade", "t3", List.of(PHYSICAL), false, Map.of(),
                com.legend.sql.NameQuoting.BARE);
        return select(SqlExpr.Column.of("t3", PHYSICAL), trade, PHYSICAL);
    }

    /** {@code SELECT "t3"."bookId" ... FROM (SELECT "t1".bookId AS "bookId" FROM Trade AS "t1") AS "t3"}: the same
     *  alias a subselect, its column the projection's quoted label -- the reference built with the table's stamp. */
    private static SqlQuery overTheSubselect() {
        SqlSource.Table trade = new SqlSource.Table("Trade", "t1", List.of(PHYSICAL), false, Map.of(),
                com.legend.sql.NameQuoting.BARE);
        SqlSelect inner = new SqlSelect(List.of(new SqlSelect.Projection(SqlExpr.Column.of("t1", PHYSICAL), "bookId",
                DERIVED)), false, trade, null, List.of(), null, null, List.of(), null, null, List.of(DERIVED));
        return select(SqlExpr.Column.of("t3", PHYSICAL), new SqlSource.Subselect(inner, "t3", null), DERIVED);
    }

    private static void eachBlockSpellsItsOwnSource(List<SqlQuery> order) {
        SqlWith fused = new SqlWith(List.of(new SqlWith.Cte("a", order.get(0), false),
                new SqlWith.Cte("b", order.get(1), false)), select(new SqlExpr.IntLit(1), new SqlSource.Dual(), null),
                false);
        String sql = new H2().render(fused);
        assertTrue(sql.contains("SELECT \"t3\".bookId AS \"v\"\n  FROM Trade AS \"t3\""), sql);
        assertTrue(sql.contains("SELECT \"t3\".\"bookId\" AS \"v\""), sql);
    }

    @Test
    void anAliasInTwoBlocksResolvesToEachBlocksOwnSource() {
        eachBlockSpellsItsOwnSource(List.of(overTheTable(), overTheSubselect()));
        eachBlockSpellsItsOwnSource(List.of(overTheSubselect(), overTheTable()));
    }
}
