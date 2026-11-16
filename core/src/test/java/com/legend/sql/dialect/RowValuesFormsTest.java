// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.executionplan.MirJson;
import com.legend.json.Json;
import com.legend.sql.NameQuoting;
import com.legend.sql.OutputCol;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A ROW'S VALUES COMPUTED ONCE ({@link SqlSource.RowValues}, the lean program's step 7), each dialect's form pinned
 * (E-v2's review, S3): Postgres reads it as a lateral of one row per row (timed as fast as inline, 2026-11-16);
 * DuckDB writes each value back where it is read (a lateral is a join there, timed slower); and the plan's typed tree
 * carries it through its JSON unchanged.
 */
class RowValuesFormsTest {

    private static final OutputCol A = new OutputCol("A", SqlType.Scalar.BIGINT, false);
    private static final OutputCol V = new OutputCol("__rv0", SqlType.Scalar.BIGINT, true);
    private static final OutputCol SQ = new OutputCol("sq", SqlType.Scalar.BIGINT, true);

    /** {@code SELECT r.v * r.v + r.v AS sq FROM T t0 [bound: A * 3 + 1 AS v]}. */
    private static SqlQuery bound() {
        SqlSource.Table t = new SqlSource.Table("T", "t0", List.of(A), false, Map.of(), NameQuoting.BARE);
        SqlExpr value = SqlExpr.Call.of(SqlFn.PLUS, SqlExpr.Call.of(SqlFn.TIMES, SqlExpr.Column.of("t0", A),
                new SqlExpr.IntLit(3)), new SqlExpr.IntLit(1));
        SqlSource rows = new SqlSource.RowValues(t, List.of(new SqlSelect.Projection(value, "__rv0", V)), "__r0");
        SqlExpr v = SqlExpr.Column.of("__r0", V);
        SqlExpr sq = SqlExpr.Call.of(SqlFn.PLUS, SqlExpr.Call.of(SqlFn.TIMES, v, v), v);
        return new SqlSelect(List.of(new SqlSelect.Projection(sq, "sq", SQ)), false, rows, null, List.of(), null, null,
                List.of(), null, null, List.of(SQ));
    }

    @Test
    void postgresReadsTheValuesAsALateral() {
        String sql = new Postgres().render(bound());
        assertTrue(sql.contains("CROSS JOIN LATERAL"), sql);
        assertEquals(4, sql.split("__rv0", -1).length - 1, sql);
    }

    @Test
    void duckDbWritesTheValueBackWhereItIsRead() {
        String sql = new DuckDb().render(bound());
        assertFalse(sql.contains("LATERAL"), sql);
        assertFalse(sql.contains("__rv0"), sql);
        assertEquals(3, sql.split("\\* 3", -1).length - 1, sql);
    }

    @Test
    void thePlansTreeCarriesTheValuesThroughItsJson() {
        SqlQuery q = bound();
        Json.Writer w = Json.writer();
        Json.write(w, MirJson.query(q));
        assertEquals(q, MirJson.readQuery(Json.parseObject(w.toString())));
    }
}
