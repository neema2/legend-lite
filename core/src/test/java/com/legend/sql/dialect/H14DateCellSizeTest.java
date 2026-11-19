// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.DateFmt;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

/** SPIKE (H14, scratch): the SQL characters of one abstract-Date cell held as a timestamp, three ways, per dialect.
 *  Measurement only. */
class H14DateCellSizeTest {

    private static final SqlExpr X = SqlExpr.Column.of("t", "x", SqlType.Scalar.TIMESTAMP, true, OutputCol.Origin.PHYSICAL);

    private static String render(SqlDialect d, SqlExpr e) {
        return d.render(new SqlSelect(List.of(new SqlSelect.Projection(e, "v", null)), false,
                new SqlSource.Table("t", "t", List.of(new OutputCol("x", SqlType.Scalar.TIMESTAMP, true)), false, Map.of(),
                        com.legend.sql.NameQuoting.BARE), null, List.of(), null, null, List.of(), null, null, List.of()));
    }

    @Test
    void sizes() {
        SqlExpr v1 = new SqlExpr.Cast(X, SqlType.Scalar.VARCHAR, SqlExpr.Reason.CARRIER);
        SqlExpr d1 = SqlExpr.Call.of(SqlFn.DATETIME_TO_TEXT, X);
        SqlExpr nine = SqlExpr.Call.of(SqlFn.STRFTIME, X, new SqlExpr.FormatLit(List.of(DateFmt.Part.SUBSEC_NANO)));
        SqlExpr d2 = SqlExpr.Call.of(SqlFn.CONCAT, SqlExpr.Call.of(SqlFn.CONCAT, SqlExpr.Call.of(SqlFn.CONCAT,
                SqlExpr.Call.of(SqlFn.STRFTIME, X, new SqlExpr.FormatLit(DateFmt.ISO_LOCAL)), new SqlExpr.StringLit(".")),
                SqlExpr.Call.of(SqlFn.SUBSTRING, nine, new SqlExpr.IntLit(1), new SqlExpr.IntLit(3))),
                new SqlExpr.StringLit("+0000"));
        for (SqlDialect d : List.of(new DuckDb(), new SQLite(), new Postgres(), new H2())) {
            int base = render(d, X).length();
            System.out.println("[h14-date-size] " + d.getClass().getSimpleName() + " cast=" + (render(d, v1).length() - base + 1)
                    + " D1=" + (render(d, d1).length() - base + 1) + " D2=" + (render(d, d2).length() - base + 1));
            System.out.println("[h14-date-sql] " + d.getClass().getSimpleName() + " D1: " + render(d, d1));
            System.out.println("[h14-date-sql] " + d.getClass().getSimpleName() + " D2: " + render(d, d2));
        }
    }
}
