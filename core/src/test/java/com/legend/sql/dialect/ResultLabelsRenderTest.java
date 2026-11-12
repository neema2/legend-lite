// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The result labels' edge (AnsiSqlRenderer#labeled; the B close's audit, SHOULD-FIX 1, 2026-11-12): a root star over a
 * source that declares no columns -- a person's raw SQL, whose names only the database knows -- is kept whole, never
 * spelled as the other sources' columns alone (which dropped the raw source's columns), and the select's other
 * columns are still labeled (a star that spelled nothing left the whole select unlabeled).
 */
class ResultLabelsRenderTest {

    @Test
    void aStarOverASourceThatDeclaresNoColumnsIsKeptWholeAndTheRestLabeled() {
        SqlSource.Table t = new SqlSource.Table("people", "t0", List.of(new OutputCol("region", SqlType.Scalar.VARCHAR,
                true)), false, Map.of(), com.legend.sql.NameQuoting.BARE);
        SqlSource.RawSql raw = new SqlSource.RawSql("SELECT 1 AS n", "r", List.of(), "DuckDB");
        SqlSource from = new SqlSource.Join(t, raw, SqlSource.Join.Kind.CROSS, null);
        SqlExpr region = SqlExpr.Column.of("t0", "region", SqlType.Scalar.VARCHAR, true, OutputCol.Origin.PHYSICAL);
        String sql = new DuckDb().render(new SqlSelect(List.of(new SqlSelect.Projection(new SqlExpr.Star(null), null,
                null), new SqlSelect.Projection(region, null, new OutputCol("region", SqlType.Scalar.VARCHAR, true))),
                false, from, null, List.of(), null, null, List.of(), null, null, List.of()));
        String head = sql.substring(0, sql.indexOf("FROM"));
        // the star stays a star: the raw source's columns are answered, as the database names them
        assertTrue(head.contains("*"), sql);
        // the other column is labeled with its Pure name
        assertTrue(head.toLowerCase(java.util.Locale.ROOT).contains("as region")
                || head.contains("AS \"region\""), sql);
    }
}
