// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.sql.DateFmt;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * DuckDB's strftime with nine fraction digits ({@code %n}): DuckDB 1.5.5 writes the code with a wrong length when
 * more of the format follows it (six digits, the rest, then bytes past the text; measured 2026-10-26 on the 1.5.5
 * wheel on x64 and aarch64, where PR #78's arm lane found the JSON wire's
 * {@code "2024-03-10T03:00:00.000000+0000e\r\u0000"}), and whole when it ends the format. The dialect splits the
 * format there ({@link DuckDb#strftime}).
 */
class DuckDbStrftimeTest {

    private static final SqlExpr X = SqlExpr.Column.of("t", "x", SqlType.Scalar.TIMESTAMP, true,
            OutputCol.Origin.PHYSICAL);

    private static String render(SqlExpr e) {
        return new DuckDb().render(new SqlSelect(List.of(new SqlSelect.Projection(e, "v", null)), false,
                new SqlSource.Table("t", "t", List.of(new OutputCol("x", SqlType.Scalar.TIMESTAMP, true)), false,
                        Map.of(), com.legend.sql.NameQuoting.BARE), null, List.of(), null, null, List.of(), null, null,
                List.of()));
    }

    @Test
    @DisplayName("a format that goes on after %n is split there: strftime(x, '...%n') || the rest")
    void theFormatEndsAtNineDigitsAndTheRestFollows() {
        // a format with text after its nine fraction digits (legend-engine's DateTime text was one until 2026-10-27; a
        // user's format can be)
        List<DateFmt> nineThenZone = List.of(DateFmt.Part.YEAR4, new DateFmt.Text("-"), DateFmt.Part.MONTH2,
                new DateFmt.Text("-"), DateFmt.Part.DAY2, new DateFmt.Text("T"), DateFmt.Part.HOUR2,
                new DateFmt.Text(":"), DateFmt.Part.MIN2, new DateFmt.Text(":"), DateFmt.Part.SEC2,
                new DateFmt.Text("."), DateFmt.Part.SUBSEC_NANO, new DateFmt.Text("+0000"));
        String sql = render(SqlExpr.Call.of(SqlFn.STRFTIME, X, new SqlExpr.FormatLit(nineThenZone)));
        assertTrue(Pattern.compile("\\(strftime\\([^()]*x[^()]*, '%Y-%m-%dT%H:%M:%S\\.%n'\\) \\|\\| '\\+0000'\\)")
                .matcher(sql).find(), sql);
    }

    @Test
    @DisplayName("%n that ends its format is written as it is")
    void nineDigitsLastAreWhole() {
        String sql = render(SqlExpr.Call.of(SqlFn.STRFTIME, X,
                new SqlExpr.FormatLit(List.of(DateFmt.Part.SUBSEC_NANO))));
        assertTrue(sql.contains("'%n')"), sql);
        assertFalse(sql.contains("||"), sql);
    }
}
