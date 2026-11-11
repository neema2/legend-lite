// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.sql.DateFmt;
import com.legend.sql.DatePart;
import com.legend.sql.DialectCapability;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Trino's date-time arms keep a nanosecond value's nine digits, or refuse by name (B, 2026-11-11, the orchestrator's
 * correction to S181's residual): measured on Trino 483 (docs/data-delivery-homework/probes/TrinoNanos.java.txt),
 * {@code CAST(t AS TIMESTAMP(6))} ROUNDS {@code .123456789} to {@code .123457}, so every arm that reads a date-time
 * reads it as a {@code TIMESTAMP(9)}; a microsecond's arithmetic goes through the value's epoch nanoseconds (date_add
 * has no microsecond unit); {@code parse_datetime} keeps milliseconds, so a format reading a finer part is refused.
 * The answers themselves are the Trino PCT lanes' (all ten green on this change).
 */
class TrinoNanosecondTest {

    private static final SqlExpr X = SqlExpr.Column.of("t", "x", SqlType.Scalar.TIMESTAMP, true,
            OutputCol.Origin.PHYSICAL);

    private static String render(SqlExpr e) {
        return new Trino().render(new SqlSelect(List.of(new SqlSelect.Projection(e, "v", null)), false,
                new SqlSource.Table("t", "t", List.of(new OutputCol("x", SqlType.Scalar.TIMESTAMP, true)), false,
                        Map.of(), com.legend.sql.NameQuoting.BARE), null, List.of(), null, null, List.of(), null, null,
                List.of()));
    }

    @Test
    @DisplayName("the time zone, a parse, the bucket, the difference and strftime read TIMESTAMP(9), never a rounding (6)")
    void everyArmReadsNineDigits() {
        String tz = render(SqlExpr.Call.of(SqlFn.TIMEZONE, new SqlExpr.StringLit("America/New_York"), X));
        String parse = render(SqlExpr.Call.of(SqlFn.PARSE_DATE, new SqlExpr.StringLit("2024-01-02 03:04:05.123456789")));
        String bucket = render(SqlExpr.Call.of(SqlFn.TIME_BUCKET, new SqlExpr.DatePartLit(DatePart.SECOND),
                new SqlExpr.IntLit(5), X));
        String diff = render(SqlExpr.Call.of(SqlFn.DATE_DIFF, new SqlExpr.DatePartLit(DatePart.MICROSECOND), X, X));
        String micro = render(SqlExpr.Call.of(SqlFn.STRFTIME, X,
                new SqlExpr.FormatLit(List.of(DateFmt.Part.SUBSEC_MICRO))));
        String nano = render(SqlExpr.Call.of(SqlFn.STRFTIME, X,
                new SqlExpr.FormatLit(List.of(DateFmt.Part.SUBSEC_NANO))));
        for (String sql : List.of(tz, parse, bucket, diff, micro, nano)) {
            assertTrue(sql.contains("TIMESTAMP(9)"), sql);
            assertFalse(sql.contains("AS TIMESTAMP(6)) AS VARCHAR"), sql);
        }
        assertTrue(micro.contains("-9, 6)"), micro);
        assertTrue(nano.contains("-9)") && !nano.contains("'000'"), nano);
    }

    @Test
    @DisplayName("a microsecond's arithmetic keeps the value's nine digits, through its epoch nanoseconds")
    void aMicrosecondIsAddedThroughTheEpochsNanoseconds() {
        String sql = render(SqlExpr.Call.of(SqlFn.ADD_INTERVAL, new SqlExpr.DatePartLit(DatePart.MICROSECOND),
                new SqlExpr.IntLit(5), X));
        assertTrue(sql.contains("from_unixtime_nanos(") && sql.contains("DECIMAL(38, 0)")
                && sql.contains("AS TIMESTAMP(9))"), sql);
    }

    @Test
    @DisplayName("a parse format reading a microsecond or nanosecond part is refused: parse_datetime keeps milliseconds")
    void aFinerParseFormatIsRefusedByName() {
        for (DateFmt.Part part : List.of(DateFmt.Part.SUBSEC_MICRO, DateFmt.Part.SUBSEC_NANO)) {
            var refused = assertThrows(DialectCapability.class, () -> render(SqlExpr.Call.of(SqlFn.STRPTIME,
                    new SqlExpr.StringLit("x"), new SqlExpr.FormatLit(List.of(DateFmt.Part.SEC2, part)))));
            assertTrue(refused.getMessage().contains("parse_datetime keeps milliseconds")
                    && refused.getMessage().contains("refused, never cut"), refused.getMessage());
        }
    }
}
