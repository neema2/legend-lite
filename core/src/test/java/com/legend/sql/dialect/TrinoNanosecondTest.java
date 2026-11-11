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

    /** A Trino runtime of no table, to plan a query of literals on. */
    private static final String TRINO_MODEL = """
            ###Relational
            Database t::DB ( )
            ###Connection
            RelationalDatabaseConnection t::C { store: t::DB; type: Trino; specification: Trino { host: '127.0.0.1';
              port: 8080; catalog: 'memory'; schema: 'default'; }; auth: Test; }
            ###Runtime
            Runtime t::RT { mappings: []; connections: [ t::DB: [ c: t::C ] ]; }
            """;

    /** The audit of 862a4d02..2f164ec4's M1 (2026-11-11): a REAL parseDate and cast(@DateTime), lowered as a query is,
     *  cast to Trino's scalar TIMESTAMP -- nine digits, never the TIMESTAMP(6) that rounds (the lowering wraps the
     *  parse in a cast, which the dialect's own arm test could not see). */
    @Test
    @DisplayName("a lowered parseDate and cast(@DateTime) cast to TIMESTAMP(9), never a rounding TIMESTAMP(6)")
    void aLoweredParseAndCastKeepNineDigits() {
        com.legend.compiler.element.ModelContext ctx = com.legend.Compiler.compileModel(TRINO_MODEL);
        for (String query : List.of("|parseDate('2024-01-02T03:04:05.123456789+0000')",
                "|'2024-01-02T03:04:05.123456789+0000'->cast(@DateTime)")) {
            String sql = com.legend.Compiler.query(ctx, query).plan("t::RT").sql();
            assertTrue(sql.contains("TIMESTAMP(9)"), query + " -> " + sql);
            assertFalse(sql.contains("TIMESTAMP(6)"), query + " -> " + sql);
        }
    }

    /** The audit of 862a4d02..2f164ec4's S13 (2026-11-11): the sub-day boundaries and the fixed-length time bucket
     *  divide the epoch's microseconds in exact BIGINT arithmetic, never a DOUBLE (exact only to about 2255, measured
     *  on Trino 483), and a truncation to the microsecond, which Trino's date_trunc refuses at run, is the epoch's
     *  floored microseconds as a timestamp. */
    @Test
    @DisplayName("boundaries and buckets floor-divide BIGINTs; a microsecond truncation is the epoch's microseconds")
    void subDayArithmeticIsExact() {
        String diff = render(SqlExpr.Call.of(SqlFn.DATE_DIFF, new SqlExpr.DatePartLit(DatePart.HOUR), X, X));
        String bucket = render(SqlExpr.Call.of(SqlFn.TIME_BUCKET, new SqlExpr.DatePartLit(DatePart.MINUTE),
                new SqlExpr.IntLit(5), X));
        String trunc = render(SqlExpr.Call.of(SqlFn.DATE_TRUNC, new SqlExpr.DatePartLit(DatePart.MICROSECOND), X));
        for (String sql : List.of(diff, bucket)) {
            assertTrue(sql.contains("mod(mod("), sql);
            assertFalse(sql.contains("AS DOUBLE) /"), sql);
        }
        assertFalse(trunc.contains("date_trunc('microsecond'"), trunc);
        assertTrue(trunc.contains("from_unixtime_nanos("), trunc);
        System.out.println("S13-SQL diff: " + diff.replace('\n', ' '));
        System.out.println("S13-SQL bucket: " + bucket.replace('\n', ' '));
        System.out.println("S13-SQL trunc: " + trunc.replace('\n', ' '));
    }
}
