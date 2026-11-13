// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlExpr;

import java.util.List;

/**
 * DuckDB's spellings of the functions only DuckDB runs, written once for the two dialects that name them: {@link DuckDb}
 * and the engine-H2 text ({@link EngineStyleH2}), which has carried them since they were the base renderer's. The engine
 * text keeps them because the corpus reads that text on DuckDB too: refusing them broke 14 passing corpus tests (the
 * dialect program's decision D1, measured 2026-10-13: 13 through {@code error()}, one through {@code to_base64}).
 * legend-engine's own H2 text spells none of them; that is the engine-text work's question, not D1's.
 */
final class DuckDbSpellings {

    private DuckDbSpellings() {
    }

    /** {@code ~x} as {@code xor(x, -1)}: no negation overflow at MIN_LONG. */
    static SqlWriter bitNot(SqlWriter writer, List<SqlExpr> a) {
        return writer.append("xor(").expr(a.get(0), 0).append(", -1)");
    }

    static SqlWriter mapEmpty(SqlWriter writer) {
        return writer.append("MAP {}");
    }

    static SqlWriter jsonMergePatch(SqlWriter writer, List<SqlExpr> a) {
        return writer.function("json_merge_patch", a);
    }

    static SqlWriter encodeBase64(SqlWriter writer, List<SqlExpr> a) {
        return writer.append("to_base64(CAST(").expr(a.get(0), 0).append(" AS BLOB))");
    }

    /** decode(blob): a CAST of the blob to VARCHAR ESCAPES quotes and non-printables (\x22), never the text itself
     *  (batch 72b). */
    static SqlWriter decodeBase64(SqlWriter writer, List<SqlExpr> a) {
        return writer.append("decode(from_base64(").expr(a.get(0), 0).append("))");
    }

    /** make_timestamp wants DOUBLE seconds; (part, value) otherwise. */
    static SqlWriter makeTimestamp(SqlWriter writer, List<SqlExpr> a) {
        return a.size() == 6
                ? writer.append("make_timestamp(").list(a.subList(0, 5)).append(", CAST(").expr(a.get(5), 0)
                        .append(" AS DOUBLE))")
                : writer.function("make_timestamp", a);
    }

    /** Week buckets align to the Monday ON/BEFORE the epoch (1969-12-29 — real pure's origin, PCT-pinned); every
     *  other unit aligns to the 1970 epoch. */
    static SqlWriter timeBucket(SqlWriter writer, List<SqlExpr> a) {
        com.legend.sql.DatePart unit = AnsiSqlRenderer.part(a.get(0));
        return writer.append("time_bucket(").append(intervalFunction(unit)).append("(").expr(a.get(1), 0).append("), ")
                .expr(a.get(2), 0)
                .append(unit == com.legend.sql.DatePart.WEEK ? ", TIMESTAMP '1969-12-29 00:00:00'"
                        : ", TIMESTAMP '1970-01-01 00:00:00'")
                .append(")");
    }

    /** A date part as DuckDB's {@code date_part}, {@code date_trunc} and {@code date_diff} name it. */
    static String datePart(com.legend.sql.DatePart p) {
        return switch (p) {
            case YEAR -> "year";
            case QUARTER -> "quarter";
            case MONTH -> "month";
            case WEEK -> "week";
            case DAY -> "day";
            case HOUR -> "hour";
            case MINUTE -> "minute";
            case SECOND -> "second";
            case MILLISECOND -> "millisecond";
            case MICROSECOND -> "microsecond";
            case NANOSECOND -> "nanosecond";
            case DAY_OF_YEAR -> "doy";
            case ISO_DAY_OF_WEEK -> "isodow";
        };
    }

    /** The function DuckDB builds an interval of {@code p} with ({@code to_days(n)}). */
    static String intervalFunction(com.legend.sql.DatePart p) {
        return switch (p) {
            case YEAR -> "to_years";
            case MONTH -> "to_months";
            case WEEK -> "to_weeks";
            case DAY -> "to_days";
            case HOUR -> "to_hours";
            case MINUTE -> "to_minutes";
            case SECOND -> "to_seconds";
            case MILLISECOND -> "to_milliseconds";
            case MICROSECOND -> "to_microseconds";
            case QUARTER, NANOSECOND, DAY_OF_YEAR, ISO_DAY_OF_WEEK -> throw new DialectCapability("an interval of " + p
                    + " reached DuckDB, which builds none (nanoseconds are added over the epoch: EPOCH_NS)");
        };
    }

    static SqlWriter fromEpochMs(SqlWriter writer, List<SqlExpr> a) {
        return writer.append("epoch_ms(CAST(").expr(a.get(0), 0).append(" AS BIGINT))");
    }

    static SqlWriter intDivide(SqlWriter writer, List<SqlExpr> a) {
        return writer.append("(").expr(a.get(0), 6).append(" // ").expr(a.get(1), 6).append(")");
    }

    private static final SqlExpr ERF_BOUND_BODY = ErfMath.ofBound(
            SqlExpr.Column.of(null, "__erf_x", com.legend.sql.SqlType.Scalar.DOUBLE, true,
                    com.legend.sql.OutputCol.Origin.DERIVED));

    /** {@code erf(x)} evaluated once via DuckDB's list-lambda binding without derived-table subselect staging. */
    static SqlWriter erf(SqlWriter writer, List<SqlExpr> a) {
        return writer.append("list_extract(list_transform([CAST(")
                .expr(a.get(0), 0)
                .append(" AS DOUBLE)], __erf_x -> ")
                .expr(ERF_BOUND_BODY, 0)
                .append("), 1)");
    }
}
