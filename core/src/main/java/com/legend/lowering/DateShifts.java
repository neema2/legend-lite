// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;

import java.util.List;

/**
 * Day-of-week anchored date shifts (engine H2 dialect formula — the dyna
 * emission duckdbExtension comments out): {@code shifted = anchor +
 * (t - isodow(anchor) [-7 when positive (mostRecent) / non-negative
 * (previous)]) days}; 1-arg forms anchor {@code today()}.
 */
final class DateShifts {

    private DateShifts() {
    }

    /** ISO day numbers of the pure {@code DayOfWeek} enum (Monday=1). */
    static int isoDayNumber(String name) {
        return switch (name) {
            case "Monday" -> 1;
            case "Tuesday" -> 2;
            case "Wednesday" -> 3;
            case "Thursday" -> 4;
            case "Friday" -> 5;
            case "Saturday" -> 6;
            case "Sunday" -> 7;
            default -> throw new com.legend.error.NotImplementedException(
                    "unknown DayOfWeek '" + name + "'");
        };
    }

    /** {@code mostRecentDayOfWeek}/{@code previousDayOfWeek}: the anchored
     * shift; {@code strict} excludes the anchor day itself (previous);
     * {@code anchor} null means today(). */
    static SqlExpr dayOfWeekShift(
            com.legend.compiler.spec.typed.TypedNativeCall n,
            List<SqlExpr> args, String dowName, SqlExpr anchor,
            boolean strict) {
        if (anchor == null) {
            anchor = new SqlExpr.Call(SqlFn.TODAY, List.of());
        }
        int t = isoDayNumber(dowName);
        SqlExpr diff = new SqlExpr.Call(SqlFn.MINUS, List.of(
                new SqlExpr.IntLit(t),
                new SqlExpr.Call(SqlFn.EXTRACT, List.of(
                        new SqlExpr.StringLit("isodow"), anchor))));
        SqlExpr cond = new SqlExpr.Call(
                strict ? SqlFn.GREATER_EQUAL : SqlFn.GREATER,
                List.of(diff, new SqlExpr.IntLit(0)));
        SqlExpr shifted = new SqlExpr.Case(List.of(new SqlExpr.Case.When(cond,
                new SqlExpr.Call(SqlFn.MINUS,
                        List.of(diff, new SqlExpr.IntLit(7))))), diff);
        return new SqlExpr.Call(SqlFn.ADD_INTERVAL, List.of(
                new SqlExpr.StringLit("to_days"), shifted, anchor));
    }
static String intervalFn(String unitName) {
        return switch (unitName) {
            case "YEARS" -> "to_years";
            case "MONTHS" -> "to_months";
            case "WEEKS" -> "to_weeks";
            case "DAYS" -> "to_days";
            case "HOURS" -> "to_hours";
            case "MINUTES" -> "to_minutes";
            case "SECONDS" -> "to_seconds";
            case "MILLISECONDS" -> "to_milliseconds";
            case "MICROSECONDS" -> "to_microseconds";
            default -> throw new IllegalStateException(
                    "unknown DurationUnit for interval arithmetic: " + unitName);
        };
    }


}
