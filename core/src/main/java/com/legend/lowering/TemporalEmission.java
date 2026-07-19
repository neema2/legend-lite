package com.legend.lowering;

import com.legend.compiler.spec.typed.TypedCDate;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.values.PureDateLiteral;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;

import java.util.List;

/**
 * TEMPORAL emission — the dateDiff/interval family's typed-MIR
 * construction, extracted VERBATIM from {@link Scalars} at the guardrail
 * ceiling (the audit-20c registration-family seam; NullSemantics is the
 * pattern: one behavior family, one owner, callers in the rule table).
 * Semantics are REAL pure's, PCT-pinned: WEEKS counts Sunday-boundary
 * crossings with the forward/backward asymmetry; HOURS/MINUTES/SECONDS
 * are truncated ELAPSED time; calendar parts ride SQL date_diff.
 */
final class TemporalEmission {

    private TemporalEmission() {
    }

    /** The DuckDB to_* interval constructor for a DurationUnit literal. */
    static String intervalFn(TypedSpec unit) {
        return switch (Scalars.enumName(unit)) {
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
                    "unknown DurationUnit for interval arithmetic: "
                            + Scalars.enumName(unit));
        };
    }

    /** The date_diff part name for a DurationUnit enum literal. */
    static String diffPart(TypedSpec unit) {
        return switch (Scalars.enumName(unit)) {
            case "YEARS" -> "year";
            case "MONTHS" -> "month";
            case "WEEKS" -> "week";
            case "DAYS" -> "day";
            case "HOURS" -> "hour";
            case "MINUTES" -> "minute";
            case "SECONDS" -> "second";
            case "MILLISECONDS" -> "millisecond";
            case "MICROSECONDS" -> "microsecond";
            default -> throw new IllegalStateException(
                    "unknown DurationUnit for dateDiff: " + Scalars.enumName(unit));
        };
    }

    /**
     * A YEAR/YEAR-MONTH date literal argument completes to its first day
     * (real pure's dateDiff coercion); everything else keeps its lowering.
     */
    static SqlExpr dateArg(TypedSpec typed, SqlExpr lowered) {
        if (typed instanceof TypedCDate d) {
            if (d.value() instanceof PureDateLiteral.Year y) {
                return new SqlExpr.DateLit(y.toEngineString() + "-01-01");
            }
            if (d.value() instanceof PureDateLiteral.YearMonth ym) {
                return new SqlExpr.DateLit(ym.toEngineString() + "-01");
            }
        }
        return lowered;
    }

    /**
     * {@code dateDiff} with REAL pure's per-unit semantics (PCT-pinned):
     * WEEKS counts SUNDAY-boundary crossings — {@code (d1, d2]} forward but
     * {@code [d2, d1)} backward (NOT the negation; the audit's asymmetry);
     * HOURS/MINUTES/SECONDS are TRUNCATED ELAPSED time (SQL date_diff
     * counts boundary crossings — a different number); the calendar parts
     * (year/month/day/ms) match SQL date_diff.
     */
    static SqlExpr dateDiffExpr(String part, SqlExpr d1, SqlExpr d2) {
        switch (part) {
            case "week" -> {
                SqlExpr forward = SqlExpr.Call.of(SqlFn.MINUS,
                        sundayIndex(d2), sundayIndex(d1));
                SqlExpr backward = SqlExpr.Call.of(SqlFn.MINUS,
                        sundayIndex(backOneDay(d2)), sundayIndex(backOneDay(d1)));
                return new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                        SqlExpr.Call.of(SqlFn.LESS_EQUAL,
                                new SqlExpr.Call(SqlFn.DATE_DIFF, List.of(
                                        new SqlExpr.StringLit("day"), d2, d1)),
                                new SqlExpr.IntLit(0)),
                        forward)), backward);
            }
            case "hour" -> {
                return elapsed(d1, d2, 3_600_000L);
            }
            case "minute" -> {
                return elapsed(d1, d2, 60_000L);
            }
            case "second" -> {
                return elapsed(d1, d2, 1_000L);
            }
            default -> {
                return new SqlExpr.Call(SqlFn.DATE_DIFF, List.of(
                        new SqlExpr.StringLit(part), d1, d2));
            }
        }
    }

    /** Truncated elapsed time in {@code unitMs} chunks (Java toHours-style). */
    static SqlExpr elapsed(SqlExpr d1, SqlExpr d2, long unitMs) {
        return SqlExpr.Call.of(SqlFn.INT_DIVIDE,
                SqlExpr.Call.of(SqlFn.MINUS,
                        new SqlExpr.Call(SqlFn.EPOCH_MS, List.of(d2)),
                        new SqlExpr.Call(SqlFn.EPOCH_MS, List.of(d1))),
                new SqlExpr.IntLit(unitMs));
    }

    /**
     * Floored week index counted from an ANCIENT Sunday epoch (0001-01-07,
     * proleptic Gregorian) — always positive for real dates, so DuckDB's
     * truncating {@code //} IS floor division (the audit's pre-1970 case).
     */
    static SqlExpr sundayIndex(SqlExpr d) {
        return SqlExpr.Call.of(SqlFn.INT_DIVIDE,
                new SqlExpr.Call(SqlFn.DATE_DIFF, List.of(
                        new SqlExpr.StringLit("day"),
                        new SqlExpr.DateLit("0001-01-07"), d)),
                new SqlExpr.IntLit(7));
    }

    static SqlExpr backOneDay(SqlExpr d) {
        return new SqlExpr.Call(SqlFn.ADD_INTERVAL, List.of(
                new SqlExpr.StringLit("to_days"),
                new SqlExpr.IntLit(-1), d));
    }
}
