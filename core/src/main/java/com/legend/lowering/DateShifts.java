// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCDate;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlType;
import com.legend.values.PureDateLiteral;

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
            List<SqlExpr> args, String dowName, @com.legend.base.Nullable SqlExpr anchor,
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
        return new SqlExpr.Call(SqlFn.ADD_INTERVAL_TEMPORAL, List.of(
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



    /** {@code uniqueValueOnly} over a VALUE collection: the single
     * distinct element, else the default/empty (engine
     * collectionExtension.pure semantics; parked here beside the other
     * composed-CASE emissions — Scalars sits at its size guardrail). */
    static SqlExpr uniqueValueOnly(List<SqlExpr> args) {
        return new SqlExpr.Case(
                List.of(new SqlExpr.Case.When(
                        SqlExpr.Call.of(SqlFn.EQUAL,
                                SqlExpr.Call.of(SqlFn.LIST_LENGTH,
                                        SqlExpr.Call.of(SqlFn.LIST_DISTINCT,
                                                args.get(0))),
                                new SqlExpr.IntLit(1)),
                        SqlExpr.Call.of(SqlFn.LIST_GET, args.get(0),
                                new SqlExpr.IntLit(1)))),
                args.size() == 2 ? args.get(1) : new SqlExpr.NullLit());
    }


    /** The adjust lowering family — the plain rule for every catalog
     * spelling, plus the {@code Pure.Lite.ADJUST_TEMPORAL} twin
     * ({@code TemporalFrame}-stamped milestoning window dates): the same
     * lowering with its interval calls retagged to the TEMPORAL spelling
     * fn (engine legacy mapToDBUnitType prints dateadd units UPPERCASE;
     * the new sqlDialectTranslation defaults print lowercase). */
    /** The truncation heads (moved from Scalars at the 3500-line
     * split seam). */
    static void registerTruncationRules(
            java.util.Map<com.legend.model.FunctionId, Scalars.Rule> rules) {
        // Truncations: DATE_TRUNC with the part literal — SEMANTICS
        // ONLY (single-compiler tenet, user ruling 2026-09-01): the
        // day-grained heads' Date result type is honored by the
        // EXECUTION DIALECTS (AnsiSqlRenderer's DATE_TRUNC arm casts
        // where that backend's date_trunc returns TIMESTAMP; the
        // engine-TEXT renderers own their verbatim spellings) — no
        // channel conditionals here. The zero-arg This* heads
        // truncate TODAY() (real pure composes today()->firstDayOf*,
        // dateExtension.pure — same by emission).
        for (var e : java.util.Map.ofEntries(
                java.util.Map.entry(com.legend.builtin.Pure.AT_DATE_FIRST_DAY_OF_MONTH, "month"), java.util.Map.entry(com.legend.builtin.Pure.AT_DATE_FIRST_DAY_OF_YEAR, "year"),
                java.util.Map.entry(com.legend.builtin.Pure.AT_DATE_FIRST_DAY_OF_WEEK, "week"), java.util.Map.entry(com.legend.builtin.Pure.AT_DATE_FIRST_DAY_OF_QUARTER, "quarter"),
                java.util.Map.entry(com.legend.builtin.Pure.AT_DATE_FIRST_HOUR_OF_DAY, "day"), java.util.Map.entry(com.legend.builtin.Pure.AT_DATE_FIRST_MINUTE_OF_HOUR, "hour"),
                java.util.Map.entry(com.legend.builtin.Pure.AT_DATE_FIRST_SECOND_OF_MINUTE, "minute"), java.util.Map.entry(com.legend.builtin.Pure.AT_DATE_FIRST_MILLISECOND_OF_SECOND, "second"),
                java.util.Map.entry(com.legend.builtin.Pure.AT_DATE_FIRST_DAY_OF_THIS_YEAR, "year"), java.util.Map.entry(com.legend.builtin.Pure.AT_DATE_FIRST_DAY_OF_THIS_QUARTER, "quarter"),
                java.util.Map.entry(com.legend.builtin.Pure.AT_DATE_FIRST_DAY_OF_THIS_MONTH, "month")).entrySet()) {
            for (com.legend.model.FunctionId f : e.getKey()) {
                rules.put(f, (n, args) -> {
                    SqlExpr trunc = new SqlExpr.Call(SqlFn.DATE_TRUNC, List.of(
                            new SqlExpr.StringLit(e.getValue()),
                            n.args().isEmpty()
                                    ? new SqlExpr.Call(SqlFn.TODAY, List.of())
                                    : Scalars.dateArg(n.args().get(0), args.get(0))));
                    return trunc;
                });
            }
        }
    }

    static void registerAdjustRules(java.util.Map<com.legend.model.FunctionId, Scalars.Rule> rules) {
        for (com.legend.model.FunctionId f : com.legend.builtin.Pure.AT_DATE_ADJUST) {
            rules.put(f, (n, args) -> {
                SqlExpr added = new SqlExpr.Call(SqlFn.ADD_INTERVAL, List.of(
                        new SqlExpr.StringLit(intervalFn(Scalars.enumName(n.args().get(2)))),
                        args.get(1), Scalars.dateArg(n.args().get(0), args.get(0))));
                // A PARTIAL-date operand keeps its precision: pad in (dateArg),
                // adjust, then truncate BACK to the written form —
                // adjust(%2016, 1, YEARS) is %2017, not 2017-01-01.
                Integer pp = Scalars.partialPrecision(n.args().get(0));
                if (pp != null) {
                    // The result's precision is the FINER of the written
                    // precision and the unit (real pure GROWS precision:
                    // adjust(%2020, 1, MONTHS) is 2020-02; a coarse unit
                    // keeps the written form: adjust(%2016, 1, YEARS) is
                    // 2017; a day-or-finer unit yields the full-precision
                    // carrier — the audit's truncate-everything write-back
                    // silently erased finer adjustments).
                    java.util.List<com.legend.sql.DateFmt> fmt =
                            switch (Scalars.enumName(n.args().get(2))) {
                        case "YEARS" -> pp == 1
                                ? java.util.List.of((com.legend.sql.DateFmt)
                                        com.legend.sql.DateFmt.Part.YEAR4)
                                : com.legend.sql.DateFmt.YEAR_MONTH;
                        case "MONTHS" -> com.legend.sql.DateFmt.YEAR_MONTH;
                        default -> null;
                    };
                    return fmt == null ? added
                            : new SqlExpr.Cast(
                                    SqlExpr.Call.of(SqlFn.STRFTIME, added,
                                            new SqlExpr.FormatLit(fmt)),
                                    SqlType.Scalar.TEMPORAL_TEXT);
                }
                // A source written with MORE subsecond digits than the
                // TIMESTAMP carrier holds (6): the result keeps the WRITTEN
                // digit count (real pure preserves subsecond print
                // precision), and digits beyond microseconds are the
                // source's own — static text an interval can never touch.
                // Emitted as the precision-faithful STRING (the wire's date
                // convention, same as timeBucket).
                if (n.args().get(0) instanceof TypedCDate cd
                        && cd.value() instanceof
                                PureDateLiteral.DateWithSubsecond sub
                        && sub.subsecond().length() > 6) {
                    return new SqlExpr.Cast(SqlExpr.Call.of(SqlFn.CONCAT,
                            SqlExpr.Call.of(SqlFn.STRFTIME, added,
                                    new SqlExpr.FormatLit(com.legend.sql.DateFmt.ISO_MICRO)),
                            new SqlExpr.StringLit(sub.subsecond().substring(6))),
                            SqlType.Scalar.TEMPORAL_TEXT);
                }
                // SQL date+interval widens to TIMESTAMP; a StrictDate input
                // adjusted by a DAY-or-coarser unit stays a StrictDate.
                boolean strictIn = n.args().get(0).info().type()
                        == Type.Primitive.STRICT_DATE;
                boolean coarse = switch (Scalars.enumName(n.args().get(2))) {
                    case "YEARS", "MONTHS", "WEEKS", "DAYS" -> true;
                    default -> false;
                };
                return strictIn && coarse
                        ? new SqlExpr.Cast(added, SqlType.Scalar.DATE)
                        : added;
            });
        }
        // the TemporalFrame-stamped legacy-print channel twin (Pure.Lite
        // .ADJUST_TEMPORAL javadoc: engine mapToDBUnitType uppercase vs
        // sqlDialectTranslation lowercase): the plain adjust lowering with
        // its interval calls retagged to the TEMPORAL spelling fn.
        com.legend.model.FunctionId adjustKey = com.legend.builtin.Pure.AT_DATE_ADJUST.get(0);
        // date::add(date, duration) — the spec body IS adjust over the
        // Duration value's fields ($date->adjust($duration.number,
        // $duration.unit)): the amount reads off the lowered struct (a
        // literal folds to its field), the unit must be a static enum (the
        // interval spelling is compile-time) — a computed unit is loud.
        for (com.legend.model.FunctionId f : com.legend.builtin.Pure.AT_DATE_ADD) {
            rules.put(f, (n, args) -> {
                if (!(n.args().get(1) instanceof com.legend.compiler.spec.typed.TypedNewInstance ni)
                        || ni.properties().get("unit") == null
                        || ni.properties().get("number") == null) {
                    throw new com.legend.error.NotImplementedException(
                            "date::add over a non-literal Duration (the unit must be static)");
                }
                com.legend.compiler.spec.typed.TypedNativeCall asAdjust =
                        new com.legend.compiler.spec.typed.TypedNativeCall(n.callee(),
                                List.of(n.args().get(0), ni.properties().get("number"),
                                        ni.properties().get("unit")), n.info());
                return java.util.Objects.requireNonNull(rules.get(adjustKey)).apply(asAdjust,
                        List.of(args.get(0), SqlExpr.StructGet.of(args.get(1), "number"), args.get(1)));
            });
        }
        for (com.legend.model.FunctionId f : com.legend.builtin.Pure.AT_LEGEND_LITE_ADJUST_TEMPORAL) {
            rules.put(f, (n, args) -> retagTemporal(
                    java.util.Objects.requireNonNull(rules.get(adjustKey))
                            .apply(n, args)));
        }
    }

    /** Every interval call inside one adjust lowering retagged to the
     * TEMPORAL spelling — the whole expression came from that adjust,
     * so the scope is exact (never a blanket fold). */
    private static SqlExpr retagTemporal(SqlExpr e) {
        SqlExpr r = e.mapChildren(DateShifts::retagTemporal);
        return r instanceof SqlExpr.Call c && c.fn() == SqlFn.ADD_INTERVAL
                ? new SqlExpr.Call(SqlFn.ADD_INTERVAL_TEMPORAL, c.args())
                : r;
    }

    /** 2-arg {@code dayOfWeekNumber(d, firstDay)} — see the in-block
     * comment (ledger cluster 25; lives here with the date-shift
     * machinery, Scalars is at its file guardrail). */
    static void registerDayOfWeekNumber2(
            java.util.Map<com.legend.model.FunctionId, Scalars.Rule> rules) {
        // 2-arg dayOfWeekNumber(d, firstDay) — engine dayOfWeekNumber.pure:
        // Monday -> isodow, Sunday -> mod(isodow,7)+1; anything else is the
        // engine's own firstDayMondayOrSundayOnly constraint (ledger
        // cluster 25). Overrides the arity-blind extract key above.
        for (com.legend.model.FunctionId f : com.legend.model.FunctionId.ofAll(com.legend.builtin.Pure.DAY_OF_WEEK_NUMBER__DATE_1__DAY_OF_WEEK_1)) {
            rules.put(f, (n, args) -> {
                SqlExpr iso = new SqlExpr.Call(SqlFn.EXTRACT, List.of(
                        new SqlExpr.StringLit("isodow"),
                        Scalars.dateArg(n.args().get(0), args.get(0))));
                return switch (Scalars.enumName(n.args().get(1))) {
                    case "Monday" -> iso;
                    case "Sunday" -> SqlExpr.Call.of(SqlFn.PLUS,
                            SqlExpr.Call.of(SqlFn.MOD, iso,
                                    new SqlExpr.IntLit(7)),
                            new SqlExpr.IntLit(1));
                    default -> throw new com.legend.error
                            .NotImplementedException("dayOfWeekNumber:"
                            + " firstDayMondayOrSundayOnly (engine"
                            + " constraint)");
                };
            });
        }
    }

    /** Day-granularity comparisons ({@code isOnDay}, {@code isAfterDay},
     * {@code isOnOrAfterDay}): both operands truncated to the day, then
     * the comparison — moved from {@link Scalars} at the shape limit. */
    static void registerDayComparisons(java.util.Map<com.legend.model.FunctionId, Scalars.Rule> rules) {
        for (var e : java.util.Map.of(
                com.legend.builtin.Pure.AT_DATE_IS_ON_DAY, SqlFn.EQUAL,
                com.legend.builtin.Pure.AT_DATE_IS_AFTER_DAY, SqlFn.GREATER,
                com.legend.builtin.Pure.AT_DATE_IS_ON_OR_AFTER_DAY, SqlFn.GREATER_EQUAL).entrySet()) {
            for (com.legend.model.FunctionId f : e.getKey()) {
                rules.put(f, (n, args) -> SqlExpr.Call.of(e.getValue(),
                        new SqlExpr.Call(SqlFn.DATE_TRUNC_DAY,
                                List.of(Scalars.dateArg(n.args().get(0), args.get(0)))),
                        new SqlExpr.Call(SqlFn.DATE_TRUNC_DAY,
                                List.of(Scalars.dateArg(n.args().get(1), args.get(1))))));
            }
        }
    }
}
