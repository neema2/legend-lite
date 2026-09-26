// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.builtin.NativeFn;

import com.legend.compiler.spec.typed.TypedAggCol;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.IntSupplier;

/**
 * CALENDAR-AGGREGATION lowering (engine {@code calendarFunctions.pure},
 * task G1): a calendar native ({@code ytd($p.date, 'NY', %end, $p.value)})
 * in an aggregate MAP joins the fiscal calendar table TWICE — once on the
 * row's date, once on the report end date — and the aggregated value
 * becomes {@code CASE WHEN <fiscal condition over the two calendar rows>
 * THEN value ELSE NULL END}. Conditions are transcribed 1:1 from the
 * engine's {@code synthetiseXYZCaseCondition} functions; an untranscribed
 * function is LOUD, never a silent pass-through.
 */
final class CalendarAgg {

    private CalendarAgg() {
    }

    /** The two calendar aliases serving one agg's calendar call. */
    record Ctx(String cal0, String cal1) {
    }

    static @com.legend.base.Nullable TypedNativeCall calendarCallOf(TypedSpec mapBody) {
        return mapBody instanceof TypedNativeCall c
                && NativeFn.Calendar.of(c.callee().qualifiedName()).isPresent()
                && c.args().size() == 4
                ? c : null;
    }

    /**
     * LEFT-join the calendar table (twice per distinct (date, end, type)
     * triple — shared across aggs that agree) onto {@code base}'s source;
     * fills {@code ctxOut} per agg.
     */
    static SqlSelect joinCalendars(SqlSelect base, List<TypedAggCol> aggs,
            Map<TypedAggCol, Ctx> ctxOut,
            Function<TypedSpec, SqlExpr> lowerInBase, IntSupplier aliasCounter) {
        SqlSource from = base.from();
        Map<String, Ctx> byKey = new LinkedHashMap<>();
        for (TypedAggCol a : aggs) {
            TypedNativeCall c = calendarCallOf(lastBody(a));
            if (c == null) {
                continue;
            }
            String type = constString(c.args().get(1), "calendarType");
            SqlExpr dateExpr = lowerInBase.apply(c.args().get(0));
            SqlExpr endExpr = lowerInBase.apply(c.args().get(2));
            String key = type + "|" + dateExpr + "|" + endExpr;
            Ctx ctx = byKey.get(key);
            if (ctx == null) {
                String table = "LegendCalendarSchema." + type + "_Calendar";
                String cal0 = "cal" + aliasCounter.getAsInt();
                String cal1 = "cal" + aliasCounter.getAsInt();
                from = new SqlSource.Join(from,
                        new SqlSource.Table(table, cal0, List.of(), false),
                        SqlSource.Join.Kind.LEFT,
                        SqlExpr.Call.of(SqlFn.EQUAL, dateExpr,
                                SqlExpr.Column.physical(cal0, "date")));
                from = new SqlSource.Join(from,
                        new SqlSource.Table(table, cal1, List.of(), false),
                        SqlSource.Join.Kind.LEFT,
                        SqlExpr.Call.of(SqlFn.EQUAL, endExpr,
                                SqlExpr.Column.physical(cal1, "date")));
                ctx = new Ctx(cal0, cal1);
                byKey.put(key, ctx);
            }
            ctxOut.put(a, ctx);
        }
        return byKey.isEmpty() ? base : base.withFrom(from);
    }

    private static TypedSpec lastBody(TypedAggCol a) {
        List<TypedSpec> b = a.map().body();
        return b.get(b.size() - 1);
    }

    private static String constString(TypedSpec s, String what) {
        if (s instanceof TypedCString cs) {
            return cs.value();
        }
        throw new IllegalStateException("calendar aggregation " + what
                + " must be a string literal, got "
                + s.getClass().getSimpleName());
    }

    /** The fn's CASE-conditioned (and possibly NORMALISED) value. */
    static SqlExpr caseValue(TypedNativeCall call, Ctx ctx, SqlExpr value) {
        // the family is a closed type: a calendar call that is not a
        // CalendarFn is a catalog/enum mismatch, loud here
        NativeFn.Calendar fn = NativeFn.Calendar.of(call.callee().qualifiedName()).orElseThrow(() -> new IllegalStateException(
                "not a calendar function: " + call.callee().qualifiedName()));
        String c = ctx.cal0();
        String e = ctx.cal1();
        // multi-arm / normalised families first (whole-case forms); the plain
        // condition families go through condition(). A switch EXPRESSION with
        // no default: a 33rd constant does not compile until it is placed.
        return switch (fn) {
            case ANNUALIZED -> {
                // value / (endDay.fiscalDay / daysInYear), year-scoped
                SqlExpr factor = div(col(e, "fiscalDay"),
                        col(e, "numberOfFiscalDaysInYear"));
                yield caseOf(eq(col(c, "currentYear"), col(e, "currentYear")),
                        div(value, factor));
            }
            case CME -> {
                SqlExpr factor = div(col(e, "fiscalDay"),
                        col(e, "numberOfFiscalDaysInMonth"));
                yield caseOf(and(
                        eq(col(c, "currentYear"), col(e, "currentYear")),
                        eq(col(c, "currentMonthNum"), col(e, "currentMonthNum"))),
                        div(value, factor));
            }
            case P4WA -> {
                yield caseOf(priorWeeksRange(c, e, "prior4WeekDate"),
                        div(value, new SqlExpr.IntLit(4)));
            }
            case P12WA -> {
                yield caseOf(priorWeeksRange(c, e, "prior12WeekDate"),
                        div(value, new SqlExpr.IntLit(12)));
            }
            case P52WA -> {
                yield caseOf(priorWeeksRange(c, e, "prior52WeekDate"),
                        div(value, new SqlExpr.IntLit(52)));
            }
            case PMA -> {
                // Jan report: previous year's average (/12); later: current
                // year's elapsed months (/previousFiscalMonth)
                SqlExpr isJan = eq(col(e, "currentMonthNum"),
                        new SqlExpr.IntLit(1));
                SqlExpr notJan = SqlExpr.Call.of(SqlFn.GREATER,
                        col(e, "currentMonthNum"), new SqlExpr.IntLit(1));
                yield new SqlExpr.Case(List.of(
                        new SqlExpr.Case.When(
                                and(isJan, eq(col(c, "currentYear"),
                                        col(e, "previousFiscalYear"))),
                                div(value, new SqlExpr.IntLit(12))),
                        new SqlExpr.Case.When(
                                and(notJan,
                                        eq(col(c, "currentYear"),
                                                col(e, "currentYear")),
                                        lte(col(c, "currentMonthNum"),
                                                col(e, "previousFiscalMonth"))),
                                div(value, col(e, "previousFiscalMonth")))),
                        new SqlExpr.NullLit());
            }
            case PWA -> {
                SqlExpr first5 = lte(col(e, "currentWeek"),
                        new SqlExpr.IntLit(5));
                SqlExpr not5 = SqlExpr.Call.of(SqlFn.GREATER,
                        col(e, "currentWeek"), new SqlExpr.IntLit(5));
                SqlExpr endPrevWeek = SqlExpr.Call.of(SqlFn.MINUS,
                        col(e, "fiscalDay"), col(e, "fiscalDayOfWeek"));
                yield new SqlExpr.Case(List.of(
                        new SqlExpr.Case.When(
                                and(first5, eq(col(c, "currentYear"),
                                        col(e, "previousFiscalYear"))),
                                SqlExpr.Call.of(SqlFn.TIMES,
                                        div(value,
                                                col(e, "numberOfFiscalDaysInYear")),
                                        new SqlExpr.IntLit(5))),
                        new SqlExpr.Case.When(
                                and(not5,
                                        eq(col(c, "currentYear"),
                                                col(e, "currentYear")),
                                        lte(col(c, "fiscalDay"), endPrevWeek)),
                                SqlExpr.Call.of(SqlFn.TIMES,
                                        div(value, endPrevWeek),
                                        new SqlExpr.IntLit(5)))),
                        new SqlExpr.NullLit());
            }
            case PYWA -> {
                SqlExpr first5 = lte(col(e, "currentWeek"),
                        new SqlExpr.IntLit(5));
                yield caseOf(and(first5, eq(col(c, "currentYear"),
                                col(e, "previousFiscalYear"))),
                        SqlExpr.Call.of(SqlFn.TIMES,
                                div(value, col(e, "numberOfFiscalDaysInYear")),
                                new SqlExpr.IntLit(5)));
            }
            case P12MTD -> {
                SqlExpr start = new SqlExpr.Call(SqlFn.ADD_INTERVAL, List.of(
                        new SqlExpr.StringLit("to_years"),
                        new SqlExpr.IntLit(-1), col(e, "date")));
                yield caseOf(and(
                        SqlExpr.Call.of(SqlFn.GREATER, col(c, "date"), start),
                        lte(col(c, "date"), col(e, "date"))), value);
            }
            case P4WTD -> {
                yield caseOf(priorWeeksRange(c, e, "prior4WeekDate"), value);
            }
            case P12WTD -> {
                yield caseOf(priorWeeksRange(c, e, "prior12WeekDate"), value);
            }
            case P52WTD -> {
                yield caseOf(priorWeeksRange(c, e, "prior52WeekDate"), value);
            }
            case REPORT_END_DAY, CW_FM, CW, WTD, MTD, QTD, YTD, PWTD, PMTD, PQTD, PYTD, PYWTD, PYMTD, PYQTD, PRIOR_DAY, PRIOR_YEAR, CY_MINUS2, CY_MINUS3, PW, PW_FM ->
                    caseOf(condition(fn, c, e), value);
        };
    }

    private static SqlExpr caseOf(SqlExpr cond, SqlExpr value) {
        return new SqlExpr.Case(
                List.of(new SqlExpr.Case.When(cond, value)),
                new SqlExpr.NullLit());
    }

    private static SqlExpr div(SqlExpr l, SqlExpr r) {
        return SqlExpr.Call.of(SqlFn.DIVIDE, l, r);
    }

    /** date in [end.<priorCol> ; end adjusted?date:previousBusinessDay]. */
    private static SqlExpr priorWeeksRange(String c, String e, String priorCol) {
        SqlExpr isAdjusted = eq(col(e, "date"), col(e, "adjustedDate"));
        SqlExpr endCol = new SqlExpr.Case(
                List.of(new SqlExpr.Case.When(isAdjusted,
                        col(e, "adjustedDate"))),
                col(e, "previousBusinessDay"));
        return and(
                SqlExpr.Call.of(SqlFn.GREATER_EQUAL, col(c, "date"),
                        col(e, priorCol)),
                lte(col(c, "date"), endCol));
    }

    // ---- the per-function fiscal conditions (engine transcriptions) ----

    private static SqlExpr col(String alias, String name) {
        // calendar-table columns exist in DDL — PHYSICAL by definition
        return SqlExpr.Column.physical(alias, name);
    }

    private static SqlExpr eq(SqlExpr l, SqlExpr r) {
        return SqlExpr.Call.of(SqlFn.EQUAL, l, r);
    }

    private static SqlExpr lte(SqlExpr l, SqlExpr r) {
        return SqlExpr.Call.of(SqlFn.LESS_EQUAL, l, r);
    }

    private static SqlExpr and(SqlExpr... cs) {
        SqlExpr out = cs[0];
        for (int i = 1; i < cs.length; i++) {
            out = SqlExpr.Call.of(SqlFn.AND, out, cs[i]);
        }
        return out;
    }

    private static SqlExpr condition(NativeFn.Calendar fn, String c, String e) {
        return switch (fn) {
            case REPORT_END_DAY -> eq(col(c, "date"), col(e, "date"));
            case CW_FM -> eq(col(c, "fiscalWeekOffset"),
                    col(e, "fiscalWeekOffset"));
            case CW -> {
                // currentWeek = fiscalWeekOffset - (endDay is a weekend ? 1 : 0)
                SqlExpr weekend = SqlExpr.Call.of(SqlFn.IN,
                        col(e, "shortNameWeekDay"),
                        new SqlExpr.StringLit("Sat"), new SqlExpr.StringLit("Sun"));
                SqlExpr offset = new SqlExpr.Case(
                        List.of(new SqlExpr.Case.When(weekend,
                                new SqlExpr.IntLit(1))),
                        new SqlExpr.IntLit(0));
                yield eq(col(c, "fiscalWeekOffset"),
                        SqlExpr.Call.of(SqlFn.MINUS,
                                col(e, "fiscalWeekOffset"), offset));
            }
            case WTD -> and(
                    eq(col(c, "currentYear"), col(e, "currentYear")),
                    eq(col(c, "currentWeek"), col(e, "currentWeek")),
                    lte(col(c, "dayOfCalendarYear"), col(e, "dayOfCalendarYear")));
            case MTD -> and(
                    eq(col(c, "currentYear"), col(e, "currentYear")),
                    eq(col(c, "currentMonthNum"), col(e, "currentMonthNum")),
                    lte(col(c, "fiscalDay"), col(e, "fiscalDay")));
            case QTD -> and(
                    eq(col(c, "currentYear"), col(e, "currentYear")),
                    eq(col(c, "currentQuarterNum"), col(e, "currentQuarterNum")),
                    lte(col(c, "fiscalDay"), col(e, "fiscalDay")));
            case YTD -> and(
                    eq(col(c, "currentYear"), col(e, "currentYear")),
                    lte(col(c, "fiscalDay"), col(e, "fiscalDay")));
            case PWTD -> and(
                    eq(col(c, "fiscalWeekOffset"),
                            SqlExpr.Call.of(SqlFn.MINUS,
                                    col(e, "fiscalWeekOffset"),
                                    new SqlExpr.IntLit(1))),
                    lte(col(c, "fiscalDayOfWeek"), col(e, "fiscalDayOfWeek")));
            case PMTD -> {
                // January's previous month is last year's December
                SqlExpr isJan = eq(col(e, "currentMonthNum"), new SqlExpr.IntLit(1));
                SqlExpr yearOfPrev = new SqlExpr.Case(
                        List.of(new SqlExpr.Case.When(isJan,
                                col(e, "previousFiscalYear"))),
                        col(e, "currentYear"));
                yield and(
                        eq(col(c, "currentYear"), yearOfPrev),
                        eq(col(c, "currentMonthNum"), col(e, "previousFiscalMonth")),
                        lte(col(c, "fiscalDayOfMonth"), col(e, "fiscalDayOfMonth")));
            }
            case PQTD -> {
                SqlExpr isQ1 = eq(col(e, "currentQuarterNum"), new SqlExpr.IntLit(1));
                SqlExpr yearOfPrev = new SqlExpr.Case(
                        List.of(new SqlExpr.Case.When(isQ1,
                                col(e, "previousFiscalYear"))),
                        col(e, "currentYear"));
                yield and(
                        eq(col(c, "currentYear"), yearOfPrev),
                        eq(col(c, "currentQuarterNum"), col(e, "previousFiscalQuarter")),
                        lte(col(c, "fiscalDayOfQuarter"), col(e, "fiscalDayOfQuarter")));
            }
            case PYTD -> and(
                    eq(col(c, "currentYear"), col(e, "previousFiscalYear")),
                    lte(col(c, "fiscalDay"), col(e, "fiscalDay")));
            case PYWTD -> and(
                    eq(col(c, "currentYear"), col(e, "previousFiscalYear")),
                    eq(col(c, "currentWeek"), col(e, "currentWeek")),
                    lte(col(c, "fiscalDayOfWeek"), col(e, "fiscalDayOfWeek")));
            case PYMTD -> and(
                    eq(col(c, "currentYear"), col(e, "previousFiscalYear")),
                    eq(col(c, "currentMonthNum"), col(e, "currentMonthNum")),
                    lte(col(c, "fiscalDayOfMonth"), col(e, "fiscalDayOfMonth")));
            case PYQTD -> and(
                    eq(col(c, "currentYear"), col(e, "previousFiscalYear")),
                    eq(col(c, "currentQuarterNum"), col(e, "currentQuarterNum")),
                    lte(col(c, "fiscalDayOfQuarter"), col(e, "fiscalDayOfQuarter")));
            case PRIOR_DAY -> eq(col(c, "date"), col(e, "previousBusinessDay"));
            case PRIOR_YEAR -> eq(col(c, "currentYear"),
                    col(e, "previousFiscalYear"));
            case CY_MINUS2 -> eq(col(c, "currentYear"),
                    SqlExpr.Call.of(SqlFn.MINUS,
                            col(e, "previousFiscalYear"), new SqlExpr.IntLit(1)));
            case CY_MINUS3 -> eq(col(c, "currentYear"),
                    SqlExpr.Call.of(SqlFn.MINUS,
                            col(e, "previousFiscalYear"), new SqlExpr.IntLit(2)));
            case PW -> {
                // weekend end-day: 'previous week' skips two offsets
                SqlExpr weekend = SqlExpr.Call.of(SqlFn.IN,
                        col(e, "shortNameWeekDay"),
                        new SqlExpr.StringLit("Sat"), new SqlExpr.StringLit("Sun"));
                SqlExpr offset = new SqlExpr.Case(
                        List.of(new SqlExpr.Case.When(weekend,
                                new SqlExpr.IntLit(2))),
                        new SqlExpr.IntLit(1));
                yield eq(col(c, "fiscalWeekOffset"),
                        SqlExpr.Call.of(SqlFn.MINUS,
                                col(e, "fiscalWeekOffset"), offset));
            }
            case PW_FM -> eq(col(c, "fiscalWeekOffset"),
                    SqlExpr.Call.of(SqlFn.MINUS,
                            col(e, "fiscalWeekOffset"), new SqlExpr.IntLit(1)));
            case ANNUALIZED, CME, P4WA, P12WA, P52WA, PMA, PWA, PYWA, P12MTD, P4WTD, P12WTD, P52WTD ->
                    throw new IllegalStateException("whole-case calendar form '" + fn
                            + "' is dispatched by caseValue, never a plain condition");
        };
    }
}
