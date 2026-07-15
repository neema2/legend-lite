// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

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

    private static final String PKG = "meta::pure::functions::date::calendar::";

    /** The two calendar aliases serving one agg's calendar call. */
    record Ctx(String cal0, String cal1) {
    }

    static TypedNativeCall calendarCallOf(TypedSpec mapBody) {
        return mapBody instanceof TypedNativeCall c
                && c.callee().qualifiedName().startsWith(PKG)
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
                        new SqlSource.Table(table, cal0, List.of()),
                        SqlSource.Join.Kind.LEFT,
                        SqlExpr.Call.of(SqlFn.EQUAL, dateExpr,
                                new SqlExpr.Column(cal0, "date")));
                from = new SqlSource.Join(from,
                        new SqlSource.Table(table, cal1, List.of()),
                        SqlSource.Join.Kind.LEFT,
                        SqlExpr.Call.of(SqlFn.EQUAL, endExpr,
                                new SqlExpr.Column(cal1, "date")));
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
        String fn = call.callee().qualifiedName().substring(PKG.length());
        String c = ctx.cal0();
        String e = ctx.cal1();
        // multi-arm / normalised families first (whole-case forms)
        switch (fn) {
            case "annualized" -> {
                // value / (endDay.fiscalDay / daysInYear), year-scoped
                SqlExpr factor = div(col(e, "fiscalDay"),
                        col(e, "numberOfFiscalDaysInYear"));
                return caseOf(eq(col(c, "currentYear"), col(e, "currentYear")),
                        div(value, factor));
            }
            case "cme" -> {
                SqlExpr factor = div(col(e, "fiscalDay"),
                        col(e, "numberOfFiscalDaysInMonth"));
                return caseOf(and(
                        eq(col(c, "currentYear"), col(e, "currentYear")),
                        eq(col(c, "currentMonthNum"), col(e, "currentMonthNum"))),
                        div(value, factor));
            }
            case "p4wa" -> {
                return caseOf(priorWeeksRange(c, e, "prior4WeekDate"),
                        div(value, new SqlExpr.IntLit(4)));
            }
            case "p12wa" -> {
                return caseOf(priorWeeksRange(c, e, "prior12WeekDate"),
                        div(value, new SqlExpr.IntLit(12)));
            }
            case "p52wa" -> {
                return caseOf(priorWeeksRange(c, e, "prior52WeekDate"),
                        div(value, new SqlExpr.IntLit(52)));
            }
            case "pma" -> {
                // Jan report: previous year's average (/12); later: current
                // year's elapsed months (/previousFiscalMonth)
                SqlExpr isJan = eq(col(e, "currentMonthNum"),
                        new SqlExpr.IntLit(1));
                SqlExpr notJan = SqlExpr.Call.of(SqlFn.GREATER,
                        col(e, "currentMonthNum"), new SqlExpr.IntLit(1));
                return new SqlExpr.Case(List.of(
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
            case "pwa" -> {
                SqlExpr first5 = lte(col(e, "currentWeek"),
                        new SqlExpr.IntLit(5));
                SqlExpr not5 = SqlExpr.Call.of(SqlFn.GREATER,
                        col(e, "currentWeek"), new SqlExpr.IntLit(5));
                SqlExpr endPrevWeek = SqlExpr.Call.of(SqlFn.MINUS,
                        col(e, "fiscalDay"), col(e, "fiscalDayOfWeek"));
                return new SqlExpr.Case(List.of(
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
            case "pywa" -> {
                SqlExpr first5 = lte(col(e, "currentWeek"),
                        new SqlExpr.IntLit(5));
                return caseOf(and(first5, eq(col(c, "currentYear"),
                                col(e, "previousFiscalYear"))),
                        SqlExpr.Call.of(SqlFn.TIMES,
                                div(value, col(e, "numberOfFiscalDaysInYear")),
                                new SqlExpr.IntLit(5)));
            }
            case "p12mtd" -> {
                SqlExpr start = new SqlExpr.Call(SqlFn.ADD_INTERVAL, List.of(
                        new SqlExpr.StringLit("to_years"),
                        new SqlExpr.IntLit(-1), col(e, "date")));
                return caseOf(and(
                        SqlExpr.Call.of(SqlFn.GREATER, col(c, "date"), start),
                        lte(col(c, "date"), col(e, "date"))), value);
            }
            case "p4wtd" -> {
                return caseOf(priorWeeksRange(c, e, "prior4WeekDate"), value);
            }
            case "p12wtd" -> {
                return caseOf(priorWeeksRange(c, e, "prior12WeekDate"), value);
            }
            case "p52wtd" -> {
                return caseOf(priorWeeksRange(c, e, "prior52WeekDate"), value);
            }
            default -> {
                // plain condition families
            }
        }
        return caseOf(condition(fn, c, e), value);
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
        return new SqlExpr.Column(alias, name);
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

    private static SqlExpr condition(String fn, String c, String e) {
        return switch (fn) {
            case "reportEndDay" -> eq(col(c, "date"), col(e, "date"));
            case "cw_fm" -> eq(col(c, "fiscalWeekOffset"),
                    col(e, "fiscalWeekOffset"));
            case "cw" -> {
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
            case "wtd" -> and(
                    eq(col(c, "currentYear"), col(e, "currentYear")),
                    eq(col(c, "currentWeek"), col(e, "currentWeek")),
                    lte(col(c, "dayOfCalendarYear"), col(e, "dayOfCalendarYear")));
            case "mtd" -> and(
                    eq(col(c, "currentYear"), col(e, "currentYear")),
                    eq(col(c, "currentMonthNum"), col(e, "currentMonthNum")),
                    lte(col(c, "fiscalDay"), col(e, "fiscalDay")));
            case "qtd" -> and(
                    eq(col(c, "currentYear"), col(e, "currentYear")),
                    eq(col(c, "currentQuarterNum"), col(e, "currentQuarterNum")),
                    lte(col(c, "fiscalDay"), col(e, "fiscalDay")));
            case "ytd" -> and(
                    eq(col(c, "currentYear"), col(e, "currentYear")),
                    lte(col(c, "fiscalDay"), col(e, "fiscalDay")));
            case "pwtd" -> and(
                    eq(col(c, "fiscalWeekOffset"),
                            SqlExpr.Call.of(SqlFn.MINUS,
                                    col(e, "fiscalWeekOffset"),
                                    new SqlExpr.IntLit(1))),
                    lte(col(c, "fiscalDayOfWeek"), col(e, "fiscalDayOfWeek")));
            case "pmtd" -> {
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
            case "pqtd" -> {
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
            case "pytd" -> and(
                    eq(col(c, "currentYear"), col(e, "previousFiscalYear")),
                    lte(col(c, "fiscalDay"), col(e, "fiscalDay")));
            case "pywtd" -> and(
                    eq(col(c, "currentYear"), col(e, "previousFiscalYear")),
                    eq(col(c, "currentWeek"), col(e, "currentWeek")),
                    lte(col(c, "fiscalDayOfWeek"), col(e, "fiscalDayOfWeek")));
            case "pymtd" -> and(
                    eq(col(c, "currentYear"), col(e, "previousFiscalYear")),
                    eq(col(c, "currentMonthNum"), col(e, "currentMonthNum")),
                    lte(col(c, "fiscalDayOfMonth"), col(e, "fiscalDayOfMonth")));
            case "pyqtd" -> and(
                    eq(col(c, "currentYear"), col(e, "previousFiscalYear")),
                    eq(col(c, "currentQuarterNum"), col(e, "currentQuarterNum")),
                    lte(col(c, "fiscalDayOfQuarter"), col(e, "fiscalDayOfQuarter")));
            case "priorDay" -> eq(col(c, "date"), col(e, "previousBusinessDay"));
            case "priorYear" -> eq(col(c, "currentYear"),
                    col(e, "previousFiscalYear"));
            case "CYMinus2" -> eq(col(c, "currentYear"),
                    SqlExpr.Call.of(SqlFn.MINUS,
                            col(e, "previousFiscalYear"), new SqlExpr.IntLit(1)));
            case "CYMinus3" -> eq(col(c, "currentYear"),
                    SqlExpr.Call.of(SqlFn.MINUS,
                            col(e, "previousFiscalYear"), new SqlExpr.IntLit(2)));
            case "pw" -> {
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
            case "pw_fm" -> eq(col(c, "fiscalWeekOffset"),
                    SqlExpr.Call.of(SqlFn.MINUS,
                            col(e, "fiscalWeekOffset"), new SqlExpr.IntLit(1)));
            default -> throw new com.legend.error.NotImplementedException(
                    "calendar function '" + fn + "' condition is not"
                    + " transcribed yet (engine synthetise"
                    + fn.substring(0, 1).toUpperCase() + fn.substring(1)
                    + "CaseCondition)");
        };
    }
}
