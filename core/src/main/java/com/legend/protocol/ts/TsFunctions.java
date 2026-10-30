// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.ts;

import com.legend.protocol.spec.CFloat;
import com.legend.protocol.spec.CInteger;
import com.legend.protocol.spec.ColSpec;
import com.legend.protocol.spec.EnumValue;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.sql.SqlAst;
import com.legend.protocol.sql.SqlTranslationException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * THE LANGUAGE'S FUNCTIONS ALONG TIME (docs/TIMESERIES_LANGUAGE_2026_10_29.md §6; P4 of
 * docs/TS_LANGUAGE_BUILD_2026_10_30.md): each the tile's function of the same name and parameters (datacube's
 * timeseries-spec.ts TS_SIGNATURES, timeseries-query.ts call), written as the timeseries library's call over its
 * line's own rows -- so a tile's series written in {@code #TS} answers as the tile does (P5). A parameter is written by
 * place after the function's lines, or by name ({@code type => 'log'}, R10); a window is a number of observations,
 * {@code all} (from the start: the tile's 0) or a span ({@code 3mo}: the tile's calendar period).
 */
final class TsFunctions {

    private TsFunctions() {
    }

    private static final String LIBRARY = TsToPure.LIBRARY;
    private static final String DURATION = "meta::pure::functions::date::DurationUnit";

    /** A parameter's kind: a window or a lag ({@code SPAN}), a whole number, a number, a word of {@code words}, or a
     *  date ({@code '2024-01-01'}, given always). */
    enum Kind { SPAN, WHOLE, NUMBER, WORD, DATE }

    /** A parameter: its name, its kind, its default, its words (a word's). */
    record Param(String name, Kind kind, Object fallback, List<String> words) {
    }

    /** A function's signature: how many lines it reads (-1: several, a basket's), and its parameters in order. */
    record Signature(int lines, List<Param> params) {
    }

    private static Param span(String name, int fallback) {
        return new Param(name, Kind.SPAN, fallback, List.of());
    }

    private static Param whole(String name, int fallback) {
        return new Param(name, Kind.WHOLE, fallback, List.of());
    }

    private static Param number(String name, double fallback) {
        return new Param(name, Kind.NUMBER, fallback, List.of());
    }

    private static Param word(String name, String fallback, String... words) {
        return new Param(name, Kind.WORD, fallback, List.of(words));
    }

    private static Param date(String name) {
        return new Param(name, Kind.DATE, "", List.of());
    }

    private static Signature one(Param... params) {
        return new Signature(1, List.of(params));
    }

    private static final Param TYPE = word("type", "simple", "simple", "log", "absolute");

    /** THE FUNCTIONS (§6), each by its language name: what a call's name is registered as, and what it dispatches
     *  on. */
    enum Fn {
        PRICES("prices"),
        ANNUALIZE("annualize"),
        INDEX("index"),
        REBASE("rebase"),
        CHANGE("change"),
        PERCENT_CHANGE("percent_change"),
        RANGE_OF("range_of"),
        ZSCORES("zscores"),
        WINSORIZE("winsorize"),
        VOLATILITY("volatility"),
        DRAWDOWN("drawdown"),
        MAX_DRAWDOWN("max_drawdown"),
        SHARPE_RATIO("sharpe_ratio"),
        RSI("rsi"),
        SMOOTHED_MOVING_AVERAGE("smoothed_moving_average"),
        EXPONENTIAL_MOVING_AVERAGE("exponential_moving_average"),
        EXPONENTIAL_STD("exponential_std"),
        EXPONENTIAL_VOLATILITY("exponential_volatility"),
        EXPONENTIAL_SPREAD_VOLATILITY("exponential_spread_volatility"),
        MACD("macd"),
        PERCENTILES("percentiles"),
        MEDIAN("median"),
        PERCENTILE("percentile"),
        DICKEY_FULLER("dickey_fuller"),
        HALF_LIFE("half_life"),
        PRODUCT("product"),
        GEOMETRICALLY_AGGREGATE("geometrically_aggregate"),
        VOL_SWAP_VOLATILITY("vol_swap_volatility"),
        SMOOTH_SPIKES("smooth_spikes"),
        CONSECUTIVE("consecutive"),
        FIRST_OF("first_of"),
        LAST_OF("last_of"),
        FILTER_VALUES("filter_values"),
        DAY_OF("day_of"),
        MONTH_OF("month_of"),
        YEAR_OF("year_of"),
        QUARTER_OF("quarter_of"),
        WEEKDAY_OF("weekday_of"),
        CORRELATION("correlation"),
        BETA("beta"),
        COV("cov"),
        ALPHA("alpha"),
        R_SQUARED("r_squared"),
        HEDGE_RATIO("hedge_ratio"),
        RESIDUAL("residual"),
        TRACKING_ERROR("tracking_error"),
        INFORMATION_RATIO("information_ratio"),
        UP_CAPTURE("up_capture"),
        DOWN_CAPTURE("down_capture"),
        LEAD_LAG("lead_lag"),
        ATR("atr"),
        STOCHASTIC("stochastic"),
        KELTNER_CHANNELS("keltner_channels"),
        DONCHIAN_CHANNELS("donchian_channels"),
        OBV("obv"),
        VWAP("vwap"),
        TOTAL_RETURN("total_return"),
        EXCESS_RETURNS("excess_returns"),
        COMPARE("compare"),
        IF("if"),
        UNION_OF("union_of"),
        APPEND("append"),
        PREPEND("prepend"),
        WEIGHTED_SUM("weighted_sum"),
        BASKET("basket"),
        BOLLINGER_BANDS("bollinger_bands"),
        INTERPOLATE("interpolate"),
        OHLC("ohlc"),
        VALUE_AT("value_at"),
        TREND("trend"),
        SEASONALLY_ADJUSTED("seasonally_adjusted");

        private static final Map<String, Fn> BY_TEXT = byText();

        private static Map<String, Fn> byText() {
            Map<String, Fn> m = new LinkedHashMap<>();
            for (Fn f : values()) {
                m.put(f.text, f);
            }
            return java.util.Collections.unmodifiableMap(m);
        }

        final String text;

        Fn(String text) {
            this.text = text;
        }

        /** The function {@code text} names (none: null). */
        static @com.legend.base.Nullable Fn of(String text) {
            return BY_TEXT.get(text);
        }
    }

    /** THE SIGNATURES, the tile's (timeseries-spec.ts TS_SIGNATURES), each function by its language name -- but the
     *  windowed basics and the lags (avg, sum, min, max, std, var, count, lag, diff, returns: the builder's own,
     *  TsToPure's along, with its date windows). */
    static final Map<Fn, Signature> SIGNATURES;

    static {
        Map<Fn, Signature> s = new java.util.EnumMap<>(Fn.class);
        s.put(Fn.PRICES, one(number("initial", 1), TYPE));
        s.put(Fn.ANNUALIZE, one());
        s.put(Fn.INDEX, one(number("initial", 1)));
        s.put(Fn.REBASE, one(number("base", 100)));
        s.put(Fn.CHANGE, one());
        s.put(Fn.PERCENT_CHANGE, one());
        s.put(Fn.RANGE_OF, one(span("w", 22)));
        s.put(Fn.ZSCORES, one(span("w", 0)));
        s.put(Fn.WINSORIZE, one(number("limit", 2.5)));
        s.put(Fn.VOLATILITY, one(span("w", 22), TYPE));
        s.put(Fn.DRAWDOWN, one(span("w", 0)));
        s.put(Fn.MAX_DRAWDOWN, one(span("w", 0)));
        s.put(Fn.SHARPE_RATIO, one(span("w", 252)));
        s.put(Fn.RSI, one(whole("w", 14)));
        s.put(Fn.SMOOTHED_MOVING_AVERAGE, one(whole("w", 14)));
        s.put(Fn.EXPONENTIAL_MOVING_AVERAGE, one(number("beta", 0.75)));
        s.put(Fn.EXPONENTIAL_STD, one(number("beta", 0.75)));
        s.put(Fn.EXPONENTIAL_VOLATILITY, one(number("beta", 0.75)));
        s.put(Fn.EXPONENTIAL_SPREAD_VOLATILITY, one(number("beta", 0.75)));
        s.put(Fn.MACD, one(whole("m", 12), whole("n", 26), whole("s", 1)));
        s.put(Fn.PERCENTILES, one(span("w", 0)));
        s.put(Fn.MEDIAN, one(span("w", 22)));
        s.put(Fn.PERCENTILE, one(number("n", 50), span("w", 0)));
        s.put(Fn.DICKEY_FULLER, one(span("w", 0)));
        s.put(Fn.HALF_LIFE, one(span("w", 0)));
        s.put(Fn.PRODUCT, one(span("w", 0)));
        s.put(Fn.GEOMETRICALLY_AGGREGATE, one());
        s.put(Fn.VOL_SWAP_VOLATILITY, one(span("w", 0)));
        s.put(Fn.SMOOTH_SPIKES, one(number("threshold", 0.5), word("type", "percentage", "percentage", "absolute")));
        s.put(Fn.CONSECUTIVE, one(word("direction", "positive", "positive", "negative")));
        s.put(Fn.FIRST_OF, one());
        s.put(Fn.LAST_OF, one());
        s.put(Fn.FILTER_VALUES, one(word("operator", "empty", "empty", "less_than", "greater_than", "l_equals",
                "g_equals", "equals", "not_equals"), number("value", 0)));
        for (String d : List.of("day_of", "month_of", "year_of", "quarter_of", "weekday_of")) {
            s.put(java.util.Objects.requireNonNull(Fn.of(d)), one());
        }
        for (String pair : List.of("correlation", "beta", "cov", "alpha", "r_squared", "hedge_ratio", "residual",
                "tracking_error", "information_ratio", "up_capture", "down_capture")) {
            s.put(java.util.Objects.requireNonNull(Fn.of(pair)), new Signature(2, List.of(span("w", 63))));
        }
        s.put(Fn.LEAD_LAG, new Signature(2, List.of(whole("lag", 1), span("w", 63))));
        s.put(Fn.ATR, new Signature(3, List.of(whole("w", 14))));
        s.put(Fn.STOCHASTIC, new Signature(3, List.of(whole("w", 14))));
        s.put(Fn.KELTNER_CHANNELS, new Signature(3, List.of(whole("w", 20), whole("a", 10), number("k", 2))));
        s.put(Fn.DONCHIAN_CHANNELS, new Signature(2, List.of(span("w", 20))));
        s.put(Fn.OBV, new Signature(2, List.of()));
        s.put(Fn.VWAP, new Signature(2, List.of(span("w", 20))));
        s.put(Fn.TOTAL_RETURN, new Signature(2, List.of()));
        s.put(Fn.EXCESS_RETURNS, new Signature(2, List.of()));
        s.put(Fn.COMPARE, new Signature(2, List.of()));
        s.put(Fn.IF, new Signature(3, List.of()));
        s.put(Fn.UNION_OF, new Signature(2, List.of()));
        s.put(Fn.APPEND, new Signature(2, List.of()));
        s.put(Fn.PREPEND, new Signature(2, List.of()));
        s.put(Fn.WEIGHTED_SUM, new Signature(-1, List.of()));
        s.put(Fn.BASKET, new Signature(-1, List.of(word("rebalance", "daily", "daily", "weekly", "monthly"))));
        s.put(Fn.BOLLINGER_BANDS, one(span("w", 20), number("k", 2)));
        s.put(Fn.INTERPOLATE, new Signature(2, List.of(word("method", "intersect", "intersect", "nan", "zero",
                "step"))));
        s.put(Fn.OHLC, new Signature(4, List.of()));
        s.put(Fn.VALUE_AT, one(date("date")));
        for (String d : List.of("trend", "seasonally_adjusted")) {
            s.put(java.util.Objects.requireNonNull(Fn.of(d)), one(word("method", "additive", "additive", "multiplicative"), word("freq", "year", "year",
                    "quarter", "month", "week")));
        }
        SIGNATURES = java.util.Collections.unmodifiableMap(s);
    }

    /** What the functions are written with: a line's node, fresh names, a per-point lambda, the query. */
    interface Ctx extends TsLines.Writer {
        TsLines.Node node(SqlAst.Expr e);

        TsAst.Query query();

        /** Records that {@code value}, a function's answer, is a struct (R10): each field's column, in order. */
        void struct(String value, Map<String, String> fields);
    }

    /** Whether {@code name} is one of the functions here. */
    static boolean has(String name) {
        return Fn.of(name) != null;
    }

    /** A window or a lag as given: a number of observations ({@code 0}: from the start, {@code all}), or a span. */
    private record Span(int obs, TsAst.@com.legend.base.Nullable Span period) {
    }

    /** A call's parameters, bound: each by its name, of its kind. */
    private record Bound(Map<String, Object> values) {

        private Object get(String name) {
            return java.util.Objects.requireNonNull(values.get(name), name);
        }

        Span span(String name) {
            return (Span) get(name);
        }

        double num(String name) {
            return ((Number) get(name)).doubleValue();
        }

        int whole(String name) {
            return ((Number) get(name)).intValue();
        }

        String word(String name) {
            return (String) get(name);
        }
    }

    /** {@code fc}'s parameters, by place after its lines then by name, each defaulted as its signature says. */
    private static Map<String, Object> bind(Ctx c, SqlAst.FunctionCall fc, String fn, Signature sig,
            List<SqlAst.Expr> positional, Map<String, SqlAst.Expr> named) {
        // a function of any number of lines (each with its weight) takes its parameters by name alone
        int lines = sig.lines() < 0 ? positional.size() : sig.lines();
        List<SqlAst.Expr> given = positional.subList(Math.min(lines, positional.size()), positional.size());
        if (given.size() > sig.params().size()) {
            throw invalid(fn + " takes " + lines + " line" + (lines == 1 ? "" : "s") + " and its parameters "
                    + String.join(", ", sig.params().stream().map(Param::name).toList()) + " (R10)", fc.pos());
        }
        Map<String, Object> out = new LinkedHashMap<>();
        for (int i = 0; i < sig.params().size(); i++) {
            Param p = sig.params().get(i);
            SqlAst.Expr e = i < given.size() ? given.get(i) : named.get(p.name());
            if (i < given.size() && named.containsKey(p.name())) {
                throw invalid(fn + "'s " + p.name() + " is given twice (R10)", fc.pos());
            }
            if (e == null && p.kind() == Kind.DATE) {
                throw invalid(fn + " reads a date: " + fn + "(x, '2024-01-01') (R9)", fc.pos());
            }
            out.put(p.name(), e == null ? fallback(p) : value(c, fn, p, e));
        }
        for (String n : named.keySet()) {
            if (sig.params().stream().noneMatch(p -> p.name().equals(n))) {
                throw invalid(fn + " has no option " + n + (sig.params().isEmpty() ? "" : ": its parameters are "
                        + String.join(", ", sig.params().stream().map(Param::name).toList())) + " (R10)", fc.pos());
            }
        }
        return out;
    }

    private static Object fallback(Param p) {
        return p.kind() == Kind.SPAN ? new Span((Integer) p.fallback(), null) : p.fallback();
    }

    /** A parameter's value as written. */
    private static Object value(Ctx c, String fn, Param p, SqlAst.Expr e) {
        switch (p.kind()) {
            case SPAN -> {
                if (e instanceof SqlAst.ColumnRef r && c.query().alls().contains(r.pos())) {
                    return new Span(0, null);
                }
                TsAst.Span s = e instanceof SqlAst.ColumnRef r ? c.query().spans().get(r.pos()) : null;
                if (s != null) {
                    return new Span(-1, s);
                }
                if (e instanceof SqlAst.IntegerLit n && n.value().signum() > 0
                        && n.value().compareTo(java.math.BigInteger.valueOf(100_000)) <= 0) {
                    return new Span(n.value().intValue(), null);
                }
                throw invalid(fn + "'s " + p.name() + " is a number of observations, a span (3mo) or all (R4)",
                        e.pos());
            }
            case WHOLE -> {
                Double d = constant(e);
                if (d == null || d != Math.rint(d)) {
                    throw invalid(fn + "'s " + p.name() + " is a whole number", e.pos());
                }
                return d.intValue();
            }
            case NUMBER -> {
                Double d = constant(e);
                if (d == null) {
                    throw invalid(fn + "'s " + p.name() + " is a number", e.pos());
                }
                return d;
            }
            case DATE -> {
                String text = e instanceof SqlAst.StringLit s ? s.value() : e instanceof SqlAst.TypedLit t
                        && "date".equalsIgnoreCase(t.type().name()) ? t.value() : null;
                String date = fn + "'s " + p.name() + " is a date: '2024-01-01' (R9)";
                if (text == null) {
                    throw invalid(date, e.pos());
                }
                try {
                    return java.time.LocalDate.parse(text).toString();
                } catch (java.time.format.DateTimeParseException bad) {
                    throw invalid(date, e.pos());
                }
            }
            case WORD -> {
                if (!(e instanceof SqlAst.StringLit s) || !p.words().contains(s.value())) {
                    throw invalid(fn + "'s " + p.name() + " is " + String.join(", ", p.words().stream()
                            .map(x -> "'" + x + "'").toList()) + " (R10)", e.pos());
                }
                return s.value();
            }
        }
        throw new IllegalStateException(p.kind().name());
    }

    /** A number written as one: {@code 2}, {@code 0.5}, {@code -1}. */
    private static @com.legend.base.Nullable Double constant(SqlAst.Expr e) {
        if (e instanceof SqlAst.IntegerLit n) {
            return n.value().doubleValue();
        }
        if (e instanceof SqlAst.NumericLit n) {
            return n.value().doubleValue();
        }
        if (e instanceof SqlAst.Unary u && u.op() == SqlAst.UnaryOp.MINUS) {
            Double d = constant(u.operand());
            return d == null ? null : -d;
        }
        return null;
    }

    /**
     * THE CALL {@code fc} MEANS: its lines' nodes (each on its own rows), its parameters bound, the library's call over
     * them -- the tile's function of the same name.
     */
    static TsLines.Valued call(Ctx c, SqlAst.FunctionCall fc, List<SqlAst.Expr> positional,
            Map<String, SqlAst.Expr> named) {
        String fn = java.util.Objects.requireNonNull(TsToPure.name(fc));
        Fn f = java.util.Objects.requireNonNull(Fn.of(fn));
        Signature sig = java.util.Objects.requireNonNull(SIGNATURES.get(f));
        if (positional.size() < sig.lines()) {
            throw invalid(fn + " reads " + sig.lines() + " line" + (sig.lines() == 1 ? "" : "s") + ": " + fn + "(x"
                    + (sig.lines() > 1 ? ", y" : "") + ", ...)", fc.pos());
        }
        Bound p = new Bound(bind(c, fc, fn, sig, positional, named));
        TsLines.Valued many = several(c, fc, f, p, positional);
        if (many != null) {
            return many;
        }
        TsLines.Valued x = line(c, fn, positional.get(0), 0);
        Lib w = new Lib(c, x.line());
        String a = x.value();
        return switch (f) {
            case PRICES -> w.done(w.ofLine("prices", "px", a, new CFloat(p.num("initial")),
                    option("Returns", p.word("type"))));
            case ANNUALIZE -> w.done(w.ofRow("annualize", "ann", new ColSpec(a), new ColSpec(w.perYear())));
            case INDEX -> w.done(w.ofLine("index", "idx", a, new CFloat(p.num("initial"))));
            case REBASE -> w.done(w.ofLine("index", "idx", a, new CFloat(p.num("base"))));
            case CHANGE -> w.done(w.ofLine("change", "chg", a));
            case PERCENT_CHANGE -> w.done(w.ofLine("percentChange", "pchg", a));
            case RANGE_OF -> {
                Span s = p.span("w");
                yield w.done(w.rolled("range", a, s, s.period() == null ? s.obs() : 0));
            }
            case ZSCORES -> {
                Span s = p.span("w");
                yield w.done(s.period() != null ? w.ofPeriod("zscoresPeriod", "z", a, s.period())
                        : w.library("zscores", "z", a, s.obs() == 0 ? "all" : s.obs(),
                                new CInteger((long) s.obs())));
            }
            case WINSORIZE -> w.done(w.library("winsorize", "wins", a, "all", new CFloat(p.num("limit"))));
            case VOLATILITY -> {
                String r = w.returns(a, new Span(1, null), p.word("type"));
                Span s = p.span("w");
                if (s.period() == null) {
                    yield w.done(w.library("volatility", "vol", r, frame(s.obs()),
                            new CInteger((long) Math.max(s.obs(), 2)), new ColSpec(w.perYear())));
                }
                Period q = period(s.period());
                yield w.done(w.ofLine("volatilityPeriod", "vol", a, new ColSpec(r), new CInteger((long) q.n()),
                        new EnumValue(DURATION, q.unit()), new ColSpec(w.perYear())));
            }
            case DRAWDOWN -> {
                Span s = p.span("w");
                yield w.done(s.period() != null ? w.ofPeriod("drawdownPeriod", "dd", a, s.period())
                        : w.library("drawdown", "dd", a, frame(s.obs()), new CInteger((long) s.obs())));
            }
            case MAX_DRAWDOWN -> {
                Span s = p.span("w");
                yield w.done(s.period() != null ? w.ofPeriod("maxDrawdownPeriod", "mdd", a, s.period())
                        : w.library("maxDrawdown", "mdd", a, frame(s.obs()), new CInteger((long) s.obs())));
            }
            case SHARPE_RATIO -> {
                Span s = p.span("w");
                String r = w.returns(a, new Span(1, null), "simple");
                if (s.period() == null) {
                    yield w.done(w.library("sharpeRatio", "sharpe", r, frame(s.obs()),
                            new CInteger((long) Math.max(s.obs(), 2)), new ColSpec(w.perYear())));
                }
                Period q = period(s.period());
                yield w.done(w.ofLine("sharpeRatioPeriod", "sharpe", a, new ColSpec(r), new CInteger((long) q.n()),
                        new EnumValue(DURATION, q.unit()), new ColSpec(w.perYear())));
            }
            case RSI -> w.done(w.ofLine("rsi", "rsi", a, new CInteger((long) p.whole("w"))));
            case SMOOTHED_MOVING_AVERAGE -> w.done(w.ofLine("smoothedMean", "smma", a,
                    new CInteger((long) Math.max(p.whole("w"), 1))));
            case EXPONENTIAL_MOVING_AVERAGE -> p.num("beta") == 0 ? x
                    : w.done(w.ofLine("ewma", "ewm", a, new CFloat(p.num("beta"))));
            case EXPONENTIAL_STD -> w.done(w.ofLine("ewmStd", "ewm_std", a, new CFloat(p.num("beta"))));
            case EXPONENTIAL_VOLATILITY -> w.done(w.ofLine("exponentialVolatility", "evol", a,
                    new CFloat(p.num("beta")), new ColSpec(w.perYear())));
            case EXPONENTIAL_SPREAD_VOLATILITY -> w.done(w.ofLine("exponentialSpreadVolatility", "esvol", a,
                    new CFloat(p.num("beta")), new ColSpec(w.perYear())));
            case MACD -> w.done(w.ofLine("macd", "macd", a, new CInteger((long) p.whole("m")),
                    new CInteger((long) p.whole("n")), new CInteger((long) p.whole("s"))));
            case PERCENTILES -> {
                Span s = p.span("w");
                yield w.done(s.period() != null ? w.ofPeriod("percentilesPeriod", "pr", a, s.period())
                        : w.ofLine("percentiles", "pr", a, new CInteger((long) s.obs())));
            }
            case MEDIAN, PERCENTILE -> {
                Span s = p.span("w");
                CFloat q = new CFloat("median".equals(fn) ? 0.5 : p.num("n") / 100);
                if ("percentile".equals(fn) && s.period() == null && s.obs() == 0) {
                    yield w.done(w.ofRow("quantileOfLine", "pct", new ColSpec(w.line.part()), new ColSpec(a), q));
                }
                yield w.done(s.period() != null ? w.ofPeriod("quantilePeriod", "pct", a, s.period(), q)
                        : w.ofLine("quantile", "pct", a, new CInteger((long) s.obs()), q));
            }
            case DICKEY_FULLER, HALF_LIFE -> {
                Span s = p.span("w");
                if (s.period() == null && s.obs() > 0 && s.obs() < 3) {
                    throw invalid(fn + " over " + s.obs() + " differences has no deviation left: w is all (every one"
                            + " to date) or at least 3", fc.pos());
                }
                String of = "half_life".equals(fn) ? "halfLife" : "dickeyFuller";
                yield w.done(s.period() != null ? w.ofPeriod(of + "Period", fn, a, s.period())
                        : w.library(of, fn, a, frame(s.obs()), new CInteger((long) Math.max(s.obs(), 3))));
            }
            case PRODUCT -> {
                Span s = p.span("w");
                yield w.done(s.period() != null ? w.ofPeriod("productPeriod", "prod", a, s.period())
                        : w.library("product", "prod", a, frame(s.obs()), new CInteger((long) s.obs())));
            }
            case GEOMETRICALLY_AGGREGATE -> w.done(w.ofLine("geometricallyAggregate", "geo", a));
            case VOL_SWAP_VOLATILITY -> {
                Span s = p.span("w");
                yield w.done(s.period() != null ? w.ofPeriod("volSwapVolatilityPeriod", "vs", a, s.period())
                        : w.library("volSwapVolatility", "vs", a, frame(s.obs()), new CInteger((long) s.obs())));
            }
            case SMOOTH_SPIKES -> w.done(w.ofLine("smoothSpikes", "smooth", a, new CFloat(p.num("threshold")),
                    option("ThresholdType", p.word("type"))));
            case CONSECUTIVE -> w.done(w.ofLine("consecutive", "cs", a, option("Direction",
                    p.word("direction"))));
            case VALUE_AT -> w.done(w.ofLine("valueAt", "va", a, new com.legend.protocol.spec.CDate(
                    com.legend.values.PureDateLiteral.parse(p.word("date")))));
            case TREND, SEASONALLY_ADJUSTED -> w.done(decomposed(c, w, fc, fn, a, p.word("method"),
                    p.word("freq")));
            case FIRST_OF -> w.done(w.ofLine("first", "first", a));
            case LAST_OF -> w.done(w.ofLine("last", "last", a));
            case FILTER_VALUES -> {
                ValueSpecification rel = TsLines.call(LIBRARY + "filterValues", w.line.rel(), new ColSpec(a),
                        option("FilterOperator", p.word("operator")), new CFloat(p.num("value")));
                yield new TsLines.Valued(w.line.with(rel), a);
            }
            case DAY_OF, MONTH_OF, YEAR_OF, QUARTER_OF, WEEKDAY_OF -> w.done(w.ofRow(
                    fn.substring(0, fn.length() - "_of".length()), fn, new ColSpec(w.line.at())));
            case BOLLINGER_BANDS -> {
                Span s = p.span("w");
                CFloat k = new CFloat(p.num("k"));
                List<String> out = s.period() == null
                        ? w.many("bollingerBands", List.of("lower", "upper", "width"), w.window(frame(s.obs())),
                                new ColSpec(a), new CInteger((long) s.obs()), k)
                        : w.many("bollingerBandsPeriod", List.of("lower", "upper", "width"),
                                new ColSpec(w.line.part()), new ColSpec(w.line.at()), new ColSpec(a),
                                new CInteger((long) period(s.period()).n()),
                                new EnumValue(DURATION, period(s.period()).unit()), k);
                yield w.band(out);
            }
            default -> throw new IllegalStateException("no writing of " + fn);
        };
    }

    /** Each pair's function of the library (the tile's PAIRWISE): a hedge ratio a beta of levels. */
    private static final Map<String, String> PAIRWISE = Map.of("correlation", "correlation", "r_squared", "rSquared",
            "cov", "covariance", "beta", "beta", "hedge_ratio", "beta", "alpha", "alpha", "residual", "residual");

    /** Each benchmark-relative statistic's (the tile's BENCHMARKED). */
    private static final Map<String, String> BENCHMARKED = Map.of("tracking_error", "trackingError",
            "information_ratio", "informationRatio", "up_capture", "upCapture", "down_capture", "downCapture");

    /**
     * THE FUNCTIONS OF SEVERAL LINES (the tile's pairs, otherCalls and analystCalls): each line on its own rows first
     * (a pair's returns each on its own), then met on the dates all have -- or, where a function reads which has a
     * row (an if's, a splice's), on every date any has; null for a function of one line.
     */
    private static TsLines.@com.legend.base.Nullable Valued several(Ctx c, SqlAst.FunctionCall fc, Fn f,
            Bound p, List<SqlAst.Expr> args) {
        String fn = f.text;
        SqlAst.Pos pos = fc.pos();
        if (PAIRWISE.containsKey(fn)) {
            TsLines.Valued a = line(c, fn, args.get(0), 0);
            TsLines.Valued b = line(c, fn, args.get(1), 1);
            boolean levels = "cov".equals(fn) || "hedge_ratio".equals(fn) || "residual".equals(fn);
            TsLines.Valued ra = levels ? a : simple(c, a);
            TsLines.Valued rb = levels ? b : simple(c, b);
            Lib w = new Lib(c, TsLines.joined(c, List.of(ra, rb), pos, null));
            Span s = p.span("w");
            List<ValueSpecification> perYear = "alpha".equals(fn) ? List.of(new ColSpec(w.perYear())) : List.of();
            String of = java.util.Objects.requireNonNull(PAIRWISE.get(fn));
            List<ValueSpecification> all = new ArrayList<>();
            if (s.period() == null) {
                all.addAll(List.of(w.window(frame(s.obs())), new ColSpec(ra.value()), new ColSpec(rb.value()),
                        new CInteger((long) Math.max(s.obs(), 2))));
                all.addAll(perYear);
                return w.done(w.ofRow(of, fn, all.toArray(new ValueSpecification[0])));
            }
            Period q = period(s.period());
            all.addAll(List.of(new ColSpec(w.line.part()), new ColSpec(w.line.at()), new ColSpec(ra.value()),
                    new ColSpec(rb.value()), new CInteger((long) q.n()), new EnumValue(DURATION, q.unit())));
            all.addAll(perYear);
            return w.done(w.ofRow(of + "Period", fn, all.toArray(new ValueSpecification[0])));
        }
        if (BENCHMARKED.containsKey(fn) || "lead_lag".equals(fn)) {
            int k = "lead_lag".equals(fn) ? p.whole("lag") : 0;
            TsLines.Valued ra = simple(c, line(c, fn, args.get(0), 0));
            TsLines.Valued rb = simple(c, line(c, fn, args.get(1), 1));
            if (k != 0) {
                // the second's k observations before (a lag), or after (a lead), on its own dates (the tile's)
                Lib l = new Lib(c, rb.line());
                rb = l.done(k > 0 ? l.library("lag", "lag", rb.value(), null, new CInteger((long) k))
                        : l.lead(rb.value(), -k));
            }
            Lib w = new Lib(c, TsLines.joined(c, List.of(ra, rb), pos, null));
            List<String> ab = w.many("pairs", List.of("pa", "pb"), new ColSpec(ra.value()), new ColSpec(rb.value()));
            Span s = p.span("w");
            boolean capture = "up_capture".equals(fn) || "down_capture".equals(fn);
            String of = "lead_lag".equals(fn) ? "leadLag" : java.util.Objects.requireNonNull(BENCHMARKED.get(fn));
            List<ValueSpecification> perYear = capture || "lead_lag".equals(fn) ? List.of()
                    : List.of(new ColSpec(w.perYear()));
            List<ValueSpecification> all = new ArrayList<>();
            if (s.period() == null) {
                all.addAll(List.of(w.window(frame(s.obs())), new ColSpec(ab.get(0)), new ColSpec(ab.get(1)),
                        new CInteger((long) (capture ? s.obs() : Math.max(s.obs(), 2)))));
                all.addAll(perYear);
                return w.done(w.ofRow(of, fn, all.toArray(new ValueSpecification[0])));
            }
            Period q = period(s.period());
            all.addAll(List.of(new ColSpec(w.line.part()), new ColSpec(w.line.at()), new ColSpec(ab.get(0)),
                    new ColSpec(ab.get(1)), new CInteger((long) q.n()), new EnumValue(DURATION, q.unit())));
            all.addAll(perYear);
            return w.done(w.ofRow(of + "Period", fn, all.toArray(new ValueSpecification[0])));
        }
        switch (f) {
            case ATR, STOCHASTIC, KELTNER_CHANNELS -> {
                List<TsLines.Valued> hlc = List.of(line(c, fn, args.get(0), 0), line(c, fn, args.get(1), 1),
                        line(c, fn, args.get(2), 2));
                Lib w = new Lib(c, TsLines.joined(c, hlc, pos, null));
                List<ValueSpecification> roles = new ArrayList<>(List.of(new ColSpec(w.line.part()),
                        new ColSpec(w.line.at())));
                hlc.forEach(v -> roles.add(new ColSpec(v.value())));
                if ("atr".equals(fn)) {
                    roles.add(new CInteger((long) Math.max(p.whole("w"), 1)));
                    return w.done(w.ofRow("atr", "atr", roles.toArray(new ValueSpecification[0])));
                }
                if ("stochastic".equals(fn)) {
                    roles.add(new CInteger((long) p.whole("w")));
                    return w.done(w.ofRow("stochastic", "stoch", roles.toArray(new ValueSpecification[0])));
                }
                roles.addAll(List.of(new CInteger((long) Math.max(p.whole("w"), 1)),
                        new CInteger((long) Math.max(p.whole("a"), 1)), new CFloat(p.num("k"))));
                return w.band(w.many("keltnerChannels", List.of("lower", "upper", "width"),
                        roles.toArray(new ValueSpecification[0])));
            }
            case DONCHIAN_CHANNELS -> {
                TsLines.Valued hi = line(c, fn, args.get(0), 0);
                TsLines.Valued lo = line(c, fn, args.get(1), 1);
                Lib w = new Lib(c, TsLines.joined(c, List.of(hi, lo), pos, null));
                Span s = p.span("w");
                return w.band(s.period() == null
                        ? w.many("donchianChannels", List.of("lower", "upper", "width"), w.window(frame(s.obs())),
                                new ColSpec(hi.value()), new ColSpec(lo.value()), new CInteger((long) s.obs()))
                        : w.many("donchianChannelsPeriod", List.of("lower", "upper", "width"),
                                new ColSpec(w.line.part()), new ColSpec(w.line.at()), new ColSpec(hi.value()),
                                new ColSpec(lo.value()), new CInteger((long) period(s.period()).n()),
                                new EnumValue(DURATION, period(s.period()).unit())));
            }
            case OBV -> {
                TsLines.Valued cl = line(c, fn, args.get(0), 0);
                TsLines.Valued vol = line(c, fn, args.get(1), 1);
                Lib w = new Lib(c, TsLines.joined(c, List.of(cl, vol), pos, null));
                return w.done(w.ofRow("obv", "obv", new ColSpec(w.line.part()), new ColSpec(w.line.at()),
                        new ColSpec(cl.value()), new ColSpec(vol.value())));
            }
            case VWAP -> {
                TsLines.Valued px = line(c, fn, args.get(0), 0);
                TsLines.Valued vol = line(c, fn, args.get(1), 1);
                Lib w = new Lib(c, TsLines.joined(c, List.of(px, vol), pos, null));
                String traded = w.ofRow("tradedValue", "rs", new ColSpec(px.value()), new ColSpec(vol.value()));
                Span s = p.span("w");
                if (s.period() == null) {
                    return w.done(w.ofRow("vwap", "vwap", w.window(frame(s.obs())), new ColSpec(px.value()),
                            new ColSpec(traded), new ColSpec(vol.value()), new CInteger((long) s.obs())));
                }
                Period q = period(s.period());
                return w.done(w.ofLine("vwapPeriod", "vwap", px.value(), new ColSpec(traded),
                        new ColSpec(vol.value()), new CInteger((long) q.n()), new EnumValue(DURATION, q.unit())));
            }
            case TOTAL_RETURN, EXCESS_RETURNS -> {
                TsLines.Valued x = line(c, fn, args.get(0), 0);
                TsLines.Node second = c.node(args.get(1));
                if (second instanceof TsLines.Constant k) {
                    Double rate = constant(k.expr());
                    if (rate == null) {
                        throw invalid(fn + "'s second is a line or a number", args.get(1).pos());
                    }
                    Lib w = new Lib(c, x.line());
                    return w.done(w.ofLine("total_return".equals(fn) ? "totalReturn" : "excessReturns", fn,
                            x.value(), new ColSpec(TsLines.ORD), new EnumValue(DURATION, x.line().seconds() ? "SECONDS"
                                    : "DAYS"), new CFloat(rate)));
                }
                TsLines.Valued y = (TsLines.Valued) second;
                if ("excess_returns".equals(fn)) {
                    Lib w = new Lib(c, TsLines.joined(c, List.of(x, y), pos, null));
                    return w.done(w.ofLine("excessReturnsOver", "er", x.value(), new ColSpec(y.value())));
                }
                Lib w = new Lib(c, TsLines.spread(c, List.of(x, y), pos).line());
                return w.done(w.ofLine("totalReturnOf", "tr", x.value(), new ColSpec(y.value())));
            }
            case INTERPOLATE -> {
                TsLines.Valued x = line(c, fn, args.get(0), 0);
                TsLines.Valued y = line(c, fn, args.get(1), 1);
                return TsLines.interpolated(c, x, y, p.word("method"), pos);
            }
            case OHLC -> {
                // a candle (R10): its four lines on the dates all have -- under SAMPLE BY each its period's own
                // aggregate, as written: ohlc(first(open['X']), max(high['X']), min(low['X']), last(close['X']))
                List<TsLines.Valued> parts = new ArrayList<>();
                for (int i = 0; i < 4; i++) {
                    parts.add(line(c, fn, args.get(i), i));
                }
                TsAst.Fill fill = c.query().fill();
                Lib w = new Lib(c, TsLines.joined(c, parts, pos, fill == null ? null : fill.how()));
                Map<String, String> fields = new LinkedHashMap<>();
                List<String> names = List.of("open", "high", "low", "close");
                for (int i = 0; i < 4; i++) {
                    fields.put(names.get(i), parts.get(i).value());
                }
                c.struct(parts.get(3).value(), java.util.Collections.unmodifiableMap(fields));
                return w.done(parts.get(3).value());
            }
            case COMPARE -> {
                TsLines.Node x = c.node(args.get(0));
                TsLines.Node y = c.node(args.get(1));
                List<TsLines.Valued> parts = lines(x, y);
                if (parts.isEmpty()) {
                    throw invalid("compare reads a line: compare(x, 0)", pos);
                }
                TsAst.Fill fill = c.query().fill();
                Lib w = new Lib(c, parts.size() == 1 ? parts.get(0).line()
                        : TsLines.joined(c, parts, pos, fill == null ? null : fill.how()));
                return w.done(w.ofRow("compare", "cmp", new ColSpec(w.side(x)), new ColSpec(w.side(y))));
            }
            case IF -> {
                // a condition's flags 1 where it holds, 0 where it does not, none where it is none (the library's
                // ifThen reads a line's flags, a figure's not 0)
                SqlAst.Expr cond = args.get(0);
                TsLines.Valued flags = line(c, fn, truth(cond) ? new SqlAst.Case(null, List.of(
                        new SqlAst.When(cond, new SqlAst.IntegerLit(java.math.BigInteger.ONE, cond.pos())),
                        new SqlAst.When(new SqlAst.Unary(SqlAst.UnaryOp.NOT, cond, cond.pos()),
                                new SqlAst.IntegerLit(java.math.BigInteger.ZERO, cond.pos()))), null, cond.pos())
                        : cond, 0);
                TsLines.Node x = c.node(args.get(1));
                TsLines.Node y = c.node(args.get(2));
                List<TsLines.Valued> parts = new ArrayList<>(List.of(flags));
                parts.addAll(lines(x, y));
                Lib w;
                if (parts.size() > 1) {
                    TsLines.Spread spread = TsLines.spread(c, parts, pos);
                    // the flags' dates alone
                    w = new Lib(c, spread.line().with(TsLines.call("filter", spread.line().rel(), TsLines.lambda("x",
                            TsLines.call("isNotEmpty", TsLines.col(spread.ords().get(0)))))));
                } else {
                    w = new Lib(c, flags.line());
                }
                return w.done(w.ofRow("ifThen", "if", new ColSpec(flags.value()), new ColSpec(w.side(x)),
                        new ColSpec(w.side(y))));
            }
            case UNION_OF, APPEND, PREPEND -> {
                TsLines.Valued x = line(c, fn, args.get(0), 0);
                TsLines.Valued y = line(c, fn, args.get(1), 1);
                TsLines.Spread spread = TsLines.spread(c, List.of(x, y), pos);
                Lib w = new Lib(c, spread.line());
                if ("union_of".equals(fn)) {
                    return w.done(w.ofRow("union", "union", new ColSpec(x.value()), new ColSpec(y.value())));
                }
                return w.done(w.ofRow(fn, fn, new ColSpec(w.line.part()), new ColSpec(w.line.at()),
                        new ColSpec(x.value()), new ColSpec(y.value()), new ColSpec(spread.ords().get(0)),
                        new ColSpec(spread.ords().get(1))));
            }
            case WEIGHTED_SUM, BASKET -> {
                List<TsLines.Valued> members = new ArrayList<>();
                List<Double> weights = new ArrayList<>();
                for (int i = 0; i < args.size(); i += 2) {
                    members.add(line(c, fn, args.get(i), i));
                    Double weight = i + 1 < args.size() ? constant(args.get(i + 1)) : null;
                    if (weight == null) {
                        throw invalid(fn + " reads each line and its weight: " + fn + "(x, 0.6, y, 0.4)",
                                i + 1 < args.size() ? args.get(i + 1).pos() : pos);
                    }
                    weights.add(weight);
                }
                Lib w = new Lib(c, TsLines.joined(c, members, pos, null));
                return w.done("basket".equals(fn) ? w.basket(members, weights, p.word("rebalance"))
                        : w.weighted(members, weights));
            }
            default -> {
                return null;
            }
        }
    }

    /** The lines among {@code nodes}. */
    private static List<TsLines.Valued> lines(TsLines.Node... nodes) {
        List<TsLines.Valued> out = new ArrayList<>();
        for (TsLines.Node n : nodes) {
            if (n instanceof TsLines.Valued v && out.stream().noneMatch(o -> o == v)) {
                out.add(v);
            }
        }
        return out;
    }

    /** {@code v}'s simple returns, on its own rows (a pair's, a benchmark's). */
    private static TsLines.Valued simple(Ctx c, TsLines.Valued v) {
        Lib l = new Lib(c, v.line());
        return l.done(l.returns(v.value(), new Span(1, null), "simple"));
    }

    /** {@code fn}'s {@code i}-th line: a node of a line, never a number alone. */
    private static TsLines.Valued line(Ctx c, String fn, SqlAst.Expr e, int i) {
        if (!(c.node(e) instanceof TsLines.Valued v)) {
            throw invalid(fn + " reads a line: its " + (i == 0 ? "first" : i == 1 ? "second" : "third")
                    + " argument is a number", e.pos());
        }
        return v;
    }

    /** Each SAMPLE BY's periods as the library's grain (§5's units). */
    private static final Map<String, String> GRAINS = Map.of("d", "day", "w", "week", "mo", "month", "q", "quarter",
            "y", "year");

    /** A decomposition's cycle in periods of a grain (the tile's CYCLES): a cycle of business days at days. */
    private static final Map<String, Map<String, Integer>> CYCLES = Map.of(
            "day", Map.of("week", 5, "month", 21, "quarter", 63, "year", 252),
            "week", Map.of("month", 4, "quarter", 13, "year", 52),
            "month", Map.of("quarter", 3, "year", 12),
            "quarter", Map.of("year", 4));

    /**
     * A CLASSICAL DECOMPOSITION (the tile's decomposed: gs-quant's trend and seasonally_adjusted, statsmodels'
     * seasonal_decompose): its cycle {@code freq} counted in the query's periods ({@code SAMPLE BY}); the trend a moving
     * average centred over it, the season each place in the cycle's mean of the detrended values -- the library's.
     */
    private static String decomposed(Ctx c, Lib w, SqlAst.FunctionCall fc, String fn, String a, String method,
            String freq) {
        TsAst.Span sample = c.query().sample();
        if (sample == null) {
            throw invalid(fn + " counts its cycle in the query's periods: add SAMPLE BY 1mo (R5)", fc.pos());
        }
        String grain = java.util.Objects.requireNonNull(GRAINS.get(sample.unit()), sample.unit());
        Map<String, Integer> cycles = CYCLES.get(grain);
        Integer p = cycles == null ? null : cycles.get(freq);
        if (p == null) {
            throw invalid(fn + ": a " + freq + " is no cycle of " + grain + "s: choose a longer one, or another SAMPLE"
                    + " BY", fc.pos());
        }
        CInteger cycle = new CInteger((long) p);
        CInteger half = new CInteger((long) (p / 2));
        if ("trend".equals(fn)) {
            return w.ofLine("trend", "dc_trend", a, cycle, half);
        }
        // its place in the cycle: the calendar's at weeks, months and quarters (a missing period never moving the
        // places after it), its row's number at days
        String place = "day".equals(grain) ? w.rowNumber()
                : w.ofRow("periodIndex", "dc_i", new ColSpec(TsLines.PERIOD), option("Grain", grain));
        return w.ofLine("seasonallyAdjusted", "dc_sa", a, new ColSpec(place), cycle, half,
                option("SeasonalModel", method));
    }

    /** Whether {@code e} is a condition: a comparison, a test, or one of them combined. */
    private static boolean truth(SqlAst.Expr e) {
        return switch (e) {
            case SqlAst.Binary b -> switch (b.op()) {
                case OR, AND, EQ, NE, LT, LE, GT, GE -> true;
                case PLUS, MINUS, TIMES, DIVIDE, MODULO, POWER, CONCAT -> false;
            };
            case SqlAst.Unary u -> u.op() == SqlAst.UnaryOp.NOT;
            case SqlAst.IsNull i -> true;
            case SqlAst.InList i -> true;
            case SqlAst.Between b -> true;
            case SqlAst.Like l -> true;
            default -> false;
        };
    }

    /** {@code w} observations (0: from the start) as a frame: the tile's Writer.frame. */
    private static Object frame(int w) {
        return w == 0 ? "running" : w;
    }

    private static EnumValue option(String enumeration, String value) {
        return new EnumValue(LIBRARY + enumeration, value);
    }

    /** A span as the library's calendar periods take it (§5): a count of a DurationUnit, a quarter three months. */
    record Period(int n, String unit) {
    }

    static Period period(TsAst.Span s) {
        return switch (s.unit()) {
            case "d" -> new Period(s.n(), "DAYS");
            case "w" -> new Period(s.n(), "WEEKS");
            case "mo" -> new Period(s.n(), "MONTHS");
            case "q" -> new Period(3 * s.n(), "MONTHS");
            case "y" -> new Period(s.n(), "YEARS");
            case "h" -> new Period(s.n(), "HOURS");
            case "min" -> new Period(s.n(), "MINUTES");
            case "s" -> new Period(s.n(), "SECONDS");
            default -> throw invalid("a span is d, w, mo, q, y, h, min or s (§5)", s.pos());
        };
    }

    /**
     * THE LIBRARY'S CALLS OVER ONE LINE (the tile's Writer): each over the line's window, or its line and its dates,
     * or each row alone, its figure a fresh column; the line as it grows.
     */
    private static final class Lib {
        private final Ctx c;
        private TsLines.Line line;
        private @com.legend.base.Nullable String perYear;

        Lib(Ctx c, TsLines.Line line) {
            this.c = c;
            this.line = line;
        }

        TsLines.Valued done(String column) {
            return new TsLines.Valued(line, column);
        }

        private String then(String name, String what, List<ValueSpecification> args) {
            String out = c.fresh(what);
            List<ValueSpecification> all = new ArrayList<>(List.of(line.rel()));
            all.addAll(args);
            all.add(new ColSpec(out));
            line = line.with(TsLines.call(LIBRARY + name, all));
            return out;
        }

        /** {@code timeseries::name(over(~[line], [~period->ascending()], frame), ~value, args, ~out)}. */
        String library(String name, String what, String value, @com.legend.base.Nullable Object frame,
                ValueSpecification... args) {
            List<ValueSpecification> all = new ArrayList<>(List.of(TsLines.over(line, frameOf(frame)),
                    new ColSpec(value)));
            all.addAll(List.of(args));
            return then(name, what, all);
        }

        /** {@code timeseries::name(~line, ~date, ~value, args, ~out)}. */
        String ofLine(String name, String what, String value, ValueSpecification... args) {
            List<ValueSpecification> all = new ArrayList<>(List.of(new ColSpec(line.part()), new ColSpec(line.at()),
                    new ColSpec(value)));
            all.addAll(List.of(args));
            return then(name, what, all);
        }

        /** {@link #ofLine} over a calendar period: its n and its unit first. */
        String ofPeriod(String name, String what, String value, TsAst.Span span, ValueSpecification... args) {
            Period p = period(span);
            List<ValueSpecification> all = new ArrayList<>(List.of(new CInteger((long) p.n()),
                    new EnumValue(DURATION, p.unit())));
            all.addAll(List.of(args));
            return ofLine(name, what, value, all.toArray(new ValueSpecification[0]));
        }

        /** {@code timeseries::name(args, ~out)}: of each row alone. */
        String ofRow(String name, String what, ValueSpecification... args) {
            return then(name, what, List.of(args));
        }

        /** {@code value} {@code k} observations later along its line ({@code $p->lead($r, k)}, the tile's): a fresh
         *  column, none after the line's last. */
        String lead(String value, int k) {
            String out = c.fresh("lead");
            List<ValueSpecification> args = new ArrayList<>(List.of(new com.legend.protocol.spec.Variable("p"),
                    new com.legend.protocol.spec.Variable("r")));
            if (k != 1) {
                args.add(new CInteger((long) k));
            }
            line = line.with(TsLines.call("extend", line.rel(), TsLines.over(line, null), new ColSpec(out,
                    new com.legend.protocol.spec.LambdaFunction(List.of(new com.legend.protocol.spec.Variable("p"),
                            new com.legend.protocol.spec.Variable("w"), new com.legend.protocol.spec.Variable("r")),
                            List.of(new com.legend.protocol.spec.AppliedProperty(TsLines.call("lead", args),
                                    value))))));
            return out;
        }

        /** Each row's number in time along its line, from 1: a fresh column. */
        String rowNumber() {
            String out = c.fresh("rn");
            line = line.with(TsLines.call("extend", line.rel(), TsLines.over(line, null), new ColSpec(out,
                    new com.legend.protocol.spec.LambdaFunction(List.of(new com.legend.protocol.spec.Variable("p"),
                            new com.legend.protocol.spec.Variable("w"), new com.legend.protocol.spec.Variable("r")),
                            List.of(TsLines.call("rowNumber", new com.legend.protocol.spec.Variable("p"),
                                    new com.legend.protocol.spec.Variable("r")))))));
            return out;
        }

        /** The line's window over {@code frame}. */
        ValueSpecification window(@com.legend.base.Nullable Object frame) {
            return TsLines.over(line, frameOf(frame));
        }

        /** {@code timeseries::name(args, ~out1, ~out2, ...)}: a function of several figures, each a fresh column. */
        List<String> many(String name, List<String> whats, ValueSpecification... args) {
            List<String> outs = whats.stream().map(c::fresh).toList();
            List<ValueSpecification> all = new ArrayList<>(List.of(line.rel()));
            all.addAll(List.of(args));
            outs.forEach(o -> all.add(new ColSpec(o)));
            line = line.with(TsLines.call(LIBRARY + name, all));
            return outs;
        }

        /** A band (R10): its lower, upper and width columns, its upper the answer's value. */
        TsLines.Valued band(List<String> lowerUpperWidth) {
            Map<String, String> fields = new LinkedHashMap<>();
            fields.put("lower", lowerUpperWidth.get(0));
            fields.put("upper", lowerUpperWidth.get(1));
            fields.put("width", lowerUpperWidth.get(2));
            c.struct(lowerUpperWidth.get(1), java.util.Collections.unmodifiableMap(fields));
            return done(lowerUpperWidth.get(1));
        }

        /** A side of a function of lines and numbers: a line's column, or a number as a column of it. */
        String side(TsLines.Node n) {
            if (n instanceof TsLines.Valued v) {
                return v.value();
            }
            String k = c.fresh("k");
            line = line.with(TsLines.call("extend", line.rel(), new ColSpec(k, c.rowLambda(((TsLines.Constant) n)
                    .expr(), line.rel(), false, "SELECT"))));
            return k;
        }

        /** THE MEMBERS AS ROWS (the library's long table): each member's line, date, number in time, its number
         *  ({@code names[0]}), its value ({@code names[1]}) and its weight ({@code names[2]}), concatenated. */
        private ValueSpecification long_(List<TsLines.Valued> members, List<Double> weights, String[] names) {
            ValueSpecification all = null;
            for (int i = 0; i < members.size(); i++) {
                ValueSpecification one = TsLines.call("select", line.rel(), new com.legend.protocol.spec.ColSpecArray(
                        List.of(new ColSpec(line.part()), new ColSpec(line.at()), new ColSpec(TsLines.ORD),
                                new ColSpec(members.get(i).value()))));
                one = TsLines.call("rename", one, new ColSpec(members.get(i).value()), new ColSpec(names[1]));
                one = TsLines.call("extend", one, new com.legend.protocol.spec.ColSpecArray(List.of(
                        new ColSpec(names[0], TsLines.lambda("x", new CInteger((long) i))),
                        new ColSpec(names[2], TsLines.lambda("x", new CFloat(weights.get(i)))))));
                all = all == null ? one : TsLines.call("concatenate", all, one);
            }
            return java.util.Objects.requireNonNull(all);
        }

        /** {@code other} (a relation of a row a line and date) joined to the line by its line and date. */
        private void joinByDate(ValueSpecification other) {
            String gp = c.fresh("jd_part");
            String ga = c.fresh("jd_at");
            ValueSpecification r = TsLines.call("rename", TsLines.call("rename", other, new ColSpec(line.part()),
                    new ColSpec(gp)), new ColSpec(line.at()), new ColSpec(ga));
            line = line.with(TsLines.call("join", line.rel(), r, new EnumValue(
                    "meta::pure::functions::relation::JoinKind", "LEFT"), TsLines.lambda2("l", "r", TsLines.call("and",
                    TsLines.call("equal", TsLines.prop("l", line.part()), TsLines.prop("r", gp)),
                    TsLines.call("equal", TsLines.prop("l", line.at()), TsLines.prop("r", ga))))));
        }

        /** Each member and its weight, on the dates all have: their weighted sum over the weights (the library's). */
        String weighted(List<TsLines.Valued> members, List<Double> weights) {
            String[] names = {c.fresh("mb_m"), c.fresh("mb_v"), c.fresh("mb_w")};
            ValueSpecification rows = long_(members, weights, names);
            String out = c.fresh("wsum");
            joinByDate(TsLines.call(LIBRARY + "weightedSum", rows, new com.legend.protocol.spec.ColSpecArray(List.of(
                    new ColSpec(line.part()), new ColSpec(line.at()))), new ColSpec(names[1]), new ColSpec(names[2]),
                    new CInteger((long) members.size()), new ColSpec(out)));
            return out;
        }

        /** A basket of the members in their weights from 100, rebalanced as told (the library's). */
        String basket(List<TsLines.Valued> members, List<Double> weights, String rebalance) {
            String[] names = {c.fresh("mb_m"), c.fresh("mb_v"), c.fresh("mb_w")};
            ValueSpecification rows = long_(members, weights, names);
            String out = c.fresh("basket");
            joinByDate(TsLines.call(LIBRARY + "basket", rows, new ColSpec(line.part()), new ColSpec(line.at()),
                    new ColSpec(TsLines.ORD), new ColSpec(names[0]), new ColSpec(names[1]), new ColSpec(names[2]),
                    new CInteger((long) members.size()), option("Rebalance", rebalance), new EnumValue(DURATION,
                            line.seconds() ? "SECONDS" : "DAYS"), new ColSpec(out)));
            return out;
        }

        /** A rolled statistic over {@code w} observations (0: from the start) with its ramp, or a calendar period. */
        String rolled(String stat, String value, Span w, int ramp) {
            if (w.period() != null) {
                return ofPeriod("overPeriod", stat, value, w.period(), option("Statistic", stat));
            }
            return library(stat, stat, value, frame(w.obs()), new CInteger((long) ramp));
        }

        /** Returns over {@code obs} observations, or as of a calendar period back, of a type. */
        String returns(String value, Span obs, String type) {
            return obs.period() == null
                    ? library("returns", "ret", value, null, new CInteger((long) obs.obs()), option("Returns", type))
                    : ofPeriod("returnsPeriod", "ret", value, obs.period(), option("Returns", type));
        }

        /** The periods a year a figure is annualized by (the tile's perYearColumn): SAMPLE BY's, else the one the
         *  line's own dates infer (the library's periodsPerYear, gs-quant's buckets). */
        String perYear() {
            if (perYear == null) {
                TsAst.Span sample = c.query().sample();
                if (sample != null) {
                    long n = switch (sample.unit()) {
                        case "d" -> 252L;
                        case "w" -> 52L;
                        case "mo" -> 12L;
                        case "q" -> 4L;
                        case "y" -> 1L;
                        default -> throw new IllegalStateException("SAMPLE BY " + sample.unit());
                    };
                    String out = c.fresh("per_year");
                    line = line.with(TsLines.call("extend", line.rel(), new ColSpec(out, TsLines.lambda("x",
                            new CInteger(n)))));
                    perYear = out;
                } else {
                    perYear = ofRow("periodsPerYear", "per_year", new ColSpec(line.part()), new ColSpec(TsLines.ORD),
                            new EnumValue(DURATION, line.seconds() ? "SECONDS" : "DAYS"));
                }
            }
            return perYear;
        }

        private static @com.legend.base.Nullable ValueSpecification frameOf(@com.legend.base.Nullable Object frame) {
            if (frame == null) {
                return null;
            }
            if ("running".equals(frame)) {
                return TsLines.call("rows", TsLines.call("unbounded"), new CInteger(0L));
            }
            if ("all".equals(frame)) {
                return TsLines.call("rows", TsLines.call("unbounded"), TsLines.call("unbounded"));
            }
            int n = (Integer) frame;
            return TsLines.call("rows", new CInteger((long) -(Math.max(1, n) - 1)), new CInteger(0L));
        }
    }

    private static SqlTranslationException invalid(String message, SqlAst.Pos pos) {
        return SqlTranslationException.invalid(message, pos);
    }
}
