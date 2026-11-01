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


    /** A parameter's kind: a window or a lag ({@code SPAN}), a whole number, a number, a word of {@code words}, or a
     *  date ({@code '2024-01-01'}: given always), or a date a base is taken as of ({@code SINCE}: none, the line's
     *  first). */
    enum Kind { SPAN, WHOLE, NUMBER, WORD, DATE, SINCE }

    /** A span's count that is the whole line ({@code whole}: the future included, D3). */
    private static final int WHOLE_LINE = -2;

    /** A parameter: its name, its kind, its default (none: a word the library defines, whose defaults are its own --
     *  TsCatalog), its words (a word's). */
    record Param(String name, Kind kind, @com.legend.base.Nullable Object fallback, List<String> words) {
    }

    /** A function's signature: how many lines it reads (-1: several, a basket's), and its parameters in order. */
    record Signature(int lines, List<Param> params) {
    }

    /** What the functions are written with: a line's node, fresh names, a per-point lambda, the query. */
    interface Ctx extends TsLines.Writer {
        TsLines.Node node(SqlAst.Expr e);

        TsAst.Query query();

        /** The model the query is of: the library's words ({@link TsWords}) among its functions. */
        TsToPure.Model model();

        /** Records that {@code value}, a function's answer, is a struct (R10): each field's column, in order. */
        void struct(String value, Map<String, String> fields);
    }

    /** Whether {@code name} is a function of the language: one written here, or a word the library defines. */
    static boolean has(Ctx c, String name) {
        return TsWords.defines(c, name);
    }

    /** A window or a lag as given: a number of observations ({@code 0}: from the start, {@code all}), or a span. */
    private record Span(int obs, TsAst.@com.legend.base.Nullable Span period) {
    }

    /** A window from a date on ({@code '2024-01-01'}): a word whose window takes one (mean's, sum's). */
    private record On(String iso) {
    }

    /** {@code fc}'s parameters as given, by place after its line then by name, each as the generic rule takes it
     *  ({@link TsWords}): none defaulted here -- a word's defaults are the library's. */
    private static Map<String, TsWords.Given> given(Ctx c, SqlAst.FunctionCall fc, String fn, Signature sig,
            List<SqlAst.Expr> positional, Map<String, SqlAst.Expr> named) {
        List<SqlAst.Expr> at = positional.subList(Math.max(0, Math.min(sig.lines(), positional.size())),
                positional.size());
        if (at.size() > sig.params().size()) {
            throw invalid(fn + " takes " + sig.lines() + " line" + (sig.lines() == 1 ? "" : "s")
                    + " and its parameters " + String.join(", ", sig.params().stream().map(Param::name).toList())
                    + " (R10)", fc.pos());
        }
        Map<String, TsWords.Given> out = new LinkedHashMap<>();
        for (int i = 0; i < sig.params().size(); i++) {
            Param p = sig.params().get(i);
            SqlAst.Expr e = i < at.size() ? at.get(i) : named.get(p.name());
            if (i < at.size() && named.containsKey(p.name())) {
                throw invalid(fn + "'s " + p.name() + " is given twice (R10)", fc.pos());
            }
            if (e == null && p.kind() == Kind.DATE) {
                throw invalid(fn + " reads a date: " + fn + "(x, '2024-01-01') (R9)", fc.pos());
            }
            if (e == null) {
                continue;
            }
            Object v = value(c, fn, p, e);
            out.put(p.name(), switch (p.kind()) {
                case SPAN -> {
                    if (v instanceof On on) {
                        yield new TsWords.Day(on.iso());
                    }
                    Span s = (Span) v;
                    if (s.period() != null) {
                        Period q = period(s.period());
                        yield new TsWords.Calendar(q.n(), q.unit());
                    }
                    yield s.obs() == 0 ? new TsWords.All() : s.obs() == WHOLE_LINE ? new TsWords.Word("whole")
                            : new TsWords.Observations(s.obs());
                }
                case WHOLE -> new TsWords.Whole(((Number) v).longValue());
                case NUMBER -> new TsWords.Num(((Number) v).doubleValue());
                case WORD -> new TsWords.Word((String) v);
                case DATE, SINCE -> new TsWords.Day((String) v);
            });
        }
        for (String n : named.keySet()) {
            if (sig.params().stream().noneMatch(p -> p.name().equals(n))) {
                throw invalid(fn + " has no option " + n + (sig.params().isEmpty() ? "" : ": its parameters are "
                        + String.join(", ", sig.params().stream().map(Param::name).toList())) + " (R10)", fc.pos());
            }
        }
        return out;
    }

    /** A parameter's value as written. */
    private static Object value(Ctx c, String fn, Param p, SqlAst.Expr e) {
        switch (p.kind()) {
            case SPAN -> {
                if (e instanceof SqlAst.ColumnRef r && c.query().alls().contains(r.pos())) {
                    // all, the line's start to date; whole, the whole line, the future included (last(x, whole),
                    // gs-quant's last: D3)
                    return new Span(TsParser.WHOLE.equals(r.parts().get(0).text()) ? WHOLE_LINE : 0, null);
                }
                TsAst.Span s = e instanceof SqlAst.ColumnRef r ? c.query().spans().get(r.pos()) : null;
                if (s != null) {
                    return new Span(-1, s);
                }
                if (e instanceof SqlAst.IntegerLit n && n.value().signum() > 0
                        && n.value().compareTo(java.math.BigInteger.valueOf(100_000)) <= 0) {
                    return new Span(n.value().intValue(), null);
                }
                String text = e instanceof SqlAst.StringLit sl ? sl.value() : e instanceof SqlAst.TypedLit t
                        && "date".equalsIgnoreCase(t.type().name()) ? t.value() : null;
                if (text != null) {
                    // a window from a date on: the word's own form, if it has one
                    try {
                        return new On(java.time.LocalDate.parse(text).toString());
                    } catch (java.time.format.DateTimeParseException bad) {
                        throw invalid(fn + "'s window from a date is a date: " + fn + "(x, '2024-01-01') (R4)",
                                e.pos());
                    }
                }
                throw invalid(fn + "'s " + p.name() + ": a window counts observations, a whole number from 1, or is a"
                        + " span (3mo) or all: " + fn + "(x, 20) (R4)", e.pos());
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
            case DATE, SINCE -> {
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
        if (TsWords.defines(c, fn)) {
            // the library's word over lines (TsWords): its lines and its parameters read from its overloads
            // (TsCatalog), the arguments as given, its defaults its own
            Signature sig = TsCatalog.signature(c.model(), fn);
            List<TsWords.Arg> args = new ArrayList<>();
            if (sig.lines() < 0) {
                // weighted members: each line and its weight, in pairs; its parameters by name alone
                List<TsLines.Valued> members = new ArrayList<>();
                List<Double> weights = new ArrayList<>();
                for (int i = 0; i < positional.size(); i += 2) {
                    members.add(line(c, fn, positional.get(i), i));
                    Double weight = i + 1 < positional.size() ? constant(positional.get(i + 1)) : null;
                    if (weight == null) {
                        throw invalid(fn + " reads each line and its weight: " + fn + "(x, 0.6, y, 0.4)",
                                i + 1 < positional.size() ? positional.get(i + 1).pos() : fc.pos());
                    }
                    weights.add(weight);
                }
                args.add(new TsWords.MembersArg(List.copyOf(members), List.copyOf(weights)));
            } else {
                if (positional.size() < sig.lines()) {
                    throw invalid(fn + " reads " + sig.lines() + " line" + (sig.lines() == 1 ? "" : "s") + ": " + fn
                            + "(x" + (sig.lines() > 1 ? ", y" : "") + (sig.lines() > 2 ? ", z" : "") + ", ...)",
                            fc.pos());
                }
                for (int i = 0; i < sig.lines(); i++) {
                    args.add(arg(c, fn, positional.get(i), i));
                }
            }
            Map<String, TsWords.Given> given = given(c, fc, fn, sig, sig.lines() < 0 ? List.of() : positional,
                    named);
            return TsWords.call(c, fn, args, given, fc.pos());
        }
        throw new IllegalStateException("no word " + fn + " in the library: has() said it was one");
    }

    /** {@code fn}'s {@code i}-th argument in a line's place: a line, a condition's flags (1 where it holds, 0 where it
     *  does not, none where it is none), or a number. */
    private static TsWords.Arg arg(Ctx c, String fn, SqlAst.Expr e, int i) {
        SqlAst.Expr read = truth(e) ? new SqlAst.Case(null, List.of(
                new SqlAst.When(e, new SqlAst.IntegerLit(java.math.BigInteger.ONE, e.pos())),
                new SqlAst.When(new SqlAst.Unary(SqlAst.UnaryOp.NOT, e, e.pos()),
                        new SqlAst.IntegerLit(java.math.BigInteger.ZERO, e.pos()))), null, e.pos()) : e;
        TsLines.Node n = c.node(read);
        if (n instanceof TsLines.Valued v) {
            return new TsWords.LineArg(v);
        }
        Double k = constant(((TsLines.Constant) n).expr());
        if (k == null) {
            throw invalid(fn + "'s " + (i == 0 ? "first" : i == 1 ? "second" : i == 2 ? "third" : (i + 1) + "th")
                    + " argument is a line or a number", e.pos());
        }
        return new TsWords.NumArg(k);
    }

    /** {@code fn}'s {@code i}-th line: a node of a line, never a number alone. */
    private static TsLines.Valued line(Ctx c, String fn, SqlAst.Expr e, int i) {
        if (!(c.node(e) instanceof TsLines.Valued v)) {
            throw invalid(fn + " reads a line: its " + (i == 0 ? "first" : i == 1 ? "second" : "third")
                    + " argument is a number", e.pos());
        }
        return v;
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

    private static SqlTranslationException invalid(String message, SqlAst.Pos pos) {
        return SqlTranslationException.invalid(message, pos);
    }
}
