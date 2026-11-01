// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.ts;

import com.legend.protocol.spec.CDate;
import com.legend.protocol.spec.CFloat;
import com.legend.protocol.spec.CInteger;
import com.legend.protocol.spec.CString;
import com.legend.protocol.spec.ColSpec;
import com.legend.protocol.spec.ColSpecArray;
import com.legend.protocol.spec.EnumValue;
import com.legend.protocol.spec.PureCollection;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.sql.SqlAst;
import com.legend.protocol.sql.SqlTranslationException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * THE GENERIC RULE (docs/TS_LIBRARY_DEFINES_THE_LANGUAGE_2026_10_31.md, M1): a word of {@code #TS} the library defines
 * over lines -- {@code timeseries::line::<word>}, of a line {@code Relation<(key, date, value)>} to a line -- is called by
 * its name, each argument the call gives bound to the parameter of its name and kind, read from the word's own
 * overloads in the model: a window of {@code w} observations to the {@code w: Integer} one, a span ({@code 3mo}) to
 * {@code w: Integer, unit: DurationUnit}, {@code all} to {@code w: timeseries::Scope}, a date to {@code w: StrictDate};
 * a word to the value of the parameter's enumeration; a number to an Integer or a Float. A parameter the call does not
 * give takes its default: none ({@code []}) where it may ({@code [0..1]}, its default in the body), else the overload
 * without it -- and {@code annualization_factor}, under SAMPLE BY, the sample's periods a year (gs-quant's parameter, the
 * language's rule). The rule knows no word: what a word means, its forms and its defaults are the library's.
 *
 * <p>A line goes in as its key, its date (the date its value is of) and its value, and comes back as a line of the
 * language's own ({@link TsLines.Line}): its period the date's (SAMPLE BY's period of it), its number in time the date's.
 */
final class TsWords {

    private TsWords() {
    }

    /** The package of the words over lines. */
    static final String PACKAGE = TsToPure.LIBRARY + "line::";

    private static final String DURATION = "meta::pure::functions::date::DurationUnit";

    /** What a call gives a parameter: a window (observations, all, a calendar span, a date on), or a value. */
    sealed interface Given permits Observations, All, Calendar, Day, Num, Whole, Word {
    }

    /** {@code n} observations to date. */
    record Observations(int n) implements Given {
    }

    /** The line's start to date. */
    record All() implements Given {
    }

    /** {@code n} of a DurationUnit back ({@code unit} its name). */
    record Calendar(int n, String unit) implements Given {
    }

    /** A date ({@code iso}, {@code 2024-01-01}). */
    record Day(String iso) implements Given {
    }

    /** A number. */
    record Num(double value) implements Given {
    }

    /** A whole number. */
    record Whole(long value) implements Given {
    }

    /** A word ({@code 'log'}). */
    record Word(String word) implements Given {
    }

    /** Whether the library defines {@code word} over lines. */
    static boolean defines(TsFunctions.Ctx c, String word) {
        return !c.model().overloads(PACKAGE + word).isEmpty();
    }

    /**
     * {@code word} of {@code x}, each of {@code given} (by parameter name; a window under {@code w}) bound to the
     * overload it fits: the word's line, its period and its number in time as the language's lines are.
     */
    static TsLines.Valued call(TsFunctions.Ctx c, String word, TsLines.Valued x, Map<String, Given> given,
            SqlAst.Pos pos) {
        List<List<TsToPure.Parameter>> overloads = c.model().overloads(PACKAGE + word);
        List<List<ValueSpecification>> fits = new ArrayList<>();
        for (List<TsToPure.Parameter> o : overloads) {
            List<ValueSpecification> args = bind(c, o, given);
            if (args != null) {
                fits.add(args);
            }
        }
        if (fits.isEmpty()) {
            throw SqlTranslationException.invalid(word + " is not called so: its forms are "
                    + String.join(", ", overloads.stream().map(o -> form(word, o)).toList()), pos);
        }
        if (fits.size() > 1) {
            throw new IllegalStateException("the library's " + PACKAGE + word + " has " + fits.size()
                    + " overloads a call fits: " + given.keySet());
        }
        TsLines.Line l = x.line();
        List<ValueSpecification> args = new ArrayList<>();
        args.add(in(l, x.value(), pos));
        args.addAll(fits.get(0));
        String out = c.fresh(word);
        ValueSpecification rel = back(c, TsLines.call(PACKAGE + word, args), l, out);
        return new TsLines.Valued(new TsLines.Line(rel, l.part(), l.byInstrument(), l.at(), l.seconds()), out);
    }

    /** {@code o}'s arguments from {@code given}, or null where {@code o} does not fit: every given value taken, every
     *  parameter given or defaulted. */
    private static @com.legend.base.Nullable List<ValueSpecification> bind(TsFunctions.Ctx c,
            List<TsToPure.Parameter> o, Map<String, Given> given) {
        Set<String> taken = new LinkedHashSet<>();
        List<ValueSpecification> args = new ArrayList<>();
        for (TsToPure.Parameter p : o) {
            ValueSpecification v;
            if ("unit".equals(p.name()) && given.get("w") instanceof Calendar cal) {
                v = DURATION.equals(p.type()) ? new EnumValue(DURATION, cal.unit()) : null;
            } else if (given.containsKey(p.name())) {
                v = value(c, given.get(p.name()), p);
                taken.add(p.name());
            } else if ("annualization_factor".equals(p.name()) && sampledPerYear(c) != null) {
                v = new CInteger(java.util.Objects.requireNonNull(sampledPerYear(c)));
            } else {
                v = p.optional() ? new PureCollection(List.of()) : null;
            }
            if (v == null) {
                return null;
            }
            args.add(v);
        }
        // a calendar window takes its unit's parameter too: an overload without one does not fit it
        if (given.get("w") instanceof Calendar && o.stream().noneMatch(p -> "unit".equals(p.name()))) {
            return null;
        }
        return taken.equals(given.keySet()) ? args : null;
    }

    /** {@code g} as the value of {@code p}, or null where it is of another kind. */
    private static @com.legend.base.Nullable ValueSpecification value(TsFunctions.Ctx c, Given g,
            TsToPure.Parameter p) {
        String t = p.type();
        return switch (g) {
            case Observations n -> "Integer".equals(t) ? new CInteger((long) n.n()) : null;
            case Calendar cal -> "Integer".equals(t) ? new CInteger((long) cal.n()) : null;
            case All a -> enumerationHas(c, t, "all") ? new EnumValue(t, "all") : null;
            case Day d -> "StrictDate".equals(t)
                    ? new CDate(com.legend.values.PureDateLiteral.parse(d.iso())) : null;
            case Whole w -> "Integer".equals(t) ? new CInteger(w.value())
                    : "Float".equals(t) || "Number".equals(t) ? new CFloat((double) w.value()) : null;
            case Num n -> "Float".equals(t) || "Number".equals(t) ? new CFloat(n.value())
                    : "Integer".equals(t) && n.value() == Math.rint(n.value()) ? new CInteger((long) n.value()) : null;
            case Word w -> enumerationHas(c, t, w.word()) ? new EnumValue(t, w.word())
                    : "String".equals(t) ? new CString(w.word()) : null;
        };
    }

    private static boolean enumerationHas(TsFunctions.Ctx c, String type, String value) {
        List<String> values = c.model().enumeration(type);
        return values != null && values.contains(value);
    }

    /** SAMPLE BY's periods a year (a figure annualized over a sample's periods), or null without one. */
    private static @com.legend.base.Nullable Long sampledPerYear(TsFunctions.Ctx c) {
        TsAst.Span sample = c.query().sample();
        if (sample == null) {
            return null;
        }
        return switch (sample.unit()) {
            case "d" -> 252L;
            case "w" -> 52L;
            case "mo" -> 12L;
            case "q" -> 4L;
            case "y" -> 1L;
            default -> throw new IllegalStateException("SAMPLE BY " + sample.unit());
        };
    }

    /** {@code word(x, ...)} as written: an overload's parameters, a refusal's words. */
    private static String form(String word, List<TsToPure.Parameter> o) {
        List<String> ps = new ArrayList<>(List.of("x"));
        for (TsToPure.Parameter p : o) {
            String type = p.type().substring(p.type().lastIndexOf(':') + 1);
            ps.add(p.name() + ": " + type + (p.optional() ? " (optional)" : ""));
        }
        return word + "(" + String.join(", ", ps) + ")";
    }

    /** {@code l}'s {@code value} as a word takes a line: its key, its date, its value. */
    private static ValueSpecification in(TsLines.Line l, String value, SqlAst.Pos pos) {
        if (List.of("date", "value").contains(l.part())) {
            throw SqlTranslationException.unsupported("an instrument column named " + l.part() + " is a line's own"
                    + " name: rename it before #TS reads it", pos);
        }
        ValueSpecification rel = TsLines.call("select", l.rel(), new ColSpecArray(List.of(new ColSpec(l.part()),
                new ColSpec(l.at()), new ColSpec(value))));
        rel = renamed(rel, l.part(), "key");
        rel = renamed(rel, l.at(), "date");
        return renamed(rel, value, "value");
    }

    /** {@code rel}, a word's line, as the language's line {@code l} is: its key, date and value under {@code l}'s names
     *  ({@code out} its value's), its period and its number in time of its date. */
    private static ValueSpecification back(TsFunctions.Ctx c, ValueSpecification rel, TsLines.Line l, String out) {
        rel = renamed(rel, "key", l.part());
        rel = renamed(rel, "date", l.at());
        rel = renamed(rel, "value", out);
        TsAst.Span sample = c.query().sample();
        return TsLines.call("extend", rel, new ColSpecArray(List.of(
                new ColSpec(TsLines.PERIOD, TsLines.lambda("x", TsLines.periodOf(sample == null ? null : sample.unit(),
                        TsLines.col(l.at())))),
                new ColSpec(TsLines.ORD, TsLines.lambda("x", TsLines.ordOf(l.at(), l.seconds()))))));
    }

    private static ValueSpecification renamed(ValueSpecification rel, String from, String to) {
        return from.equals(to) ? rel : TsLines.call("rename", rel, new ColSpec(from), new ColSpec(to));
    }

    /** {@code given}, keyed as {@link #call} takes it, from a window argument as written. */
    static Map<String, Given> window(@com.legend.base.Nullable Given w) {
        Map<String, Given> out = new LinkedHashMap<>();
        if (w != null) {
            out.put("w", w);
        }
        return out;
    }
}
