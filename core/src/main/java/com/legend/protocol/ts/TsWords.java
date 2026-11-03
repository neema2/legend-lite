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
 * {@code w: Integer} and the {@code DurationUnit} parameter after it (a lag's {@code obs} likewise), {@code all} to
 * {@code w: timeseries::Scope}, a date to {@code w: StrictDate}; a word to the value of the parameter's enumeration; a
 * number to an Integer or a Float. The query's SAMPLE BY and FILL go to a parameter of their types
 * ({@code timeseries::Sample}, {@code timeseries::Fill}). A parameter the call does not give takes its default: none
 * ({@code []}) where it may ({@code [0..1]}, its default in the body), else the overload without it. The rule knows no
 * word and no parameter by its name: what a word means, its forms and its defaults -- an annualization's periods a year
 * under SAMPLE BY, a rate's time, a basket's count -- are the library's.
 *
 * <p>A line goes in as its key, its date (the date its value is of) and its value, and comes back as a line of the
 * language's own ({@link TsLines.Line}): its period the date's (SAMPLE BY's period of it), its number in time the date's.
 */
final class TsWords {

    private TsWords() {
    }

    /** The package of the words over lines. */
    static final String PACKAGE = TsToPure.LIBRARY + "line::";

    /** The type of a calendar span's unit: a word's parameter after its count's ({@code w}, {@code obs}). */
    static final String DURATION = "meta::pure::functions::date::DurationUnit";

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

    /** What a call gives a word in a line's place: a line, a number (a word some overload of which takes one there:
     *  {@code compare(x, 0)}), or lines with their weights (a basket's members). */
    sealed interface Arg permits LineArg, NumArg, MembersArg {
    }

    /** A line. */
    record LineArg(TsLines.Valued line) implements Arg {
    }

    /** A number. */
    record NumArg(double value) implements Arg {
    }

    /** Lines, each with its weight. */
    record MembersArg(List<TsLines.Valued> lines, List<Double> weights) implements Arg {
    }

    /** Whether the library defines {@code word} over lines (by its name or an alias). */
    static boolean defines(TsFunctions.Ctx c, String word) {
        return !c.model().overloads(fqn(c.model(), word)).isEmpty();
    }

    /** The words the language spells as Pure keywords: the library's are spelled with an underscore after, gs-quant's
     *  {@code if_}. */
    private static final Set<String> KEYWORDS = Set.of("if", "let", "true", "false");

    /** The path of the library's function {@code word} names: its own ({@code if} written {@code if_}, a Pure
     *  keyword), else the word whose alias it is ({@code timeseries::Words.alias}: avg is mean's). */
    static String fqn(TsToPure.Model model, String word) {
        String own = PACKAGE + (KEYWORDS.contains(word) ? word + "_" : word);
        if (!model.overloads(own).isEmpty()) {
            return own;
        }
        for (String fn : model.functionsIn(PACKAGE.substring(0, PACKAGE.length() - 2))) {
            if (model.overloads(PACKAGE + fn).stream().anyMatch(o -> o.aliases().contains(word))) {
                return PACKAGE + fn;
            }
        }
        return own;
    }

    /** The language's name of the library's function {@code name} (its own, but a keyword's: {@code if_} is
     *  {@code if}). */
    static String languageName(String name) {
        return name.endsWith("_") && KEYWORDS.contains(name.substring(0, name.length() - 1))
                ? name.substring(0, name.length() - 1) : name;
    }

    /** The type of the parameter a word's FILL is: never given, the query's. */
    static final String FILL = TsToPure.LIBRARY + "Fill";

    /**
     * {@code word} of {@code args} (its lines, in place), each of {@code given} (by parameter name; a window under
     * {@code w}) bound to the overload it fits: the word's line, its period and its number in time as the language's
     * lines are. A line of no instrument given with lines per instrument is every instrument's (R2: the library's each).
     */
    static TsLines.Valued call(TsFunctions.Ctx c, String word, List<Arg> written, Map<String, Given> given,
            SqlAst.Pos pos) {
        String fqn = fqn(c.model(), word);
        List<TsToPure.Overload> overloads = c.model().overloads(fqn);
        // each line as its own line, its table's rows it is a point of (a word's answer is no point of its ramp's)
        List<Arg> args = written.stream().map(a -> switch (a) {
            case LineArg l -> (Arg) new LineArg(c.points(l.line()));
            case MembersArg m -> new MembersArg(m.lines().stream().map(c::points).toList(), m.weights());
            case NumArg n -> n;
        }).toList();
        TsLines.Line key = keyLine(args, word, pos);
        List<List<ValueSpecification>> fits = new ArrayList<>();
        List<TsToPure.Overload> fitting = new ArrayList<>();
        for (TsToPure.Overload o : overloads) {
            List<ValueSpecification> bound = bind(c, o.params(), args, key, given, pos);
            if (bound != null) {
                fits.add(bound);
                fitting.add(o);
            }
        }
        if (fits.isEmpty() && c.query().sample() == null && !overloads.isEmpty() && overloads.stream().allMatch(o ->
                o.params().stream().anyMatch(p -> TsCatalog.SAMPLE.equals(p.type()) && !p.optional()))) {
            // a word that counts in the query's periods (a decomposition's cycle)
            throw SqlTranslationException.invalid(word + " counts in the query's periods: add SAMPLE BY 1mo (R5)", pos);
        }
        if (fits.isEmpty()) {
            throw SqlTranslationException.invalid(word + " is not called so: its forms are "
                    + String.join(", ", overloads.stream().map(o -> form(word, o.params())).toList()), pos);
        }
        if (fits.size() > 1) {
            throw new IllegalStateException("the library's " + fqn + " has " + fits.size()
                    + " overloads a call fits: " + given.keySet());
        }
        // a word that keeps its line's rows: its rows form, a column of its line's own table (R7, the time model's D5)
        TsLines.Valued kept = rowsForm(c, fqn, word, fitting.get(0), fits.get(0), written);
        if (kept != null) {
            return kept;
        }
        // the answer's value columns: a word's value, or a struct's fields (R10), each under a fresh name
        List<String> gives = fitting.get(0).gives();
        Map<String, String> outs = new LinkedHashMap<>();
        for (String g : gives) {
            outs.put(g, c.fresh("value".equals(g) ? word : word + "_" + g));
        }
        ValueSpecification rel = back(c, TsLines.call(fqn, fits.get(0)), key, outs);
        TsLines.Line line = new TsLines.Line(rel, key.part(), key.byInstrument(), key.at());
        String first = outs.values().iterator().next();
        if (!gives.equals(List.of("value"))) {
            c.struct(first, java.util.Collections.unmodifiableMap(new LinkedHashMap<>(outs)));
        }
        return new TsLines.Valued(line, first);
    }

    /** The package of the words' rows forms (projects/timeseries/model.pure, A WORD'S ROWS FORM). */
    static final String ROWS = TsToPure.LIBRARY + "rows::";

    /**
     * {@code o}, the overload of the word at {@code fqn} a call of one line fits, as its rows form (R7, the time model's
     * D5): the library's {@code timeseries::rows::} function of the word's name whose parameters are a table, its key,
     * date and value columns and the value's points, then {@code o}'s own after its line, then the answer's column and
     * its points -- called on the line's base's newest table, its answer a column of it. Null where the library
     * declares none (the word makes a new line) or the call is of several lines.
     */
    private static TsLines.@com.legend.base.Nullable Valued rowsForm(TsFunctions.Ctx c, String fqn, String word,
            TsToPure.Overload o, List<ValueSpecification> bound, List<Arg> args) {
        if (args.size() != 1 || !(args.get(0) instanceof LineArg l) || o.params().isEmpty()
                || !TsToPure.LINE.equals(o.params().get(0).type()) || !o.gives().equals(List.of("value"))) {
            return null;
        }
        String rows = ROWS + fqn.substring(PACKAGE.length());
        List<String> own = o.params().subList(1, o.params().size()).stream().map(TsToPure.Parameter::type).toList();
        for (TsToPure.Overload r : c.model().overloads(rows)) {
            List<TsToPure.Parameter> p = r.params();
            if (p.size() != own.size() + 7 || !p.subList(5, p.size() - 2).stream().map(TsToPure.Parameter::type)
                    .toList().equals(own)) {
                continue;
            }
            TsLines.Valued v = l.line();
            String present = c.presence(v);
            String out = c.fresh(word);
            String points = c.fresh(word + "_pts");
            TsLines.Line line = c.grown(v.line(), rel -> {
                List<ValueSpecification> a = new ArrayList<>(List.of(rel, new ColSpec(v.line().part()),
                        new ColSpec(v.line().at()), new ColSpec(v.value()), new ColSpec(present)));
                a.addAll(bound.subList(1, bound.size()));
                a.add(new ColSpec(out));
                a.add(new ColSpec(points));
                return TsLines.call(rows, a);
            });
            c.present(out, points);
            return new TsLines.Valued(line, out);
        }
        return null;
    }

    /** The line whose key and date the answer takes: the first line per instrument among {@code args}, else the first
     *  line. Refused (R2): lines by two instrument columns. */
    private static TsLines.Line keyLine(List<Arg> args, String word, SqlAst.Pos pos) {
        List<TsLines.Line> lines = new ArrayList<>();
        for (Arg a : args) {
            switch (a) {
                case LineArg l -> lines.add(l.line().line());
                case MembersArg m -> m.lines().forEach(v -> lines.add(v.line()));
                case NumArg n -> {
                }
            }
        }
        if (lines.isEmpty()) {
            throw SqlTranslationException.invalid(word + " reads a line: its arguments are numbers", pos);
        }
        return TsLines.keyOf(lines, pos);
    }

    /** {@code o}'s arguments from {@code args} (in its leading parameters' places) and {@code given}, or null where
     *  {@code o} does not fit: every argument taken, every parameter given or defaulted. */
    private static @com.legend.base.Nullable List<ValueSpecification> bind(TsFunctions.Ctx c,
            List<TsToPure.Parameter> o, List<Arg> args, TsLines.Line key, Map<String, Given> given, SqlAst.Pos pos) {
        if (o.size() < args.size()) {
            return null;
        }
        Set<String> taken = new LinkedHashSet<>();
        List<ValueSpecification> out = new ArrayList<>();
        for (int i = 0; i < o.size(); i++) {
            TsToPure.Parameter p = o.get(i);
            ValueSpecification v;
            if (i < args.size()) {
                v = argument(c, args.get(i), p, key, pos);
            } else if (given.get(p.name()) instanceof Calendar cal) {
                // a calendar span (a window w, a lag's obs): its count this parameter, its unit the DurationUnit one
                // after it -- an overload without one does not fit it
                if (!"Integer".equals(p.type()) || i + 1 >= o.size() || !DURATION.equals(o.get(i + 1).type())) {
                    return null;
                }
                out.add(new CInteger((long) cal.n()));
                v = new EnumValue(DURATION, cal.unit());
                taken.add(p.name());
                i++;
            } else if (given.containsKey(p.name())) {
                v = value(c, given.get(p.name()), p);
                taken.add(p.name());
            } else if (TsCatalog.SAMPLE.equals(p.type()) && c.query().sample() != null) {
                // the lines meet on SAMPLE BY's periods
                v = TsLines.sample(java.util.Objects.requireNonNull(c.query().sample()).unit());
            } else if (FILL.equals(p.type()) && c.query().fill() != null) {
                // the lines meet under FILL, filled
                v = TsLines.fill(java.util.Objects.requireNonNull(c.query().fill()).how());
            } else {
                v = p.optional() ? new PureCollection(List.of()) : null;
            }
            if (v == null) {
                return null;
            }
            out.add(v);
        }
        return taken.equals(given.keySet()) ? out : null;
    }

    /** {@code a}, given in {@code p}'s place, as its value: a line as a word takes it (every instrument's of
     *  {@code key}'s, where it is of none and {@code key} per instrument); a number; members as their long relation.
     *  Null where {@code p} takes another kind. */
    private static @com.legend.base.Nullable ValueSpecification argument(TsFunctions.Ctx c, Arg a,
            TsToPure.Parameter p, TsLines.Line key, SqlAst.Pos pos) {
        return switch (a) {
            case LineArg l -> TsToPure.LINE.equals(p.type()) ? each(l.line(), key, pos)
                    : TsToPure.TIMED_LINE.equals(p.type()) ? timed(l.line(), key, pos) : null;
            case NumArg n -> "Float".equals(p.type()) || "Number".equals(p.type()) ? new CFloat(n.value()) : null;
            case MembersArg m -> TsToPure.MEMBERS.equals(p.type()) ? members(m, key, pos, false)
                    : TsToPure.TIMED_MEMBERS.equals(p.type()) ? members(m, key, pos, true) : null;
        };
    }

    /** {@code v} as a word takes it, every instrument's of {@code key}'s where it is of none and {@code key} per
     *  instrument (R2, the library's each). */
    private static ValueSpecification each(TsLines.Valued v, TsLines.Line key, SqlAst.Pos pos) {
        ValueSpecification line = in(v.line(), v.value(), pos);
        if (key.byInstrument() && !v.line().byInstrument()) {
            return TsLines.call(PACKAGE + "helpers::each", line, in(key, keyValue(key), pos));
        }
        return line;
    }

    /** {@code v} as a word that accrues over time takes it: its key, its date, its number in time (the line's own, in
     *  seconds since 1970: under SAMPLE BY its period's last tick's), its value. Refused (R2): one of no instrument with
     *  lines per instrument. */
    private static ValueSpecification timed(TsLines.Valued v, TsLines.Line key, SqlAst.Pos pos) {
        if (key.byInstrument() && !v.line().byInstrument()) {
            throw SqlTranslationException.unsupported("a line of no instrument accrued with lines per instrument:"
                    + " pick one line with a subscript (R2)", pos);
        }
        TsLines.Line l = v.line();
        ValueSpecification rel = TsLines.call("select", l.rel(), new ColSpecArray(List.of(new ColSpec(l.part()),
                new ColSpec(l.at()), new ColSpec(TsLines.ORD), new ColSpec(v.value()))));
        rel = renamed(rel, l.part(), "key");
        rel = renamed(rel, l.at(), "date");
        rel = renamed(rel, TsLines.ORD, "time");
        return renamed(rel, v.value(), "value");
    }

    /** A value column of {@code key}'s, for its keys alone (each). */
    private static String keyValue(TsLines.Line key) {
        return TsLines.ORD;
    }

    /** Members as the long relation a basket reads, as written: each line (every instrument's, as {@link #each}; with
     *  its time, {@code timed}, as {@link #timed}), its number from 0, its weight and how many they are, concatenated. */
    private static ValueSpecification members(MembersArg m, TsLines.Line key, SqlAst.Pos pos, boolean timed) {
        ValueSpecification all = null;
        for (int i = 0; i < m.lines().size(); i++) {
            ValueSpecification line = timed ? timed(m.lines().get(i), key, pos) : each(m.lines().get(i), key, pos);
            ValueSpecification one = TsLines.call("extend", line, new ColSpecArray(List.of(
                    new ColSpec("member", TsLines.lambda("x", new CInteger((long) i))),
                    new ColSpec("weight", TsLines.lambda("x", new CFloat(m.weights().get(i)))),
                    new ColSpec("members", TsLines.lambda("x", new CInteger((long) m.lines().size()))))));
            List<ColSpec> columns = new ArrayList<>(List.of(new ColSpec("key"), new ColSpec("date")));
            if (timed) {
                columns.add(new ColSpec("time"));
            }
            columns.addAll(List.of(new ColSpec("member"), new ColSpec("weight"), new ColSpec("members"),
                    new ColSpec("value")));
            one = TsLines.call("select", one, new ColSpecArray(List.copyOf(columns)));
            all = all == null ? one : TsLines.call("concatenate", all, one);
        }
        return java.util.Objects.requireNonNull(all, "members");
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

    /** {@code word(x, ...)} as written: an overload's parameters, a refusal's words. */
    static String form(String word, List<TsToPure.Parameter> o) {
        List<String> ps = new ArrayList<>();
        for (TsToPure.Parameter p : o) {
            if (TsCatalog.SAMPLE.equals(p.type()) || FILL.equals(p.type())) {
                continue;
            }
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

    /** {@code rel}, a word's line, as the language's line {@code l} is: its key and date under {@code l}'s names, each
     *  value column under its name in {@code outs}, its period and its number in time of its date. */
    private static ValueSpecification back(TsFunctions.Ctx c, ValueSpecification rel, TsLines.Line l,
            Map<String, String> outs) {
        rel = renamed(rel, "key", l.part());
        rel = renamed(rel, "date", l.at());
        for (Map.Entry<String, String> o : outs.entrySet()) {
            rel = renamed(rel, o.getKey(), o.getValue());
        }
        // its period and its number in time, of its date (the library's timed)
        TsAst.Span sample = c.query().sample();
        return sample == null
                ? TsLines.call(TsLines.LINES + "timed", rel, new ColSpec(l.at()), new ColSpec(TsLines.PERIOD),
                        new ColSpec(TsLines.ORD))
                : TsLines.call(TsLines.LINES + "timed", rel, new ColSpec(l.at()), TsLines.sample(sample.unit()),
                        new ColSpec(TsLines.PERIOD), new ColSpec(TsLines.ORD));
    }

    private static ValueSpecification renamed(ValueSpecification rel, String from, String to) {
        return from.equals(to) ? rel : TsLines.call("rename", rel, new ColSpec(from), new ColSpec(to));
    }
}
