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
    sealed interface Given permits Observations, All, Calendar, Day, Num, Whole, Word, Var, Computed {
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

    /** A variable of the enclosing Pure, {@code $w} (decision 7 of docs/TS_DECISIONS_2026_11_07.md): its value, of the
     *  type the variable is, for a parameter of that type. */
    record Var(String name, com.legend.protocol.sql.SqlAst.Pos pos) implements Given {
    }

    /** A date the query computes, the data's last date moved by a span ({@code latest - 3y}; decision 8): a StrictDate
     *  value, for a date parameter. */
    record Computed(ValueSpecification value) implements Given {
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
        String aliased = model.aliased(PACKAGE.substring(0, PACKAGE.length() - 2), word);
        return aliased != null ? PACKAGE + aliased : own;
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
        TsLines.Valued kept = keptForm(c, fqn, word, overloads, fitting.get(0), fits.get(0), written, pos);
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

    /**
     * {@code o}'s rows form ({@link #rowsForm}); a default's ({@link TsToPure.Overload#delegate}, W5, 2026-11-06)
     * where it has none of its own: the rows form of the overload its body calls, of the arguments the body gives --
     * each parameter the call's own, each literal itself -- so a default's value is written once, in its body (no rows
     * form writes 63 again). Null where neither has one.
     */
    private static TsLines.@com.legend.base.Nullable Valued keptForm(TsFunctions.Ctx c, String fqn, String word,
            List<TsToPure.Overload> overloads, TsToPure.Overload o, List<ValueSpecification> bound, List<Arg> args,
            SqlAst.Pos pos) {
        TsLines.Valued own = rowsForm(c, fqn, word, o, bound, args, pos);
        List<ValueSpecification> body = o.delegate();
        if (own != null || body == null) {
            return own;
        }
        List<String> names = o.params().stream().map(TsToPure.Parameter::name).toList();
        List<ValueSpecification> given = new ArrayList<>();
        List<String> types = new ArrayList<>();
        for (ValueSpecification v : body) {
            int i = v instanceof com.legend.protocol.spec.Variable p ? names.indexOf(p.name()) : -1;
            given.add(i >= 0 ? bound.get(i) : v);
            types.add(i >= 0 ? o.params().get(i).type() : literalType(v));
        }
        for (TsToPure.Overload t : overloads) {
            if (t != o && t.params().stream().map(TsToPure.Parameter::type).toList().equals(types)) {
                return rowsForm(c, fqn, word, t, given, args, pos);
            }
        }
        return null;
    }

    /** The type's path of a literal a default's body gives ({@link #keptForm}); a parameter's read, none. */
    private static @com.legend.base.Nullable String literalType(ValueSpecification v) {
        return switch (v) {
            case CInteger i -> "Integer";
            case CFloat f -> "Float";
            case CString s -> "String";
            case com.legend.protocol.spec.CBoolean b -> "Boolean";
            default -> null;
        };
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
            TsToPure.Overload o, List<ValueSpecification> bound, List<Arg> args, SqlAst.Pos pos) {
        String rowsFqn = ROWS + fqn.substring(PACKAGE.length());
        if (args.size() == 2) {
            return pairForm(c, rowsFqn, word, o, bound, args, pos);
        }
        if (args.size() != 1 || !(args.get(0) instanceof LineArg l) || o.params().isEmpty()
                || !TsToPure.LINE.equals(o.params().get(0).type()) || !o.gives().equals(List.of("value"))) {
            return null;
        }
        List<String> own = o.params().subList(1, o.params().size()).stream().map(TsToPure.Parameter::type).toList();
        for (TsToPure.Overload r : c.model().overloads(rowsFqn)) {
            List<TsToPure.Parameter> p = r.params();
            if (p.size() != own.size() + 7 || !p.subList(5, p.size() - 2).stream().map(TsToPure.Parameter::type)
                    .toList().equals(own) || !computes(r, bound)) {
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
                return TsLines.call(rowsFqn, a);
            });
            c.present(out, points);
            return new TsLines.Valued(line, out);
        }
        return null;
    }

    /** The tag of a rows form computing one argument's value alone ({@code timeseries::Words.when}, 'mode: truncate'). */
    static final String WHEN = "when";

    /**
     * Whether the call's arguments {@code bound} -- its line, then its own -- are those the rows form {@code r} computes
     * ({@code timeseries::Words.when}: lag's rows form is its truncate alone, a column of its line's table, where its
     * extend grows the line past its end): each argument it names spelled as it says, an option's word compared, never
     * computed (the world map's compare-not-compute). A rows form that says none computes every call.
     */
    private static boolean computes(TsToPure.Overload r, List<ValueSpecification> bound) {
        for (String w : r.facts().getOrDefault(WHEN, List.of())) {
            int colon = w.indexOf(": ");
            String name = w.substring(0, colon);
            String value = w.substring(colon + 2);
            int at = -1;
            for (int i = 5; i < r.params().size() - 2; i++) {
                if (r.params().get(i).name().equals(name)) {
                    at = i;
                }
            }
            if (at < 0) {
                throw new IllegalStateException("a rows form's when names no parameter of it: " + w);
            }
            // its own parameters after the table, its columns and the presence; the call's after its line
            ValueSpecification given = bound.get(at - 4);
            if (!(given instanceof EnumValue e) || !e.value().equals(value)) {
                return false;
            }
        }
        return true;
    }

    /**
     * {@code o}, the overload of a word a call of two lines fits, as its rows form (R7 part 3, the time model's D5): the
     * library's {@code timeseries::rows::} function of the word's name, {@code rowsFqn}, whose parameters are a table,
     * its key and date, the two values' columns, then {@code o}'s own after its lines but the query's periods (the
     * lines meet on them already), then the answer's column -- called on the two lines met ({@link TsFunctions.Ctx#met}:
     * one table for the same two lines, every word of them a column of it), each of its rows a pair, so the answer a
     * point of every row. Null where the library declares none, under FILL (the line form meets its lines unfilled), or
     * where both values are of one table (met with itself, a table of several rows on a date is refused, D1: the line
     * form's to say).
     */
    private static TsLines.@com.legend.base.Nullable Valued pairForm(TsFunctions.Ctx c, String rowsFqn, String word,
            TsToPure.Overload o, List<ValueSpecification> bound, List<Arg> args, SqlAst.Pos pos) {
        if (!(args.get(0) instanceof LineArg x) || !(args.get(1) instanceof LineArg y) || o.params().size() < 2
                || !TsToPure.LINE.equals(o.params().get(0).type()) || !TsToPure.LINE.equals(o.params().get(1).type())
                || !o.gives().equals(List.of("value")) || c.query().fill() != null
                || c.newest(x.line().line()) == c.newest(y.line().line())) {
            return null;
        }
        // its own parameters after its lines: without the query's periods (the lines meet on them already), or with
        // them where the rows form declares them (a figure annualized by SAMPLE BY's periods a year)
        List<Integer> own = new ArrayList<>();
        List<Integer> all = new ArrayList<>();
        for (int i = 2; i < o.params().size(); i++) {
            all.add(i);
            if (!TsCatalog.SAMPLE.equals(o.params().get(i).type())) {
                own.add(i);
            }
        }
        for (TsToPure.Overload r : c.model().overloads(rowsFqn)) {
            for (List<Integer> pick : List.of(own, all)) {
                List<String> types = pick.stream().map(i -> o.params().get(i).type()).toList();
                List<TsToPure.Parameter> p = r.params();
                if (p.size() != pick.size() + 6 || !p.subList(5, p.size() - 1).stream().map(TsToPure.Parameter::type)
                        .toList().equals(types)) {
                    continue;
                }
                // each line taken through the word the library names first (timeseries::Words.pairs: correlation's
                // lines are their returns, each a column of its own line's table)
                TsLines.Valued xv = x.line();
                TsLines.Valued yv = y.line();
                for (String through : c.model().overloads(fqn(c.model(), word)).stream()
                        .flatMap(ov -> ov.facts().getOrDefault(TsCatalog.PAIRS, List.of()).stream()).distinct()
                        .toList()) {
                    // once a line and word: every word of the pair reads the same returns, so their meetings are one
                    TsLines.Valued ox = xv;
                    TsLines.Valued oy = yv;
                    xv = c.read(List.of(TsCatalog.PAIRS, through, ox.value()),
                            () -> call(c, through, List.of(new LineArg(ox)), Map.of(), pos));
                    yv = c.read(List.of(TsCatalog.PAIRS, through, oy.value()),
                            () -> call(c, through, List.of(new LineArg(oy)), Map.of(), pos));
                }
                TsLines.Valued px = xv;
                TsLines.Valued py = yv;
                TsLines.Line met = c.met(List.of(px, py), pos);
                String out = c.fresh(word);
                TsLines.Line line = c.grown(met, rel -> {
                    List<ValueSpecification> a = new ArrayList<>(List.of(rel, new ColSpec(met.part()),
                            new ColSpec(met.at()), new ColSpec(px.value()), new ColSpec(py.value())));
                    pick.forEach(i -> a.add(bound.get(i)));
                    a.add(new ColSpec(out));
                    return TsLines.call(rowsFqn, a);
                });
                return new TsLines.Valued(line, out);
            }
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

    /** {@code v} as a word takes it beside {@code key}, the line whose keys the word's answer has: the library's
     *  {@code helpers::each}, picked by the two keys' types (2a: a line of no instrument beside lines per instrument
     *  every instrument's, R2's broadcast; else itself). */
    private static ValueSpecification each(TsLines.Valued v, TsLines.Line key, SqlAst.Pos pos) {
        return TsLines.call(PACKAGE + "helpers::each", in(v.line(), v.value(), pos), in(key, keyValue(key), pos));
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
        List<ValueSpecification> all = new ArrayList<>();
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
            all.add(one);
        }
        if (all.isEmpty()) {
            throw new IllegalStateException("a basket of no members");
        }
        return TsLines.balanced(all, (a, b) -> TsLines.call("concatenate", a, b));
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
            case Var v -> fits(variable(c, v), t) ? new com.legend.protocol.spec.Variable(v.name()) : null;
            case Computed d -> "StrictDate".equals(t) ? d.value() : null;
        };
    }

    /** The type of the variable {@code v} names: in scope, of one value; else refused by name. */
    private static String variable(TsFunctions.Ctx c, Var v) {
        com.legend.protocol.sql.SqlToPure.VariableType type = com.legend.protocol.sql.SqlToPure.variable(c.model(),
                v.name());
        if (type == null) {
            throw com.legend.protocol.sql.SqlTranslationException.invalid("$" + v.name() + " is no variable of the"
                    + " enclosing Pure in scope", v.pos());
        }
        if (type.many() || type.nullable()) {
            throw com.legend.protocol.sql.SqlTranslationException.unsupported("$" + v.name() + " may hold "
                    + (type.many() ? "more than one value" : "no value") + ": #TS reads a variable of one value",
                    v.pos());
        }
        return type.type();
    }

    /** Whether a value of the type {@code given} is one of the parameter type {@code t}'s: the same, or a number for
     *  a Number. */
    private static boolean fits(String given, String t) {
        return given.equals(t) || "Number".equals(t) && ("Integer".equals(given) || "Float".equals(given));
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
