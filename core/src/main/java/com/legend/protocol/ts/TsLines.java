// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.ts;

import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.AppliedProperty;
import com.legend.protocol.spec.CString;
import com.legend.protocol.spec.ColSpec;
import com.legend.protocol.spec.ColSpecArray;
import com.legend.protocol.spec.EnumValue;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.PureCollection;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.spec.Variable;
import com.legend.protocol.sql.SqlAst;
import com.legend.protocol.sql.SqlTranslationException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * THE LINES OF A {@code #TS{...}#} QUERY (docs/TIMESERIES_LANGUAGE_2026_10_29.md R2, R4, R6): a line is one
 * instrument's column along its dates -- a relation of one row per line and period: the source's rows that have a
 * date and a value (and meet the line's subscript), taken by period ({@code last} of a day's rows by default), the
 * periods it has a value in alone. A figure along time runs over its line's own rows (R4: never dates another line
 * brought), and lines meet where an expression reads them together, on the dates every one has (R6, without FILL).
 * The tile's model (datacube/src/timeseries-query.ts's columnPart and joined), so a tile's series and its {@code #TS}
 * answer alike (docs/TS_LANGUAGE_BUILD_2026_10_30.md P5).
 */
final class TsLines {

    private TsLines() {
    }

    /** A line's period: every window's order, the column lines meet on. */
    static final String PERIOD = "__ts_p0";

    /** A line's period's number in time (days since 1970, or seconds for a date-time): its last row's. */
    static final String ORD = "__ts_ord";

    /** A line's period's date: its last row's, the date its value is of. */
    static final String AT = "__ts_at";

    /** The line column of a line of no instrument (a subscripted one, or a source with no {@code BY}): one value. */
    static final String ONE_LINE = "__ts_line";

    private static final String DURATION = "meta::pure::functions::date::DurationUnit";

    /** A relation of one row per line and period: its line column ({@link #ONE_LINE}, or the instrument column of a
     *  line per instrument), whether it is a line per instrument, its period's date column, and whether its number in
     *  time ({@link #ORD}) counts seconds (a date-time source's) rather than days. */
    record Line(ValueSpecification rel, String part, boolean byInstrument, String at, boolean seconds) {

        Line with(ValueSpecification other) {
            return new Line(other, part, byInstrument, at, seconds);
        }
    }

    /** An expression's value: a constant (no line in it), or a column of a line. */
    sealed interface Node permits Constant, Valued {
    }

    /** An expression of no line: a per-point constant. */
    record Constant(SqlAst.Expr expr) implements Node {
    }

    /** A line's column. */
    record Valued(Line line, String value) implements Node {
    }

    /** A source a line reads: its relation, its date column (and that column's type), its instrument column (none:
     *  null), and what the query calls it. */
    record Source(ValueSpecification relation, String date, String dateType, @com.legend.base.Nullable String instrument,
            String what) {
    }

    /** What the lines are written with: a per-point expression's lambda (the translator's, R13's), and fresh names. */
    interface Writer {
        LambdaFunction rowLambda(SqlAst.Expr e, ValueSpecification relation, boolean condition, String clause);

        String fresh(String what);
    }

    /**
     * A COLUMN'S OWN LINE (R2): the rows of {@code s} that meet {@code matches} (a subscript's conditions, Postgres's)
     * and have a date and a value -- its instrument ({@code split}), its period (its date: a line is its column's rows,
     * R5, a period's rows taken by SAMPLE BY's aggregate), its number in time, its date, its value under a fresh name;
     * a line of no instrument carrying {@link #ONE_LINE}.
     */
    static Valued column(Writer w, Source s, String column, List<SqlAst.Expr> matches, boolean split) {
        ValueSpecification rel = s.relation();
        for (SqlAst.Expr m : matches) {
            rel = call("filter", rel, w.rowLambda(m, rel, true, "a subscript"));
        }
        String instrument = split ? java.util.Objects.requireNonNull(s.instrument(),
                "a line per instrument of a source of none") : ONE_LINE;
        if (!split) {
            rel = call("extend", rel, new ColSpec(ONE_LINE, lambda("x", new CString("all"))));
        }
        String value = w.fresh("base");
        boolean seconds = !"StrictDate".equals(s.dateType()) && !"Date".equals(s.dateType());
        // the library's line (timeseries::lines::line): its rows with a date and a value, its period, its number in
        // time, its date and its value
        rel = call(LINES + "line", rel, new ColSpec(instrument), new ColSpec(s.date()), new ColSpec(column),
                new EnumValue(DURATION, seconds ? "SECONDS" : "DAYS"), new ColSpec(instrument), new ColSpec(PERIOD),
                new ColSpec(ORD), new ColSpec(AT), new ColSpec(value));
        return new Valued(new Line(rel, instrument, split, AT, seconds), value);
    }

    /** The package of the language's lines' rules (projects/timeseries/model.pure, THE LANGUAGE'S LINES). */
    static final String LINES = TsToPure.LIBRARY + "lines::";

    /** SAMPLE BY's unit as the library's {@code timeseries::Sample}. */
    static EnumValue sample(String unit) {
        return new EnumValue(TsToPure.LIBRARY + "Sample", switch (unit) {
            case "d" -> "day";
            case "w" -> "week";
            case "mo" -> "month";
            case "q" -> "quarter";
            case "y" -> "year";
            default -> throw new IllegalStateException("SAMPLE BY " + unit);
        });
    }

    /** How a period's rows become one value (R5): the language's name, the library's {@code timeseries::Taken}. */
    static final Map<String, String> TAKEN = Map.of("last", "last", "first", "first", "max", "max", "min", "min",
            "sum", "sum", "avg", "avg", "mean", "avg");

    /**
     * A COLUMN'S LINE BY PERIOD (R5, SAMPLE BY): its rows as {@link #column}'s, each row's period the first day of its
     * calendar {@code unit} ({@code d} its day), grouped by the instrument and the period, its value {@code taken}
     * ({@code last}, the period's last row's; {@code first}; {@code max}, {@code min}, {@code sum}, {@code avg}), the
     * period dated at its last row's day -- the tile's frequency (timeseries-query.ts columnPart, periodOf).
     */
    static Valued sampled(Writer w, Source s, String column, List<SqlAst.Expr> matches, boolean split, String unit,
            String taken, String dated) {
        ValueSpecification rel = s.relation();
        for (SqlAst.Expr m : matches) {
            rel = call("filter", rel, w.rowLambda(m, rel, true, "a subscript"));
        }
        String instrument = split ? java.util.Objects.requireNonNull(s.instrument(),
                "a line per instrument of a source of none") : ONE_LINE;
        if (!split) {
            rel = call("extend", rel, new ColSpec(ONE_LINE, lambda("x", new CString("all"))));
        }
        boolean day = "StrictDate".equals(s.dateType()) || "Date".equals(s.dateType());
        String dayColumn = s.date();
        if (!day) {
            // a date-time's day (the library's dayOf)
            dayColumn = w.fresh("day");
            rel = call(LINES + "dayOf", rel, new ColSpec(s.date()), new ColSpec(dayColumn));
        }
        String value = w.fresh("base");
        // the library's line by period (timeseries::lines::sample)
        rel = call(LINES + "sample", rel, new ColSpec(instrument), new ColSpec(s.date()), new ColSpec(dayColumn),
                new ColSpec(column), new EnumValue(DURATION, day ? "DAYS" : "SECONDS"), sample(unit),
                new EnumValue(TsToPure.LIBRARY + "Taken", java.util.Objects.requireNonNull(TAKEN.get(taken))),
                new EnumValue(TsToPure.LIBRARY + "Dated", dated.toLowerCase(java.util.Locale.ROOT)),
                new ColSpec(instrument), new ColSpec(PERIOD), new ColSpec(ORD), new ColSpec(AT), new ColSpec(value));
        return new Valued(new Line(rel, instrument, split, AT, !day), value);
    }

    /** Lines spread onto every period any of them has ({@link #spread}): the line, and each line's own number in
     *  time there (none where it has no row), in the lines' order. */
    record Spread(Line line, List<String> ords) {
    }

    /**
     * EVERY PERIOD ANY OF {@code nodes} HAS (R6): each (instrument, period) one of them has a row in -- a line of one
     * instrument under every instrument of the lines per instrument (R2's broadcast, the instruments those lines have)
     * -- dated at its latest line's date and number in time (R5: one date for every line), each line's value LEFT-joined
     * onto them under its own name with its own number in time (a line several nodes read, a struct's fields, joined
     * once with all their values). Each line is read twice, linearly: no FULL join nested in another (H2, which writes
     * one as a UNION, plans a nesting of them exponentially).
     */
    static Spread spread(Writer w, List<Valued> nodes, SqlAst.Pos pos) {
        List<Valued> split = nodes.stream().filter(n -> n.line().byInstrument()).toList();
        String part = split.isEmpty() ? ONE_LINE : split.get(0).line().part();
        for (Valued n : split) {
            if (!n.line().part().equals(part)) {
                throw SqlTranslationException.invalid("lines by " + part + " and lines by " + n.line().part()
                        + " meet on no instrument: pick one line of one with a subscript (R2)", pos);
            }
        }
        // each line once, with the values the nodes read of it
        List<Line> distinct = new ArrayList<>();
        List<List<String>> values = distinct(nodes, distinct);
        String at = w.fresh("sp_at");
        String ord = w.fresh("sp_ord");
        // the instruments the lines per instrument have (each line's own: the library's broadcast reads them)
        ValueSpecification instruments = null;
        for (Line l : distinct) {
            if (l.byInstrument()) {
                ValueSpecification own = call("select", l.rel(), new ColSpecArray(List.of(new ColSpec(part))));
                instruments = instruments == null ? own : call("concatenate", instruments, own);
            }
        }
        // the periods each line has, keyed alike: a line of no instrument's under every instrument (the library's
        // periodsOf, periodsOfOne); the answer's periods, each dated at its latest line's date and number in time (R5:
        // one date for every line), the library's spine
        ValueSpecification keys = null;
        for (Line l : distinct) {
            ValueSpecification k = instruments != null && !l.byInstrument()
                    ? call(LINES + "periodsOfOne", l.rel(), new ColSpec(PERIOD), new ColSpec(l.at()), new ColSpec(ORD),
                            instruments, new ColSpec(part), new ColSpec(part), new ColSpec(PERIOD), new ColSpec(at),
                            new ColSpec(ord))
                    : call(LINES + "periodsOf", l.rel(), new ColSpec(l.part()), new ColSpec(PERIOD), new ColSpec(l.at()),
                            new ColSpec(ORD), new ColSpec(part), new ColSpec(PERIOD), new ColSpec(at), new ColSpec(ord));
            keys = keys == null ? k : call("concatenate", keys, k);
        }
        ValueSpecification spine = call(LINES + "spine", java.util.Objects.requireNonNull(keys, "lines to spread"),
                new ColSpec(part), new ColSpec(PERIOD), new ColSpec(at), new ColSpec(ord), new ColSpec(part),
                new ColSpec(PERIOD), new ColSpec(at), new ColSpec(ord));
        // each line's values onto them (the library's onto; a line of no instrument's among lines per instrument,
        // ontoOne)
        List<String> owns = new ArrayList<>();
        List<ColSpec> kept = new ArrayList<>(List.of(new ColSpec(part), new ColSpec(PERIOD), new ColSpec(at),
                new ColSpec(ord)));
        for (int d = 0; d < distinct.size(); d++) {
            Line l = distinct.get(d);
            boolean byLine = l.byInstrument() || instruments == null;
            String p = w.fresh("sp_p");
            String per = w.fresh("sp_per");
            String own = w.fresh("sp_ord");
            List<ColSpec> cols = new ArrayList<>();
            if (byLine) {
                cols.add(new ColSpec(l.part()));
            }
            cols.add(new ColSpec(PERIOD));
            cols.add(new ColSpec(ORD));
            values.get(d).forEach(v -> cols.add(new ColSpec(v)));
            ValueSpecification r = call("select", l.rel(), new ColSpecArray(cols));
            if (byLine) {
                r = call("rename", r, new ColSpec(l.part()), new ColSpec(p));
            }
            r = call("rename", call("rename", r, new ColSpec(PERIOD), new ColSpec(per)), new ColSpec(ORD),
                    new ColSpec(own));
            spine = byLine
                    ? call(LINES + "onto", spine, new ColSpec(part), new ColSpec(PERIOD), r, new ColSpec(p),
                            new ColSpec(per))
                    : call(LINES + "ontoOne", spine, new ColSpec(PERIOD), r, new ColSpec(per));
            kept.add(new ColSpec(own));
            values.get(d).forEach(v -> kept.add(new ColSpec(v)));
            spine = call("select", spine, new ColSpecArray(List.copyOf(kept)));
            owns.add(own);
        }
        spine = call("rename", spine, new ColSpec(ord), new ColSpec(ORD));
        List<String> ords = nodes.stream().map(n -> owns.get(indexOf(distinct, n.line()))).toList();
        return new Spread(new Line(spine, part, instruments != null, at, nodes.get(0).line().seconds()), ords);
    }

    /**
     * LINES MEETING UNDER FILL (R6): every period any line has ({@link #spread}), each one's value put onto them by
     * {@code how}: {@code PREV}, its last value at or before the date (none before its first: never a later value);
     * {@code LINEAR}, linear in time between its values either side (none outside them); {@code ZERO}, 0; {@code NULL},
     * none -- the library's alignment functions (the tile's align, timeseries-query.ts fill).
     */
    static Line aligned(Writer w, List<Valued> nodes, String how, SqlAst.Pos pos) {
        Spread s = spread(w, nodes, pos);
        Line l = s.line();
        for (int i = 0; i < nodes.size(); i++) {
            l = fill(w, l, List.of(nodes.get(i).value()), s.ords().get(i), how);
        }
        return l;
    }

    /** Each line of {@code nodes} once, in order, into {@code lines} (several nodes may read one: a struct's fields);
     *  the values the nodes read of each, in its place. */
    private static List<List<String>> distinct(List<Valued> nodes, List<Line> lines) {
        List<List<String>> values = new ArrayList<>();
        for (Valued n : nodes) {
            int i = indexOf(lines, n.line());
            if (i < 0) {
                lines.add(n.line());
                values.add(new ArrayList<>());
                i = lines.size() - 1;
            }
            if (!values.get(i).contains(n.value())) {
                values.get(i).add(n.value());
            }
        }
        return values;
    }

    /** Where {@code line} is in {@code lines}, the very line (none: -1). */
    private static int indexOf(List<Line> lines, Line line) {
        for (int i = 0; i < lines.size(); i++) {
            if (lines.get(i) == line) {
                return i;
            }
        }
        return -1;
    }

    /** THE ANSWER'S ROWS (R6): every period any item has a value in ({@link #spread}), the items' columns as named. */
    static Line answer(Writer w, List<Valued> items, SqlAst.Pos pos) {
        Line l = items.stream().allMatch(v -> v.line() == items.get(0).line()) ? items.get(0).line()
                : spread(w, items, pos).line();
        List<ColSpec> keep = new ArrayList<>(List.of(new ColSpec(l.part()), new ColSpec(PERIOD), new ColSpec(l.at())));
        items.forEach(v -> keep.add(new ColSpec(v.value())));
        return l.with(call("select", l.rel(), new ColSpecArray(keep)));
    }

    /** {@code line}'s {@code values} put onto its merged dates by {@code how} (R6, the library's fill), each under its
     *  own name; {@code ord} the number in time of the periods they had a row in, the first value its rows' marker. */
    private static Line fill(Writer w, Line line, List<String> values, String ord, String how) {
        if ("NULL".equals(how) || values.isEmpty()) {
            return line;
        }
        Line l = line;
        // every value filled before any is replaced: the marker is the line's first value as it came
        List<String> outs = new ArrayList<>();
        for (String v : values) {
            String out = w.fresh("al_fill");
            l = l.with(call(LINES + "fill", l.rel(), new ColSpec(l.part()), new ColSpec(PERIOD),
                    new ColSpec(values.get(0)), new ColSpec(ord), new ColSpec(ORD), new ColSpec(v),
                    new EnumValue(TsToPure.LIBRARY + "Fill", how.toLowerCase(java.util.Locale.ROOT)),
                    new ColSpec(out)));
            outs.add(out);
        }
        for (int i = 0; i < values.size(); i++) {
            l = replaced(w, l, values.get(i), outs.get(i));
        }
        return l;
    }

    /** {@code l} with {@code filled} under {@code value}'s name, the column it filled kept under another. */
    private static Line replaced(Writer w, Line l, String value, String filled) {
        ValueSpecification rel = call("rename", l.rel(), new ColSpec(value), new ColSpec(w.fresh("al_raw")));
        return l.with(call("rename", rel, new ColSpec(filled), new ColSpec(value)));
    }

    /**
     * LINES MEETING on the dates every one has (R6, without FILL; the tile's intersect): joined on the period, and on
     * the instrument where both are lines per instrument -- a line of one instrument meets each instrument's (R2's
     * broadcast); each period dated at its latest line's. The first line's columns kept, each other's value carried.
     */
    static Line joined(Writer w, List<Valued> nodes, SqlAst.Pos pos, @com.legend.base.Nullable String fill) {
        if (fill != null) {
            return aligned(w, nodes, fill, pos);
        }
        List<Valued> order = new ArrayList<>(nodes.stream().filter(n -> n.line().byInstrument()).toList());
        order.addAll(nodes.stream().filter(n -> !n.line().byInstrument()).toList());
        // each line once (a struct's fields read one line), with the values the nodes read of it
        List<Line> distinct = new ArrayList<>();
        List<List<String>> values = distinct(order, distinct);
        Line left = distinct.get(0);
        for (int d = 1; d < distinct.size(); d++) {
            Line r = distinct.get(d);
            boolean both = left.byInstrument() && r.byInstrument();
            if (both && !left.part().equals(r.part())) {
                throw SqlTranslationException.invalid("lines by " + left.part() + " and lines by " + r.part()
                        + " meet on no instrument: pick one line of one with a subscript (R2)", pos);
            }
            String p = w.fresh("jp");
            String a = w.fresh("ja");
            String l = both ? w.fresh("jl") : null;
            ValueSpecification rel = call("rename", r.rel(), new ColSpec(PERIOD), new ColSpec(p));
            rel = call("rename", rel, new ColSpec(r.at()), new ColSpec(a));
            List<ColSpec> kept = new ArrayList<>(List.of(new ColSpec(p), new ColSpec(a)));
            if (l != null) {
                rel = call("rename", rel, new ColSpec(r.part()), new ColSpec(l));
                kept.add(new ColSpec(l));
            }
            values.get(d).forEach(v -> kept.add(new ColSpec(v)));
            rel = call("select", rel, new ColSpecArray(kept));
            // the library's meeting (timeseries::lines::meet; a line of no instrument, meetOne): on the period, and the
            // instrument where both are lines per instrument; dated at the later date
            String at = w.fresh("at");
            ValueSpecification met = l != null
                    ? call(LINES + "meet", left.rel(), new ColSpec(left.part()), new ColSpec(PERIOD),
                            new ColSpec(left.at()), rel, new ColSpec(l), new ColSpec(p), new ColSpec(a), new ColSpec(at))
                    : call(LINES + "meetOne", left.rel(), new ColSpec(PERIOD), new ColSpec(left.at()), rel,
                            new ColSpec(p), new ColSpec(a), new ColSpec(at));
            left = new Line(met, left.part(), left.byInstrument(), at, left.seconds());
        }
        return left;
    }

    // ------------------------------------------------------------------ Pure

    static AppliedFunction call(String fn, ValueSpecification... args) {
        return new AppliedFunction(fn, List.of(args));
    }

    static AppliedFunction call(String fn, List<ValueSpecification> args) {
        return new AppliedFunction(fn, args);
    }

    static LambdaFunction lambda(String parameter, ValueSpecification body) {
        return new LambdaFunction(List.of(new Variable(parameter)), List.of(body));
    }

    /** {@code $x.c}. */
    static ValueSpecification col(String c) {
        return new AppliedProperty(new Variable("x"), c);
    }

}
