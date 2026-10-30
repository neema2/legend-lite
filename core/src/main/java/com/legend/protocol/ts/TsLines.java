// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.ts;

import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.AppliedProperty;
import com.legend.protocol.spec.CInteger;
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
     *  line per instrument), whether it is a line per instrument, and its period's date column. */
    record Line(ValueSpecification rel, String part, boolean byInstrument, String at) {

        Line with(ValueSpecification other) {
            return new Line(other, part, byInstrument, at);
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
        rel = call("filter", rel, lambda("x", call("and", call("isNotEmpty", col(s.date())),
                call("isNotEmpty", col(column)))));
        String instrument = split ? java.util.Objects.requireNonNull(s.instrument(),
                "a line per instrument of a source of none") : ONE_LINE;
        String value = w.fresh("base");
        boolean seconds = !"StrictDate".equals(s.dateType()) && !"Date".equals(s.dateType());
        List<ColSpec> derived = new ArrayList<>();
        if (!split) {
            derived.add(new ColSpec(ONE_LINE, lambda("x", new CString("all"))));
        }
        derived.add(new ColSpec(PERIOD, lambda("x", col(s.date()))));
        derived.add(new ColSpec(ORD, lambda("x", call("dateDiff", epoch(), one(s.date()),
                new EnumValue(DURATION, seconds ? "SECONDS" : "DAYS")))));
        derived.add(new ColSpec(AT, lambda("x", col(s.date()))));
        derived.add(new ColSpec(value, lambda("x", col(column))));
        rel = call("extend", rel, new ColSpecArray(derived));
        rel = call("select", rel, new ColSpecArray(List.of(new ColSpec(instrument), new ColSpec(PERIOD),
                new ColSpec(ORD), new ColSpec(AT), new ColSpec(value))));
        return new Valued(new Line(rel, instrument, split, AT), value);
    }

    /** A period's function of a date (R5's calendar periods): its first day. */
    private static final Map<String, String> PERIOD_START = Map.of("w", "firstDayOfWeek", "mo", "firstDayOfMonth",
            "q", "firstDayOfQuarter", "y", "firstDayOfYear");

    /** How a period's rows become one value (R5): the language's name, the aggregate. */
    static final Map<String, String> TAKEN = Map.of("last", "maxBy", "first", "minBy", "max", "max", "min", "min",
            "sum", "sum", "avg", "average", "mean", "average");

    /**
     * A COLUMN'S LINE BY PERIOD (R5, SAMPLE BY): its rows as {@link #column}'s, each row's period the first day of its
     * calendar {@code unit} ({@code d} its day), grouped by the instrument and the period, its value {@code taken}
     * ({@code last}, the period's last row's; {@code first}; {@code max}, {@code min}, {@code sum}, {@code avg}), the
     * period dated at its last row's day -- the tile's frequency (timeseries-query.ts columnPart, periodOf).
     */
    static Valued sampled(Writer w, Source s, String column, List<SqlAst.Expr> matches, boolean split, String unit,
            String taken) {
        ValueSpecification rel = s.relation();
        for (SqlAst.Expr m : matches) {
            rel = call("filter", rel, w.rowLambda(m, rel, true, "a subscript"));
        }
        rel = call("filter", rel, lambda("x", call("and", call("isNotEmpty", col(s.date())),
                call("isNotEmpty", col(column)))));
        boolean day = "StrictDate".equals(s.dateType()) || "Date".equals(s.dateType());
        ValueSpecification date = day ? col(s.date()) : call("datePart", one(s.date()));
        String start = PERIOD_START.get(unit);
        ValueSpecification period = start == null ? date : call(start, call("toOne", date));
        String ordRow = w.fresh("or");
        String atRow = w.fresh("ar");
        rel = call("extend", rel, new ColSpecArray(List.of(
                new ColSpec(PERIOD, lambda("x", period)),
                new ColSpec(ordRow, lambda("x", call("dateDiff", epoch(), one(s.date()),
                        new EnumValue(DURATION, day ? "DAYS" : "SECONDS")))),
                new ColSpec(atRow, lambda("x", date)))));
        String instrument = split ? java.util.Objects.requireNonNull(s.instrument(),
                "a line per instrument of a source of none") : ONE_LINE;
        List<ColSpec> keys = split ? List.of(new ColSpec(instrument), new ColSpec(PERIOD))
                : List.of(new ColSpec(PERIOD));
        String value = w.fresh("base");
        String fn = java.util.Objects.requireNonNull(TAKEN.get(taken));
        boolean byRow = "maxBy".equals(fn) || "minBy".equals(fn);
        List<ColSpec> aggs = List.of(
                new ColSpec(value, lambda("x", byRow ? call("rowMapper", col(column), col(ordRow)) : col(column)),
                        lambda("y", call(fn, new Variable("y")))),
                new ColSpec(ORD, lambda("x", col(ordRow)), lambda("y", call("max", new Variable("y")))),
                new ColSpec(AT, lambda("x", col(atRow)), lambda("y", call("max", new Variable("y")))));
        rel = call("groupBy", rel, new ColSpecArray(keys), new ColSpecArray(aggs));
        if (!split) {
            rel = call("extend", rel, new ColSpec(ONE_LINE, lambda("x", new CString("all"))));
        }
        rel = call("filter", rel, lambda("x", call("isNotEmpty", col(value))));
        return new Valued(new Line(rel, instrument, split, AT), value);
    }

    /** Lines spread onto every period any of them has ({@link #spread}): the line, and each line's own number in
     *  time there (none where it has no row), in the lines' order. */
    record Spread(Line line, List<String> ords) {
    }

    /**
     * EVERY PERIOD ANY OF {@code nodes} HAS (R6): each (instrument, period) one of them has a row in -- a line of one
     * instrument under every instrument of the lines per instrument (R2's broadcast, the instruments those lines have)
     * -- dated at its latest line's date and number in time (R5: one date for every line), each line's value LEFT-joined
     * onto them under its own name with its own number in time. Each line is read twice, linearly: no FULL join nested
     * in another (H2, which writes one as a UNION, plans a nesting of them exponentially).
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
        String at = w.fresh("sp_at");
        String ord = w.fresh("sp_ord");
        // the instruments the lines per instrument have
        ValueSpecification instruments = null;
        for (Valued n : split) {
            ValueSpecification own = call("select", n.line().rel(), new ColSpecArray(List.of(new ColSpec(part))));
            instruments = instruments == null ? own : call("concatenate", instruments, own);
        }
        String k1 = w.fresh("sp_k");
        String k2 = w.fresh("sp_k");
        String inst = w.fresh("sp_i");
        if (instruments != null) {
            instruments = call("extend", call("rename", call("distinct", instruments), new ColSpec(part),
                    new ColSpec(inst)), new ColSpec(k1, lambda("x", new CInteger(1L))));
        }
        // the periods each line has, keyed alike
        ValueSpecification keys = null;
        for (Valued n : nodes) {
            Line l = n.line();
            ValueSpecification k = call("rename", call("rename", call("select", l.rel(), new ColSpecArray(List.of(
                    new ColSpec(l.part()), new ColSpec(PERIOD), new ColSpec(l.at()), new ColSpec(ORD)))),
                    new ColSpec(l.at()), new ColSpec(at)), new ColSpec(ORD), new ColSpec(ord));
            if (instruments != null && !l.byInstrument()) {
                // one line under every instrument
                k = call("extend", call("select", k, new ColSpecArray(List.of(new ColSpec(PERIOD), new ColSpec(at),
                        new ColSpec(ord)))), new ColSpec(k2, lambda("x", new CInteger(1L))));
                k = call("join", instruments, k, new EnumValue("meta::pure::functions::relation::JoinKind", "INNER"),
                        lambda2("l", "r", call("equal", prop("l", k1), prop("r", k2))));
                k = call("rename", k, new ColSpec(inst), new ColSpec(part));
                k = call("select", k, new ColSpecArray(List.of(new ColSpec(part), new ColSpec(PERIOD), new ColSpec(at),
                        new ColSpec(ord))));
            }
            keys = keys == null ? k : call("concatenate", keys, k);
        }
        ValueSpecification spine = call("groupBy", java.util.Objects.requireNonNull(keys, "lines to spread"),
                new ColSpecArray(List.of(new ColSpec(part),
                new ColSpec(PERIOD))), new ColSpecArray(List.of(
                new ColSpec(at, lambda("x", col(at)), lambda("y", call("max", new Variable("y")))),
                new ColSpec(ord, lambda("x", col(ord)), lambda("y", call("max", new Variable("y")))))));
        // each line's value onto them
        List<String> ords = new ArrayList<>();
        List<ColSpec> kept = new ArrayList<>(List.of(new ColSpec(part), new ColSpec(PERIOD), new ColSpec(at),
                new ColSpec(ord)));
        for (Valued n : nodes) {
            Line l = n.line();
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
            cols.add(new ColSpec(n.value()));
            ValueSpecification r = call("select", l.rel(), new ColSpecArray(cols));
            if (byLine) {
                r = call("rename", r, new ColSpec(l.part()), new ColSpec(p));
            }
            r = call("rename", call("rename", r, new ColSpec(PERIOD), new ColSpec(per)), new ColSpec(ORD),
                    new ColSpec(own));
            ValueSpecification on = call("equal", prop("l", PERIOD), prop("r", per));
            if (byLine) {
                on = call("and", on, call("equal", prop("l", part), prop("r", p)));
            }
            spine = call("join", spine, r, new EnumValue("meta::pure::functions::relation::JoinKind", "LEFT"),
                    lambda2("l", "r", on));
            kept.add(new ColSpec(own));
            kept.add(new ColSpec(n.value()));
            spine = call("select", spine, new ColSpecArray(List.copyOf(kept)));
            ords.add(own);
        }
        spine = call("rename", spine, new ColSpec(ord), new ColSpec(ORD));
        return new Spread(new Line(spine, part, instruments != null, at), ords);
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

    /** THE ANSWER'S ROWS (R6): every period any item has a value in ({@link #spread}), the items' columns as named. */
    static Line answer(Writer w, List<Valued> items, SqlAst.Pos pos) {
        Line l = items.size() == 1 ? items.get(0).line() : spread(w, items, pos).line();
        List<ColSpec> keep = new ArrayList<>(List.of(new ColSpec(l.part()), new ColSpec(PERIOD), new ColSpec(l.at())));
        items.forEach(v -> keep.add(new ColSpec(v.value())));
        return l.with(call("select", l.rel(), new ColSpecArray(keep)));
    }

    /** {@code line}'s {@code values} put onto its merged dates by {@code how} (R6), each under its own name; {@code ord}
     *  the number in time of the periods they had a row in. */
    private static Line fill(Writer w, Line line, List<String> values, String ord, String how) {
        if ("NULL".equals(how) || values.isEmpty()) {
            return line;
        }
        Line l = line;
        if ("ZERO".equals(how)) {
            for (String v : values) {
                String out = w.fresh("al_zero");
                l = replaced(w, l.with(call(TsToPure.LIBRARY + "fillZero", l.rel(), new ColSpec(v), new ColSpec(out))),
                        v, out);
            }
            return l;
        }
        String lastP = w.fresh("al_lastp");
        l = l.with(call(TsToPure.LIBRARY + "lastValued", l.rel(), new ColSpec(l.part()), new ColSpec(PERIOD),
                new ColSpec(values.get(0)), new ColSpec(lastP)));
        if ("PREV".equals(how)) {
            for (String v : values) {
                String[] out = new String[1];
                l = valueAt(w, l, lastP, v, out);
                l = replaced(w, l, v, out[0]);
            }
            return l;
        }
        String nextP = w.fresh("al_nextp");
        l = l.with(call(TsToPure.LIBRARY + "nextValued", l.rel(), new ColSpec(l.part()), new ColSpec(PERIOD),
                new ColSpec(values.get(0)), new ColSpec(nextP)));
        String[] t0 = new String[1];
        String[] t1 = new String[1];
        l = valueAt(w, l, lastP, ord, t0);
        l = valueAt(w, l, nextP, ord, t1);
        for (String v : values) {
            String[] a = new String[1];
            String[] b = new String[1];
            l = valueAt(w, l, lastP, v, a);
            l = valueAt(w, l, nextP, v, b);
            String out = w.fresh("al_lin");
            l = l.with(call(TsToPure.LIBRARY + "linearAt", l.rel(), new ColSpec(t0[0]), new ColSpec(a[0]),
                    new ColSpec(t1[0]), new ColSpec(b[0]), new ColSpec(ORD), new ColSpec(out)));
            l = replaced(w, l, v, out);
        }
        return l;
    }

    /** {@code l} with {@code value}'s value at the period {@code at} names (a column of periods) as a fresh column,
     *  named in {@code out[0]}: the library's valueAt. */
    private static Line valueAt(Writer w, Line l, String at, String value, String[] out) {
        out[0] = w.fresh("al_v");
        return l.with(call(TsToPure.LIBRARY + "valueAt", l.rel(), new ColSpec(l.part()), new ColSpec(PERIOD),
                new ColSpec(at), new ColSpec(value), new ColSpec(out[0])));
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
        Line left = order.get(0).line();
        for (Valued right : order.subList(1, order.size())) {
            Line r = right.line();
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
            kept.add(new ColSpec(right.value()));
            rel = call("select", rel, new ColSpecArray(kept));
            ValueSpecification on = call("equal", prop("l", PERIOD), prop("r", p));
            if (l != null) {
                on = call("and", on, call("equal", prop("l", left.part()), prop("r", l)));
            }
            ValueSpecification met = call("join", left.rel(), rel,
                    new EnumValue("meta::pure::functions::relation::JoinKind", "INNER"), lambda2("l", "r", on));
            String at = w.fresh("at");
            met = call("extend", met, new ColSpec(at, lambda("x", call("if", call("greaterThanEqual", one(left.at()),
                    one(a)), thunk(col(left.at())), thunk(col(a))))));
            left = new Line(met, left.part(), left.byInstrument(), at);
        }
        return left;
    }

    /** The window of a figure along {@code line}: its line, its periods in order, and {@code frame} (none: null). */
    static ValueSpecification over(Line line, @com.legend.base.Nullable ValueSpecification frame) {
        List<ValueSpecification> args = new ArrayList<>(List.of(new ColSpecArray(List.of(new ColSpec(line.part()))),
                new PureCollection(List.of(call("ascending", new ColSpec(PERIOD))))));
        if (frame != null) {
            args.add(frame);
        }
        return call("over", args);
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

    private static LambdaFunction lambda2(String a, String b, ValueSpecification body) {
        return new LambdaFunction(List.of(new Variable(a), new Variable(b)), List.of(body));
    }

    private static LambdaFunction thunk(ValueSpecification body) {
        return new LambdaFunction(List.of(), List.of(body));
    }

    /** {@code $x.c}. */
    static ValueSpecification col(String c) {
        return new AppliedProperty(new Variable("x"), c);
    }

    private static ValueSpecification prop(String v, String c) {
        return new AppliedProperty(new Variable(v), c);
    }

    /** {@code $x.c->toOne()}. */
    private static ValueSpecification one(String c) {
        return call("toOne", col(c));
    }

    private static ValueSpecification epoch() {
        return new com.legend.protocol.spec.CDate(com.legend.values.PureDateLiteral.parse("1970-01-01"));
    }
}
