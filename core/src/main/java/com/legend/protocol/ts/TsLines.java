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

    /**
     * LINES MEETING on the dates every one has (R6, without FILL; the tile's intersect): joined on the period, and on
     * the instrument where both are lines per instrument -- a line of one instrument meets each instrument's (R2's
     * broadcast); each period dated at its latest line's. The first line's columns kept, each other's value carried.
     */
    static Line joined(Writer w, List<Valued> nodes, SqlAst.Pos pos) {
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

    /**
     * THE ANSWER'S ROWS (R6): the items' lines, every period any item has a value in -- a FULL join on the period and
     * the instrument, a line of one instrument under every instrument of the lines per instrument -- each period dated
     * at its latest item's date. The items' columns kept as they are named.
     */
    static Line answer(Writer w, List<Valued> items, SqlAst.Pos pos) {
        List<Valued> order = new ArrayList<>(items.stream().filter(n -> n.line().byInstrument()).toList());
        order.addAll(items.stream().filter(n -> !n.line().byInstrument()).toList());
        Line left = order.get(0).line();
        List<String> carried = new ArrayList<>(List.of(order.get(0).value()));
        left = left.with(call("select", left.rel(), new ColSpecArray(columns(left, carried))));
        for (Valued right : order.subList(1, order.size())) {
            Line r = right.line();
            if (left.byInstrument() && r.byInstrument() && !left.part().equals(r.part())) {
                throw SqlTranslationException.invalid("lines by " + left.part() + " and lines by " + r.part()
                        + " share no row: pick one line of one with a subscript (R2)", pos);
            }
            if (left.byInstrument() && !r.byInstrument()) {
                // one line under every instrument: each period's value beside each instrument's row of that period
                left = under(w, left, carried, right);
                carried.add(right.value());
                continue;
            }
            String p = w.fresh("ap");
            String a = w.fresh("aa");
            String l = w.fresh("al");
            ValueSpecification rel = call("rename", r.rel(), new ColSpec(PERIOD), new ColSpec(p));
            rel = call("rename", rel, new ColSpec(r.at()), new ColSpec(a));
            rel = call("rename", rel, new ColSpec(r.part()), new ColSpec(l));
            rel = call("select", rel, new ColSpecArray(List.of(new ColSpec(p), new ColSpec(a), new ColSpec(l),
                    new ColSpec(right.value()))));
            ValueSpecification on = call("and", call("equal", prop("l", PERIOD), prop("r", p)),
                    call("equal", prop("l", left.part()), prop("r", l)));
            ValueSpecification met = call("join", left.rel(), rel,
                    new EnumValue("meta::pure::functions::relation::JoinKind", "FULL"), lambda2("l", "r", on));
            String part = w.fresh("apart");
            String period = w.fresh("aper");
            String at = w.fresh("aat");
            met = call("extend", met, new ColSpecArray(List.of(
                    new ColSpec(part, lambda("x", either(left.part(), l))),
                    new ColSpec(period, lambda("x", either(PERIOD, p))),
                    new ColSpec(at, lambda("x", latest(left.at(), a))))));
            carried.add(right.value());
            List<ColSpec> keep = new ArrayList<>(List.of(new ColSpec(part), new ColSpec(period), new ColSpec(at)));
            carried.forEach(c -> keep.add(new ColSpec(c)));
            met = call("select", met, new ColSpecArray(keep));
            met = call("rename", met, new ColSpec(part), new ColSpec(left.part()));
            met = call("rename", met, new ColSpec(period), new ColSpec(PERIOD));
            left = new Line(met, left.part(), left.byInstrument(), at);
        }
        return left;
    }

    /** {@code left}'s rows (lines per instrument) with {@code one}'s value (a line of one instrument) beside each of
     *  its periods -- and the periods {@code one} has that no instrument has, under every instrument {@code left}
     *  has: a FULL join on the period, then each instrument's. */
    private static Line under(Writer w, Line left, List<String> carried, Valued one) {
        // the instruments: every one left has
        String k1 = w.fresh("uk");
        String k2 = w.fresh("uk");
        String inst = w.fresh("ui");
        ValueSpecification lines = call("extend", call("distinct", call("rename", call("select", left.rel(),
                new ColSpecArray(List.of(new ColSpec(left.part())))), new ColSpec(left.part()), new ColSpec(inst)),
                new ColSpecArray(List.of(new ColSpec(inst)))), new ColSpec(k1, lambda("x", new com.legend.protocol.spec
                .CInteger(1L))));
        String p = w.fresh("up");
        String a = w.fresh("ua");
        ValueSpecification r = call("rename", one.line().rel(), new ColSpec(PERIOD), new ColSpec(p));
        r = call("rename", r, new ColSpec(one.line().at()), new ColSpec(a));
        r = call("extend", call("select", r, new ColSpecArray(List.of(new ColSpec(p), new ColSpec(a),
                new ColSpec(one.value())))), new ColSpec(k2, lambda("x", new com.legend.protocol.spec.CInteger(1L))));
        // the one line under every instrument
        ValueSpecification spread = call("join", lines, r, new EnumValue("meta::pure::functions::relation::JoinKind",
                "INNER"), lambda2("l", "r", call("equal", prop("l", k1), prop("r", k2))));
        ValueSpecification on = call("and", call("equal", prop("l", PERIOD), prop("r", p)),
                call("equal", prop("l", left.part()), prop("r", inst)));
        ValueSpecification met = call("join", left.rel(), call("select", spread, new ColSpecArray(List.of(
                new ColSpec(inst), new ColSpec(p), new ColSpec(a), new ColSpec(one.value())))),
                new EnumValue("meta::pure::functions::relation::JoinKind", "FULL"), lambda2("l", "r", on));
        String part = w.fresh("apart");
        String period = w.fresh("aper");
        String at = w.fresh("aat");
        met = call("extend", met, new ColSpecArray(List.of(
                new ColSpec(part, lambda("x", either(left.part(), inst))),
                new ColSpec(period, lambda("x", either(PERIOD, p))),
                new ColSpec(at, lambda("x", latest(left.at(), a))))));
        List<ColSpec> keep = new ArrayList<>(List.of(new ColSpec(part), new ColSpec(period), new ColSpec(at)));
        carried.forEach(c -> keep.add(new ColSpec(c)));
        keep.add(new ColSpec(one.value()));
        met = call("select", met, new ColSpecArray(keep));
        met = call("rename", met, new ColSpec(part), new ColSpec(left.part()));
        met = call("rename", met, new ColSpec(period), new ColSpec(PERIOD));
        return new Line(met, left.part(), true, at);
    }

    /** The columns of {@code l} an answer keeps: its line, its period, its date, and {@code values}. */
    private static List<ColSpec> columns(Line l, List<String> values) {
        List<ColSpec> out = new ArrayList<>(List.of(new ColSpec(l.part()), new ColSpec(PERIOD), new ColSpec(l.at())));
        values.forEach(v -> out.add(new ColSpec(v)));
        return out;
    }

    /** {@code x}'s value where it has one, else {@code y}'s. */
    private static ValueSpecification either(String x, String y) {
        return call("if", call("isEmpty", col(x)), thunk(col(y)), thunk(col(x)));
    }

    /** The later of {@code x} and {@code y}, either where the other has none. */
    private static ValueSpecification latest(String x, String y) {
        return call("if", call("isEmpty", col(x)), thunk(col(y)), thunk(call("if", call("isEmpty", col(y)),
                thunk(col(x)), thunk(call("if", call("greaterThanEqual", one(x), one(y)), thunk(col(x)),
                        thunk(col(y)))))));
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
