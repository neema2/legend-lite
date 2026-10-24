// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.sql;

import static com.legend.protocol.sql.SqlToPure.*;
import static com.legend.protocol.sql.PostgresOperators.*;
import static com.legend.protocol.sql.PostgresCasts.*;
import static com.legend.protocol.sql.PostgresDates.*;
import static com.legend.protocol.sql.PostgresJsonb.*;

import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.CBoolean;
import com.legend.protocol.spec.CDecimal;
import com.legend.protocol.spec.CInteger;
import com.legend.protocol.spec.CString;
import com.legend.protocol.spec.EnumValue;
import com.legend.protocol.spec.ValueSpecification;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Postgres's scalar function catalog as the {@code #SQL{...}#} island reads it: each function by its name in
 * Postgres, with its translation ({@link PostgresDialect#function}; moved out of {@link SqlToPure} with the dialect
 * program's D11, 2026-10-18).
 */
final class PostgresFunctions {

    private PostgresFunctions() {
    }

    /** A Postgres function's translation: from its call, its arguments' translations, and the scope it is in. */
    @FunctionalInterface
    private interface Translation {
        Tx apply(SqlToPure t, SqlAst.FunctionCall f, List<Tx> args, Scope scope, String row);
    }

    /** The catalog's function {@code name} ({@link #FUNCTIONS}), as {@link PostgresDialect} supplies it; null for
     *  none. */
    static InputDialect.@com.legend.base.Nullable FunctionRule rule(String name) {
        Translation translation = FUNCTIONS.get(name);
        return translation == null ? null
                : (f, args, at) -> translation.apply(at.translator(), f, args, at.scope(), at.row());
    }

    /** The catalog: Postgres's scalar functions this step reads, by their names in Postgres, each with its
     *  translation (a name it does not hold is refused as not supported yet). */
    private static final Map<String, Translation> FUNCTIONS = functions();

    private static Map<String, Translation> functions() {
        Map<String, Translation> m = new HashMap<>();
        m.put("upper", (t, f, a, s, r) -> text1(f, a, "toUpper", Kind.STRING));
        m.put("lower", (t, f, a, s, r) -> text1(f, a, "toLower", Kind.STRING));
        for (String length : List.of("length", "char_length", "character_length")) {
            m.put(length, (t, f, a, s, r) -> text1(f, a, "length", Kind.INTEGER));
        }
        // the spaces at an end (Postgres's default characters to trim, named or not): Pure's trims; other characters
        // to trim are not supported yet
        String[][] trims = {{"trim", "trim"}, {"btrim", "trim"}, {"ltrim", "ltrim"}, {"rtrim", "rtrim"}};
        for (String[] trim : trims) {
            String pure = trim[1];
            m.put(trim[0], (t, f, a, s, r) -> {
                if (a.size() == 2) {
                    if (!(f.args().get(1) instanceof SqlAst.StringLit chars && chars.value().equals(" "))) {
                        throw SqlTranslationException.unsupported(functionName(f.name().get(0)) + " with characters to"
                                + " trim other than ' ' is not supported yet", f.args().get(1).pos());
                    }
                    return text1(f, a.subList(0, 1), pure, Kind.STRING);
                }
                return text1(f, a, pure, Kind.STRING);
            });
        }
        m.put("reverse", (t, f, a, s, r) -> text1(f, a, "reverseString", Kind.STRING));
        m.put("ascii", (t, f, a, s, r) -> text1(f, a, "ascii", Kind.INTEGER));
        m.put("abs", (t, f, a, s, r) -> {
            if (a.size() == 1 && a.get(0).kind() == Kind.INTEGER && narrowValue(t, f.args().get(0), a.get(0), s)) {
                throw SqlTranslationException.unsupported("abs of a smallint value is not supported yet: the database"
                        + " computes it as a smallint, raising for -32768, where the island's integer is wider",
                        f.pos());
            }
            return numeric1(f, a, "abs");
        });
        m.put("ceil", (t, f, a, s, r) -> numeric1(f, a, "ceiling"));
        m.put("ceiling", (t, f, a, s, r) -> numeric1(f, a, "ceiling"));
        m.put("floor", (t, f, a, s, r) -> numeric1(f, a, "floor"));
        m.put("coalesce", (t, f, a, s, r) -> coalesce(t, f, a));
        m.put("current_date", (t, f, a, s, r) -> nullary(f, a, "today", Kind.DATE));
        for (String now : List.of("current_timestamp", "now", "localtimestamp")) {
            m.put(now, (t, f, a, s, r) -> nullary(f, a, "now", Kind.DATETIME));
        }
        // Postgres's functions of a double precision and Pure's (Postgres's log is base 10; log(b, x) below)
        String[][] floats = {{"sqrt", "sqrt"}, {"exp", "exp"}, {"ln", "log"}, {"log", "log10"}, {"cbrt", "cbrt"},
                {"sin", "sin"}, {"cos", "cos"}, {"tan", "tan"}, {"asin", "asin"}, {"acos", "acos"}, {"atan", "atan"},
                {"cot", "cot"}, {"sinh", "sinh"}, {"cosh", "cosh"}, {"tanh", "tanh"}, {"degrees", "toDegrees"},
                {"radians", "toRadians"}};
        for (String[] fl : floats) {
            String pure = fl[1];
            // sqrt, exp, ln and log have numeric forms in Postgres, which a numeric reads (an integer reads the
            // double precision one)
            boolean numericForm = Set.of("sqrt", "exp", "ln", "log").contains(fl[0]);
            m.put(fl[0], (t, f, a, s, r) -> {
                arity(f, a, 1);
                numericOperand(a.get(0), f.args().get(0));
                Kind k = a.get(0).kind();
                return strict(k == Kind.INEXACT || numericForm && (exactNumeric(k) || k == Kind.WHOLE) ? Kind.INEXACT
                        : Kind.FLOAT, a,
                        v -> call(pure, v.get(0)));
            });
        }
        Translation log10 = java.util.Objects.requireNonNull(m.get("log"));
        m.put("log", (t, f, a, s, r) -> a.size() == 2 ? logBase(f, a) : log10.apply(t, f, a, s, r));
        m.put("atan2", (t, f, a, s, r) -> {
            arity(f, a, 2);
            numericOperand(a.get(0), f.args().get(0));
            numericOperand(a.get(1), f.args().get(1));
            return strict(a.get(0).kind() == Kind.INEXACT || a.get(1).kind() == Kind.INEXACT ? Kind.INEXACT
                    : Kind.FLOAT, a, v -> call("atan2", v.get(0), v.get(1)));
        });
        m.put("pi", (t, f, a, s, r) -> nullary(f, a, "pi", Kind.FLOAT));
        m.put("sign", (t, f, a, s, r) -> {
            arity(f, a, 1);
            numericOperand(a.get(0), f.args().get(0));
            inexact(a.get(0), "sign", f.pos());
            return a.get(0).kind() == Kind.INTEGER ? strict(Kind.FLOAT, a, v -> call("toFloat", call("sign", v.get(0))))
                    : wholeNumber(a.get(0), v -> call("sign", v));
        });
        m.put("mod", (t, f, a, s, r) -> {
            arity(f, a, 2);
            numericOperand(a.get(0), f.args().get(0));
            numericOperand(a.get(1), f.args().get(1));
            return remainder(a.get(0), a.get(1), "function mod(" + postgresName(a.get(0).kind()) + ", "
                    + postgresName(a.get(1).kind()) + ") does not exist", "mod(...)", f.pos());
        });
        m.put("log10", (t, f, a, s, r) -> log10.apply(t, f, a, s, r));
        m.put("round", (t, f, a, s, r) -> round(f, a));
        m.put("div", (t, f, a, s, r) -> div(f, a));
        m.put("width_bucket", (t, f, a, s, r) -> widthBucket(f, a));
        // jsonb (a Variant, register S49): json's functions read it as jsonb's do
        for (String family : List.of("json", "jsonb")) {
            // an array's length is refused: Pure's toMany(@Variant) drops an array's null elements on DuckDB, and
            // reads a value that is not an array (Postgres's error) as an empty list on Postgres
            m.put(family + "_array_length", (t, f, a, s, r) -> {
                throw SqlTranslationException.unsupported(functionName(f.name().get(0)) + "(...) is not supported"
                        + " yet", f.pos());
            });
            m.put(family + "_extract_path", (t, f, a, s, r) -> extractPath(f, a, false));
            m.put(family + "_extract_path_text", (t, f, a, s, r) -> extractPath(f, a, true));
            m.put("to_" + family, (t, f, a, s, r) -> toJson(f, a));
        }
        m.put("nullif", (t, f, a, s, r) -> nullif(t, f, s, r));
        m.put("concat", (t, f, a, s, r) -> concat(f, a));
        m.put("concat_ws", (t, f, a, s, r) -> concatWs(f, a));
        m.put("format", (t, f, a, s, r) -> format(f, a));
        m.put("overlay", (t, f, a, s, r) -> overlay(f, a));
        m.put("quote_literal", (t, f, a, s, r) -> quoteLiteral(f, a, false));
        m.put("quote_nullable", (t, f, a, s, r) -> quoteLiteral(f, a, true));
        m.put("translate", (t, f, a, s, r) -> translate(f, a));
        m.put("split_part", (t, f, a, s, r) -> splitPart(f, a));
        m.put("greatest", (t, f, a, s, r) -> extreme(f, a, true));
        m.put("least", (t, f, a, s, r) -> extreme(f, a, false));
        m.put("num_nulls", (t, f, a, s, r) -> countNulls(f, a, true));
        m.put("num_nonnulls", (t, f, a, s, r) -> countNulls(f, a, false));
        m.put("md5", (t, f, a, s, r) -> {
            kinds(f, a, Kind.STRING);
            return strict(Kind.STRING, a, v -> call("hash", v.get(0), new EnumValue(HASH_TYPE, "MD5")));
        });
        m.put("chr", (t, f, a, s, r) -> {
            kinds(f, a, Kind.INTEGER);
            return strict(Kind.STRING, a, v -> call("char", v.get(0)));
        });
        for (String side : List.of("left", "right")) {
            m.put(side, (t, f, a, s, r) -> {
                kinds(f, a, Kind.STRING, Kind.INTEGER);
                // a negative count leaves that many off the other end, in Postgres and in Pure's relational left and
                // right alike (measured on DuckDB and Postgres, 2026-10-12)
                return strict(Kind.STRING, a, v -> call(side, v.get(0), v.get(1)));
            });
        }
        for (String pad : List.of("lpad", "rpad")) {
            m.put(pad, (t, f, a, s, r) -> {
                if (a.size() == 3) {
                    kinds(f, a, Kind.STRING, Kind.INTEGER, Kind.STRING);
                } else {
                    kinds(f, a, Kind.STRING, Kind.INTEGER);
                }
                nonNegative(f, 1);
                return strict(Kind.STRING, a, v -> call(pad, v));
            });
        }
        m.put("replace", (t, f, a, s, r) -> {
            kinds(f, a, Kind.STRING, Kind.STRING, Kind.STRING);
            return strict(Kind.STRING, a, v -> call("replace", v));
        });
        m.put("repeat", (t, f, a, s, r) -> {
            kinds(f, a, Kind.STRING, Kind.INTEGER);
            nonNegative(f, 1);
            return strict(Kind.STRING, a, v -> call("repeatString", v));
        });
        for (String position : List.of("strpos", "position")) {
            m.put(position, (t, f, a, s, r) -> {
                // the RELATIONAL indexOf is strpos: 1-based, 0 when not found, 1 for '' -- legend-engine's relational
                // lowering (locate()), the acceptance surface; platform Pure's is 0-based, a divergence PCT ledgers
                // (Test_LegendLite_EssentialFunctions_PCT), and the parity corpus pins the relational one here
                kinds(f, a, Kind.STRING, Kind.STRING);
                return strict(Kind.INTEGER, a, v -> call("indexOf", v.get(0), v.get(1)));
            });
        }
        for (String sub : List.of("substring", "substr")) {
            m.put(sub, (t, f, a, s, r) -> substring(f, a));
        }
        m.put("trunc", (t, f, a, s, r) -> trunc(f, a));
        for (String power : List.of("power", "pow")) {
            m.put(power, (t, f, a, s, r) -> {
                arity(f, a, 2);
                List<Tx> lr = typedOperands(a.get(0), a.get(1), ",", f.pos());
                numericOperand(lr.get(0), f.args().get(0));
                numericOperand(lr.get(1), f.args().get(1));
                // power of a numeric is numeric in Postgres, exact; of integers and doubles, double precision
                if (exactNumeric(lr.get(0).kind()) || exactNumeric(lr.get(1).kind())) {
                    return numericPower(t, f.args().get(0), f.args().get(1), lr, functionName(f.name().get(0))
                            + "(...)", f.pos(), s, r);
                }
                return strict(powerKind(lr.get(0).kind(), lr.get(1).kind()), lr, v -> call("pow", v.get(0),
                        v.get(1)));
            });
        }
        m.put("make_date", (t, f, a, s, r) -> {
            kinds(f, a, Kind.INTEGER, Kind.INTEGER, Kind.INTEGER);
            return strict(Kind.DATE, a, v -> call("date", v.get(0), v.get(1), v.get(2)));
        });
        m.put("make_timestamp", (t, f, a, s, r) -> {
            arity(f, a, 6);
            for (int i = 0; i < 6; i++) {
                if (i < 5 ? a.get(i).kind() != Kind.INTEGER && a.get(i).kind() != Kind.NULL
                        : !a.get(i).kind().numeric() && a.get(i).kind() != Kind.NULL) {
                    throw SqlTranslationException.unsupported("make_timestamp of other than five integers and a"
                            + " number of seconds is not supported yet", f.args().get(i).pos());
                }
            }
            return strict(Kind.DATETIME, a, v -> call("date", v.get(0), v.get(1), v.get(2), v.get(3), v.get(4),
                    v.get(5)));
        });
        m.put("date_trunc", (t, f, a, s, r) -> dateTrunc(f, a));
        m.put("isfinite", (t, f, a, s, r) -> {
            arity(f, a, 1);
            if (isNullConstant(a.get(0))) {
                throw SqlTranslationException.invalid("function isfinite(unknown) is not unique", f.pos());
            }
            if (!isDate(a.get(0).kind())) {
                throw SqlTranslationException.invalid("function isfinite(" + postgresName(a.get(0).kind()) + ") does"
                        + " not exist", f.pos());
            }
            // no value here is infinite (Postgres's infinity constants are refused)
            return strictTest(a, v -> new CBoolean(true));
        });
        m.put("starts_with", (t, f, a, s, r) -> {
            kinds(f, a, Kind.STRING, Kind.STRING);
            return strictTest(a, v -> call("startsWith", v.get(0), v.get(1)));
        });
        m.put("extract", (t, f, a, s, r) -> datePart(f, a, true));
        m.put("date_part", (t, f, a, s, r) -> datePart(f, a, false));
        return java.util.Collections.unmodifiableMap(m);
    }

    private static Tx text1(SqlAst.FunctionCall f, List<Tx> args, String pure, Kind result) {
        arity(f, args, 1);
        if (args.get(0).kind() == Kind.NULL) {
            // Postgres reads the NULL as text: the function's NULL
            return nullOf(result);
        }
        if (args.get(0).kind() != Kind.STRING) {
            throw SqlTranslationException.invalid("function " + functionName(f.name().get(0)) + " needs a text"
                    + " value", f.pos());
        }
        return strict(result, args, v -> call(pure, v.get(0)));
    }

    private static Tx numeric1(SqlAst.FunctionCall f, List<Tx> args, String pure) {
        arity(f, args, 1);
        numericOperand(args.get(0), f.args().get(0));
        if (args.get(0).kind() == Kind.NULL) {
            return nullOf(Kind.FLOAT);
        }
        if (pure.equals("abs")) {
            return strict(args.get(0).kind(), args, v -> call(pure, v.get(0)));
        }
        inexact(args.get(0), functionName(f.name().get(0)), f.pos());
        return wholeNumber(args.get(0), v -> call(pure, v));
    }

    /** A whole number Postgres computes in its argument's type, as ceil, floor, sign and round(x) do: numeric for a
     *  numeric, double precision for a double -- and for an integer, which Postgres reads as a double (the preferred
     *  numeric type). Its kind is that type, so that what reads it next (a division above all) reads what Postgres
     *  does: {@code ceil(x) / 4} divides exactly. A double is converted (toFloat); a numeric's whole number stays
     *  Pure's Integer, an exact number either way (a toDecimal of it is a 38-digit decimal of scale 0, which DuckDB's
     *  CASE then imposes on a branch's decimals). */
    private static Tx wholeNumber(Tx x, java.util.function.UnaryOperator<ValueSpecification> pure) {
        if (x.kind() == Kind.NULL) {
            return nullOf(Kind.FLOAT);
        }
        if (x.kind() == Kind.INTEGER || x.kind() == Kind.FLOAT) {
            return strict(Kind.FLOAT, List.of(x), v -> call("toFloat", x.kind() == Kind.INTEGER ? v.get(0)
                    : pure.apply(v.get(0))));
        }
        return strict(Kind.DECIMAL, List.of(x), v -> pure.apply(v.get(0)));
    }

    private static Tx nullary(SqlAst.FunctionCall f, List<Tx> args, String pure, Kind result) {
        arity(f, args, 0);
        return Tx.value(call(pure), result, false);
    }

    /** {@code coalesce(a, b, ...)}: the first value that is not NULL. Pure's takes two to four values, so a longer
     *  list nests from the right. */
    private static Tx coalesce(SqlToPure translator, SqlAst.FunctionCall f, List<Tx> arguments) {
        if (arguments.size() < 2) {
            throw SqlTranslationException.unsupported("coalesce takes two or more arguments here", f.pos());
        }
        Common common = translator.dialect.unify("COALESCE", f.args(), arguments, f.pos());
        List<Tx> args = common.values();
        Kind kind = common.kind();
        if (kind == Kind.NULL) {
            return nullOf(Kind.STRING);
        }
        // the arguments after the first that is never NULL are never read (Postgres evaluates none of them): it is
        // the last, so the result is one value where Pure types it (by its last argument)
        for (int i = 0; i < args.size() - 1; i++) {
            if (!args.get(i).nullable()) {
                args = args.subList(0, i + 1);
                break;
            }
        }
        if (args.size() == 1) {
            Tx only = args.get(0);
            if (kind == Kind.FLOAT && only.kind() != Kind.FLOAT && only.kind() != Kind.INEXACT) {
                // the double of an exact number (coalesce(0.28, x::float8) is the double 0.28)
                return strict(Kind.FLOAT, List.of(only), v -> call("toFloat", v.get(0)));
            }
            return kind == Kind.BOOLEAN || only.kind() == kind ? only : Tx.value(valueOf(only), kind, only.nullable());
        }
        boolean nullable = true;
        for (Tx a : args) {
            nullable &= a.nullable();
        }
        ValueSpecification tail = valueOf(args.get(args.size() - 1));
        int i = args.size() - 1;
        while (i > 0) {
            int take = Math.min(3, i);
            List<ValueSpecification> group = new ArrayList<>();
            for (int k = i - take; k < i; k++) {
                group.add(valueOf(args.get(k)));
            }
            group.add(tail);
            tail = call("coalesce", group);
            i -= take;
        }
        return kind == Kind.BOOLEAN ? booleanValue(tail, nullable) : Tx.value(tail, kind, nullable);
    }

    private static final String HASH_TYPE = "meta::pure::functions::hash::HashType";

    private static final Set<String> TIME_FIELDS = Set.of("hour", "hours", "minute", "minutes");

    /** The date fields that are whole numbers, and Pure's functions for them; Postgres's seconds and epochs carry
     *  fractions. */
    private static final Map<String, String> DATE_FIELDS = Map.ofEntries(Map.entry("year", "year"),
            Map.entry("years", "year"), Map.entry("quarter", "quarterNumber"), Map.entry("month", "monthNumber"),
            Map.entry("months", "monthNumber"), Map.entry("day", "dayOfMonth"), Map.entry("days", "dayOfMonth"),
            Map.entry("doy", "dayOfYear"), Map.entry("isodow", "dayOfWeekNumber"), Map.entry("hour", "hour"),
            Map.entry("hours", "hour"), Map.entry("minute", "minute"), Map.entry("minutes", "minute"));

    /** The date fields computed from Pure's: Postgres's day of the week from Sunday (0), from ISO's (Sunday 7); its
     *  ISO week, as Pure's weekOfYear is (measured); a year's decade (year / 10), century ((year + 99) / 100) and
     *  millennium ((year + 999) / 1000), Postgres's for years from 1; a day's epoch, its seconds since 1970-01-01. */
    private static final Map<String, java.util.function.UnaryOperator<ValueSpecification>> COMPUTED_FIELDS =
            computedFields();

    private static Map<String, java.util.function.UnaryOperator<ValueSpecification>> computedFields() {
        Map<String, java.util.function.UnaryOperator<ValueSpecification>> m = new LinkedHashMap<>();
        m.put("dow", v -> call("rem", call("dayOfWeekNumber", v), new CInteger(7L)));
        for (String week : List.of("week", "weeks")) {
            m.put(week, v -> call("weekOfYear", v));
        }
        for (String decade : List.of("decade", "decades")) {
            m.put(decade, v -> yearsDivided(v, 0, 10));
        }
        for (String century : List.of("century", "centuries")) {
            m.put(century, v -> yearsDivided(v, 99, 100));
        }
        for (String millennium : List.of("millennium", "millennia")) {
            m.put(millennium, v -> yearsDivided(v, 999, 1000));
        }
        m.put("epoch", v -> call("toEpochValue", v));
        return java.util.Collections.unmodifiableMap(m);
    }

    /** {@code (year + offset) / by}, truncated (a year here is at least 1): exact, the quotient's double far from the
     *  next whole number. */
    private static ValueSpecification yearsDivided(ValueSpecification v, long offset, long by) {
        ValueSpecification year = offset == 0 ? call("year", v) : AppliedFunction.infixRun("plus", List.of(
                call("year", v), new CInteger(offset)));
        return call("floor", call("divide", year, new CInteger(by)));
    }

    /** The arguments' kinds, as Postgres resolves the function: a NULL constant makes it NULL (not supported here); a
     *  value of another kind has no such function. */
    private static void kinds(SqlAst.FunctionCall f, List<Tx> args, Kind... expected) {
        arity(f, args, expected.length);
        for (int i = 0; i < expected.length; i++) {
            Kind k = args.get(i).kind();
            if (k == Kind.NULL) {
                // Postgres reads the NULL as the argument's type: strict makes the function NULL
                continue;
            }
            if (k != expected[i]) {
                throw SqlTranslationException.invalid("function " + functionName(f.name().get(0)) + " needs "
                        + expected[i].name().toLowerCase(Locale.ROOT) + " argument " + (i + 1) + ", not "
                        + k.name().toLowerCase(Locale.ROOT), f.args().get(i).pos());
            }
        }
    }

    /** Argument {@code i} as an integer constant of at least 0: a negative one means something else in Postgres. */
    private static void nonNegative(SqlAst.FunctionCall f, int i) {
        constantAtLeast(f, i, 0);
    }

    private static void constantAtLeast(SqlAst.FunctionCall f, int i, int min) {
        if (!(f.args().get(i) instanceof SqlAst.IntegerLit n) || n.value().compareTo(
                java.math.BigInteger.valueOf(min)) < 0) {
            throw SqlTranslationException.unsupported(functionName(f.name().get(0)) + "(...) with argument " + (i + 1)
                    + " other than an integer constant of at least " + min + " is not supported yet",
                    f.args().get(i).pos());
        }
    }

    /** {@code round(x)} (and {@code round(x, 0)}) of an exact number: Postgres's numeric round, half away from
     *  zero. Pure's round is half to even, so this is {@link #halfAwayFromZero}. A double precision rounds half to
     *  even in Postgres; {@code round(x, n)} for another n has no Pure spelling here yet: both refused. */
    /** {@code substring(s, start [, length])}, {@code substr}: the characters from {@code start} (1-based) on, {@code
     *  length} of them -- Pure's relational substring, SQL's substring(start, length). A start before 1 still counts
     *  its places in Postgres ({@code substring('hello', 0, 3)} is {@code 'he'}): constants are moved to start at 1.
     *  A negative length is Postgres's error. */
    private static Tx substring(SqlAst.FunctionCall f, List<Tx> a) {
        if (a.size() == 2 && f.args().get(1) instanceof SqlAst.StringLit) {
            throw SqlTranslationException.unsupported("substring(... FROM pattern) (a regular expression) is not"
                    + " supported yet", f.pos());
        }
        if (a.size() == 2) {
            kinds(f, a, Kind.STRING, Kind.INTEGER);
            return strict(Kind.STRING, a, v -> call("substring", v.get(0), v.get(1)));
        }
        kinds(f, a, Kind.STRING, Kind.INTEGER, Kind.INTEGER);
        for (Tx x : a) {
            if (isNullConstant(x)) {
                return nullOf(Kind.STRING);
            }
        }
        Long start = constantInteger(f.args().get(1));
        Long length = constantInteger(f.args().get(2));
        if (start == null || length == null) {
            throw SqlTranslationException.unsupported(functionName(f.name().get(0)) + " with a start or a length other"
                    + " than a constant is not supported yet", f.pos());
        }
        if (length < 0) {
            throw SqlTranslationException.invalid("negative substring length not allowed", f.args().get(2).pos());
        }
        long from = Math.max(start, 1);
        long count = Math.max(start + length - from, 0);
        return strict(Kind.STRING, List.of(a.get(0)), v -> call("substring", v.get(0),
                integer(java.math.BigInteger.valueOf(from)), integer(java.math.BigInteger.valueOf(count))));
    }

    /** An integer constant ({@code 3}, {@code -3}), or null. */
    private static @com.legend.base.Nullable Long constantInteger(SqlAst.Expr e) {
        java.math.BigInteger k = e instanceof SqlAst.IntegerLit n ? n.value()
                : e instanceof SqlAst.Unary u && u.op() == SqlAst.UnaryOp.MINUS
                        && u.operand() instanceof SqlAst.IntegerLit n ? n.value().negate() : null;
        return k == null || k.bitLength() >= 63 ? null : k.longValue();
    }

    /** {@code x} to {@code places} decimal places (negative: to tens, hundreds...), exactly: the scaled value rounded
     *  half away from zero (Postgres's numeric round) or truncated (trunc), then scaled back by a decimal constant --
     *  decimal multiplication, exact on every database (a divide would go through a double). */
    static ValueSpecification toPlaces(ValueSpecification x, long places, boolean halfAway) {
        BigDecimal up = BigDecimal.ONE.scaleByPowerOfTen((int) places);
        ValueSpecification scaled = places == 0 ? x : places > 0
                ? AppliedFunction.infixRun("times", List.of(x, integer(up.toBigIntegerExact())))
                : AppliedFunction.infixRun("times", List.of(x, new CDecimal(up)));
        ValueSpecification whole = halfAway ? halfAwayFromZero(scaled) : towardZero(scaled);
        BigDecimal down = BigDecimal.ONE.scaleByPowerOfTen((int) -places);
        return places == 0 ? whole : places > 0
                ? AppliedFunction.infixRun("times", List.of(whole, new CDecimal(down)))
                : AppliedFunction.infixRun("times", List.of(whole, integer(down.toBigIntegerExact())));
    }

    /** The whole number toward zero. */
    private static ValueSpecification towardZero(ValueSpecification x) {
        return call("if", call("greaterThanEqual", x, new CInteger(0L)), thunk(call("floor", x)),
                thunk(call("ceiling", x)));
    }

    /** {@code trunc(x [, places])}: toward zero -- numeric for a numeric (exact), double precision otherwise. */
    private static Tx trunc(SqlAst.FunctionCall f, List<Tx> args) {
        if (args.size() != 2) {
            arity(f, args, 1);
        }
        Tx x = args.get(0);
        numericOperand(x, f.args().get(0));
        if (args.size() == 1) {
            inexact(x, "trunc(x)", f.pos());
            if (x.kind() == Kind.NULL) {
                // trunc has numeric, double precision and macaddr forms: a NULL is no one of them
                throw SqlTranslationException.invalid("function trunc(unknown) is not unique", f.pos());
            }
            return wholeNumber(x, PostgresFunctions::towardZero);
        }
        return places(f, args, false);
    }

    /** round(x, n) or trunc(x, n): a numeric (an integer read as one) to a constant number of places, exactly. */
    private static Tx places(SqlAst.FunctionCall f, List<Tx> args, boolean halfAway) {
        Tx x = args.get(0);
        String fn = functionName(f.name().get(0));
        if (isNullConstant(x) || isNullConstant(args.get(1))) {
            return nullOf(Kind.DECIMAL);
        }
        if (x.kind() == Kind.FLOAT) {
            throw SqlTranslationException.invalid("function " + fn + "(double precision, "
                    + postgresName(args.get(1).kind()) + ") does not exist", f.pos());
        }
        inexact(x, fn + "(x, n)", f.pos());
        Long n = constantInteger(f.args().get(1));
        if (n == null || Math.abs(n) > 18) {
            throw SqlTranslationException.unsupported(fn + "(x, n) with n other than a constant (of at most 18) is not"
                    + " supported yet", f.args().get(1).pos());
        }
        long places = n;
        // an integer is read as Postgres's numeric first: n places of it (round(42, 1) is 42.0), and no overflow of
        // its own width in the scaling
        boolean whole = x.kind() == Kind.INTEGER || x.kind() == Kind.WHOLE;
        return strict(Kind.DECIMAL, List.of(x), v -> toPlaces(whole ? call("toDecimal", v.get(0)) : v.get(0), places,
                halfAway));
    }

    /** Refuses what rounds or spells an inexact numeric (see {@link Kind}): it may fall either side of Postgres's
     *  exact value, and a rounding or a spelling of it then differs from Postgres's. */
    static void inexact(Tx x, String what, SqlAst.Pos pos) {
        if (x.kind() == Kind.INEXACT) {
            throw inexactRefused(what, pos);
        }
    }

    private static SqlTranslationException inexactRefused(String what, SqlAst.Pos pos) {
        return SqlTranslationException.unsupported(what + " of a quotient, average or root of numerics (exact in"
                + " Postgres, a double here) is not supported yet", pos);
    }

    private static Tx round(SqlAst.FunctionCall f, List<Tx> args) {
        if (args.size() == 2) {
            numericOperand(args.get(0), f.args().get(0));
            // Postgres's numeric round: half away from zero (Pure's round(Decimal, n) breaks ties to even)
            return places(f, args, true);
        }
        arity(f, args, 1);
        Tx x = args.get(0);
        numericOperand(x, f.args().get(0));
        inexact(x, "round(x)", f.pos());
        if (x.kind() == Kind.FLOAT) {
            // Postgres's round of a double precision is rint, half to even, as Pure's round of a Float is (measured)
            return strict(Kind.FLOAT, List.of(x), v -> call("toFloat", call("round", v.get(0))));
        }
        if (x.kind() == Kind.INTEGER) {
            // round(int) is round(double precision) in Postgres: the value itself, a double
            return strict(Kind.FLOAT, List.of(x), v -> call("toFloat", v.get(0)));
        }
        return wholeNumber(x, PostgresFunctions::halfAwayFromZero);
    }

    /** The nearest integer, a tie away from zero (Postgres's numeric rounding; Pure's round breaks ties to even). */
    static ValueSpecification halfAwayFromZero(ValueSpecification x) {
        return call("if", call("greaterThanEqual", x, new CInteger(0L)),
                thunk(call("floor", AppliedFunction.infixRun("plus", List.of(x, new CDecimal(new BigDecimal("0.5")))))),
                thunk(call("ceiling", AppliedFunction.infixRun("minus",
                        List.of(x, new CDecimal(new BigDecimal("0.5")))))));
    }

    /** {@code nullif(a, b)}: NULL when {@code a = b} is TRUE, else {@code a}. */
    private static Tx nullif(SqlToPure translator, SqlAst.FunctionCall f, Scope scope, String row) {
        if (f.args().size() != 2) {
            throw SqlTranslationException.invalid("function nullif takes two arguments", f.pos());
        }
        List<Tx> sides = translator.comparable(f.args().get(0), f.args().get(1), scope, row, f.pos());
        Tx a = sides.get(0);
        Tx equal = compare(SqlAst.BinaryOp.EQ, a, sides.get(1));
        ValueSpecification v = call("if", equal.vs(), thunk(empty()), thunk(valueOf(a)));
        return a.kind() == Kind.BOOLEAN ? booleanValue(v, true) : Tx.value(v, a.kind(), true);
    }

    /** {@code concat(a, b, ...)}: the values end to end, a NULL as nothing; never NULL. Text values only so far
     *  (Postgres writes any other value by its type's output rules). */
    private static Tx concat(SqlAst.FunctionCall f, List<Tx> args) {
        if (args.isEmpty()) {
            throw SqlTranslationException.invalid("function concat needs an argument", f.pos());
        }
        List<ValueSpecification> parts = new ArrayList<>();
        for (int i = 0; i < args.size(); i++) {
            Tx a = args.get(i);
            if (a.kind() == Kind.NULL) {
                continue;
            }
            ValueSpecification v = asText(a, f.args().get(i).pos(), "concat(...)");
            parts.add(a.nullable() ? call("if", call("isEmpty", v), thunk(new CString("")), thunk(call("toOne", v)))
                    : v);
        }
        ValueSpecification all = parts.isEmpty() ? new CString("") : parts.size() == 1 ? parts.get(0)
                : AppliedFunction.infixRun("plus", parts);
        return Tx.value(all, Kind.STRING, false);
    }

    /** A value as concat writes it, by Postgres's output function: an integer's digits, t or f, a date's ISO spelling,
     *  a numeric's digits with its scale -- Pure's toString writes each so on every database (judged), the boolean
     *  spelled here. A double's text (Postgres's shortest that reads back), a timestamp's and others are not yet. */
    static ValueSpecification asText(Tx a, SqlAst.Pos pos, String where) {
        return switch (a.kind()) {
            case STRING -> valueOf(a);
            // concat writes a boolean with its output function, t or f (|| casts it: true, false)
            case BOOLEAN -> {
                ValueSpecification v = valueOf(a);
                ValueSpecification tf = call("if", a.nullable() ? call("toOne", v) : v, thunk(new CString("t")),
                        thunk(new CString("f")));
                yield a.nullable() ? call("if", call("isEmpty", v), thunk(empty()), thunk(tf)) : tf;
            }
            case INTEGER, WHOLE, DATE, DECIMAL -> {
                ValueSpecification v = valueOf(a);
                yield a.nullable() ? call("if", call("isEmpty", v), thunk(empty()), thunk(call("toString",
                        call("toOne", v)))) : call("toString", v);
            }
            case INEXACT -> throw inexactRefused(where, pos);
            default -> throw SqlTranslationException.unsupported(where + " of " + postgresName(a.kind())
                    + " values is not supported yet", pos);
        };
    }

    /** {@code concat_ws(sep, a, b, ...)}: the values that are not NULL, each written as concat writes it, with {@code
     *  sep} between them; NULL only where {@code sep} is. Each value present is written after a separator, and the
     *  first separator taken off (after a first value that is never NULL, none is). */
    private static Tx concatWs(SqlAst.FunctionCall f, List<Tx> args) {
        if (args.size() < 2) {
            throw SqlTranslationException.invalid("function concat_ws needs a separator and a value", f.pos());
        }
        if (args.get(0).kind() != Kind.STRING && args.get(0).kind() != Kind.NULL) {
            throw SqlTranslationException.unsupported("concat_ws(...) with a separator other than text is not"
                    + " supported yet", f.args().get(0).pos());
        }
        List<ValueSpecification> texts = new ArrayList<>();
        List<Boolean> nullable = new ArrayList<>();
        for (int i = 1; i < args.size(); i++) {
            Tx a = args.get(i);
            if (isNullConstant(a)) {
                continue;
            }
            texts.add(asText(a, f.args().get(i).pos(), "concat_ws(...)"));
            nullable.add(a.nullable());
        }
        boolean leading = !texts.isEmpty() && !nullable.get(0);
        return strict(Kind.STRING, List.of(args.get(0)), v -> {
            ValueSpecification sep = v.get(0);
            List<ValueSpecification> parts = new ArrayList<>();
            for (int i = 0; i < texts.size(); i++) {
                ValueSpecification text = texts.get(i);
                if (i == 0 && leading) {
                    parts.add(text);
                } else if (nullable.get(i)) {
                    parts.add(call("if", call("isEmpty", text), thunk(new CString("")), thunk(
                            AppliedFunction.infixRun("plus", List.of(sep, call("toOne", text))))));
                } else {
                    parts.add(AppliedFunction.infixRun("plus", List.of(sep, text)));
                }
            }
            if (parts.isEmpty()) {
                return new CString("");
            }
            ValueSpecification all = parts.size() == 1 ? parts.get(0) : AppliedFunction.infixRun("plus", parts);
            return leading ? all : call("substring", all, AppliedFunction.infixRun("plus", List.of(call("length", sep),
                    new CInteger(1L))));
        });
    }

    /** {@code format(fmt, a, b, ...)} of a constant format: its text, each {@code %s} the next value written as concat
     *  writes it (a NULL as nothing), {@code %%} a percent sign. {@code %I}, {@code %L}, positions and widths are not
     *  supported yet. */
    private static Tx format(SqlAst.FunctionCall f, List<Tx> args) {
        if (args.isEmpty()) {
            throw SqlTranslationException.invalid("function format() needs a format", f.pos());
        }
        if (isNullConstant(args.get(0))) {
            return nullOf(Kind.STRING);
        }
        if (!(f.args().get(0) instanceof SqlAst.StringLit fmt)) {
            throw SqlTranslationException.unsupported("format(...) of a format other than a constant is not supported"
                    + " yet", f.args().get(0).pos());
        }
        String text = fmt.value();
        List<ValueSpecification> parts = new ArrayList<>();
        StringBuilder literal = new StringBuilder();
        int next = 1;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c != '%') {
                literal.append(c);
                continue;
            }
            if (i + 1 == text.length()) {
                throw SqlTranslationException.invalid("unterminated format() type specifier", fmt.pos());
            }
            char spec = text.charAt(++i);
            if (spec == '%') {
                literal.append('%');
                continue;
            }
            if (spec != 's') {
                if (spec == 'I' || spec == 'L' || spec == '-' || spec == '*' || Character.isDigit(spec)) {
                    throw SqlTranslationException.unsupported("format(...) with %" + spec + " is not supported yet",
                            fmt.pos());
                }
                throw SqlTranslationException.invalid("unrecognized format() type specifier \"" + spec + "\"",
                        fmt.pos());
            }
            if (next >= args.size()) {
                throw SqlTranslationException.invalid("too few arguments for format()", f.pos());
            }
            Tx a = args.get(next);
            SqlAst.Pos pos = f.args().get(next).pos();
            next++;
            if (literal.length() > 0) {
                parts.add(new CString(literal.toString()));
                literal.setLength(0);
            }
            if (isNullConstant(a)) {
                continue;
            }
            ValueSpecification v = asText(a, pos, "format(...)");
            parts.add(a.nullable() ? call("if", call("isEmpty", v), thunk(new CString("")), thunk(call("toOne", v)))
                    : v);
        }
        if (literal.length() > 0 || parts.isEmpty()) {
            parts.add(new CString(literal.toString()));
        }
        return Tx.value(parts.size() == 1 ? parts.get(0) : AppliedFunction.infixRun("plus", parts), Kind.STRING,
                false);
    }

    /** {@code overlay(s PLACING r FROM n [FOR m])} of a constant n (and m): s with its m characters from the n-th
     *  (1-based) replaced by r, m being r's length when not given. Postgres's definition: {@code substring(s, 1, n - 1)
     *  || r || substring(s, n + m)}, the second from the first character when n + m is before it; n before 1 is
     *  Postgres's error. */
    private static Tx overlay(SqlAst.FunctionCall f, List<Tx> args) {
        if (args.size() == 4) {
            kinds(f, args, Kind.STRING, Kind.STRING, Kind.INTEGER, Kind.INTEGER);
        } else {
            kinds(f, args, Kind.STRING, Kind.STRING, Kind.INTEGER);
        }
        for (Tx a : args) {
            if (isNullConstant(a)) {
                return nullOf(Kind.STRING);
            }
        }
        Long n = constantInteger(f.args().get(2));
        Long m = args.size() == 4 ? constantInteger(f.args().get(3)) : null;
        if (n == null || args.size() == 4 && m == null || Math.abs(n) > Integer.MAX_VALUE
                || m != null && Math.abs(m) > Integer.MAX_VALUE) {
            throw SqlTranslationException.unsupported("overlay(...) with a start or a count other than a constant is"
                    + " not supported yet", f.pos());
        }
        if (n <= 0) {
            throw SqlTranslationException.invalid("negative substring length not allowed", f.args().get(2).pos());
        }
        return strict(Kind.STRING, args.subList(0, 2), v -> {
            ValueSpecification head = call("substring", v.get(0), new CInteger(1L), new CInteger(n - 1));
            ValueSpecification tail = m == null ? call("substring", v.get(0), AppliedFunction.infixRun("plus",
                    List.of(new CInteger(n), call("length", v.get(1))))) : call("substring", v.get(0),
                    new CInteger(Math.max(n + m, 1)));
            return AppliedFunction.infixRun("plus", List.of(head, v.get(1), tail));
        });
    }

    /** {@code quote_literal(x)}: x's text as a string constant -- in quotes, each quote doubled; with a backslash in
     *  it, Postgres's escape string ({@code E'...'}), each backslash doubled too. NULL for a NULL, which {@code
     *  quote_nullable} writes {@code NULL}. A value other than text is its text, as {@code x::text} writes it. */
    private static Tx quoteLiteral(SqlAst.FunctionCall f, List<Tx> args, boolean orNull) {
        arity(f, args, 1);
        Tx x = args.get(0);
        if (isNullConstant(x)) {
            return orNull ? Tx.value(new CString("NULL"), Kind.STRING, false) : nullOf(Kind.STRING);
        }
        if (x.kind() != Kind.STRING) {
            asText(x, f.args().get(0).pos(), functionName(f.name().get(0)) + "(...)");
        }
        Kind kind = x.kind();
        Tx quoted = strict(Kind.STRING, List.of(x), v -> {
            ValueSpecification text = kind == Kind.STRING ? v.get(0) : call("toString", v.get(0));
            ValueSpecification body = call("replace", call("replace", text, new CString("\\"), new CString("\\\\")),
                    new CString("'"), new CString("''"));
            return AppliedFunction.infixRun("plus", List.of(call("if", call("contains", text, new CString("\\")),
                    thunk(new CString("E'")), thunk(new CString("'"))), body, new CString("'")));
        });
        if (!orNull || !quoted.nullable()) {
            return quoted;
        }
        return Tx.value(call("if", call("isEmpty", quoted.vs()), thunk(new CString("NULL")), thunk(call("toOne",
                quoted.vs()))), Kind.STRING, false);
    }

    /** {@code translate(s, from, to)} of constant {@code from} and {@code to}: each character of s found in from (at
     *  its first place there) is the character at that place in to, or is taken out where to is shorter. Pure's
     *  replace, a character at a time, each replaced before any character that becomes it, so that no replacement is
     *  replaced again; characters that replace one another in a cycle ({@code translate(s, 'ab', 'ba')}) are not
     *  supported yet. */
    private static Tx translate(SqlAst.FunctionCall f, List<Tx> args) {
        kinds(f, args, Kind.STRING, Kind.STRING, Kind.STRING);
        for (Tx a : args) {
            if (isNullConstant(a)) {
                return nullOf(Kind.STRING);
            }
        }
        if (!(f.args().get(1) instanceof SqlAst.StringLit from) || !(f.args().get(2) instanceof SqlAst.StringLit to)) {
            throw SqlTranslationException.unsupported("translate(...) of characters other than constants is not"
                    + " supported yet", f.pos());
        }
        int[] fromChars = from.value().codePoints().toArray();
        int[] toChars = to.value().codePoints().toArray();
        Map<Integer, String> replacement = new LinkedHashMap<>();
        for (int i = 0; i < fromChars.length; i++) {
            replacement.putIfAbsent(fromChars[i], i < toChars.length ? character(toChars[i]) : "");
        }
        replacement.entrySet().removeIf(e -> e.getValue().equals(character(e.getKey())));
        List<Map.Entry<Integer, String>> order = new ArrayList<>();
        Set<Integer> pending = new java.util.LinkedHashSet<>(replacement.keySet());
        while (!pending.isEmpty()) {
            Map.Entry<Integer, String> ready = null;
            for (Map.Entry<Integer, String> e : replacement.entrySet()) {
                if (pending.contains(e.getKey()) && (e.getValue().isEmpty()
                        || !pending.contains(e.getValue().codePointAt(0)))) {
                    ready = e;
                    break;
                }
            }
            if (ready == null) {
                throw SqlTranslationException.unsupported("translate(...) whose characters replace one another in a"
                        + " cycle is not supported yet", f.args().get(1).pos());
            }
            order.add(ready);
            pending.remove(ready.getKey());
        }
        return strict(Kind.STRING, List.of(args.get(0)), v -> {
            ValueSpecification result = v.get(0);
            for (Map.Entry<Integer, String> e : order) {
                result = call("replace", result, new CString(character(e.getKey())),
                        new CString(e.getValue()));
            }
            return result;
        });
    }

    /** The one character {@code codePoint} as text ({@code Character.toString(int)} is not in TeaVM's class
     *  library, which compiles this package for the browser). */
    private static String character(int codePoint) {
        return new String(Character.toChars(codePoint));
    }

    /** {@code split_part(s, delimiter, n)} of a constant delimiter and a constant n from 1 to 5: the n-th field of s,
     *  fields separated by the delimiter ('' past the last). A field is the text before the delimiter's first place
     *  (Pure's relational indexOf, as strpos), the rest after it; an empty delimiter makes s one field. A field
     *  counted from the end (n below 0) is not supported yet; n = 0 is Postgres's error. */
    private static Tx splitPart(SqlAst.FunctionCall f, List<Tx> args) {
        kinds(f, args, Kind.STRING, Kind.STRING, Kind.INTEGER);
        for (Tx a : args) {
            if (isNullConstant(a)) {
                return nullOf(Kind.STRING);
            }
        }
        Long n = constantInteger(f.args().get(2));
        if (!(f.args().get(1) instanceof SqlAst.StringLit d) || n == null) {
            throw SqlTranslationException.unsupported("split_part(...) with a delimiter or a field other than a"
                    + " constant is not supported yet", f.pos());
        }
        if (n == 0) {
            throw SqlTranslationException.invalid("field position must not be zero", f.args().get(2).pos());
        }
        if (n < 0 || n > 5) {
            throw SqlTranslationException.unsupported("split_part(...) of a field other than the first to the fifth is"
                    + " not supported yet", f.args().get(2).pos());
        }
        String delimiter = d.value();
        return strict(Kind.STRING, List.of(args.get(0)), v -> delimiter.isEmpty() ? (n == 1 ? v.get(0)
                : new CString("")) : field(v.get(0), new CString(delimiter), n));
    }

    private static ValueSpecification field(ValueSpecification rest, CString delimiter, long n) {
        ValueSpecification at = call("indexOf", rest, delimiter);
        ValueSpecification none = call("equal", at, new CInteger(0L));
        if (n == 1) {
            return call("if", none, thunk(rest), thunk(call("substring", rest, new CInteger(1L),
                    AppliedFunction.infixRun("minus", List.of(at, new CInteger(1L))))));
        }
        // the delimiter's length in characters, as SQL's substring counts them (not UTF-16's units)
        ValueSpecification after = call("substring", rest, AppliedFunction.infixRun("plus", List.of(at,
                new CInteger(delimiter.value().codePointCount(0, delimiter.value().length())))));
        return call("if", none, thunk(new CString("")), thunk(field(after, delimiter, n - 1)));
    }

    /** {@code a % b}, {@code mod(a, b)}: the remainder, with the dividend's sign -- Pure's rem, exact for integers and
     *  numerics alike (DuckDB's remainder of decimals too, measured), a numeric's scale the larger operand's in both. A
     *  double has none in Postgres. */
    static Tx remainder(Tx l, Tx r, String noSuch, String what, SqlAst.Pos pos) {
        for (Tx x : List.of(l, r)) {
            if (x.kind() == Kind.FLOAT) {
                throw SqlTranslationException.invalid(noSuch, pos);
            }
            inexact(x, what, pos);
        }
        boolean integers = (l.kind() == Kind.INTEGER || l.kind() == Kind.WHOLE || l.kind() == Kind.NULL)
                && (r.kind() == Kind.INTEGER || r.kind() == Kind.WHOLE || r.kind() == Kind.NULL);
        Kind kind = !integers ? (l.kind() == Kind.NUMBER || r.kind() == Kind.NUMBER ? Kind.NUMBER : Kind.DECIMAL)
                : l.kind() == Kind.WHOLE || r.kind() == Kind.WHOLE ? Kind.WHOLE : Kind.INTEGER;
        return strict(kind, List.of(l, r), v -> call("rem", v.get(0), v.get(1)));
    }

    /** {@code div(a, b)}: the quotient truncated toward zero, Postgres's numeric of scale 0 (it has div of numerics
     *  only, an integer read as one). Exact, as integer division is ({@link SqlToPure#integerQuotient}): Pure's rem of
     *  decimals is exact on both databases, with the dividend's sign. */
    private static Tx div(SqlAst.FunctionCall f, List<Tx> args) {
        arity(f, args, 2);
        boolean decimal = false;
        for (int i = 0; i < 2; i++) {
            Tx x = args.get(i);
            numericOperand(x, f.args().get(i));
            if (x.kind() == Kind.FLOAT) {
                throw SqlTranslationException.invalid("function div(" + postgresName(args.get(0).kind()) + ", "
                        + postgresName(args.get(1).kind()) + ") does not exist", f.pos());
            }
            inexact(x, "div(...)", f.pos());
            decimal |= exactNumeric(x.kind());
        }
        boolean decimals = decimal;
        return strict(Kind.DECIMAL, args, v -> decimals ? integerQuotient(call("toDecimal", v.get(0)),
                call("toDecimal", v.get(1))) : integerQuotient(v.get(0), v.get(1)));
    }

    /** {@code log(b, x)}: the logarithm of x to the base b. Postgres has it for numerics only (exact there; an integer
     *  is read as one). Of the constant base 10 it is Pure's log10, as {@code log(x)} is (an inexact numeric, exact
     *  at 10's powers). Another base is refused: ln(x) / ln(b) misses the exact powers that compare equal in Postgres
     *  ({@code log(10, 1000)} by natural logarithms is 2.9999999999999996). */
    private static Tx logBase(SqlAst.FunctionCall f, List<Tx> args) {
        for (int i = 0; i < 2; i++) {
            numericOperand(args.get(i), f.args().get(i));
        }
        if (args.get(0).kind() == Kind.FLOAT || args.get(1).kind() == Kind.FLOAT) {
            throw SqlTranslationException.invalid("function log(" + postgresName(args.get(0).kind()) + ", "
                    + postgresName(args.get(1).kind()) + ") does not exist", f.pos());
        }
        SqlAst.Expr b = f.args().get(0);
        if (b instanceof SqlAst.Cast c && c.type().modifiers().isEmpty() && NUMERIC_TYPES.contains(c.type().name())) {
            b = c.operand();
        }
        BigDecimal base = numberConstant(b);
        if (base == null || base.compareTo(BigDecimal.TEN) != 0) {
            throw SqlTranslationException.unsupported("log(b, x) of a base other than the constant 10 is not supported"
                    + " yet: Postgres's is exact, and a quotient of natural logarithms misses the exact powers",
                    f.args().get(0).pos());
        }
        return strict(Kind.INEXACT, args.subList(1, 2), v -> call("log10", v.get(0)));
    }

    /** The names of Postgres's numeric type. */
    private static final Set<String> NUMERIC_TYPES = Set.of("numeric", "decimal");

    /** {@code power(x, n)} of numerics where x is an integer read as one ({@code x::numeric}, or an integer with a
     *  numeric exponent) and n a whole constant from 0 to 16: Postgres's numeric power, exact for a base of scale 0
     *  (it rounds to a scale no less than the base's). x times itself n times, in decimal arithmetic. Postgres writes
     *  it to 16 places or more, so its text is refused. A base with a fraction is not supported yet: Postgres rounds
     *  its power to about 16 significant digits. */
    static Tx numericPower(SqlToPure translator, SqlAst.Expr base, SqlAst.Expr exponent, List<Tx> lr, String what,
            SqlAst.Pos pos,
            Scope scope, String row) {
        if (isNullConstant(lr.get(0)) || isNullConstant(lr.get(1))) {
            return nullOf(Kind.NUMBER);
        }
        // the integer itself: Pure's toDecimal of it is a decimal of scale 0 (of a decimal, of scale 18)
        Tx integer = lr.get(0).kind() == Kind.INTEGER ? lr.get(0) : base instanceof SqlAst.Cast c
                && c.type().modifiers().isEmpty() && NUMERIC_TYPES.contains(c.type().name())
                ? translator.tx(c.operand(), scope,
                row) : null;
        BigDecimal n = numberConstant(exponent);
        if (integer == null || integer.kind() != Kind.INTEGER || n == null || n.signum() < 0
                || n.compareTo(BigDecimal.valueOf(16)) > 0
                || n.stripTrailingZeros().scale() > 0) {
            throw SqlTranslationException.unsupported(what + " of numeric values other than an integer to a whole"
                    + " power from 0 to 16 is not supported yet (Postgres rounds a numeric's power to about 16"
                    + " significant digits)", pos);
        }
        int times = n.intValue();
        return strict(Kind.NUMBER, List.of(integer), v -> {
            ValueSpecification x = call("toDecimal", v.get(0));
            return times == 0 ? new CDecimal(BigDecimal.ONE) : times == 1 ? x
                    : AppliedFunction.infixRun("times", java.util.Collections.nCopies(times, x));
        });
    }

    /** {@code width_bucket(x, low, high, count)} of constant bounds and a constant count up to 10,000: which of count
     *  equal buckets from low to high holds x (0 before them, count + 1 after). Postgres has it for doubles and for
     *  numerics, read by x's type (an integer with integer bounds reads the double one):
     *  - doubles: Postgres's own steps, {@code count * ((x - low) / (high - low))} truncated, at most count - 1, plus
     *    1 -- the same double operations on every database, the bounds the doubles nearest their text;
     *  - numerics: Postgres divides (x - low) * count by high - low to 16 places or more (rounded), and takes its
     *    floor plus 1. The floor here is exact, from the remainder; within 5e-17 below a bucket's edge (screened at
     *    1e-15 of a bucket), where Postgres's rounded quotient may reach the edge, it is an error. */
    private static Tx widthBucket(SqlAst.FunctionCall f, List<Tx> args) {
        arity(f, args, 4);
        BigDecimal low = numberConstant(f.args().get(1));
        BigDecimal high = numberConstant(f.args().get(2));
        Long count = constantInteger(f.args().get(3));
        if (low == null || high == null || count == null) {
            throw SqlTranslationException.unsupported("width_bucket(...) with bounds or a count other than constants"
                    + " is not supported yet", f.pos());
        }
        if (count <= 0) {
            throw SqlTranslationException.invalid("count must be greater than zero", f.args().get(3).pos());
        }
        if (count > 10000 || low.abs().compareTo(BigDecimal.TEN.pow(300)) > 0
                || high.abs().compareTo(BigDecimal.TEN.pow(300)) > 0) {
            throw SqlTranslationException.unsupported("width_bucket(...) of more than 10000 buckets, or of bounds"
                    + " beyond 1e300, is not supported yet", f.pos());
        }
        int order = low.compareTo(high);
        if (order == 0) {
            throw SqlTranslationException.invalid("lower bound cannot equal upper bound", f.pos());
        }
        Tx x = args.get(0);
        numericOperand(x, f.args().get(0));
        if (isNullConstant(x)) {
            return nullOf(Kind.INTEGER);
        }
        inexact(x, "width_bucket(...)", f.pos());
        boolean integerBounds = f.args().get(1) instanceof SqlAst.IntegerLit
                || f.args().get(1) instanceof SqlAst.Unary u1 && u1.operand() instanceof SqlAst.IntegerLit;
        integerBounds &= f.args().get(2) instanceof SqlAst.IntegerLit
                || f.args().get(2) instanceof SqlAst.Unary u2 && u2.operand() instanceof SqlAst.IntegerLit;
        if (x.kind() == Kind.WHOLE && integerBounds) {
            throw SqlTranslationException.unsupported("width_bucket(...) of a sum of integers and integer bounds is"
                    + " not supported yet: Postgres's sum is a bigint (read as a double) or a numeric by the column's"
                    + " width (cast it)", f.pos());
        }
        boolean doubles = x.kind() == Kind.FLOAT || x.kind() == Kind.INTEGER && integerBounds;
        boolean ascending = order < 0;
        return strict(Kind.INTEGER, List.of(x), v -> doubles
                ? floatBucket(x.kind() == Kind.FLOAT ? v.get(0) : call("toFloat", v.get(0)), low, high, count,
                        ascending)
                : numericBucket(v.get(0), low, high, count, ascending));
    }

    private static ValueSpecification floatBucket(ValueSpecification x, BigDecimal low, BigDecimal high, long count,
            boolean ascending) {
        ValueSpecification lo = call("toFloat", new CDecimal(low));
        ValueSpecification hi = call("toFloat", new CDecimal(high));
        ValueSpecification part = ascending
                ? call("divide", AppliedFunction.infixRun("minus", List.of(x, lo)),
                        AppliedFunction.infixRun("minus", List.of(hi, lo)))
                : call("divide", AppliedFunction.infixRun("minus", List.of(lo, x)),
                        AppliedFunction.infixRun("minus", List.of(lo, hi)));
        ValueSpecification bucket = call("floor", AppliedFunction.infixRun("times", List.of(
                call("toFloat", new CInteger(count)), part)));
        ValueSpecification inside = AppliedFunction.infixRun("plus", List.of(call("if", call("greaterThanEqual",
                bucket, new CInteger(count)), thunk(new CInteger(count - 1)), thunk(bucket)), new CInteger(1L)));
        return buckets(x, lo, hi, count, ascending, inside);
    }

    private static ValueSpecification numericBucket(ValueSpecification x, BigDecimal low, BigDecimal high, long count,
            boolean ascending) {
        ValueSpecification lo = new CDecimal(low);
        ValueSpecification hi = new CDecimal(high);
        ValueSpecification a = AppliedFunction.infixRun("times", List.of(ascending
                ? AppliedFunction.infixRun("minus", List.of(x, lo))
                : AppliedFunction.infixRun("minus", List.of(lo, x)), new CInteger(count)));
        ValueSpecification b = ascending ? AppliedFunction.infixRun("minus", List.of(hi, lo))
                : AppliedFunction.infixRun("minus", List.of(lo, hi));
        // a is at least 0 and below count * b: a - rem(a, b) is b times the floor exactly, a whole number below
        // 10,000 that the quotient's double rounds to
        ValueSpecification rest = call("rem", a, b);
        ValueSpecification floor = call("round", call("divide", AppliedFunction.infixRun("minus", List.of(a, rest)),
                b));
        // within 5e-17 of the next edge (as a part of b) Postgres's rounded quotient may reach it: the double of that
        // part, within 2^-52 of it, is screened at 1e-15 (a product with a 17-digit constant overflows DuckDB's
        // 18-digit decimal arithmetic)
        ValueSpecification near = call("lessThan", call("divide", AppliedFunction.infixRun("minus", List.of(b, rest)),
                b), new com.legend.protocol.spec.CFloat(1e-15));
        ValueSpecification inside = call("if", near, thunk(call("toOne", call("cast", empty(), typeArg("Integer")))),
                thunk(AppliedFunction.infixRun("plus", List.of(floor, new CInteger(1L)))));
        return buckets(x, lo, hi, count, ascending, inside);
    }

    /** A bucket: 0 before the first, count + 1 after the last, else {@code inside}. */
    private static ValueSpecification buckets(ValueSpecification x, ValueSpecification lo, ValueSpecification hi,
            long count, boolean ascending, ValueSpecification inside) {
        ValueSpecification before = call(ascending ? "lessThan" : "greaterThan", x, lo);
        ValueSpecification after = call(ascending ? "greaterThanEqual" : "lessThanEqual", x, hi);
        return call("if", before, thunk(new CInteger(0L)), thunk(call("if", after, thunk(new CInteger(count + 1)),
                thunk(inside))));
    }

    /** {@code greatest(...)}, {@code least(...)}: the largest (smallest) of the values, NULLs ignored; NULL only when
     *  every value is. */
    private static Tx extreme(SqlAst.FunctionCall f, List<Tx> arguments, boolean greatest) {
        List<Tx> args = common(greatest ? "GREATEST" : "LEAST", f.args(), arguments, f.pos()).values();
        Tx best = null;
        for (int i = 0; i < args.size(); i++) {
            Tx a = args.get(i);
            if (a.kind() == Kind.NULL) {
                continue;
            }
            if (a.kind() == Kind.BOOLEAN) {
                throw SqlTranslationException.unsupported(functionName(f.name().get(0)) + "(...) of boolean values is"
                        + " not supported yet", f.args().get(i).pos());
            }
            if (best == null) {
                best = a;
                continue;
            }
            comparableKinds(best, a, f.pos());
            best = better(best, a, greatest);
        }
        if (best == null) {
            // every value NULL: Postgres reads them as text
            return nullOf(Kind.STRING);
        }
        return best;
    }

    private static Tx better(Tx l, Tx r, boolean greatest) {
        ValueSpecification lv = valueOf(l);
        ValueSpecification rv = valueOf(r);
        // where both hold a value, each is one value: the pick is never empty (its type says so)
        ValueSpecification lOne = l.nullable() ? call("toOne", lv) : lv;
        ValueSpecification rOne = r.nullable() ? call("toOne", rv) : rv;
        ValueSpecification v = call("if", call(greatest ? "greaterThanEqual" : "lessThanEqual", lOne, rOne),
                thunk(lOne), thunk(rOne));
        if (r.nullable()) {
            v = call("if", call("isEmpty", rv), thunk(lv), thunk(v));
        }
        if (l.nullable()) {
            v = call("if", call("isEmpty", lv), thunk(rv), thunk(v));
        }
        Kind kind = l.kind() == r.kind() ? l.kind() : l.kind().numeric() && r.kind().numeric()
                ? arithmetic(l.kind(), r.kind()) : l.kind();
        return Tx.value(v, kind, l.nullable() && r.nullable());
    }

    /** {@code num_nulls(...)}, {@code num_nonnulls(...)}: how many of the values are NULL (are not). */
    private static Tx countNulls(SqlAst.FunctionCall f, List<Tx> args, boolean nulls) {
        if (args.isEmpty()) {
            throw SqlTranslationException.invalid("function " + functionName(f.name().get(0)) + " needs an argument",
                    f.pos());
        }
        List<ValueSpecification> ones = new ArrayList<>();
        long fixed = 0;
        for (Tx a : args) {
            if (a.kind() == Kind.NULL || !a.nullable()) {
                fixed += (a.kind() == Kind.NULL) == nulls ? 1 : 0;
                continue;
            }
            ones.add(call("if", call(nulls ? "isEmpty" : "isNotEmpty", valueOf(a)), thunk(new CInteger(1L)),
                    thunk(new CInteger(0L))));
        }
        if (fixed != 0 || ones.isEmpty()) {
            ones.add(new CInteger(fixed));
        }
        return Tx.value(ones.size() == 1 ? ones.get(0) : AppliedFunction.infixRun("plus", ones), Kind.INTEGER, false);
    }

    /** {@code extract(field FROM x)}, {@code date_part('field', x)}: the whole-number fields of a date or timestamp
     *  (DATE_FIELDS); a time of day is a timestamp's only. */
    /** {@code extract(field FROM x)} ({@code numeric}, Postgres 14 on) and {@code date_part('field', x)} (double
     *  precision): a field's whole number. */
    private static Tx datePart(SqlAst.FunctionCall f, List<Tx> args, boolean numeric) {
        arity(f, args, 2);
        String fn = functionName(f.name().get(0));
        if (!(f.args().get(0) instanceof SqlAst.StringLit field)) {
            throw SqlTranslationException.unsupported(fn + "(...) of a computed field is not supported yet",
                    f.args().get(0).pos());
        }
        Tx x = args.get(1);
        String name = fold(field.value());
        String simple = DATE_FIELDS.get(name);
        java.util.function.UnaryOperator<ValueSpecification> computed = COMPUTED_FIELDS.get(name);
        java.util.function.UnaryOperator<ValueSpecification> pure = simple != null ? v -> call(simple, v) : computed;
        if (pure == null) {
            throw SqlTranslationException.unsupported(fn + "(" + name + " ...) is not supported yet", field.pos());
        }
        boolean timeOfDay = TIME_FIELDS.contains(name);
        // Pure's toEpochValue is whole seconds: a timestamp's fraction would be lost
        boolean dayOnly = name.equals("epoch");
        if (!isDate(x.kind()) || timeOfDay && x.kind() != Kind.DATETIME || dayOnly && x.kind() != Kind.DATE) {
            throw SqlTranslationException.unsupported(fn + "(" + name + " ...) of a " + x.kind().name().toLowerCase(
                    Locale.ROOT) + " value is not supported yet", f.args().get(1).pos());
        }
        // extract is numeric in Postgres (14 on), date_part double precision: a field's whole number as either
        return numeric ? strict(Kind.DECIMAL, List.of(x), v -> pure.apply(v.get(0)))
                : strict(Kind.FLOAT, List.of(x), v -> call("toFloat", pure.apply(v.get(0))));
    }

    /** Refuses another number of arguments as not supported: Postgres may have that overload (btrim(x, chars)). */
    static void arity(SqlAst.FunctionCall f, List<Tx> args, int n) {
        if (args.size() != n) {
            throw SqlTranslationException.unsupported("function " + functionName(f.name().get(0)) + " with "
                    + args.size() + " argument" + (args.size() == 1 ? "" : "s") + " is not supported yet; it takes "
                    + n + " here", f.pos());
        }
    }

    /** A number constant ({@code 3}, {@code -2.5}), or null. */
    private static @com.legend.base.Nullable BigDecimal numberConstant(SqlAst.Expr e) {
        boolean negative = e instanceof SqlAst.Unary u && u.op() == SqlAst.UnaryOp.MINUS;
        SqlAst.Expr literal = e instanceof SqlAst.Unary u && negative ? u.operand() : e;
        BigDecimal v = literal instanceof SqlAst.IntegerLit n ? new BigDecimal(n.value())
                : literal instanceof SqlAst.NumericLit d ? d.value() : null;
        return v == null || !negative ? v : v.negate();
    }
}
