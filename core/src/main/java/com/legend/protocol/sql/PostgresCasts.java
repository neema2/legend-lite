// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.sql;

import com.legend.protocol.spec.CBoolean;
import com.legend.protocol.spec.CDate;
import com.legend.protocol.spec.CDecimal;
import com.legend.values.PureDateLiteral;
import java.math.BigDecimal;
import static com.legend.protocol.sql.SqlToPure.*;
import static com.legend.protocol.sql.PostgresFunctions.*;
import static com.legend.protocol.sql.PostgresJsonb.*;

import com.legend.protocol.spec.CInteger;
import com.legend.protocol.spec.CString;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Postgres's casts and its integer widths as the {@code #SQL{...}#} island reads them: CAST and {@code ::}, typed
 * constants, and the smallint, integer and bigint ranges the database computes in ({@link PostgresDialect}'s, moved
 * out of {@link SqlToPure} with the dialect program's D11, 2026-10-18).
 */
final class PostgresCasts {

    private PostgresCasts() {
    }

    /** Postgres's 4-byte integer's names: a numeric or text value it cannot hold is Postgres's error. */
    private static final Set<String> NARROW_INTEGERS = Set.of("integer", "int", "int4");

    /** Postgres's 8-byte integer's names. */
    private static final Set<String> WIDE_INTEGERS = Set.of("bigint", "int8");

    /** The names of text with a length ({@code varchar(n)}): an explicit cast cuts a longer value to n characters. */
    private static final Set<String> VARCHAR_TYPES = Set.of("varchar", "character varying");

    /** The types a cast reads, as Postgres names them, and their kinds: real
     *  (single precision), char(n) (padded), time zones, intervals, json and arrays are not among them yet. */
    private static final Map<String, Kind> CAST_TYPES = Map.ofEntries(Map.entry("integer", Kind.INTEGER),
            Map.entry("int", Kind.INTEGER), Map.entry("int4", Kind.INTEGER), Map.entry("bigint", Kind.INTEGER),
            Map.entry("smallint", Kind.INTEGER), Map.entry("int2", Kind.INTEGER),
            Map.entry("int8", Kind.INTEGER), Map.entry("numeric", Kind.DECIMAL), Map.entry("decimal", Kind.DECIMAL),
            Map.entry("double precision", Kind.FLOAT), Map.entry("float8", Kind.FLOAT), Map.entry("float", Kind.FLOAT),
            Map.entry("text", Kind.STRING), Map.entry("varchar", Kind.STRING),
            Map.entry("character varying", Kind.STRING), Map.entry("boolean", Kind.BOOLEAN),
            Map.entry("bool", Kind.BOOLEAN), Map.entry("date", Kind.DATE), Map.entry("timestamp", Kind.DATETIME),
            Map.entry("timestamp without time zone", Kind.DATETIME), Map.entry("json", Kind.JSON),
            Map.entry("jsonb", Kind.JSON));

    /**
     * {@code CAST(x AS type)}, {@code x::type}: a string constant is read by the type's input rules (as a typed
     * constant is); a value converts where one Pure function gives Postgres's answer: a number to another number
     * (an exact one to an integer rounds half away from zero, a double precision half to even), to text; a
     * timestamp to its date; an integer to a boolean. An integer or smallint holds a value in its range, else it
     * is Postgres's error; {@code varchar(n)} cuts the text to n characters. Refused: what rounds, formats or reads
     * otherwise in Postgres (a double precision to a numeric, a double precision or a timestamp to text, text to a
     * number), another type modifier, the other types.
     */
    static Tx cast(SqlToPure translator, SqlAst.Cast c, Scope scope, String row) {
        String type = c.type().name();
        Kind target = CAST_TYPES.get(type);
        if (target == null) {
            throw SqlTranslationException.unsupported("casts to " + type + " are not supported yet", c.pos());
        }
        if (!c.type().modifiers().isEmpty()) {
            if (VARCHAR_TYPES.contains(type) && c.type().modifiers().size() == 1 && c.type().modifiers().get(0) > 0) {
                // varchar(n): the value's text, cut to n characters (an explicit cast truncates in Postgres)
                long n = c.type().modifiers().get(0);
                Tx text = cast(translator, new SqlAst.Cast(c.operand(), new SqlAst.TypeName(type, List.of(),
                        c.type().pos()),
                        c.pos()), scope, row);
                return strict(Kind.STRING, List.of(text), v -> call("left", v.get(0), new CInteger(n)));
            }
            if (target == Kind.DECIMAL && c.type().modifiers().size() <= 2) {
                return toScale(translator, c, scope, row);
            }
            throw SqlTranslationException.unsupported("casts to " + type + " with a type modifier are not supported"
                    + " yet", c.pos());
        }
        // Postgres's integer (int4) and smallint (int2) are narrower than Pure's: a value they cannot hold is
        // Postgres's error ("integer out of range")
        long[] range = NARROW_INTEGERS.contains(type) ? new long[] {Integer.MIN_VALUE, Integer.MAX_VALUE}
                : SMALL_INTEGERS.contains(type) ? new long[] {Short.MIN_VALUE, Short.MAX_VALUE} : null;
        if (c.operand() instanceof SqlAst.StringLit s) {
            return stringAs(s, type, target, range);
        }
        Tx x = translator.tx(c.operand(), scope, row);
        if (x.kind() == Kind.NULL) {
            return nullOf(target);
        }
        if (target == Kind.STRING && x.kind() == Kind.STRING && translator.dialect.padded(x.detail())) {
            // a char(n) value as text or varchar drops its padding: Postgres's cast removes the trailing blanks
            // (2026-10-17, the regression suite: CAST(f1 AS text) of a char(4) answered 'a   ')
            return strict(Kind.STRING, List.of(x), v -> call("rtrim", v.get(0)));
        }
        if (target == Kind.INTEGER && range != null && x.kind() == Kind.INTEGER && x.vs() instanceof CInteger n) {
            if (n.value().longValue() > range[1] || n.value().longValue() < range[0]) {
                throw SqlTranslationException.invalid((SMALL_INTEGERS.contains(type) ? "smallint" : "integer")
                        + " out of range", c.pos());
            }
            return x;
        }
        // an integer an enclosed cast already holds to this range needs no second test (nested casts tested at every
        // level wrote the value three times a level: the stress corpus's ten casts were 9 MB of SQL)
        boolean held = range != null && within(provenRange(c.operand()), range);
        if (x.kind() == target || target == Kind.DECIMAL && x.kind() == Kind.NUMBER) {
            return range == null || held ? x : inRange(x, range, type);
        }
        if (x.kind() == Kind.WHOLE && target == Kind.INTEGER) {
            // a sum of integers as a bigint (or narrower): its width now known; a strict value keeps its guard
            Tx whole = new Tx(x.vs(), Kind.INTEGER, x.nullable(), null, null, x.guard(), x.body());
            return range == null || held ? whole : inRange(whole, range, type);
        }
        if (x.kind() == Kind.INEXACT) {
            if (target == Kind.DECIMAL || target == Kind.FLOAT) {
                // already Postgres's numeric; as a double precision, Postgres's double of the exact value, which this
                // one may miss: inexact either way
                return x;
            }
            inexact(x, "a cast to " + type, c.pos());
        }
        String from = x.kind().name().toLowerCase(Locale.ROOT);
        String convert = switch (target) {
            case STRING -> x.kind() == Kind.INTEGER || x.kind() == Kind.WHOLE || x.kind() == Kind.DECIMAL
                    || x.kind() == Kind.BOOLEAN || x.kind() == Kind.DATE ? "toString" : null;
            // a numeric rounds half away from zero, a double half to even (Pure's round of a Float, measured). Text
            // is not read: a database's cast reads what Postgres's input rejects ('1.5' and '1e3' are 2 and 1000 on
            // DuckDB, errors in Postgres)
            case INTEGER -> x.kind() == Kind.DECIMAL || x.kind() == Kind.NUMBER ? "halfAwayFromZero"
                    : x.kind() == Kind.FLOAT ? "round" : null;
            // text to numeric: Pure's parseDecimal lowers to a fixed DECIMAL(5, 2), not Postgres's unbounded numeric
            case DECIMAL -> x.kind() == Kind.INTEGER || x.kind() == Kind.WHOLE ? "toDecimal" : null;
            // text likewise: DuckDB reads '1_000' as 1000 and '1e400' as Infinity, Postgres rejects both
            case FLOAT -> x.kind().numeric() ? "toFloat" : null;
            case DATE -> x.kind() == Kind.DATETIME ? "datePart" : null;
            case BOOLEAN -> x.kind() == Kind.INTEGER ? "notEqual" : null;
            // text parses into jsonb; a jsonb's text (with its strings quoted) is not Pure's to(@String)
            case JSON -> x.kind() == Kind.STRING ? FROM_JSON : null;
            default -> null;
        };
        if (convert == null) {
            throw SqlTranslationException.unsupported("casts from " + from + " to " + type + " are not supported yet",
                    c.pos());
        }
        if (target == Kind.BOOLEAN) {
            // Postgres: 0 is false, any other integer true
            return strictTest(List.of(x), v -> call("not", call("equal", v.get(0), new CInteger(0L))));
        }
        Tx converted = convert.equals("halfAwayFromZero") ? strict(target, List.of(x), v -> halfAwayFromZero(v.get(0)))
                : strict(target, List.of(x), v -> call(convert, v.get(0)));
        return range == null ? converted : inRange(converted, range, type);
    }

    /** The range {@code e}'s value is already held to by a cast to a narrow integer ({@link #NARROW_INTEGERS},
     *  {@link SqlToPure#SMALL_INTEGERS}: Postgres raised past it), seen through casts to other integers, which keep an
     *  integer's value; the narrowest such range, or null when no cast holds it. */
    private static long @com.legend.base.Nullable [] provenRange(SqlAst.Expr e) {
        long[] proven = null;
        SqlAst.Expr at = e;
        while (at instanceof SqlAst.Cast c && c.type().modifiers().isEmpty()
                && CAST_TYPES.get(c.type().name()) == Kind.INTEGER) {
            String type = c.type().name();
            long[] range = NARROW_INTEGERS.contains(type) ? new long[] {Integer.MIN_VALUE, Integer.MAX_VALUE}
                    : SMALL_INTEGERS.contains(type) ? new long[] {Short.MIN_VALUE, Short.MAX_VALUE} : null;
            if (range != null && (proven == null || within(range, proven))) {
                proven = range;
            }
            at = c.operand();
        }
        return proven;
    }

    /** Whether {@code inner} is a range and lies within {@code outer}. */
    private static boolean within(long @com.legend.base.Nullable [] inner, long[] outer) {
        return inner != null && inner[0] >= outer[0] && inner[1] <= outer[1];
    }

    /** {@code x}, the integer value of {@code e}, held to the range of the integer type Postgres gives {@code e} where
     *  {@code e} is integer arithmetic over constants alone ({@link #constantWidth}): past it, Postgres's error
     *  ("smallint out of range", "integer out of range", "bigint out of range"), where the planner's fold of the
     *  constants would answer the exact value (2026-10-17, the regression suite: {@code (-32768)::int2 * (-1)::int2},
     *  {@code '9223372036854775800'::int8 + '9223372036854775800'::int8}). Over a column the database computes in
     *  the column's own type, as Postgres does, so nothing is added. The outermost arithmetic of constants alone is
     *  tested, not each operator in it (each test writes the value three times, so every level tested was 3^n);
     *  where an operator inside it may pass its own type's range part-way, which the outer test cannot see, it is
     *  refused (independent review, 2026-10-17: {@code (32767::int2 + 1::int2) * 0::int2} answered 0). A minus before
     *  a constant is that constant, in its range by construction; arithmetic whose constants bound it within the
     *  range ({@link #magnitude}: 1 + 2 is at most 3) needs no test. */
    static Tx heldToWidth(SqlToPure translator, Tx x, SqlAst.Expr e) {
        if (translator.heldOutside.contains(e) || e instanceof SqlAst.Unary u
                && u.operand() instanceof SqlAst.IntegerLit) {
            return x;
        }
        Integer width = x.kind() == Kind.INTEGER ? constantWidth(e) : null;
        if (width == null || fits(magnitude(e), width)) {
            return x; // not arithmetic of constants, or within the type at every step
        }
        for (SqlAst.Expr inner : operands(e)) {
            mayNotPass(inner);
        }
        return width == 2 ? inRange(x, new long[] {Short.MIN_VALUE, Short.MAX_VALUE}, "smallint")
                : width == 4 ? inRange(x, new long[] {Integer.MIN_VALUE, Integer.MAX_VALUE}, "integer")
                : inRange(x, new long[] {Long.MIN_VALUE, Long.MAX_VALUE}, "bigint");
    }

    /** Whether {@code bound}, a value's size, is within the integer type of {@code width} bytes: at most
     *  2^(8w-1) - 1 either way. */
    private static boolean fits(java.math.@com.legend.base.Nullable BigInteger bound, int width) {
        return bound != null && bound.bitLength() < 8 * width;
    }

    /** The operands of {@code e}'s operator, where {@code e} is integer arithmetic or a sign; none otherwise. */
    private static List<SqlAst.Expr> operands(SqlAst.Expr e) {
        return switch (e) {
            case SqlAst.Binary b when ARITHMETIC.contains(b.op()) -> List.of(b.left(), b.right());
            case SqlAst.Unary u when u.op() != SqlAst.UnaryOp.NOT -> List.of(u.operand());
            default -> List.of();
        };
    }

    /** Refuses {@code e}, integer arithmetic of constants inside arithmetic whose outer test holds it, where its own
     *  type's range may be passed: the outer test sees only the end of the computation. */
    private static void mayNotPass(SqlAst.Expr e) {
        Integer width = operands(e).isEmpty() ? null : constantWidth(e);
        if (width == null || e instanceof SqlAst.Unary u && u.operand() instanceof SqlAst.IntegerLit) {
            return;
        }
        if (!fits(magnitude(e), width)) {
            throw SqlTranslationException.unsupported("integer arithmetic of constants that may pass its type's range"
                    + " part-way is not supported yet", e.pos());
        }
        for (SqlAst.Expr inner : operands(e)) {
            mayNotPass(inner);
        }
    }

    /** An upper bound of the size of {@code e}'s value and of every step of arithmetic inside it, from the sizes its
     *  constants spell (each operator's bound of its operands' bounds, a cast's its type's), null where there is
     *  none: whether a range test is needed is decided by the constants as written, and the value is still computed
     *  where it always was. */
    private static java.math.@com.legend.base.Nullable BigInteger magnitude(SqlAst.Expr e) {
        return switch (e) {
            case SqlAst.IntegerLit n -> n.value().abs();
            case SqlAst.Unary u when u.op() != SqlAst.UnaryOp.NOT -> magnitude(u.operand());
            case SqlAst.Cast c when constant(c.operand()) && constantWidth(c) != null -> {
                int width = java.util.Objects.requireNonNull(castWidth(c.type().name()));
                java.math.BigInteger type = java.math.BigInteger.ONE.shiftLeft(8 * width - 1);
                java.math.BigInteger operand = c.operand() instanceof SqlAst.StringLit str
                        ? stringMagnitude(str.value()) : magnitude(c.operand());
                yield operand == null ? type : operand.min(type);
            }
            case SqlAst.TypedLit t when constantWidth(t) != null -> {
                int width = java.util.Objects.requireNonNull(castWidth(t.type().name()));
                java.math.BigInteger type = java.math.BigInteger.ONE.shiftLeft(8 * width - 1);
                java.math.BigInteger value = stringMagnitude(t.value());
                yield value == null ? type : value.min(type);
            }
            case SqlAst.Binary b when ARITHMETIC.contains(b.op()) -> {
                java.math.BigInteger l = magnitude(b.left());
                java.math.BigInteger r = magnitude(b.right());
                // a bound of the result and of each operand, so of every step inside (a product with 0 is no bound of
                // its other factor; a quotient or a remainder is no larger than its dividend)
                yield l == null || r == null ? null : switch (b.op()) {
                    case PLUS, MINUS -> l.add(r);
                    case TIMES -> l.multiply(r).max(l).max(r);
                    default -> l.max(r);
                };
            }
            default -> null;
        };
    }

    /** The size of a string constant's integer, as an integer type reads it (its plain spelling), else null. */
    private static java.math.@com.legend.base.Nullable BigInteger stringMagnitude(String v) {
        String n = plainNumber(v.strip(), false);
        return n == null ? null : new java.math.BigInteger(n).abs();
    }

    /** Refuses integer arithmetic of a constant Postgres types smallint ({@code 2::int2}, {@code int2 '2'}) and a
     *  value that is not a constant: over a smallint column Postgres computes a smallint, raising past 32767, and the
     *  island's constant is an integer, so the database would compute wider and answer (2026-10-17, the regression
     *  suite: {@code i.f1 * int2 '2'}). */
    static void noSmallintConstantBeside(SqlAst.Binary b, Tx l, Tx r) {
        Integer lw = constantWidth(b.left());
        Integer rw = constantWidth(b.right());
        if (l.kind() == Kind.INTEGER && r.kind() == Kind.INTEGER
                && (lw != null && lw == 2 && rw == null || rw != null && rw == 2 && lw == null)) {
            throw SqlTranslationException.unsupported("arithmetic of a smallint constant and a value that is not a"
                    + " constant is not supported yet: Postgres computes it as a smallint where the value is one",
                    b.pos());
        }
    }

    /** Refuses integer arithmetic over a widened value ({@link SqlToPure#widened}) that the database computes at its
     *  narrow width: beside another such value, or an integer that is not a wider constant (a column the island cannot
     *  tell from one of that width). Constants alone are held to their width ({@link #heldToWidth}); beside an integer
     *  or bigint constant the database computes wider too. */
    static void noNarrowArithmetic(SqlToPure translator, SqlAst.Binary b, Tx l, Tx r, Scope scope) {
        if (l.kind() != Kind.INTEGER || r.kind() != Kind.INTEGER || constantWidth(b) != null) {
            return;
        }
        if (narrowValue(translator, b.left(), l, scope) && !widerConstant(b.right()) || narrowValue(translator,
                b.right(), r, scope)
                && !widerConstant(b.left())) {
            throw SqlTranslationException.unsupported("integer arithmetic of a smallint value (a cast to smallint, an"
                    + " int2 constant, or a column of one) beside a value that is not an integer constant is not"
                    + " supported yet: the database computes it as a smallint, raising past 32767, where the island's"
                    + " integer is wider", b.pos());
        }
    }

    private static boolean widerConstant(SqlAst.Expr e) {
        Integer width = constantWidth(e);
        return width != null && width > 2;
    }

    /** Whether {@code e}, translated as {@code t}, is a widened value ({@link SqlToPure#widened}), or its translation
     *  carries the narrow detail (a scalar subquery of one). */
    static boolean narrowValue(SqlToPure translator, SqlAst.Expr e, Tx t, Scope scope) {
        InputDialect.TypeDetail narrow = translator.dialect.narrowInteger();
        return narrow != null && (narrow.equals(t.detail()) || translator.widened(e, scope, narrow));
    }

    /** Marks {@code operands} as held by {@code e} when {@code e} is integer arithmetic over constants alone. */
    static void holdsOperands(SqlToPure translator, SqlAst.Expr e, SqlAst.Expr... operands) {
        if (constantWidth(e) != null) {
            translator.heldOutside.addAll(List.of(operands));
        }
    }

    /** The bytes of the integer type Postgres gives {@code e} when it is integer arithmetic over constants (its
     *  operators' result types: int2 with int2 is int2, else the wider), null otherwise: a constant is int4 where it
     *  fits, else int8 (a minus before a constant negates the constant, Postgres's doNegate); a cast of a constant to
     *  smallint, integer or bigint is that type. */
    static @com.legend.base.Nullable Integer constantWidth(SqlAst.Expr e) {
        return switch (e) {
            case SqlAst.IntegerLit n -> literalWidth(n.value());
            case SqlAst.Unary u when u.op() == SqlAst.UnaryOp.MINUS && u.operand() instanceof SqlAst.IntegerLit n ->
                    literalWidth(n.value().negate());
            case SqlAst.Unary u when u.op() != SqlAst.UnaryOp.NOT -> constantWidth(u.operand());
            case SqlAst.Cast c when c.type().modifiers().isEmpty() && constant(c.operand()) ->
                    castWidth(c.type().name());
            // int2 '1' is '1'::int2
            case SqlAst.TypedLit t when t.type().modifiers().isEmpty() -> castWidth(t.type().name());
            case SqlAst.Binary b when ARITHMETIC.contains(b.op()) -> {
                Integer l = constantWidth(b.left());
                Integer r = constantWidth(b.right());
                yield l == null || r == null ? null : Integer.valueOf(Math.max(l, r));
            }
            default -> null;
        };
    }

    /** A constant integer's width: int4 where it fits, else int8, else none (Postgres's numeric). */
    private static @com.legend.base.Nullable Integer literalWidth(java.math.BigInteger v) {
        if (v.bitLength() < 32) {
            return 4;
        }
        return v.bitLength() < 64 ? Integer.valueOf(8) : null;
    }

    /** An integer type's width by its name, none for another type. */
    private static @com.legend.base.Nullable Integer castWidth(String type) {
        if (SMALL_INTEGERS.contains(type)) {
            return 2;
        }
        if (NARROW_INTEGERS.contains(type)) {
            return 4;
        }
        return WIDE_INTEGERS.contains(type) ? Integer.valueOf(8) : null;
    }

    /** {@code numeric(p, s)} (writes leg 4, 2026-10-20: a value as a column of that type holds it): the value as
     *  numeric, rounded to {@code s} places half away from zero, as Postgres rounds it; one of {@code p - s} or more
     *  digits before the point is Postgres's error ("numeric field overflow"), the database's to raise. */
    private static Tx toScale(SqlToPure translator, SqlAst.Cast c, Scope scope, String row) {
        List<Integer> modifiers = c.type().modifiers();
        int precision = modifiers.get(0);
        int scale = modifiers.size() > 1 ? modifiers.get(1) : 0;
        if (precision < 1 || precision > 1000) {
            throw SqlTranslationException.invalid("NUMERIC precision " + precision + " must be between 1 and 1000",
                    c.pos());
        }
        if (scale < 0 || scale > precision || scale > 18) {
            throw SqlTranslationException.unsupported("casts to numeric(p, s) of a scale below 0, above the precision"
                    + " or above 18 are not supported yet", c.pos());
        }
        Tx x = cast(translator, new SqlAst.Cast(c.operand(), new SqlAst.TypeName(c.type().name(), List.of(),
                c.type().pos()), c.pos()), scope, row);
        if (x.kind() == Kind.NULL || isNullConstant(x)) {
            return nullOf(Kind.DECIMAL);
        }
        inexact(x, "a cast to numeric(p, s)", c.pos());
        boolean whole = x.kind() == Kind.INTEGER || x.kind() == Kind.WHOLE;
        BigDecimal limit = BigDecimal.TEN.pow(precision - scale);
        return strict(Kind.DECIMAL, List.of(x), v -> {
            com.legend.protocol.spec.ValueSpecification rounded = PostgresFunctions.toPlaces(whole ? call("toDecimal", v.get(0)) : v.get(0),
                    scale, true);
            return call("if", call("greaterThanEqual", call("abs", rounded), new CDecimal(limit)),
                    thunk(call("toOne", call("cast", empty(), typeArg("Decimal")))), thunk(rounded));
        });
    }

    /** An integer that a narrower Postgres integer must hold: past its range, Postgres's error -- an empty value
     *  read as one ({@code []->cast(@Integer)->toOne()} raises, typed as the integer it stands for). */
    private static Tx inRange(Tx x, long[] range, String type) {
        return strict(Kind.INTEGER, List.of(x), v -> call("if", call("or", call("lessThan", v.get(0),
                new CInteger(range[0])), call("greaterThan", v.get(0), new CInteger(range[1]))),
                thunk(call("toOne", call("cast", empty(), typeArg("Integer")))), thunk(v.get(0))));
    }

    /** {@code DATE '...'} and {@code TIMESTAMP '...'} in ISO 8601 spelling: Pure date constants; {@code JSONB '...'}
     *  and {@code JSON '...'}: a Variant constant. */
    static Tx typedLiteral(SqlAst.TypedLit lit) {
        if (JSON_TYPES.contains(lit.type().name())) {
            return jsonConstant(new SqlAst.StringLit(lit.value(), lit.pos()));
        }
        return switch (lit.type().name()) {
            case "date" -> dateFromString(new SqlAst.StringLit(lit.value(), lit.pos()), Kind.DATE);
            case "timestamp", "timestamp without time zone" ->
                    dateFromString(new SqlAst.StringLit(lit.value(), lit.pos()), Kind.DATETIME);
            default -> {
                // type 'string' is the string cast to the type, as Postgres reads it: int2 '1', bool 't', float8
                // '1.5' (2026-10-17, the regression suite's 269 such constants)
                String type = lit.type().name();
                Kind target = CAST_TYPES.get(type);
                if (target == null || !lit.type().modifiers().isEmpty()) {
                    throw SqlTranslationException.unsupported("constants of type " + type + " are not supported yet",
                            lit.pos());
                }
                long[] range = NARROW_INTEGERS.contains(type) ? new long[] {Integer.MIN_VALUE, Integer.MAX_VALUE}
                        : SMALL_INTEGERS.contains(type) ? new long[] {Short.MIN_VALUE, Short.MAX_VALUE} : null;
                yield stringAs(new SqlAst.StringLit(lit.value(), lit.pos()), type, target, range);
            }
        };
    }

    /** A string constant cast to {@code type}, of kind {@code target}: read by the type's input rules; an integer
     *  past {@code range} (an integer's or a smallint's) is Postgres's error. */
    private static Tx stringAs(SqlAst.StringLit s, String type, Kind target, long @com.legend.base.Nullable [] range) {
        if (target == Kind.JSON) {
            return jsonConstant(s);
        }
        Tx read = target == Kind.STRING ? Tx.value(new CString(s.value()), Kind.STRING, false) : fromString(s, target);
        if (range != null && read.vs() instanceof CInteger n && (n.value().longValue() > range[1]
                || n.value().longValue() < range[0])) {
            throw SqlTranslationException.invalid("value \"" + s.value() + "\" is out of range for type "
                    + (SMALL_INTEGERS.contains(type) ? "smallint" : "integer"), s.pos());
        }
        return read;
    }

    /** A string constant read as {@code kind}, by Postgres's input rules: dates and timestamps in ISO 8601, integers
     *  and decimals in their plain spellings (others, which Postgres may read too, are refused), booleans as
     *  Postgres's boolin reads them (any unambiguous prefix of true/false/yes/no, on/off, 1/0; nothing else). */
    static Tx fromString(SqlAst.StringLit s, Kind kind) {
        String v = s.value();
        switch (kind) {
            case DATE, DATETIME -> {
                return dateFromString(s, kind);
            }
            case INTEGER -> {
                String n = plainNumber(v.strip(), false);
                if (n != null && new java.math.BigInteger(n).bitLength() < 64) {
                    return Tx.value(integer(new java.math.BigInteger(n)), Kind.INTEGER, false);
                }
            }
            case DECIMAL, FLOAT, INEXACT -> {
                String n = plainNumber(v.strip(), true);
                if (n != null) {
                    // a double precision constant is the double nearest its text (round('2.5'::float8) is rint's 2);
                    // one past a double's range, or a nonzero one too small for any double, is Postgres's float8in
                    // error (2026-10-17, the regression suite: '10e-400'::float8)
                    double nearest = Double.parseDouble(n);
                    if (kind == Kind.FLOAT && (Double.isInfinite(nearest)
                            || nearest == 0 && new BigDecimal(n).signum() != 0)) {
                        throw SqlTranslationException.invalid("\"" + v + "\" is out of range for type double"
                                + " precision", s.pos());
                    }
                    return kind == Kind.FLOAT ? Tx.value(call("toFloat", new CDecimal(new BigDecimal(n))), Kind.FLOAT,
                            false) : Tx.value(new CDecimal(new BigDecimal(n)), Kind.DECIMAL, false);
                }
            }
            case BOOLEAN -> {
                Boolean b = postgresBoolean(v.strip().toLowerCase(Locale.ROOT));
                if (b == null) {
                    throw SqlTranslationException.invalid("invalid input syntax for type boolean: \"" + v + "\"",
                            s.pos());
                }
                return booleanValue(new CBoolean(b), false);
            }
            default -> {
            }
        }
        throw SqlTranslationException.unsupported("the string '" + v + "' read as " + kind.name()
                .toLowerCase(Locale.ROOT) + " is not supported yet", s.pos());
    }

    /** {@code v} when it is a plain number: an optional sign and digits, and (when {@code decimal}) an optional point
     *  with digits on at least one side and an optional exponent; without its {@code +}. Null otherwise. */
    static @com.legend.base.Nullable String plainNumber(String v, boolean decimal) {
        int i = v.startsWith("+") || v.startsWith("-") ? 1 : 0;
        int digits = 0;
        while (i < v.length() && Character.isDigit(v.charAt(i)) && v.charAt(i) < 0x80) {
            i++;
            digits++;
        }
        if (decimal && i < v.length() && v.charAt(i) == '.') {
            i++;
            while (i < v.length() && Character.isDigit(v.charAt(i)) && v.charAt(i) < 0x80) {
                i++;
                digits++;
            }
        }
        if (digits == 0) {
            return null;
        }
        if (decimal && i < v.length() && (v.charAt(i) == 'e' || v.charAt(i) == 'E')) {
            int e = i + 1 < v.length() && (v.charAt(i + 1) == '+' || v.charAt(i + 1) == '-') ? i + 2 : i + 1;
            int start = e;
            while (e < v.length() && Character.isDigit(v.charAt(e)) && v.charAt(e) < 0x80) {
                e++;
            }
            if (e == start) {
                return null;
            }
            i = e;
        }
        if (i != v.length()) {
            return null;
        }
        return v.startsWith("+") ? v.substring(1) : v;
    }

    /** Postgres's parse_bool_with_len: true, false, or null for a spelling it refuses. */
    private static @com.legend.base.Nullable Boolean postgresBoolean(String v) {
        if (v.isEmpty()) {
            return null;
        }
        if ("true".startsWith(v) || "yes".startsWith(v) || v.equals("on") || v.equals("1")) {
            return Boolean.TRUE;
        }
        if ("false".startsWith(v) || "no".startsWith(v) || v.length() >= 2 && "off".startsWith(v)
                || v.equals("0")) {
            return Boolean.FALSE;
        }
        return null;
    }

    /** A string constant read as a date or a timestamp by Postgres's rules ({@link PostgresDateTime}): ISO 8601 and
     *  Postgres's other spellings (month names, month/day/year, digits run together, a year's day, a Julian day, AM and
     *  PM, epoch); a date drops a time of day. Its Pure constant spells the same day or instant, {@code
     *  yyyy-mm-ddThh:mm:ss[.ffffff]} for a timestamp (24:00:00 is the next midnight). */
    private static Tx dateFromString(SqlAst.StringLit s, Kind target) {
        PostgresDateTime.Value v = PostgresDateTime.read(s.value(), target == Kind.DATE ? "date" : "timestamp",
                s.pos());
        if (target == Kind.DATE) {
            String body = v.date().toString();
            return Tx.value(new CDate(PureDateLiteral.parse(body), body, null), Kind.DATE, false);
        }
        java.time.LocalDateTime t = v.date().atStartOfDay().plusNanos(v.micros() * 1000);
        if (t.getYear() > 9999) {
            throw SqlTranslationException.unsupported("'" + s.value() + "' as a timestamp is not supported yet (a"
                    + " timestamp outside the years 1 to 9999)", s.pos());
        }
        String body = String.format(Locale.ROOT, "%sT%02d:%02d:%02d", t.toLocalDate(), t.getHour(), t.getMinute(),
                t.getSecond());
        if (t.getNano() != 0) {
            body += "." + String.format(Locale.ROOT, "%06d", t.getNano() / 1000);
        }
        return Tx.value(new CDate(PureDateLiteral.parse(body), body, null), Kind.DATETIME, false);
    }
}
