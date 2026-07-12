package com.legend.lowering;

import com.legend.builtin.Pure;
import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * Scalar native dispatch, keyed by the RESOLVED overload's identity &mdash; the
 * {@link Pure} catalog constant Phase G chose ({@code TypedFunction.definition()}).
 * Dispatch never touches name strings; registration is catalog-driven: a
 * {@code family} maps EVERY overload of a Pure function to one semantic name
 * (all {@code lessThan} overloads mean {@code <}), and specific-overload
 * overrides land where the overload IS the decision &mdash; {@code plus} on
 * Strings is {@code ||}, on Numbers {@code +}; the type checker already chose.
 * An unregistered overload is a loud error naming the signature.
 */
final class Scalars {

    /** A rule receives the ALREADY-LOWERED argument expressions. */
    interface Rule extends BiFunction<TypedNativeCall, List<SqlExpr>, SqlExpr> {
    }

    private static final Map<String, Rule> RULES = new java.util.HashMap<>();

    private Scalars() {
    }

    /** Register every catalog overload of {@code pureName} under one semantic entry. */
    private static void family(SqlFn semantic, String pureName) {
        var overloads = Pure.nativeKeysAt(pureName);
        if (overloads.isEmpty()) {
            throw new IllegalStateException("no catalog overloads for '" + pureName + "'");
        }
        for (var f : overloads) {
            RULES.put(f, (n, args) -> new SqlExpr.Call(semantic, args));
        }
    }

    static {
        // equal/eq are PRECISION-AWARE over dates: a partial-date literal
        // (%2014, %2014-01) equals only a SAME-precision value — real pure's
        // rule, decided STATICALLY (partial precision is a literal-only
        // phenomenon; columns are always full-precision). Same-precision
        // partials compare as their ISO-prefix strings. eq = strict equality;
        // over our SQL value set it IS the = operator.
        for (String name : List.of("equal", "eq")) {
            for (String f : Pure.nativeKeysAt(name)) {
                RULES.put(f, (n, args) -> {
                    Integer p0 = partialPrecision(n.args().get(0));
                    Integer p1 = partialPrecision(n.args().get(1));
                    if (p0 != null || p1 != null) {
                        if (java.util.Objects.equals(p0, p1)) {
                            return new SqlExpr.Call(SqlFn.EQUAL, args);
                        }
                        Type other = (p0 != null ? n.args().get(1) : n.args().get(0))
                                .info().type();
                        // Covers both a full-precision opposite side AND a
                        // different-precision partial (whose static type is
                        // also Date) — never equal.
                        if (isFullPrecisionDate(other)) {
                            return new SqlExpr.BoolLit(false);
                        }
                        // A Date is never equal to a NON-date kind — the
                        // string carrier must not leak into '2014'=='2014'
                        // being true (audit). Any stays dynamic (fall through).
                        if (!PlatformTypes.isAny(other)) {
                            return new SqlExpr.BoolLit(false);
                        }
                    }
                    return new SqlExpr.Call(SqlFn.EQUAL, args);
                });
            }
        }
        // Ordering comparisons PAD partial-date literals to their instant
        // (dateArg) — the string carrier must never meet a DATE operand
        // (audit: '2014' < DATE '…' is a conversion error).
        for (var cmp : Map.of("lessThan", SqlFn.LESS, "lessThanEqual", SqlFn.LESS_EQUAL,
                "greaterThan", SqlFn.GREATER, "greaterThanEqual", SqlFn.GREATER_EQUAL)
                .entrySet()) {
            for (String f : Pure.nativeKeysAt(cmp.getKey())) {
                RULES.put(f, (n, args) -> {
                    List<SqlExpr> padded = new ArrayList<>(args.size());
                    for (int i = 0; i < args.size(); i++) {
                        padded.add(dateArg(n.args().get(i), args.get(i)));
                    }
                    return new SqlExpr.Call(cmp.getValue(), padded);
                });
            }
        }
        // and(Boolean[*]) / or(Boolean[*]) are the COLLECTION reductions
        // (real pure) — the infix renderer would emit the lone list bare.
        // The EMPTY collection takes each reduction's IDENTITY (and([]) is
        // true, or([]) is false — list_aggregate over [] is NULL; audit).
        for (String f : Pure.nativeKeysAt("and")) {
            RULES.put(f, (n, args) -> args.size() == 1
                    ? (isToOne(n.args().get(0))
                            && !(args.get(0) instanceof SqlExpr.ArrayLit)
                            ? args.get(0)
                            : SqlExpr.Call.of(SqlFn.COALESCE,
                                    new SqlExpr.Call(SqlFn.LIST_BOOL_AND, args),
                                    new SqlExpr.BoolLit(true)))
                    : new SqlExpr.Call(SqlFn.AND, args));
        }
        for (String f : Pure.nativeKeysAt("or")) {
            RULES.put(f, (n, args) -> args.size() == 1
                    ? (isToOne(n.args().get(0))
                            && !(args.get(0) instanceof SqlExpr.ArrayLit)
                            ? args.get(0)
                            : SqlExpr.Call.of(SqlFn.COALESCE,
                                    new SqlExpr.Call(SqlFn.LIST_BOOL_OR, args),
                                    new SqlExpr.BoolLit(false)))
                    : new SqlExpr.Call(SqlFn.OR, args));
        }
        // not(equal(a,b)) renders as a <> b (lean SQL): the peephole keeps
        // SqlFn.NOT_EQUAL alive even though the QUERY spelling is not(equal)
        // (real pure has no notEqual — FQN_MIGRATION finding).
        for (String f : Pure.nativeKeysAt("not")) {
            RULES.put(f, (n, args) ->
                    args.get(0) instanceof SqlExpr.Call c && c.fn() == SqlFn.EQUAL
                            ? new SqlExpr.Call(SqlFn.NOT_EQUAL, c.args())
                            : new SqlExpr.Call(SqlFn.NOT, args));
        }
        // UNARY plus/minus (the parser's -x => minus(x) desugar): a 1-arg
        // minus NEGATES — the binary operator renderer would silently DROP
        // the sign of a lone operand (audit: [-5, -3] executed as [5, 3]).
        for (String f : Pure.nativeKeysAt("plus")) {
            RULES.put(f, (n, args) -> {
                if (args.size() == 1 && isToOne(n.args().get(0))
                        && !(args.get(0) instanceof SqlExpr.ArrayLit)) {
                    return args.get(0);   // unary +x
                }
                // plus<T>(values:T[*]) is the COLLECTION SUM (real pure) —
                // the infix renderer would emit a lone list bare (audit).
                if (args.size() == 1) {
                    return new SqlExpr.Call(SqlFn.LIST_SUM, args);
                }
                return new SqlExpr.Call(SqlFn.PLUS, hugeWiden(args));
            });
        }
        for (String f : Pure.nativeKeysAt("times")) {
            RULES.put(f, (n, args) -> {
                if (args.size() == 1 && isToOne(n.args().get(0))
                        && !(args.get(0) instanceof SqlExpr.ArrayLit)) {
                    return args.get(0);
                }
                // times<T>(values:T[*]) is the COLLECTION PRODUCT (real pure).
                if (args.size() == 1) {
                    return new SqlExpr.Call(SqlFn.LIST_PRODUCT, args);
                }
                return new SqlExpr.Call(SqlFn.TIMES, hugeWiden(args));
            });
        }
        for (String f : Pure.nativeKeysAt("minus")) {
            RULES.put(f, (n, args) -> {
                if (args.size() != 1) {
                    return new SqlExpr.Call(SqlFn.MINUS, hugeWiden(args));
                }
                // minus<T>(values:T[*]) LEFT-FOLDS subtraction (real pure:
                // [10,3,2] -> 5); the seed is the first element. A SINGLETON
                // LIST LITERAL is a list (the reduction of [x] is x, via the
                // fold), not a unary negate (audit).
                if (!isToOne(n.args().get(0))
                        || args.get(0) instanceof SqlExpr.ArrayLit) {
                    return SqlExpr.Call.of(SqlFn.LIST_REDUCE, args.get(0),
                            new SqlExpr.Lambda(List.of("_ma", "_mb"),
                                    SqlExpr.Call.of(SqlFn.MINUS,
                                            new SqlExpr.Column(null, "_ma"),
                                            new SqlExpr.Column(null, "_mb"))));
                }
                return switch (args.get(0)) {
                    case SqlExpr.IntLit i -> new SqlExpr.IntLit(-i.value());
                    case SqlExpr.FloatLit fl -> new SqlExpr.FloatLit(-fl.value());
                    case SqlExpr.DecimalLit d -> new SqlExpr.DecimalLit(d.value().negate());
                    case SqlExpr e -> new SqlExpr.Call(SqlFn.MINUS,
                            List.of(new SqlExpr.IntLit(0), e));
                };
            });
        }
        // times registers ABOVE (collection-product overload needs its own rule).
        // Bit shifts: the shifted value casts to BIGINT (a bare literal is
        // INT32 to DuckDB, and 1 << 46 overflows it); real pure bounds the
        // shift amount at 62 — beyond is a LOUD error, not a silent 0.
        for (String name : List.of("bitShiftLeft", "bitShiftRight")) {
            SqlFn fn = name.equals("bitShiftLeft")
                    ? SqlFn.BIT_SHIFT_LEFT : SqlFn.BIT_SHIFT_RIGHT;
            for (String f : Pure.nativeKeysAt(name)) {
                RULES.put(f, (n, args) -> {
                    if (args.get(1) instanceof SqlExpr.IntLit sh
                            && (sh.value() < 0 || sh.value() > 62)) {
                        throw new IllegalStateException(name + " shift amount "
                                + sh.value() + " is out of range [0, 62]");
                    }
                    return SqlExpr.Call.of(fn,
                            new SqlExpr.Cast(args.get(0),
                                    com.legend.sql.SqlType.Scalar.BIGINT),
                            args.get(1));
                });
            }
        }
        // divide: the 3-arg overload carries a SCALE — BigDecimal HALF_UP
        // (SQL ROUND, half away from zero); plain division otherwise.
        // Integer arithmetic near the INT64 edge computes in HUGEINT
        // (2 * maxLong is a real PCT value).
        for (String f : Pure.nativeKeysAt("divide")) {
            RULES.put(f, (n, args) -> args.size() == 3
                    ? new SqlExpr.Call(SqlFn.ROUND_HALF_UP, List.of(
                            SqlExpr.Call.of(SqlFn.DIVIDE, args.get(0), args.get(1)),
                            args.get(2)))
                    : new SqlExpr.Call(SqlFn.DIVIDE, args));
        }
        family(SqlFn.MOD, "mod");
        family(SqlFn.REM, "rem");
        family(SqlFn.ABS, "abs");
        // isEmpty/isNotEmpty are TYPE-aware: a to-MANY argument is a SQL
        // LIST value (toMany(@T) et al.) — emptiness is length, not
        // NULL-ness (isEmpty([]) = true; IS NULL said false). Scalar
        // ([0..1]) stays the null test.
        for (String f : Pure.nativeKeysAt("isEmpty")) {
            RULES.put(f, (n, args) -> listValued(n.args().get(0))
                    ? new SqlExpr.Call(SqlFn.EQUAL, List.of(
                            SqlExpr.Call.of(SqlFn.COALESCE,
                                    SqlExpr.Call.of(SqlFn.LIST_LENGTH, args.get(0)),
                                    new SqlExpr.IntLit(0)),
                            new SqlExpr.IntLit(0)))
                    : new SqlExpr.Call(SqlFn.IS_NULL, args));
        }
        for (String f : Pure.nativeKeysAt("isNotEmpty")) {
            RULES.put(f, (n, args) -> listValued(n.args().get(0))
                    ? new SqlExpr.Call(SqlFn.GREATER, List.of(
                            SqlExpr.Call.of(SqlFn.COALESCE,
                                    SqlExpr.Call.of(SqlFn.LIST_LENGTH, args.get(0)),
                                    new SqlExpr.IntLit(0)),
                            new SqlExpr.IntLit(0)))
                    : new SqlExpr.Call(SqlFn.IS_NOT_NULL, args));
        }
        family(SqlFn.LENGTH, "length");
        family(SqlFn.UPPER, "toUpper");
        family(SqlFn.LOWER, "toLower");

        // toOne erases in SQL (MUST-honor: multiplicity narrowing is a no-op value-wise).
        for (String f : Pure.nativeKeysAt("toOne")) {
            RULES.put(f, (n, args) -> args.get(0));
        }

        // exists/forAll over collections: SEMANTIC vocabulary entries whose
        // CONTRACT includes Pure's empty-collection semantics (exists([]) =
        // false, forAll([]) = true) — every dialect's expansion must honor
        // them (DuckDB: coalesce over list_bool_* lambdas).
        family(SqlFn.LIST_EXISTS, "exists");
        family(SqlFn.LIST_FOR_ALL, "forAll");

        // ---- the registration grind (corpus-driven; MUST-honor templates) ----
        // Math (ROUND is banker's per the semantics contract).
        for (var e : Map.ofEntries(
                Map.entry("sqrt", SqlFn.SQRT), Map.entry("cbrt", SqlFn.CBRT),
                Map.entry("exp", SqlFn.EXP), Map.entry("log", SqlFn.LN),
                Map.entry("log10", SqlFn.LOG10), Map.entry("pow", SqlFn.POW),
                Map.entry("pi", SqlFn.PI),
                Map.entry("sin", SqlFn.SIN), Map.entry("cos", SqlFn.COS),
                Map.entry("tan", SqlFn.TAN), Map.entry("asin", SqlFn.ASIN),
                Map.entry("acos", SqlFn.ACOS), Map.entry("atan", SqlFn.ATAN),
                Map.entry("atan2", SqlFn.ATAN2), Map.entry("sinh", SqlFn.SINH),
                Map.entry("cosh", SqlFn.COSH), Map.entry("tanh", SqlFn.TANH),
                Map.entry("ceiling", SqlFn.CEILING), Map.entry("floor", SqlFn.FLOOR),
                Map.entry("sign", SqlFn.SIGN),
                Map.entry("xor", SqlFn.XOR),
                Map.entry("bitAnd", SqlFn.BIT_AND), Map.entry("bitOr", SqlFn.BIT_OR),
                Map.entry("bitXor", SqlFn.BIT_XOR),

                // Strings — plain families first; index-shifted below.
                Map.entry("startsWith", SqlFn.STARTS_WITH),
                Map.entry("endsWith", SqlFn.ENDS_WITH),
                Map.entry("matches", SqlFn.MATCHES),
                Map.entry("left", SqlFn.LEFT), Map.entry("right", SqlFn.RIGHT),

                Map.entry("trim", SqlFn.TRIM), Map.entry("ltrim", SqlFn.LTRIM),
                Map.entry("rtrim", SqlFn.RTRIM), Map.entry("replace", SqlFn.REPLACE),
                Map.entry("split", SqlFn.SPLIT),
                Map.entry("reverseString", SqlFn.REVERSE_STRING),
                Map.entry("ascii", SqlFn.ASCII_CODE), Map.entry("char", SqlFn.CHR),
                Map.entry("toUpperFirstCharacter", SqlFn.UC_FIRST),
                Map.entry("toLowerFirstCharacter", SqlFn.LC_FIRST),
                Map.entry("encodeBase64", SqlFn.ENCODE_BASE64),
                Map.entry("levenshteinDistance", SqlFn.LEVENSHTEIN),
                Map.entry("generateGuid", SqlFn.GUID),
                Map.entry("hash", SqlFn.HASH), Map.entry("hashCode", SqlFn.HASH),
                Map.entry("coalesce", SqlFn.COALESCE),
                // Temporal
                Map.entry("today", SqlFn.TODAY), Map.entry("now", SqlFn.NOW),

                // Lists / collections


                Map.entry("median", SqlFn.LIST_MEDIAN),

                Map.entry("range", SqlFn.RANGE_FN),
                Map.entry("toVariant", SqlFn.TO_VARIANT)).entrySet()) {
            familyIfPresent(e.getValue(), e.getKey());
        }
        // ---- Date family (H-audit registrations bucket) ----
        // Numeric extractions ride EXTRACT with a part literal.
        for (var e : Map.of("dayOfYear", "doy", "weekOfYear", "week",
                "dayOfWeekNumber", "isodow", "quarterNumber", "quarter").entrySet()) {
            for (String f : Pure.nativeKeysAt(e.getKey())) {
                RULES.put(f, (n, args) -> new SqlExpr.Call(SqlFn.EXTRACT, List.of(
                        new SqlExpr.StringLit(e.getValue()),
                        dateArg(n.args().get(0), args.get(0)))));
            }
        }
        // Calendar-enum extractions: names match the Pure enum values
        // (Monday…, January… — the corpus's enum-by-name convention).
        // dayOfWeek()/month(): real pure returns calendar ENUMS (Monday…,
        // January…); the engine surface is NUMERIC (DuckDB dow: Sunday=0;
        // month 1-12) — the corpus reads both as Numbers.
        for (String f : Pure.nativeKeysAt("dayOfWeek")) {
            // real dayOfWeek():DayOfWeek — the value surface is the ENUM
            // NAME ('Saturday'), the enum-by-name convention every other
            // enum position uses (was DuckDB dow numbers, an engine-lite
            // relic the corpus no longer pins).
            RULES.put(f, (n, args) -> SqlExpr.Call.of(SqlFn.STRFTIME,
                    dateArg(n.args().get(0), args.get(0)),
                    new SqlExpr.StringLit("%A")));
        }
        for (String f : Pure.nativeKeysAt("month")) {
            RULES.put(f, (n, args) -> new SqlExpr.Call(SqlFn.EXTRACT, List.of(
                    new SqlExpr.StringLit("month"),
                    dateArg(n.args().get(0), args.get(0)))));
        }
        // quarter(): real pure returns the Quarter ENUM (Q1..Q4, with an
        // upstream TODO to make them numbers); the engine surface is the
        // bare integer — the corpus reads it as a Number.
        for (String f : Pure.nativeKeysAt("quarter")) {
            RULES.put(f, (n, args) -> new SqlExpr.Call(SqlFn.EXTRACT, List.of(
                    new SqlExpr.StringLit("quarter"),
                    dateArg(n.args().get(0), args.get(0)))));
        }
        // Truncations: DATE_TRUNC with the part literal.
        for (var e : Map.of("firstDayOfMonth", "month", "firstDayOfYear", "year",
                "firstDayOfWeek", "week", "firstDayOfQuarter", "quarter",
                "firstHourOfDay", "day", "firstMinuteOfHour", "hour",
                "firstSecondOfMinute", "minute",
                "firstMillisecondOfSecond", "second").entrySet()) {
            for (String f : Pure.nativeKeysAt(e.getKey())) {
                RULES.put(f, (n, args) -> new SqlExpr.Call(SqlFn.DATE_TRUNC, List.of(
                        new SqlExpr.StringLit(e.getValue()),
                        dateArg(n.args().get(0), args.get(0)))));
            }
        }
        // adjust(d, n, unit) / timeBucket(d, n, unit): the DurationUnit enum
        // literal selects DuckDB's interval-constructor function.
        for (String f : Pure.nativeKeysAt("adjust")) {
            RULES.put(f, (n, args) -> {
                SqlExpr added = new SqlExpr.Call(SqlFn.ADD_INTERVAL, List.of(
                        new SqlExpr.StringLit(intervalFn(n.args().get(2))),
                        args.get(1), dateArg(n.args().get(0), args.get(0))));
                // A PARTIAL-date operand keeps its precision: pad in (dateArg),
                // adjust, then truncate BACK to the written form —
                // adjust(%2016, 1, YEARS) is %2017, not 2017-01-01.
                Integer pp = partialPrecision(n.args().get(0));
                if (pp != null) {
                    // The result's precision is the FINER of the written
                    // precision and the unit (real pure GROWS precision:
                    // adjust(%2020, 1, MONTHS) is 2020-02; a coarse unit
                    // keeps the written form: adjust(%2016, 1, YEARS) is
                    // 2017; a day-or-finer unit yields the full-precision
                    // carrier — the audit's truncate-everything write-back
                    // silently erased finer adjustments).
                    String fmt = switch (enumName(n.args().get(2))) {
                        case "YEARS" -> pp == 1 ? "%Y" : "%Y-%m";
                        case "MONTHS" -> "%Y-%m";
                        default -> null;
                    };
                    return fmt == null ? added
                            : SqlExpr.Call.of(SqlFn.STRFTIME, added,
                                    new SqlExpr.StringLit(fmt));
                }
                // SQL date+interval widens to TIMESTAMP; a StrictDate input
                // adjusted by a DAY-or-coarser unit stays a StrictDate.
                boolean strictIn = n.args().get(0).info().type()
                        == com.legend.compiler.element.type.Type.Primitive.STRICT_DATE;
                boolean coarse = switch (enumName(n.args().get(2))) {
                    case "YEARS", "MONTHS", "WEEKS", "DAYS" -> true;
                    default -> false;
                };
                return strictIn && coarse
                        ? new SqlExpr.Cast(added, com.legend.sql.SqlType.Scalar.DATE)
                        : added;
            });
        }
        // datePart of a PARTIAL literal is the IDENTITY (a year has no finer
        // date part); full-precision values truncate to the day.
        for (String f : Pure.nativeKeysAt("datePart")) {
            RULES.put(f, (n, args) -> partialPrecision(n.args().get(0)) != null
                    ? args.get(0)
                    : new SqlExpr.Call(SqlFn.DATE_TRUNC_DAY, args));
        }
        for (String f : Pure.nativeKeysAt("timeBucket")) {
            RULES.put(f, (n, args) -> {
                boolean strict = n.args().get(0).info().type()
                        == com.legend.compiler.element.type.Type.Primitive.STRICT_DATE;
                // real timeBucket REJECTS sub-day units on StrictDate —
                // message verbatim (strictDate assertError family)
                if (strict) {
                    switch (enumName(n.args().get(2))) {
                        case "HOURS", "MINUTES", "SECONDS", "MILLISECONDS",
                                "MICROSECONDS", "NANOSECONDS" ->
                            throw new com.legend.error.ModelException(
                                    com.legend.error.LegendCompileException.Phase.LOWER,
                                    "Unsupported duration unit for StrictDate. Units"
                                            + " can only be: [YEARS, DAYS, MONTHS, WEEKS]");
                        default -> { }
                    }
                }
                SqlExpr bucketed = new SqlExpr.Call(SqlFn.TIME_BUCKET, List.of(
                        new SqlExpr.StringLit(intervalFn(n.args().get(2))),
                        args.get(1), dateArg(n.args().get(0), args.get(0))));
                if (strict) {
                    return new SqlExpr.Cast(bucketed, com.legend.sql.SqlType.Scalar.DATE);
                }
                // The result keeps the INPUT LITERAL's print precision: a
                // 9-digit-subsecond input buckets to a 9-digit-zero result
                // (real pure preserves subsecond DIGIT COUNT; bucketed
                // subseconds are always zero). Emitted as the precision-
                // faithful STRING — the wire's date convention.
                if (n.args().get(0) instanceof com.legend.compiler.spec.typed.TypedCDate cd
                        && cd.value() instanceof
                                com.legend.values.PureDateLiteral.DateWithSubsecond sub) {
                    return SqlExpr.Call.of(SqlFn.CONCAT,
                            SqlExpr.Call.of(SqlFn.STRFTIME, bucketed,
                                    new SqlExpr.StringLit("%Y-%m-%dT%H:%M:%S")),
                            new SqlExpr.StringLit(
                                    "." + "0".repeat(sub.subsecond().length())));
                }
                return bucketed;
            });
        }
        // dateDiff(d1, d2, unit): Pure semantics per unit (PCT-pinned) —
        // see dateDiffExpr.
        for (String f : Pure.nativeKeysAt("dateDiff")) {
            RULES.put(f, (n, args) -> dateDiffExpr(diffPart(n.args().get(2)),
                    dateArg(n.args().get(0), args.get(0)),
                    dateArg(n.args().get(1), args.get(1))));
        }
        // Epoch conversions: toEpochValue(d, unit) IS dateDiff(epoch, d,
        // unit) for EVERY DurationUnit (real pure dateExtension); the bare
        // form is SECONDS. (The audit: non-MILLISECONDS units were silently
        // epoch seconds.)
        for (String f : Pure.nativeKeysAt("toEpochValue")) {
            RULES.put(f, (n, args) -> dateDiffExpr(
                    n.args().size() > 1 ? diffPart(n.args().get(1)) : "second",
                    new SqlExpr.TimestampLit("1970-01-01 00:00:00"),
                    dateArg(n.args().get(0), args.get(0))));
        }
        // fromEpochValue(n, unit) = epoch + n unit-intervals.
        for (String f : Pure.nativeKeysAt("fromEpochValue")) {
            RULES.put(f, (n, args) -> new SqlExpr.Call(SqlFn.ADD_INTERVAL, List.of(
                    new SqlExpr.StringLit(n.args().size() > 1
                            ? intervalFn(n.args().get(1)) : "to_seconds"),
                    args.get(0),
                    new SqlExpr.TimestampLit("1970-01-01 00:00:00"))));
        }
        // Day-granularity comparisons.
        for (String f : Pure.nativeKeysAt("isOnDay")) {
            RULES.put(f, (n, args) -> SqlExpr.Call.of(SqlFn.EQUAL,
                    new SqlExpr.Call(SqlFn.DATE_TRUNC_DAY,
                            List.of(dateArg(n.args().get(0), args.get(0)))),
                    new SqlExpr.Call(SqlFn.DATE_TRUNC_DAY,
                            List.of(dateArg(n.args().get(1), args.get(1))))));
        }
        for (String f : Pure.nativeKeysAt("isAfterDay")) {
            RULES.put(f, (n, args) -> SqlExpr.Call.of(SqlFn.GREATER,
                    new SqlExpr.Call(SqlFn.DATE_TRUNC_DAY,
                            List.of(dateArg(n.args().get(0), args.get(0)))),
                    new SqlExpr.Call(SqlFn.DATE_TRUNC_DAY,
                            List.of(dateArg(n.args().get(1), args.get(1))))));
        }
        for (String f : Pure.nativeKeysAt("isOnOrAfterDay")) {
            RULES.put(f, (n, args) -> SqlExpr.Call.of(SqlFn.GREATER_EQUAL,
                    new SqlExpr.Call(SqlFn.DATE_TRUNC_DAY,
                            List.of(dateArg(n.args().get(0), args.get(0)))),
                    new SqlExpr.Call(SqlFn.DATE_TRUNC_DAY,
                            List.of(dateArg(n.args().get(1), args.get(1))))));
        }
        // Precision predicates: a LITERAL answers from its own written
        // precision; a column answers from its Pure type (StrictDate =
        // day precision, DateTime = SQL TIMESTAMP = full precision).
        for (var e : Map.of("hasMonth", 1, "hasDay", 2, "hasHour", 3,
                "hasMinute", 4, "hasSecond", 5, "hasSubsecond", 6).entrySet()) {
            for (String f : Pure.nativeKeysAt(e.getKey())) {
                RULES.put(f, (n, args) -> {
                    boolean has = datePrecision(n.args().get(0)) >= e.getValue();
                    // A LITERAL answers boolean (the PCT spelling); a COLUMN
                    // answers 1/0 — the engine's integer surface for date
                    // precision checks over stored values.
                    return n.args().get(0) instanceof com.legend.compiler.spec.typed.TypedCDate
                            ? new SqlExpr.BoolLit(has)
                            : new SqlExpr.IntLit(has ? 1 : 0);
                });
            }
        }
        for (String f : Pure.nativeKeysAt("hasSubsecondWithAtLeastPrecision")) {
            RULES.put(f, (n, args) -> {
                if (!(n.args().get(1)
                        instanceof com.legend.compiler.spec.typed.TypedCInteger i)) {
                    throw new IllegalStateException("hasSubsecondWithAtLeastPrecision"
                            + " needs a literal precision");
                }
                long p2 = i.value().longValue();
                // A LITERAL answers from its WRITTEN digit count (PCT); a
                // TIMESTAMP column is microsecond-precision.
                if (n.args().get(0) instanceof com.legend.compiler.spec.typed.TypedCDate d
                        && d.value() instanceof com.legend.values.PureDateLiteral.DateWithSubsecond ds) {
                    return new SqlExpr.BoolLit(ds.subsecond().length() >= p2);
                }
                return new SqlExpr.BoolLit(datePrecision(n.args().get(0)) >= 6 && p2 <= 6);
            });
        }
        // ---- Misc (registrations bucket) ----
        for (String f : Pure.nativeKeysAt("between")) {
            RULES.put(f, (n, args) -> SqlExpr.Call.of(SqlFn.AND,
                    SqlExpr.Call.of(SqlFn.GREATER_EQUAL, args.get(0), args.get(1)),
                    SqlExpr.Call.of(SqlFn.LESS_EQUAL, args.get(0), args.get(2))));
        }
        for (String f : Pure.nativeKeysAt("compare")) {
            RULES.put(f, (n, args) -> {
                // CROSS-KIND compare is a CONSTANT: real Compare.java orders
                // Numbers < Dates < Booleans < Strings and never coerces —
                // SQL's coercion made compare(5, '5') zero.
                int k0 = compareKind(n.args().get(0).info().type());
                int k1 = compareKind(n.args().get(1).info().type());
                if (k0 >= 0 && k1 >= 0 && k0 != k1) {
                    return new SqlExpr.IntLit(Integer.compare(k0, k1));
                }
                // DATE operands compare CHRONOLOGICALLY on their padded
                // TIMESTAMP comparables — the partial-date STRING carrier
                // orders '2001' > '10999' lexically. Value work is SQL
                // (strptime pads; the compiler only names the format).
                SqlExpr lhs = dateComparableOrSelf(n.args().get(0), args.get(0));
                SqlExpr rhs = dateComparableOrSelf(n.args().get(1), args.get(1));
                return new SqlExpr.Case(List.of(
                        new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.LESS,
                                lhs, rhs), new SqlExpr.IntLit(-1)),
                        new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.GREATER,
                                lhs, rhs), new SqlExpr.IntLit(1))),
                        new SqlExpr.IntLit(0));
            });
        }
        for (String f : Pure.nativeKeysAt("sqlTrue")) {
            RULES.put(f, (n, args) -> new SqlExpr.BoolLit(true));
        }
        for (String f : Pure.nativeKeysAt("sqlFalse")) {
            RULES.put(f, (n, args) -> new SqlExpr.BoolLit(false));
        }
        familyIfPresent(SqlFn.CURRENT_USER_FN, "currentUserId");
        familyIfPresent(SqlFn.COT, "cot");
        familyIfPresent(SqlFn.RADIANS, "toRadians");
        familyIfPresent(SqlFn.DEGREES, "toDegrees");
        familyIfPresent(SqlFn.REPEAT_STR, "repeatString");
        familyIfPresent(SqlFn.JARO_WINKLER, "jaroWinklerSimilarity");
        // decodeBase64 accepts UNPADDED input (real pure; SQL from_base64
        // demands padding) — restore the '=' tail: literal-folded, or
        // s || repeat('=', (4 - length(s) % 4) % 4) at runtime.
        for (String f : Pure.nativeKeysAt("decodeBase64")) {
            RULES.put(f, (n, args) -> {
                SqlExpr in = args.get(0);
                if (in instanceof SqlExpr.StringLit lit) {
                    String v = lit.value();
                    in = new SqlExpr.StringLit(v + "=".repeat((4 - v.length() % 4) % 4));
                } else {
                    SqlExpr pad = SqlExpr.Call.of(SqlFn.MOD,
                            SqlExpr.Call.of(SqlFn.MINUS, new SqlExpr.IntLit(4),
                                    SqlExpr.Call.of(SqlFn.MOD,
                                            SqlExpr.Call.of(SqlFn.LENGTH, in),
                                            new SqlExpr.IntLit(4))),
                            new SqlExpr.IntLit(4));
                    in = SqlExpr.Call.of(SqlFn.CONCAT, in,
                            SqlExpr.Call.of(SqlFn.REPEAT_STR,
                                    new SqlExpr.StringLit("="), pad));
                }
                return SqlExpr.Call.of(SqlFn.DECODE_BASE64, in);
            });
        }
        // size(NULL list) is pure's EMPTY collection: 0, never NULL
        for (String f : Pure.nativeKeysAt("size")) {
            RULES.put(f, (n, args) -> SqlExpr.Call.of(SqlFn.COALESCE,
                    SqlExpr.Call.of(SqlFn.LIST_LENGTH, args.get(0)),
                    new SqlExpr.IntLit(0)));
        }
        familyIfPresent(SqlFn.MINUS, "sub");
        // joinStrings over a LIST value: (list), (list, sep), or
        // (list, prefix, sep, suffix).
        for (String f : Pure.nativeKeysAt("joinStrings")) {
            RULES.put(f, (n, args) -> {
                // A TO-ONE source IS the joined string; an EMPTY list joins
                // to '' (list_aggregate over NULL/[] is NULL — coalesce).
                SqlExpr joined;
                if (isToOne(n.args().get(0)) && !(args.get(0) instanceof SqlExpr.ArrayLit)) {
                    joined = args.get(0);
                } else {
                    SqlExpr sep = args.size() == 2 ? args.get(1)
                            : args.size() == 4 ? args.get(2) : new SqlExpr.StringLit("");
                    joined = SqlExpr.Call.of(SqlFn.COALESCE,
                            new SqlExpr.Call(SqlFn.LIST_AGG, List.of(
                                    new SqlExpr.StringLit("string_agg"), args.get(0), sep)),
                            new SqlExpr.StringLit(""));
                }
                if (args.size() == 4) {
                    return SqlExpr.Call.of(SqlFn.CONCAT,
                            SqlExpr.Call.of(SqlFn.CONCAT, args.get(1), joined),
                            args.get(3));
                }
                return joined;
            });
        }
        // percentile family over LIST values; the 4-arg overload's
        // ascending/continuous flags choose the quantile flavor, and a
        // DESCENDING percentile is the 1-p quantile.
        for (String f : Pure.nativeKeysAt("percentile")) {
            RULES.put(f, (n, args) -> {
                boolean asc = true;
                boolean cont = true;
                if (n.args().size() == 4) {
                    asc = boolLiteral(n.args().get(2), "percentile ascending");
                    cont = boolLiteral(n.args().get(3), "percentile continuous");
                }
                if (cont) {
                    SqlExpr p2 = asc ? args.get(1)
                            : SqlExpr.Call.of(SqlFn.MINUS,
                                    new SqlExpr.IntLit(1), args.get(1));
                    return new SqlExpr.Call(SqlFn.LIST_AGG, List.of(
                            new SqlExpr.StringLit("quantile_cont"),
                            args.get(0), p2));
                }
                return pureDiscretePercentile(args.get(0), args.get(1), asc);
            });
        }
        for (String f : Pure.nativeKeysAt("percentileCont")) {
            RULES.put(f, (n, args) -> new SqlExpr.Call(SqlFn.LIST_AGG, List.of(
                    new SqlExpr.StringLit("quantile_cont"), args.get(0), args.get(1))));
        }
        for (String f : Pure.nativeKeysAt("percentileDisc")) {
            RULES.put(f, (n, args) -> new SqlExpr.Call(SqlFn.LIST_AGG, List.of(
                    new SqlExpr.StringLit("quantile_disc"), args.get(0), args.get(1))));
        }
        // collection sort: bare list_sort; a COMPARATOR must be a bare
        // compare over the two parameters (its argument order IS the
        // direction); a KEY function sorts {k, i, v} structs by key —
        // index second, so equal keys stay stable — then unwraps.
        for (String f : Pure.nativeKeysAt("sort")) {
            RULES.put(f, (n, args) -> {
                if (n.args().size() == 1) {
                    MixedElems mx = mixedElems(n.args().get(0), args.get(0));
                    if (mx != null) {
                        // identity-preserving mixed sort: order the ids by
                        // their comparables (parallel select-list unnests)
                        var inner = new com.legend.sql.SqlSelect(List.of(
                                new com.legend.sql.SqlSelect.Projection(
                                        SqlExpr.Call.of(SqlFn.UNNEST, mx.idList()), "i"),
                                new com.legend.sql.SqlSelect.Projection(
                                        SqlExpr.Call.of(SqlFn.UNNEST, mx.valList()), "v")),
                                false, null, null, List.of(), null, null, List.of(),
                                null, null, List.of());
                        var src = new com.legend.sql.SqlSource.Subselect(inner, "_mx");
                        var outer = new com.legend.sql.SqlSelect(List.of(
                                new com.legend.sql.SqlSelect.Projection(
                                        new SqlExpr.OrderedListAgg(
                                                new SqlExpr.Column("_mx", "i"),
                                                new SqlExpr.Column("_mx", "v")), "s")),
                                false, src, null, List.of(), null, null, List.of(),
                                null, null, List.of());
                        return new SqlExpr.ScalarSubquery(outer);
                    }
                    return new SqlExpr.Call(SqlFn.LIST_SORT, List.of(args.get(0)));
                }
                Boolean asc = comparatorDirection(
                        n.args().get(n.args().size() - 1));
                if (asc == null) {
                    throw new IllegalStateException("sort comparator must be a"
                            + " bare compare over its two parameters");
                }
                if (n.args().size() == 2) {
                    return new SqlExpr.Call(
                            asc ? SqlFn.LIST_SORT : SqlFn.LIST_SORT_DESC,
                            List.of(args.get(0)));
                }
                if (!(args.get(1) instanceof SqlExpr.Lambda key)
                        || key.params().size() != 1) {
                    throw new IllegalStateException(
                            "sort expects (values, key-function, comparator)");
                }
                SqlExpr i = new SqlExpr.Column(null, "_st_i");
                SqlExpr valAt = SqlExpr.Call.of(SqlFn.LIST_GET, args.get(0), i);
                SqlExpr keyExpr = substituteRef(key.body(), key.params().get(0), valAt);
                SqlExpr idxField = asc ? i
                        : SqlExpr.Call.of(SqlFn.MINUS, new SqlExpr.IntLit(0), i);
                SqlExpr pairs = SqlExpr.Call.of(SqlFn.LIST_TRANSFORM,
                        SqlExpr.Call.of(SqlFn.RANGE_FN, new SqlExpr.IntLit(1),
                                plusOne(SqlExpr.Call.of(SqlFn.LIST_LENGTH, args.get(0)))),
                        new SqlExpr.Lambda(List.of("_st_i"),
                                new SqlExpr.StructLit(List.of(
                                        new SqlExpr.StructLit.Field("k", keyExpr),
                                        new SqlExpr.StructLit.Field("i", idxField),
                                        new SqlExpr.StructLit.Field("v", valAt)))));
                SqlExpr sorted = new SqlExpr.Call(
                        asc ? SqlFn.LIST_SORT : SqlFn.LIST_SORT_DESC, List.of(pairs));
                return SqlExpr.Call.of(SqlFn.LIST_TRANSFORM, sorted,
                        new SqlExpr.Lambda(List.of("_st_e"),
                                new SqlExpr.StructGet(
                                        new SqlExpr.Column(null, "_st_e"), "v")));
            });
        }
        for (String f : Pure.nativeKeysAt("isBeforeDay")) {
            RULES.put(f, (n, args) -> SqlExpr.Call.of(SqlFn.LESS,
                    new SqlExpr.Call(SqlFn.DATE_TRUNC_DAY,
                            List.of(dateArg(n.args().get(0), args.get(0)))),
                    new SqlExpr.Call(SqlFn.DATE_TRUNC_DAY,
                            List.of(dateArg(n.args().get(1), args.get(1))))));
        }
        for (String f : Pure.nativeKeysAt("isOnOrBeforeDay")) {
            RULES.put(f, (n, args) -> SqlExpr.Call.of(SqlFn.LESS_EQUAL,
                    new SqlExpr.Call(SqlFn.DATE_TRUNC_DAY,
                            List.of(dateArg(n.args().get(0), args.get(0)))),
                    new SqlExpr.Call(SqlFn.DATE_TRUNC_DAY,
                            List.of(dateArg(n.args().get(1), args.get(1))))));
        }
        for (String f : Pure.nativeKeysAt("toDecimal")) {
            // LITERALS fold to a true-scale decimal (toDecimal(3.8) is 3.8D,
            // toDecimal(8) is 8D) — CAST(x AS DECIMAL(38,18)) fabricates
            // eighteen zeros of scale the value never had.
            RULES.put(f, (n, args) -> switch (args.get(0)) {
                case SqlExpr.IntLit i ->
                        // a bare integral literal types INTEGER — cast keeps
                        // it a scale-0 DECIMAL (8D)
                        new SqlExpr.Cast(i, new com.legend.sql.SqlType.Decimal(38, 0));
                case SqlExpr.DecimalLit d -> d;
                case SqlExpr.FloatLit fl ->
                        new SqlExpr.DecimalLit(java.math.BigDecimal.valueOf(fl.value()));
                default -> new SqlExpr.Cast(args.get(0),
                        new com.legend.sql.SqlType.Decimal(38, 18));
            });
        }
        for (String f : Pure.nativeKeysAt("divideRound")) {
            RULES.put(f, (n, args) -> new SqlExpr.Call(SqlFn.ROUND, List.of(
                    SqlExpr.Call.of(SqlFn.DIVIDE, args.get(0), args.get(1)),
                    args.get(2))));
        }
        for (String f : Pure.nativeKeysAt("notEqualAnsi")) {
            RULES.put(f, (n, args) -> SqlExpr.Call.of(SqlFn.NOT_EQUAL,
                    args.get(0), args.get(1)));
        }

        // Temporal EXTRACT parts: one SqlFn entry, part-name literal first.
        for (var e : Map.of(
                "year", "year", "monthNumber", "month", "dayOfMonth", "day",
                "hour", "hour", "minute", "minute", "second", "second").entrySet()) {
            for (String f : Pure.nativeKeysAt(e.getKey())) {
                RULES.put(f, (n, args) -> {
                    List<SqlExpr> withPart = new ArrayList<>();
                    withPart.add(new SqlExpr.StringLit(e.getValue()));
                    withPart.addAll(args);
                    return new SqlExpr.Call(SqlFn.EXTRACT, withPart);
                });
            }
        }
        // Collection min/max/sum: 1-arg = over a LIST; 2-arg = least/greatest.
        // A TO-ONE argument (sum(7), average of one value) is the IDENTITY —
        // the list encodings choke on scalars.
        for (String f : Pure.nativeKeysAt("min")) {
            RULES.put(f, (n, args) -> {
                MixedElems mx = args.size() == 1 ? mixedElems(n.args().get(0), args.get(0)) : null;
                if (mx != null) {
                    return mx.select(SqlExpr.Call.of(SqlFn.LIST_MIN, mx.valList()));
                }
                if (args.size() == 2 && args.get(1) instanceof SqlExpr.Lambda cmp) {
                    // a TO-ONE collection is its own extreme
                    return isToOne(n.args().get(0)) ? args.get(0)
                            : comparatorSelect(args.get(0), cmp, false);
                }
                if (args.size() > 1) {
                    MixedElems ma = mixedArgs(n.args(), args);
                    return ma != null
                            ? ma.select(SqlExpr.Call.of(SqlFn.LIST_MIN, ma.valList()))
                            : new SqlExpr.Call(SqlFn.LEAST, args);
                }
                return isToOne(n.args().get(0)) ? args.get(0)
                        : new SqlExpr.Call(SqlFn.LIST_MIN, args);
            });
        }
        for (String f : Pure.nativeKeysAt("max")) {
            RULES.put(f, (n, args) -> {
                MixedElems mx = args.size() == 1 ? mixedElems(n.args().get(0), args.get(0)) : null;
                if (mx != null) {
                    return mx.select(SqlExpr.Call.of(SqlFn.LIST_MAX, mx.valList()));
                }
                if (args.size() == 2 && args.get(1) instanceof SqlExpr.Lambda cmp) {
                    // a TO-ONE collection is its own extreme
                    return isToOne(n.args().get(0)) ? args.get(0)
                            : comparatorSelect(args.get(0), cmp, true);
                }
                if (args.size() > 1) {
                    MixedElems ma = mixedArgs(n.args(), args);
                    return ma != null
                            ? ma.select(SqlExpr.Call.of(SqlFn.LIST_MAX, ma.valList()))
                            : new SqlExpr.Call(SqlFn.GREATEST, args);
                }
                return isToOne(n.args().get(0)) ? args.get(0)
                        : new SqlExpr.Call(SqlFn.LIST_MAX, args);
            });
        }
        for (String f : Pure.nativeKeysAt("sum")) {
            RULES.put(f, (n, args) -> isToOne(n.args().get(0)) ? args.get(0)
                    : new SqlExpr.Call(SqlFn.LIST_SUM, args));
        }
        // round(Number[1]) RETURNS Integer (real pure) — banker's round,
        // then the integral cast the signature promises; round(x, scale)
        // keeps its operand's type.
        for (String f : Pure.nativeKeysAt("round")) {
            RULES.put(f, (n, args) -> args.size() == 1
                    ? new SqlExpr.Cast(new SqlExpr.Call(SqlFn.ROUND, args),
                            com.legend.sql.SqlType.Scalar.BIGINT)
                    : new SqlExpr.Call(SqlFn.ROUND, args));
        }
        // greatest/least/mode take ONE collection argument (real pure: values:X[*]);
        // like min/max/sum, a to-one argument is the identity and a list reduces
        // with the list encoding — SQL's variadic GREATEST/LEAST never applies.
        for (String f : Pure.nativeKeysAt("greatest")) {
            RULES.put(f, (n, args) -> {
                MixedElems mx = mixedElems(n.args().get(0), args.get(0));
                if (mx != null) {
                    return mx.select(SqlExpr.Call.of(SqlFn.LIST_MAX, mx.valList()));
                }
                return isToOne(n.args().get(0)) ? args.get(0)
                        : new SqlExpr.Call(SqlFn.LIST_MAX, args);
            });
        }
        for (String f : Pure.nativeKeysAt("least")) {
            RULES.put(f, (n, args) -> {
                MixedElems mx = mixedElems(n.args().get(0), args.get(0));
                if (mx != null) {
                    return mx.select(SqlExpr.Call.of(SqlFn.LIST_MIN, mx.valList()));
                }
                return isToOne(n.args().get(0)) ? args.get(0)
                        : new SqlExpr.Call(SqlFn.LIST_MIN, args);
            });
        }
        for (String f : Pure.nativeKeysAt("mode")) {
            RULES.put(f, (n, args) -> {
                MixedElems mx = mixedElems(n.args().get(0), args.get(0));
                if (mx != null) {
                    // real mode.pure SORTS then folds runs: the representative
                    // is the LAST-ENCOUNTERED equal element (stable sort keeps
                    // encounter order) — the winner's last position in vals
                    SqlExpr winner = SqlExpr.Call.of(SqlFn.LIST_MODE, mx.valList());
                    SqlExpr lastPos = SqlExpr.Call.of(SqlFn.MINUS,
                            SqlExpr.Call.of(SqlFn.PLUS,
                                    SqlExpr.Call.of(SqlFn.LIST_LENGTH, mx.valList()),
                                    new SqlExpr.IntLit(1)),
                            SqlExpr.Call.of(SqlFn.LIST_POSITION,
                                    SqlExpr.Call.of(SqlFn.LIST_REVERSE, mx.valList()),
                                    winner));
                    return SqlExpr.Call.of(SqlFn.LIST_GET, mx.idList(), lastPos);
                }
                return isToOne(n.args().get(0)) ? args.get(0)
                        : new SqlExpr.Call(SqlFn.LIST_MODE, args);
            });
        }
        // zip(a, b) -> Pair<T,U>[*]: index over the SHORTER list (real pure
        // truncates; DuckDB's native list_zip PADS with NULL — wrong
        // semantics), each element a struct with Pair's first/second layout.
        for (String f : Pure.nativeKeysAt("zip")) {
            RULES.put(f, (n, args) -> {
                SqlExpr a = args.get(0), b = args.get(1);
                // An EMPTY side is SQL NULL and len(NULL) is NULL — which
                // LEAST would IGNORE (it skips nulls), silently zipping
                // against the non-empty side. Zero it explicitly.
                SqlExpr count = SqlExpr.Call.of(SqlFn.LEAST,
                        SqlExpr.Call.of(SqlFn.COALESCE,
                                SqlExpr.Call.of(SqlFn.LIST_LENGTH, a), new SqlExpr.IntLit(0)),
                        SqlExpr.Call.of(SqlFn.COALESCE,
                                SqlExpr.Call.of(SqlFn.LIST_LENGTH, b), new SqlExpr.IntLit(0)));
                SqlExpr i = new SqlExpr.Column(null, "_zip_i");
                SqlExpr body = new SqlExpr.StructLit(List.of(
                        new SqlExpr.StructLit.Field("first",
                                SqlExpr.Call.of(SqlFn.LIST_GET, a, i)),
                        new SqlExpr.StructLit.Field("second",
                                SqlExpr.Call.of(SqlFn.LIST_GET, b, i))));
                // An EMPTY side lowers as SQL NULL — the whole zip is then
                // NULL; the Pure contract is the EMPTY list.
                return SqlExpr.Call.of(SqlFn.COALESCE,
                        SqlExpr.Call.of(SqlFn.LIST_TRANSFORM,
                                SqlExpr.Call.of(SqlFn.RANGE_FN,
                                        new SqlExpr.IntLit(1), plusOne(count)),
                                new SqlExpr.Lambda(List.of("_zip_i"), body)),
                        new SqlExpr.ArrayLit(List.of()));
            });
        }
        for (String name : List.of("mean", "average")) {
            for (String f : Pure.nativeKeysAt(name)) {
                RULES.put(f, (n, args) -> isToOne(n.args().get(0)) ? args.get(0)
                        : new SqlExpr.Call(SqlFn.LIST_AVG, args));
            }
        }
        // Statistical list reductions: DuckDB list_aggregate(x, '<agg>').
        for (var e : Map.of(
                "stdDevSample", "stddev_samp", "stdDev", "stddev_samp",
                "stdDevPopulation", "stddev_pop",
                "varianceSample", "var_samp",
                "variancePopulation", "var_pop").entrySet()) {
            for (String f : Pure.nativeKeysAt(e.getKey())) {
                RULES.put(f, (n, args) -> new SqlExpr.Call(SqlFn.LIST_AGG, List.of(
                        new SqlExpr.StringLit(e.getValue()), args.get(0))));
            }
        }
        // variance(list, isBiasCorrected): true => sample, false => population.
        for (String f : Pure.nativeKeysAt("variance")) {
            RULES.put(f, (n, args) -> {
                boolean sample = n.args().size() <= 1
                        || boolLiteral(n.args().get(1), "variance isBiasCorrected");
                return new SqlExpr.Call(SqlFn.LIST_AGG, List.of(
                        new SqlExpr.StringLit(sample ? "var_samp" : "var_pop"),
                        args.get(0)));
            });
        }
        // first/head/last over a TO-ONE value are the IDENTITY — the list
        // encoding CHAR-INDEXES a lone string ('Doe'[1] = 'D', the at()/last()
        // trap; audit made the family uniform).
        for (String f : Pure.nativeKeysAt("first")) {
            RULES.put(f, (n, args) -> isToOne(n.args().get(0))
                    && !(args.get(0) instanceof SqlExpr.ArrayLit) ? args.get(0)
                    : new SqlExpr.Call(SqlFn.LIST_GET,
                            List.of(args.get(0), new SqlExpr.IntLit(1))));
        }
        for (String f : Pure.nativeKeysAt("head")) {
            RULES.put(f, (n, args) -> isToOne(n.args().get(0))
                    && !(args.get(0) instanceof SqlExpr.ArrayLit) ? args.get(0)
                    : new SqlExpr.Call(SqlFn.LIST_GET,
                            List.of(args.get(0), new SqlExpr.IntLit(1))));
        }
        for (String f : Pure.nativeKeysAt("last")) {
            RULES.put(f, (n, args) -> isToOne(n.args().get(0))
                    && !(args.get(0) instanceof SqlExpr.ArrayLit) ? args.get(0)
                    : new SqlExpr.Call(SqlFn.LIST_GET,
                            List.of(args.get(0), new SqlExpr.IntLit(-1))));
        }
        // 0-based Pure -> 1-based SQL shifts (the semantics contract).
        for (String f : Pure.nativeKeysAt("substring")) {
            RULES.put(f, (n, args) -> {
                List<SqlExpr> shifted = new ArrayList<>(args);
                shifted.set(1, plusOne(args.get(1)));
                return new SqlExpr.Call(SqlFn.SUBSTRING, shifted);
            });
        }
        for (String f : Pure.nativeKeysAt("indexOf")) {
            RULES.put(f, (n, args) -> {
                // A String[*] source is a LIST of strings — list search, not
                // substring search (audit: the type-only gate sent
                // ['a','b']->indexOf('b') to strpos).
                if (n.args().get(0).info().type() != Type.Primitive.STRING
                        || !isToOne(n.args().get(0))
                        || args.get(0) instanceof SqlExpr.ArrayLit) {
                    // LIST indexOf: 0-based, -1 on a miss.
                    return new SqlExpr.Call(SqlFn.MINUS, List.of(
                            new SqlExpr.Call(SqlFn.COALESCE, List.of(
                                    new SqlExpr.Call(SqlFn.LIST_POSITION,
                                            List.of(args.get(0), args.get(1))),
                                    new SqlExpr.IntLit(0))),
                            new SqlExpr.IntLit(1)));
                }
                if (args.size() == 3) {
                    // indexOf(s, sub, from): search the suffix; re-base hits,
                    // misses stay -1.
                    SqlExpr suffix = new SqlExpr.Call(SqlFn.SUBSTRING, List.of(
                            args.get(0), plusOne(args.get(2))));
                    SqlExpr k = new SqlExpr.Call(SqlFn.STRPOS,
                            List.of(suffix, args.get(1)));
                    return new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                            SqlExpr.Call.of(SqlFn.GREATER, k, new SqlExpr.IntLit(0)),
                            SqlExpr.Call.of(SqlFn.MINUS,
                                    SqlExpr.Call.of(SqlFn.PLUS, k, args.get(2)),
                                    new SqlExpr.IntLit(1)))),
                            new SqlExpr.IntLit(-1));
                }
                return new SqlExpr.Call(SqlFn.MINUS, List.of(
                        new SqlExpr.Call(SqlFn.STRPOS, args), new SqlExpr.IntLit(1)));
            });
        }
        for (String f : Pure.nativeKeysAt("at")) {
            // at(x, 0) over a TO-ONE value is the IDENTITY — the list encoding
            // would CHAR-INDEX a lone string ('Doe'[1] = 'D' in DuckDB).
            RULES.put(f, (n, args) -> isToOne(n.args().get(0))
                    && !(args.get(0) instanceof SqlExpr.ArrayLit)
                    && args.get(1) instanceof SqlExpr.IntLit i && i.value() == 0
                    ? args.get(0)
                    : new SqlExpr.Call(SqlFn.LIST_GET,
                            List.of(args.get(0), plusOne(args.get(1)))));
        }
        // list(items): the List<T> CARRIER — at SQL level the list value
        // itself (a to-one item wraps as a singleton).
        for (String f : Pure.nativeKeysAt("list")) {
            RULES.put(f, (n, args) -> isToOne(n.args().get(0))
                    ? new SqlExpr.ArrayLit(List.of(args.get(0)))
                    : args.get(0));
        }
        // add(set, val) appends; add(set, index, val) INSERTS at the 0-based
        // index: prefix || [val] || suffix.
        for (String f : Pure.nativeKeysAt("add")) {
            RULES.put(f, (n, args) -> {
                if (args.size() == 2) {
                    return new SqlExpr.Call(SqlFn.LIST_APPEND, args);
                }
                SqlExpr l = args.get(0);
                SqlExpr idx = args.get(1);
                SqlExpr inserted = SqlExpr.Call.of(SqlFn.LIST_CONCAT,
                        SqlExpr.Call.of(SqlFn.LIST_CONCAT,
                                SqlExpr.Call.of(SqlFn.LIST_SLICE, l,
                                        new SqlExpr.IntLit(1), idx),
                                new SqlExpr.ArrayLit(List.of(args.get(2)))),
                        SqlExpr.Call.of(SqlFn.LIST_SLICE, l, plusOne(idx),
                                SqlExpr.Call.of(SqlFn.LIST_LENGTH, l)));
                // An out-of-range index ERRORS (real pure) — the slice
                // recipe would silently clamp to an append (audit).
                return new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                        SqlExpr.Call.of(SqlFn.GREATER, idx,
                                SqlExpr.Call.of(SqlFn.LIST_LENGTH, l)),
                        SqlExpr.Call.of(SqlFn.ERROR, new SqlExpr.StringLit(
                                "add(set, index, value): index out of bounds")))),
                        inserted);
            });
        }
        // removeDuplicates: bare distinct; an EQUALITY comparator (the
        // eta-expanded eq/equal reference) is distinct's own semantics —
        // anything richer has no set-based shape.
        for (String f : Pure.nativeKeysAt("removeDuplicates")) {
            RULES.put(f, (n, args) -> {
                if (args.size() >= 2 && !isEqualityComparator(n.args().get(n.args().size() - 1))) {
                    throw new IllegalStateException("removeDuplicates with a"
                            + " non-equality comparator has no scalar lowering");
                }
                return orderedDedup(args.get(0));
            });
        }
        // collection::distinct = removeDuplicates (real distinct.pure) —
        // registered by the EXACT collection overload key.
        RULES.put(Pure.DISTINCT_COLLECTION_KEY,
                (n, args) -> orderedDedup(args.get(0)));
        // regexp family (real regex/*.pure): DuckDB regexp_* with the
        // RegexpParameter enums translated to RE2 option chars —
        // CASE_SENSITIVE 'c', CASE_INSENSITIVE 'i', MULTILINE 'm',
        // NON_NEWLINE_SENSITIVE 's' (POSIX '.' matches newline).
        for (String f : Pure.nativeKeysAt("regexpLike")) {
            RULES.put(f, (n, args) -> new SqlExpr.Call(SqlFn.MATCHES, List.of(
                    args.get(0),
                    n.args().size() > 2
                            ? inlineFlags(args.get(1), regexpFlags(n.args().get(2)))
                            : args.get(1))));
        }
        for (String f : Pure.nativeKeysAt("regexpCount")) {
            RULES.put(f, (n, args) -> SqlExpr.Call.of(SqlFn.LIST_LENGTH,
                    regexpAll(n, args, 2)));
        }
        for (String f : Pure.nativeKeysAt("regexpExtract")) {
            RULES.put(f, (n, args) -> {
                if (!(args.get(2) instanceof SqlExpr.BoolLit all)) {
                    throw new IllegalStateException("regexpExtract extractAll must be literal");
                }
                SqlExpr allMatches = regexpAll(n, args, 3);
                // extract-one stays LIST-shaped ([first] / []) — the String[*]
                // result contract unnests it
                return all.value() ? allMatches
                        : SqlExpr.Call.of(SqlFn.LIST_SLICE, allMatches,
                                new SqlExpr.IntLit(1), new SqlExpr.IntLit(1));
            });
        }
        for (String f : Pure.nativeKeysAt("regexpIndexOf")) {
            RULES.put(f, (n, args) -> {
                SqlExpr first = SqlExpr.Call.of(SqlFn.LIST_GET,
                        regexpAll(n, args, 2), new SqlExpr.IntLit(1));
                // real regexpIndexOf is 0-BASED (testRegexpIndexOf pins 3 for
                // strpos 4); no match -> -1. Group text located lexically.
                return new SqlExpr.Case(
                        List.of(new SqlExpr.Case.When(
                                SqlExpr.Call.of(SqlFn.IS_NULL, first),
                                new SqlExpr.IntLit(-1))),
                        SqlExpr.Call.of(SqlFn.MINUS,
                                SqlExpr.Call.of(SqlFn.STRPOS, args.get(0), first),
                                new SqlExpr.IntLit(1)));
            });
        }
        for (String f : Pure.nativeKeysAt("regexpReplace")) {
            RULES.put(f, (n, args) -> {
                if (!(args.get(3) instanceof SqlExpr.BoolLit all)) {
                    throw new IllegalStateException("regexpReplace replaceAll must be literal");
                }
                String flags = n.args().size() > 4 ? regexpFlags(n.args().get(4)) : "";
                SqlExpr pattern = inlineFlags(args.get(1), flags);
                // 'g' (global) is a true OPTION, not an inline flag
                return new SqlExpr.Call(SqlFn.REGEXP_REPLACE, List.of(
                        args.get(0), pattern, args.get(2),
                        new SqlExpr.StringLit(all.value() ? "g" : "")));
            });
        }
        // lpad/rpad: an EMPTY pad char returns the subject unchanged (real
        // testLpadEmptyChar) — DuckDB raises 'Insufficient padding' instead.
        for (String name : List.of("lpad", "rpad")) {
            SqlFn padFn = name.equals("lpad") ? SqlFn.LPAD : SqlFn.RPAD;
            for (String f : Pure.nativeKeysAt(name)) {
                RULES.put(f, (n, args) ->
                        args.size() == 3 && args.get(2) instanceof SqlExpr.StringLit lit
                                && lit.value().isEmpty()
                        ? args.get(0)
                        : new SqlExpr.Call(padFn, args));
            }
        }
        // ---- Map<U,V>: the DuckDB MAP carrier ----
        // pair(a,b) travels as STRUCT(first, second) — map_from_entries
        // takes exactly that shape.
        RULES.put(Pure.PAIR_KEY, (n, args) ->
                new SqlExpr.StructLit(List.of(
                        new SqlExpr.StructLit.Field("first", args.get(0)),
                        new SqlExpr.StructLit.Field("second", args.get(1)))));
        for (String f : Pure.nativeKeysAt("newMap")) {
            RULES.put(f, (n, args) -> mapFromPairs(n, args.get(0)));
        }
        for (String f : Pure.nativeKeysAt("put")) {
            // both operands cast to the RESOLVED map type — DuckDB's
            // map_concat rejects INTEGER-vs-BIGINT value mismatches
            RULES.put(f, (n, args) -> SqlExpr.Call.of(SqlFn.MAP_CONCAT,
                    castToMapType(n, args.get(0)),
                    castToMapType(n, SqlExpr.Call.of(SqlFn.MAP_FROM_LISTS,
                            new SqlExpr.ArrayLit(List.of(args.get(1))),
                            new SqlExpr.ArrayLit(List.of(args.get(2)))))));
        }
        for (String f : Pure.nativeKeysAt("putAll")) {
            RULES.put(f, (n, args) -> {
                boolean mapArg = com.legend.compiler.element.type.PlatformTypes
                        .isMapCarrier(n.args().get(1).info().type());
                SqlExpr other = mapArg ? args.get(1) : mapFromPairs(n, args.get(1));
                return SqlExpr.Call.of(SqlFn.MAP_CONCAT,
                        castToMapType(n, args.get(0)), castToMapType(n, other));
            });
        }
        for (String f : Pure.nativeKeysAt("keys")) {
            RULES.put(f, (n, args) -> SqlExpr.Call.of(SqlFn.MAP_KEYS, args.get(0)));
        }
        for (String f : Pure.nativeKeysAt("values")) {
            RULES.put(f, (n, args) -> SqlExpr.Call.of(SqlFn.MAP_VALUES, args.get(0)));
        }
        // get: the MAP overload only — the bare-name set is shared with
        // variant get(v, key), whose rule is registered separately above.
        RULES.put(Pure.MAP_GET_KEY, (n, args) ->
                SqlExpr.Call.of(SqlFn.LIST_GET,
                        SqlExpr.Call.of(SqlFn.MAP_EXTRACT, args.get(0), args.get(1)),
                        new SqlExpr.IntLit(1)));
        family(SqlFn.BIT_NOT, "bitNot");
        // formatDate(date, Strict/DateTimeFormat): the two real ISO forms.
        for (String f : Pure.nativeKeysAt("formatDate")) {
            RULES.put(f, (n, args) -> switch (enumName(n.args().get(1))) {
                case "ISO8601" -> SqlExpr.Call.of(SqlFn.STRFTIME, args.get(0),
                        new SqlExpr.StringLit("%Y-%m-%d"));
                // 9-digit nanos: DuckDB %f is micros — pad three zeros.
                case "ISO8601_NanoSecondPrecision" -> SqlExpr.Call.of(SqlFn.CONCAT,
                        SqlExpr.Call.of(SqlFn.STRFTIME, args.get(0),
                                new SqlExpr.StringLit("%Y-%m-%dT%H:%M:%S.%f")),
                        new SqlExpr.StringLit("000"));
                default -> throw new IllegalStateException(
                        "unsupported date format " + enumName(n.args().get(1)));
            });
        }
        // fromJson(String): the string IS the variant — a JSON cast.
        for (String f : Pure.nativeKeysAt("fromJson")) {
            RULES.put(f, (n, args) -> new SqlExpr.Cast(args.get(0),
                    com.legend.sql.SqlType.Scalar.JSON));
        }
        // Collection concatenate only — the relation overload is the
        // TypedConcatenate set-op and never reaches scalar lowering. A
        // MIXED concatenation (T solved to Any) travels as the variant
        // carrier: each non-Any side's elements wrap TO_VARIANT so DuckDB
        // concatenates JSON[] to JSON[].
        for (String f : Pure.nativeKeysAt("concatenate")) {
            RULES.put(f, (n, args) -> {
                if (!PlatformTypes.isAny(n.info().type())) {
                    return new SqlExpr.Call(SqlFn.LIST_CONCAT, args);
                }
                List<SqlExpr> wrapped = new ArrayList<>(args.size());
                for (int i = 0; i < args.size(); i++) {
                    if (PlatformTypes.isAny(n.args().get(i).info().type())) {
                        wrapped.add(args.get(i));
                    } else {
                        wrapped.add(SqlExpr.Call.of(SqlFn.LIST_TRANSFORM, args.get(i),
                                new SqlExpr.Lambda(List.of("_cv"),
                                        SqlExpr.Call.of(SqlFn.TO_VARIANT,
                                                new SqlExpr.Column(null, "_cv")))));
                    }
                }
                return new SqlExpr.Call(SqlFn.LIST_CONCAT, wrapped);
            });
        }
        // tail/init of a TO-ONE value is the EMPTY collection (all-but-first
        // / all-but-last of a singleton).
        for (String f : Pure.nativeKeysAt("tail")) {
            RULES.put(f, (n, args) -> args.get(0) instanceof SqlExpr.NullLit
                    || (isToOne(n.args().get(0))
                            && !(args.get(0) instanceof SqlExpr.ArrayLit))
                    ? new SqlExpr.NullLit()
                    : new SqlExpr.Call(SqlFn.LIST_TAIL, args));
        }
        for (String f : Pure.nativeKeysAt("init")) {
            RULES.put(f, (n, args) -> args.get(0) instanceof SqlExpr.NullLit
                    || (isToOne(n.args().get(0))
                            && !(args.get(0) instanceof SqlExpr.ArrayLit))
                    ? new SqlExpr.NullLit()
                    : new SqlExpr.Call(SqlFn.LIST_INIT, args));
        }
        // reverse(T[*]): the list reversed; a to-one value is its own reverse.
        for (String f : Pure.nativeKeysAt("reverse")) {
            RULES.put(f, (n, args) -> isToOne(n.args().get(0)) ? args.get(0)
                    : new SqlExpr.Call(SqlFn.LIST_REVERSE, args));
        }
        // type(x): the value's runtime SQL type name (engine-lite parity —
        // DuckDB's typeof; the corpus pins 'INTEGER' for 1).
        for (String f : Pure.nativeKeysAt("type")) {
            RULES.put(f, (n, args) -> new SqlExpr.Call(SqlFn.TYPEOF, args));
        }
        // minBy/maxBy(values, key[, count]): sort {k,v} structs by key (list
        // sort over structs orders by the FIRST field), take the head or the
        // top count, then unwrap the values.
        for (String name : List.of("minBy", "maxBy")) {
            boolean asc = name.equals("minBy");
            for (String f : Pure.nativeKeysAt(name)) {
                RULES.put(f, (n, args) -> {
                    if (args.size() < 2) {
                        throw new IllegalStateException(name
                                + " expects (values, key-function|keys[, count]) here");
                    }
                    // Pair BY INDEX for both forms so ties resolve to the
                    // FIRST occurrence (real pure): the middle sort field is
                    // the original position — negated under the descending
                    // sort so ties still come out first-occurrence.
                    SqlExpr i = new SqlExpr.Column(null, "_by_i");
                    SqlExpr valAt = SqlExpr.Call.of(SqlFn.LIST_GET, args.get(0), i);
                    SqlExpr keyExpr = args.get(1) instanceof SqlExpr.Lambda key
                            && key.params().size() == 1
                            ? substituteRef(key.body(), key.params().get(0), valAt)
                            : SqlExpr.Call.of(SqlFn.LIST_GET, args.get(1), i);
                    SqlExpr idxField = asc ? i
                            : SqlExpr.Call.of(SqlFn.MINUS, new SqlExpr.IntLit(0), i);
                    SqlExpr pairs = SqlExpr.Call.of(SqlFn.LIST_TRANSFORM,
                            SqlExpr.Call.of(SqlFn.RANGE_FN, new SqlExpr.IntLit(1),
                                    plusOne(SqlExpr.Call.of(SqlFn.LIST_LENGTH, args.get(0)))),
                            new SqlExpr.Lambda(List.of("_by_i"),
                                    new SqlExpr.StructLit(List.of(
                                            new SqlExpr.StructLit.Field("k", keyExpr),
                                            new SqlExpr.StructLit.Field("i", idxField),
                                            new SqlExpr.StructLit.Field("v", valAt)))));
                    SqlExpr sorted = new SqlExpr.Call(
                            asc ? SqlFn.LIST_SORT : SqlFn.LIST_SORT_DESC, List.of(pairs));
                    if (args.size() == 3) {
                        String e = "_by_e";
                        return SqlExpr.Call.of(SqlFn.LIST_TRANSFORM,
                                SqlExpr.Call.of(SqlFn.LIST_SLICE, sorted,
                                        new SqlExpr.IntLit(1), args.get(2)),
                                new SqlExpr.Lambda(List.of(e),
                                        new SqlExpr.StructGet(new SqlExpr.Column(null, e), "v")));
                    }
                    return new SqlExpr.StructGet(
                            SqlExpr.Call.of(SqlFn.LIST_GET, sorted, new SqlExpr.IntLit(1)), "v");
                });
            }
        }
        // removeDuplicatesBy(values, key): keep each key's FIRST occurrence —
        // an element survives iff the first position of its key is its own.
        for (String f : Pure.nativeKeysAt("removeDuplicatesBy")) {
            RULES.put(f, (n, args) -> {
                if (!(args.get(1) instanceof SqlExpr.Lambda key && key.params().size() == 1)) {
                    throw new IllegalStateException(
                            "removeDuplicatesBy expects (values, key-function)");
                }
                String x = key.params().get(0);
                SqlExpr keys = SqlExpr.Call.of(SqlFn.LIST_TRANSFORM, args.get(0), key);
                return SqlExpr.Call.of(SqlFn.LIST_FILTER, args.get(0),
                        new SqlExpr.Lambda(List.of(x, "_rd_i"),
                                SqlExpr.Call.of(SqlFn.EQUAL,
                                        SqlExpr.Call.of(SqlFn.LIST_POSITION, keys, key.body()),
                                        new SqlExpr.Column(null, "_rd_i"))));
            });
        }
        // corr/covarPopulation/covarSample over two LISTS: the paired-unnest
        // subquery recipe — (SELECT CORR(a, b) FROM (SELECT unnest(x) AS a,
        // unnest(y) AS b)); DuckDB zips parallel select-list unnests.
        for (var e : Map.of("corr", "CORR", "covarPopulation", "COVAR_POP",
                "covarSample", "COVAR_SAMP").entrySet()) {
            for (String f : Pure.nativeKeysAt(e.getKey())) {
                RULES.put(f, (n, args) -> {
                    if (args.size() != 2) {
                        throw new IllegalStateException(e.getKey()
                                + " expects two value lists in scalar position");
                    }
                    // An EMPTY side has no pairs: the statistic is empty
                    // (NULL) — and unnest(NULL) can't correlate anyway.
                    if (args.get(0) instanceof SqlExpr.NullLit
                            || args.get(1) instanceof SqlExpr.NullLit) {
                        return new SqlExpr.NullLit();
                    }
                    // A TO-ONE side is the single-element list ([1] fits
                    // Number[*]) — unnest needs the list shape.
                    SqlExpr xs = n.args().get(0).info().multiplicity().isMany()
                            ? args.get(0) : new SqlExpr.ArrayLit(List.of(args.get(0)));
                    SqlExpr ys = n.args().get(1).info().multiplicity().isMany()
                            ? args.get(1) : new SqlExpr.ArrayLit(List.of(args.get(1)));
                    var inner = new com.legend.sql.SqlSelect(List.of(
                            new com.legend.sql.SqlSelect.Projection(
                                    SqlExpr.Call.of(SqlFn.UNNEST, xs), "a"),
                            new com.legend.sql.SqlSelect.Projection(
                                    SqlExpr.Call.of(SqlFn.UNNEST, ys), "b")),
                            false, null, null, List.of(), null, null, List.of(), null, null,
                            List.of());
                    var outer = new com.legend.sql.SqlSelect(List.of(
                            new com.legend.sql.SqlSelect.Projection(
                                    new com.legend.sql.SqlAgg.Reducer(e.getValue(),
                                            List.of(new SqlExpr.Column(null, "a"),
                                                    new SqlExpr.Column(null, "b")), false),
                                    null)),
                            false, new com.legend.sql.SqlSource.Subselect(inner, "_uz"),
                            null, List.of(), null, null, List.of(), null, null, List.of());
                    // MISMATCHED lengths would zip-pad with NULLs and the
                    // reducer would silently drop the unpaired tail (audit:
                    // corr([1,2,3],[2,4]) said 1.0) — unpaired data is LOUD.
                    return new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                            SqlExpr.Call.of(SqlFn.NOT_EQUAL,
                                    SqlExpr.Call.of(SqlFn.LIST_LENGTH, args.get(0)),
                                    SqlExpr.Call.of(SqlFn.LIST_LENGTH, args.get(1))),
                            SqlExpr.Call.of(SqlFn.ERROR, new SqlExpr.StringLit(
                                    e.getKey() + ": the two value lists differ"
                                            + " in length")))),
                            new SqlExpr.ScalarSubquery(outer));
                });
            }
        }
        // find(coll, pred): the FIRST satisfying element, [0..1] — filter, then head.
        for (String f : Pure.nativeKeysAt("find")) {
            RULES.put(f, (n, args) -> new SqlExpr.Call(SqlFn.LIST_GET, List.of(
                    new SqlExpr.Call(SqlFn.LIST_FILTER, args), new SqlExpr.IntLit(1))));
        }
        for (String f : Pure.nativeKeysAt("splitPart")) {
            RULES.put(f, (n, args) -> {
                // An EMPTY delimiter never splits: index 0 IS the whole
                // string (PCT; SQL split_part('', …) returns '' instead).
                if (args.get(1) instanceof SqlExpr.StringLit d && d.value().isEmpty()) {
                    return args.get(2) instanceof SqlExpr.IntLit i && i.value() == 0
                            ? args.get(0) : new SqlExpr.NullLit();
                }
                List<SqlExpr> shifted = new ArrayList<>(args);
                shifted.set(2, plusOne(args.get(2)));
                return new SqlExpr.Call(SqlFn.SPLIT_PART, shifted);
            });
        }
        // contains on a TO-ONE STRING: strpos > 0. A String[*] source is a
        // LIST of strings — list containment, not substring search (the
        // to-one gate; audit: ['x','y']->contains('x') hit strpos).
        for (String f : Pure.nativeKeysAt("contains")) {
            RULES.put(f, (n, args) -> {
                // contains(coll, val, comparator): filter by the comparator
                // against the needle, then non-empty. SQL lambdas are
                // positional and list_filter is 1-param — the needle
                // parameter closes over by SUBSTITUTION.
                if (args.size() == 3 && args.get(2) instanceof SqlExpr.Lambda comp
                        && comp.params().size() == 2) {
                    SqlExpr body = substituteRef(comp.body(), comp.params().get(1), args.get(1));
                    return new SqlExpr.Call(SqlFn.GREATER, List.of(
                            SqlExpr.Call.of(SqlFn.LIST_LENGTH,
                                    SqlExpr.Call.of(SqlFn.LIST_FILTER, args.get(0),
                                            new SqlExpr.Lambda(
                                                    List.of(comp.params().get(0)), body))),
                            new SqlExpr.IntLit(0)));
                }
                Type elem = n.args().get(0).info().type();
                Type val = n.args().get(1).info().type();
                if (elem == Type.Primitive.STRING && isToOne(n.args().get(0))
                        && !(args.get(0) instanceof SqlExpr.ArrayLit)) {
                    return new SqlExpr.Call(SqlFn.GREATER, List.of(
                            new SqlExpr.Call(SqlFn.STRPOS, args), new SqlExpr.IntLit(0)));
                }
                // A heterogeneous (Any) list is variant-wrapped — wrap the
                // needle the same way so containment compares JSON to JSON.
                // This MUST precede the cross-kind rule: an Any list can
                // legitimately contain an instance (audit: class-in-mixed-list
                // containment was constant FALSE).
                if (PlatformTypes.isAny(elem)) {
                    return new SqlExpr.Call(SqlFn.LIST_CONTAINS, List.of(args.get(0),
                            SqlExpr.Call.of(SqlFn.TO_VARIANT, args.get(1))));
                }
                // Pure equality never relates an instance to a primitive —
                // CONCRETE cross-kind containment is statically FALSE (SQL
                // list_contains would refuse to even type it).
                if (isClassish(elem) != isClassish(val)) {
                    return new SqlExpr.BoolLit(false);
                }
                // NULL-safe: containment in a NULL list (toMany over JSON
                // null) is pure's empty-collection FALSE, not SQL NULL.
                return SqlExpr.Call.of(SqlFn.COALESCE,
                        new SqlExpr.Call(SqlFn.LIST_CONTAINS, args),
                        new SqlExpr.BoolLit(false));
            });
        }
        // format('%s...', [args]) -> printf(fmt, args...): the array spreads.
        // Two directives printf cannot honor rewrite to %s over a literal
        // format string: %t{javaDatePattern} formats its date argument
        // (strftime, pattern converted), and bare %f is pure's MINIMAL float
        // repr, not printf's fixed six decimals.
        for (String f : Pure.nativeKeysAt("format")) {
            RULES.put(f, (n, args) -> {
                List<SqlExpr> spread = new ArrayList<>();
                spread.add(args.get(0));
                // typed elements ride along so %s can print CLASS-typed
                // values (Pair -> '<f, s>') by their STATIC type
                java.util.List<com.legend.compiler.spec.typed.TypedSpec> typedElems =
                        n.args().get(1) instanceof com.legend.compiler.spec.typed.TypedCollection tc
                                ? tc.elements() : java.util.List.of(n.args().get(1));
                if (args.get(1) instanceof SqlExpr.ArrayLit arr) {
                    // A MIXED argument list arrives variant-wrapped (its LUB
                    // is Any) — printf wants the raw values back, each
                    // substitution slot carries its own kind already.
                    for (int i = 0; i < arr.elements().size(); i++) {
                        SqlExpr e = arr.elements().get(i);
                        e = e instanceof SqlExpr.Call c && c.fn() == SqlFn.TO_VARIANT
                                ? c.args().get(0) : e;
                        Type et = i < typedElems.size()
                                ? typedElems.get(i).info().type() : null;
                        // class-typed slots pre-print via the pure toString
                        // (printf's %s would show the raw struct)
                        if (et != null
                                && com.legend.compiler.element.type.PlatformTypes.isPairCarrier(et)) {
                            e = pureToString(et, e);
                        }
                        spread.add(e);
                    }
                } else {
                    spread.add(args.get(1));
                }
                if (spread.get(0) instanceof SqlExpr.StringLit fmt) {
                    rewriteFormatDirectives(fmt.value(), spread);
                }
                return new SqlExpr.Call(SqlFn.FORMAT, spread);
            });
        }
        // REAL pure hash(text, HashType.X): the enum value picks the digest
        // (the relational md5/sha dynafunctions translate here — the lite
        // md5/sha natives are gone).
        for (String f : Pure.nativeKeysAt("meta::pure::functions::hash::hash")) {
            RULES.put(f, (n, args) -> {
                if (!(n.args().get(1) instanceof com.legend.compiler.spec.typed.TypedEnumValue ev)) {
                    throw new IllegalStateException("hash(text, hashType) needs a HashType literal");
                }
                SqlFn digest = switch (ev.value()) {
                    case "MD5" -> SqlFn.MD5;
                    case "SHA1" -> SqlFn.SHA1;
                    case "SHA256" -> SqlFn.SHA256;
                    default -> throw new IllegalStateException(
                            "unknown HashType." + ev.value());
                };
                return new SqlExpr.Call(digest, List.of(args.get(0)));
            });
        }

        // Parses and toString are CASTS (the Type rides the IR).
        // toString of a DATETIME prints Pure's ISO form
        // (2014-01-01T00:00:00.000+0000) — SQL's VARCHAR cast uses a space
        // separator and no offset. Other types keep the plain cast.
        for (String f : Pure.nativeKeysAt("toString")) {
            RULES.put(f, (n, args) -> {
                Type t = n.args().get(0).info().type();
                if (t == Type.Primitive.DATE_TIME) {
                    return SqlExpr.Call.of(SqlFn.STRFTIME, args.get(0),
                            new SqlExpr.StringLit("%Y-%m-%dT%H:%M:%S.%g+0000"));
                }
                if (t == Type.Primitive.FLOAT) {
                    return floatRepr(args.get(0));
                }
                return pureToString(t, args.get(0));
            });
        }
        family(SqlFn.IS_DISTINCT, "isDistinct");
        castFamily("parseInteger", Type.Primitive.INTEGER);
        castFamily("parseFloat", Type.Primitive.FLOAT);
        castFamily("toFloat", Type.Primitive.FLOAT);
        // parseDecimal accepts the 'd'/'D' Pure-literal suffix ('3.14159d');
        // SQL DECIMAL casts do not — strip it (literal-folded or RTRIM).
        for (String f : Pure.nativeKeysAt("parseDecimal")) {
            RULES.put(f, (n, args) -> {
                SqlExpr in = args.get(0) instanceof SqlExpr.StringLit lit
                        ? new SqlExpr.StringLit(lit.value().replaceAll("[dD]$", ""))
                        : SqlExpr.Call.of(SqlFn.RTRIM, args.get(0),
                                new SqlExpr.StringLit("dD"));
                return new SqlExpr.Cast(in, PureSql.type(Type.Primitive.DECIMAL));
            });
        }
        castFamily("parseBoolean", Type.Primitive.BOOLEAN);
        // parseDate accepts PARTIAL-time text ('2015-04-15T17') — pad the
        // literal to a full timestamp shape (SQL's cast demands one).
        for (String f : Pure.nativeKeysAt("parseDate")) {
            RULES.put(f, (n, args) -> {
                SqlExpr in = args.get(0);
                if (in instanceof SqlExpr.StringLit lit) {
                    // A ZONE-carrying literal keeps its instant: TIMESTAMPTZ
                    // (the JDBC cell is an OffsetDateTime — real pure's
                    // parseDate preserves the offset).
                    if (lit.value().matches(".*([+-]\\d{4}|[+-]\\d{2}:\\d{2}|Z)$")) {
                        return new SqlExpr.Cast(in,
                                com.legend.sql.SqlType.Scalar.TIMESTAMPTZ);
                    }
                    String v = lit.value().replace('T', ' ');
                    if (v.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}")) {
                        v += ":00:00";
                    } else if (v.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}")) {
                        v += ":00";
                    }
                    in = new SqlExpr.StringLit(v);
                }
                return new SqlExpr.Cast(in, PureSql.type(Type.Primitive.DATE_TIME));
            });
        }
        // date(y,m,d[,h,mi,s]) constructors.
        // date(y[,m[,d[,h[,mi[,s]]]]]): every arity SHORT of seconds is a
        // PARTIAL date — the ISO-prefix string carrier at that precision
        // (real pure prints date(1973,11,13,23) as 1973-11-13T23). Only the
        // full six-part form is a real timestamp; three parts is make_date.
        for (String f : Pure.nativeKeysAt("date")) {
            RULES.put(f, (n, args) -> {
                if (args.size() == 3) {
                    return new SqlExpr.Call(SqlFn.MAKE_DATE, args);
                }
                if (args.size() == 6) {
                    // FLOAT seconds = SUB-SECOND precision: real pure prints
                    // the ISO form with the fraction trimmed to its minimal
                    // digits (11.0, not 11.000) — the string carrier again.
                    if (n.args().get(5).info().type() == Type.Primitive.FLOAT
                            || n.args().get(5).info().type() == Type.Primitive.DECIMAL) {
                        SqlExpr iso = SqlExpr.Call.of(SqlFn.STRFTIME,
                                new SqlExpr.Call(SqlFn.MAKE_TIMESTAMP, args),
                                // %f = MICROseconds — %g's milliseconds
                                // silently truncated 59.999999 (audit); the
                                // zero-trim below reduces to minimal digits.
                                new SqlExpr.StringLit("%Y-%m-%dT%H:%M:%S.%f"));
                        SqlExpr trimmed = SqlExpr.Call.of(SqlFn.RTRIM, iso,
                                new SqlExpr.StringLit("0"));
                        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                                SqlExpr.Call.of(SqlFn.ENDS_WITH, trimmed,
                                        new SqlExpr.StringLit(".")),
                                SqlExpr.Call.of(SqlFn.CONCAT, trimmed,
                                        new SqlExpr.StringLit("0")))),
                                trimmed);
                    }
                    return new SqlExpr.Call(SqlFn.MAKE_TIMESTAMP, args);
                }
                String[] seps = {"", "-", "-", "T", ":"};
                int[] widths = {4, 2, 2, 2, 2};
                SqlExpr out = null;
                for (int i = 0; i < args.size(); i++) {
                    SqlExpr part = SqlExpr.Call.of(SqlFn.LPAD,
                            new SqlExpr.Cast(args.get(i),
                                    com.legend.sql.SqlType.Scalar.VARCHAR),
                            new SqlExpr.IntLit(widths[i]), new SqlExpr.StringLit("0"));
                    out = out == null ? part
                            : SqlExpr.Call.of(SqlFn.CONCAT, SqlExpr.Call.of(SqlFn.CONCAT,
                                    out, new SqlExpr.StringLit(seps[i])), part);
                }
                return out;
            });
        }

        // Overload-specific overrides — the resolved signature IS the decision.
        RULES.put(Pure.keyPlusString(), (n, args) -> new SqlExpr.Call(SqlFn.CONCAT, args));
        RULES.put(Pure.keyIn(), (n, args) -> {
            // in(x, []) is FALSE in pure; the empty collection lowers to
            // NULL in scalar position, and `x IN (NULL)` would be NULL —
            // silently dropping rows under negation (audit finding).
            if (args.get(1) instanceof SqlExpr.NullLit) {
                return new SqlExpr.BoolLit(false);
            }
            // A heterogeneous (Any) collection is variant-wrapped — wrap the
            // needle the same way so IN compares JSON to JSON.
            SqlExpr needle = PlatformTypes.isAny(n.args().get(1).info().type())
                    ? SqlExpr.Call.of(SqlFn.TO_VARIANT, args.get(0))
                    : args.get(0);
            List<SqlExpr> flat = new ArrayList<>();
            flat.add(needle);
            if (args.get(1) instanceof SqlExpr.ArrayLit arr) {
                flat.addAll(arr.elements());
            } else {
                flat.add(args.get(1));
            }
            return new SqlExpr.Call(SqlFn.IN, flat);
        });
    }

    /**
     * Names known to be ABSENT from our catalog (engine-only or not yet
     * signed). Anything else missing at registration is a TYPO and dies.
     */
    private static final Set<String> KNOWN_ABSENT = Set.of(
            "cbrt", "log10", "atan2", "sinh", "cosh", "tanh", "ascii", "char",
            "encodeBase64", "levenshteinDistance", "generateGuid", "hashCode",
            "toUpperFirstCharacter", "toLowerFirstCharacter", "matches",
            "lpad", "rpad", "ltrim", "rtrim", "reverseString", "splitPart",
            "left", "right", "mode", "median", "mean", "datePart", "today",
            "now", "hash", "zip", "toVariant", "split", "xor",
            "bitAnd", "bitOr", "bitXor", "bitShiftLeft", "bitShiftRight");

    private static void familyIfPresent(SqlFn semantic, String pureName) {
        if (!Pure.nativeFunctionsAt(pureName).isEmpty()) {
            family(semantic, pureName);
        } else if (!KNOWN_ABSENT.contains(pureName)) {
            throw new IllegalStateException("registration typo: no catalog overloads for '"
                    + pureName + "' and it is not in KNOWN_ABSENT");
        }
    }

    private static void castFamily(String pureName, Type target) {
        for (String f : Pure.nativeKeysAt(pureName)) {
            RULES.put(f, (n, args) -> new SqlExpr.Cast(args.get(0), PureSql.type(target)));
        }
    }

    /** {@code i + 1} — constant-folded for literals (the common case). */
    private static SqlExpr plusOne(SqlExpr e) {
        return e instanceof SqlExpr.IntLit i
                ? new SqlExpr.IntLit(i.value() + 1)
                : new SqlExpr.Call(SqlFn.PLUS, List.of(e, new SqlExpr.IntLit(1)));
    }

    /** The lowering for {@code call}'s resolved overload; loud error when unregistered. */
    static SqlExpr lower(TypedNativeCall call, List<SqlExpr> loweredArgs) {
        Rule rule = RULES.get(call.callee().signatureKey());
        if (rule == null) {
            throw new IllegalStateException("no scalar lowering registered for resolved overload '"
                    + call.callee().qualifiedName() + "' with " + call.callee().parameters().size()
                    + " parameter(s)");
        }
        return rule.apply(call, loweredArgs);
    }

    /**
     * Integer arithmetic NEAR THE INT64 EDGE computes in HUGEINT (real
     * pure's 2 * maxLong PCT value): a literal within a factor of ~2 of
     * overflow widens the first operand, and DuckDB propagates.
     */
    private static List<SqlExpr> hugeWiden(List<SqlExpr> args) {
        // Widen the near-edge INTEGER LITERAL itself — never a float
        // operand (CAST(2.5 AS HUGEINT) rounds to 3 and poisons the
        // product; audit). DuckDB propagates HUGEINT from either side.
        List<SqlExpr> out = null;
        for (int i = 0; i < args.size(); i++) {
            if (args.get(i) instanceof SqlExpr.IntLit lit
                    && (lit.value() > (Long.MAX_VALUE >> 2)
                            || lit.value() < (Long.MIN_VALUE >> 2))) {
                if (out == null) {
                    out = new ArrayList<>(args);
                }
                out.set(i, new SqlExpr.Cast(lit, com.legend.sql.SqlType.Scalar.HUGEINT));
            }
        }
        return out == null ? args : out;
    }

    /**
     * Whether a typed argument lowers to a SQL LIST value: an upper bound
     * beyond one. Relation columns are at most [0..1], so to-many here means
     * a collection expression (toMany(@T), literal lists, split, ...).
     */
    private static boolean listValued(com.legend.compiler.spec.typed.TypedSpec arg) {
        return arg.info().multiplicity().isMany();
    }

    /**
     * FIRST-OCCURRENCE dedup (real removeDuplicates semantics — its PCT
     * asserts order without sorting). LIST_DISTINCT is UNORDERED in DuckDB;
     * keep element x at 1-based index i iff its first position is i.
     */
    private static SqlExpr orderedDedup(SqlExpr list) {
        return new SqlExpr.Call(SqlFn.LIST_FILTER, List.of(list,
                new SqlExpr.Lambda(List.of("_ddx", "_ddi"),
                        new SqlExpr.Call(SqlFn.EQUAL, List.of(
                                SqlExpr.Call.of(SqlFn.LIST_POSITION, list,
                                        new SqlExpr.Column(null, "_ddx")),
                                new SqlExpr.Column(null, "_ddi"))))));
    }

    /** Real Compare.java's KIND ordering: Numbers < Dates < Booleans < Strings; -1 = not a primitive kind. */
    private static int compareKind(Type t) {
        if (t == Type.Primitive.INTEGER || t == Type.Primitive.FLOAT
                || t == Type.Primitive.NUMBER || t == Type.Primitive.DECIMAL
                || t instanceof Type.PrecisionDecimal) {
            return 0;
        }
        if (t == Type.Primitive.DATE || t == Type.Primitive.STRICT_DATE
                || t == Type.Primitive.DATE_TIME) {
            return 1;
        }
        if (t == Type.Primitive.BOOLEAN) {
            return 2;
        }
        if (t == Type.Primitive.STRING) {
            return 3;
        }
        return -1;
    }


    /**
     * The MIXED-ELEMENT two-channel encoding — DATABASE-EXECUTED. A
     * collection whose elements carry DIFFERENT concrete kinds under the
     * Number or Date LUB (2 vs 2.0 vs 7.345D; %2014 vs a DateTime) splits
     * into an IDENTITY channel (each element's pure PRINT FORM, computed
     * BY SQL from the element expression) and a COMPARABLE channel
     * (CAST(e AS DOUBLE) / strptime-padded TIMESTAMP — also SQL).
     * Selections order by the comparable and RETURN the identity.
     *
     * <p>TENET: the encodings are chosen by each element's STATIC TYPE
     * (compiler knowledge); every VALUE computation — printing, casting,
     * padding, comparison, selection — runs in the database. Elements may
     * be arbitrary expressions, not just literals.
     *
     * <p>Null when the shape is not per-element encodable or not mixed.
     */
    record MixedElems(java.util.List<SqlExpr> ids, java.util.List<SqlExpr> vals) {

        SqlExpr idList() {
            return new SqlExpr.ArrayLit(ids);
        }

        SqlExpr valList() {
            return new SqlExpr.ArrayLit(vals);
        }

        /** {@code ids[list_position(vals, <winner>)]} — the selection recipe. */
        SqlExpr select(SqlExpr winner) {
            return SqlExpr.Call.of(SqlFn.LIST_GET, idList(),
                    SqlExpr.Call.of(SqlFn.LIST_POSITION, valList(), winner));
        }
    }

    static MixedElems mixedElems(com.legend.compiler.spec.typed.TypedSpec arg,
                                 SqlExpr lowered) {
        if (!(arg instanceof com.legend.compiler.spec.typed.TypedCollection c)
                || c.elements().size() < 2
                || !(lowered instanceof SqlExpr.ArrayLit la)
                || la.elements().size() != c.elements().size()) {
            return null;
        }
        Type lub = c.info().type();
        if (lub != Type.Primitive.NUMBER && lub != Type.Primitive.DATE) {
            return null;   // uniform-kind collections keep their native carrier
        }
        return encodeAll(c.elements(), la.elements());
    }

    /** The n-ary form: max(2D, 1.23) — each ARG one element. */
    static MixedElems mixedArgs(java.util.List<com.legend.compiler.spec.typed.TypedSpec> args,
                                java.util.List<SqlExpr> lowered) {
        java.util.Set<Type> kinds = new java.util.HashSet<>();
        for (var a : args) {
            kinds.add(a.info().type());
        }
        return kinds.size() > 1 ? encodeAll(args, lowered) : null;
    }

    private static MixedElems encodeAll(
            java.util.List<com.legend.compiler.spec.typed.TypedSpec> elems,
            java.util.List<SqlExpr> lowered) {
        java.util.List<SqlExpr> ids = new java.util.ArrayList<>();
        java.util.List<SqlExpr> vals = new java.util.ArrayList<>();
        for (int i = 0; i < elems.size(); i++) {
            if (!encodeMixed(elems.get(i), lowered.get(i), ids, vals)) {
                return null;
            }
        }
        return new MixedElems(ids, vals);
    }

    /**
     * One element's (identity, comparable) SQL pair, dispatched on its
     * STATIC type. All value work happens in SQL.
     */
    private static boolean encodeMixed(com.legend.compiler.spec.typed.TypedSpec e,
                                       SqlExpr x,
                                       java.util.List<SqlExpr> ids,
                                       java.util.List<SqlExpr> vals) {
        Type t = e.info().type();
        if (t == Type.Primitive.INTEGER) {
            ids.add(new SqlExpr.Cast(x, com.legend.sql.SqlType.Scalar.VARCHAR));
            vals.add(new SqlExpr.Cast(x, com.legend.sql.SqlType.Scalar.DOUBLE));
            return true;
        }
        if (t == Type.Primitive.FLOAT) {
            ids.add(floatRepr(x));   // pure float print, in SQL
            vals.add(x);
            return true;
        }
        if (t == Type.Primitive.DECIMAL || t instanceof Type.PrecisionDecimal) {
            ids.add(SqlExpr.Call.of(SqlFn.CONCAT,
                    new SqlExpr.Cast(x, com.legend.sql.SqlType.Scalar.VARCHAR),
                    new SqlExpr.StringLit("D")));
            vals.add(new SqlExpr.Cast(x, com.legend.sql.SqlType.Scalar.DOUBLE));
            return true;
        }
        if (t == Type.Primitive.STRICT_DATE) {
            ids.add(SqlExpr.Call.of(SqlFn.STRFTIME, x,
                    new SqlExpr.StringLit("%Y-%m-%d")));
            vals.add(new SqlExpr.Cast(x, com.legend.sql.SqlType.Scalar.TIMESTAMP));
            return true;
        }
        if (t == Type.Primitive.DATE_TIME) {
            ids.add(SqlExpr.Call.of(SqlFn.CONCAT,
                    SqlExpr.Call.of(SqlFn.STRFTIME, x,
                            new SqlExpr.StringLit(dateTimeFormatOf(e))),
                    new SqlExpr.StringLit("+0000")));
            vals.add(x);
            return true;
        }
        if (t == Type.Primitive.DATE) {
            // PARTIAL dates travel as STRINGS (master's pinned carrier): the
            // string IS the print form; the comparable composes via
            // make_timestamp from split components (strptime %Y rejects
            // 5-digit years; make_timestamp reaches year 294246).
            SqlExpr cmp = partialComparable(e, x);
            if (cmp == null) {
                return false;
            }
            ids.add(x);
            vals.add(cmp);
            return true;
        }
        return false;
    }

    /** A date operand's chronological comparable (strptime-padded partials); non-dates pass through. */
    private static SqlExpr dateComparableOrSelf(com.legend.compiler.spec.typed.TypedSpec e,
                                                SqlExpr x) {
        Type t = e.info().type();
        if (t == Type.Primitive.DATE) {
            SqlExpr cmp = partialComparable(e, x);
            if (cmp != null) {
                return cmp;
            }
        }
        if (t == Type.Primitive.STRICT_DATE) {
            return new SqlExpr.Cast(x, com.legend.sql.SqlType.Scalar.TIMESTAMP);
        }
        return x;
    }

    /** DateTime print format — subsecond DIGIT COUNT is a static attribute of the literal. */
    private static String dateTimeFormatOf(com.legend.compiler.spec.typed.TypedSpec e) {
        if (e instanceof com.legend.compiler.spec.typed.TypedCDate cd
                && cd.value() instanceof com.legend.values.PureDateLiteral.DateWithSubsecond) {
            return "%Y-%m-%dT%H:%M:%S.%f";
        }
        return "%Y-%m-%dT%H:%M:%S";
    }

    /**
     * A PARTIAL date string's chronological comparable, composed IN SQL:
     * {@code make_timestamp(split_part(x,'-',i)...)} per the STATIC
     * precision; null when the precision is not a known partial form.
     */
    private static SqlExpr partialComparable(com.legend.compiler.spec.typed.TypedSpec e,
                                             SqlExpr x) {
        int prec = datePrecision(e);
        if (prec < 0 || prec > 2) {
            return null;
        }
        SqlExpr one = new SqlExpr.IntLit(1);
        SqlExpr zero = new SqlExpr.IntLit(0);
        SqlExpr year = new SqlExpr.Cast(
                SqlExpr.Call.of(SqlFn.SPLIT_PART, x, new SqlExpr.StringLit("-"), one),
                com.legend.sql.SqlType.Scalar.BIGINT);
        SqlExpr month = prec >= 1 ? new SqlExpr.Cast(
                SqlExpr.Call.of(SqlFn.SPLIT_PART, x, new SqlExpr.StringLit("-"),
                        new SqlExpr.IntLit(2)),
                com.legend.sql.SqlType.Scalar.BIGINT) : one;
        SqlExpr day = prec >= 2 ? new SqlExpr.Cast(
                SqlExpr.Call.of(SqlFn.SPLIT_PART, x, new SqlExpr.StringLit("-"),
                        new SqlExpr.IntLit(3)),
                com.legend.sql.SqlType.Scalar.BIGINT) : one;
        return SqlExpr.Call.of(SqlFn.MAKE_TIMESTAMP, year, month, day, zero, zero, zero);
    }

    /**
     * The pure PRINT of a value by its STATIC type, composed IN SQL —
     * Pair prints {@code '<first, second>'} (real anonymousCollections
     * toString), recursively; everything else is the VARCHAR cast.
     */
    private static SqlExpr pureToString(Type t, SqlExpr x) {
        if (t == Type.Primitive.FLOAT) {
            return floatRepr(x);
        }
        if (t instanceof Type.ClassType ac
                && com.legend.compiler.element.type.PlatformTypes.isAny(ac)) {
            // an ANY slot is variant-carried: root TEXT extraction strips
            // the JSON quoting ('b', not '"b"')
            return new SqlExpr.Cast(
                    SqlExpr.Call.of(SqlFn.VARIANT_GET, x, new SqlExpr.StringLit("$")),
                    PureSql.type(Type.Primitive.STRING));
        }
        if (com.legend.compiler.element.type.PlatformTypes.isPairCarrier(t)) {
            Type ft = ((Type.GenericType) t).arguments().get(0);
            Type st = ((Type.GenericType) t).arguments().get(1);
            return SqlExpr.Call.of(SqlFn.CONCAT,
                    SqlExpr.Call.of(SqlFn.CONCAT,
                            SqlExpr.Call.of(SqlFn.CONCAT, new SqlExpr.StringLit("<"),
                                    pureToString(ft, new SqlExpr.StructGet(x, "first"))),
                            new SqlExpr.StringLit(", ")),
                    SqlExpr.Call.of(SqlFn.CONCAT,
                            pureToString(st, new SqlExpr.StructGet(x, "second")),
                            new SqlExpr.StringLit(">")));
        }
        return new SqlExpr.Cast(x, PureSql.type(Type.Primitive.STRING));
    }

    /** Cast a map operand to the call's RESOLVED Map(K, V) SQL type. */
    private static SqlExpr castToMapType(com.legend.compiler.spec.typed.TypedNativeCall n,
                                         SqlExpr m) {
        return n.info().type() instanceof Type.GenericType g && g.arguments().size() == 2
                ? new SqlExpr.Cast(m, new com.legend.sql.SqlType.Map(
                        PureSql.type(g.arguments().get(0)),
                        PureSql.type(g.arguments().get(1))))
                : m;
    }

    /**
     * A PAIR COLLECTION as a MAP value: map_from_entries over the lowered
     * STRUCT(first, second) list; the statically-EMPTY collection is the
     * typed empty map (CAST(MAP {{}} AS MAP(K, V)) from the resolved output).
     */
    private static SqlExpr mapFromPairs(com.legend.compiler.spec.typed.TypedNativeCall n,
                                        SqlExpr pairs) {
        // a SINGLE pair ([1] fits Pair[*]) wraps into the entry list
        if (pairs instanceof SqlExpr.StructLit) {
            pairs = new SqlExpr.ArrayLit(List.of(pairs));
        }
        if (pairs instanceof SqlExpr.NullLit) {
            Type out = n.info().type();
            if (out instanceof Type.GenericType g && g.arguments().size() == 2) {
                return new SqlExpr.Cast(SqlExpr.Call.of(SqlFn.MAP_EMPTY),
                        new com.legend.sql.SqlType.Map(
                                PureSql.type(g.arguments().get(0)),
                                PureSql.type(g.arguments().get(1))));
            }
            return SqlExpr.Call.of(SqlFn.MAP_EMPTY);
        }
        return SqlExpr.Call.of(SqlFn.MAP_FROM_ENTRIES, pairs);
    }

    /**
     * Comparator max/min (real collection max.pure: fold with STRICT
     * {@code >} — the FIRST max wins ties). The comparator must be a
     * KEY DIFFERENCE ({@code {x,y| f($x) - f($y)}}); the winner is the
     * element with the extreme key, earliest index on ties:
     * {@code (SELECT x FROM (UNNEST(l) x, UNNEST(range) i) ORDER BY key
     * DESC/ASC, i LIMIT 1)}.
     */
    private static SqlExpr comparatorSelect(SqlExpr list, SqlExpr.Lambda cmp, boolean maxIn) {
        boolean max = maxIn;
        if (!(cmp.body() instanceof SqlExpr.Call mc) || mc.fn() != SqlFn.MINUS
                || mc.args().size() != 2) {
            throw new IllegalStateException("comparator max/min supports key-difference"
                    + " comparators ({x,y | f($x) - f($y)}) only");
        }
        String px = cmp.params().get(0);
        String py = cmp.params().get(1);
        SqlExpr keyOfX = mc.args().get(0);
        // the two sides must be the SAME key over the two params —
        // {x,y | f($x) - f($y)} ascending, {x,y | f($y) - f($x)} REVERSED
        SqlExpr rightAsX = substituteRef(mc.args().get(1), py, new SqlExpr.Column(null, px));
        if (!keyOfX.equals(rightAsX)) {
            SqlExpr leftAsY = substituteRef(mc.args().get(0), py, new SqlExpr.Column(null, px));
            SqlExpr rightSide = mc.args().get(1);
            if (leftAsY.equals(rightSide)
                    || substituteRef(rightSide, px, new SqlExpr.Column(null, py)).equals(
                            substituteRef(mc.args().get(0),
                                    px, new SqlExpr.Column(null, py)))) {
                // reversed comparator: max-by-it is MIN by the key
                keyOfX = substituteRef(mc.args().get(1), px, new SqlExpr.Column(null, px));
                keyOfX = substituteRef(keyOfX, py, new SqlExpr.Column(null, px));
                max = !max;
            } else {
                throw new IllegalStateException("comparator max/min: the two comparator"
                        + " sides must apply the SAME key to each parameter");
            }
        }
        SqlExpr keyOverElem = substituteRef(keyOfX, px, new SqlExpr.Column("_cx", "x"));
        var inner = new com.legend.sql.SqlSelect(List.of(
                new com.legend.sql.SqlSelect.Projection(
                        SqlExpr.Call.of(SqlFn.UNNEST, list), "x"),
                new com.legend.sql.SqlSelect.Projection(
                        SqlExpr.Call.of(SqlFn.UNNEST, SqlExpr.Call.of(SqlFn.RANGE_FN,
                                new SqlExpr.IntLit(1),
                                SqlExpr.Call.of(SqlFn.PLUS,
                                        SqlExpr.Call.of(SqlFn.LIST_LENGTH, list),
                                        new SqlExpr.IntLit(1)))), "i")),
                false, null, null, List.of(), null, null, List.of(), null, null, List.of());
        var src = new com.legend.sql.SqlSource.Subselect(inner, "_cx");
        var outer = new com.legend.sql.SqlSelect(List.of(
                new com.legend.sql.SqlSelect.Projection(new SqlExpr.Column("_cx", "x"), "w")),
                false, src, null, List.of(), null, null,
                List.of(new com.legend.sql.SqlSelect.SortKey(keyOverElem, !max,
                                com.legend.sql.SqlSelect.SortKey.NullOrder.NULLS_LAST),
                        com.legend.sql.SqlSelect.SortKey.asc(new SqlExpr.Column("_cx", "i"))),
                1L, null, List.of());
        return new SqlExpr.ScalarSubquery(outer);
    }

    /** Literal cell of a TDS row → typed SQL literal, by the column's Pure type. */
    static SqlExpr tdsCell(String cell, Type type) {
        if (cell == null || cell.isEmpty()
                || (cell.equals("null") && !PlatformTypes.isVariant(type))) {
            // A bare 'null' cell is SQL NULL for EVERY non-variant type —
            // String included (a 'null' name must vanish from joinStrings
            // window collections, pure's empty semantics). A VARIANT 'null'
            // is the JSON null VALUE (variant arm below).
            return new SqlExpr.NullLit();
        }
        if (type == Type.Primitive.INTEGER) {
            return new SqlExpr.IntLit(Long.parseLong(cell));
        }
        if (type == Type.Primitive.FLOAT || type == Type.Primitive.NUMBER
                || type == Type.Primitive.DECIMAL || type instanceof Type.PrecisionDecimal) {
            // pure DECIMAL-suffix cells (21d) carry the marker in the TEXT
            String digits = cell.matches("[+-]?\\d+(\\.\\d+)?[dD]")
                    ? cell.substring(0, cell.length() - 1) : cell;
            return new SqlExpr.DecimalLit(new java.math.BigDecimal(digits));
        }
        if (type == Type.Primitive.BOOLEAN) {
            return new SqlExpr.BoolLit(Boolean.parseBoolean(cell));
        }
        if (type == Type.Primitive.STRICT_DATE) {
            return new SqlExpr.DateLit(cell.startsWith("%") ? cell.substring(1) : cell);
        }
        if (type == Type.Primitive.DATE_TIME || type == Type.Primitive.DATE) {
            String v = cell.startsWith("%") ? cell.substring(1) : cell;
            // Normalize the PCT fixture spelling: a +0000/Z suffix drops
            // (values are UTC) and sub-second digits truncate to DuckDB's
            // microsecond precision.
            v = v.replaceFirst("(\\+0000|Z)$", "");
            java.util.regex.Matcher frac = java.util.regex.Pattern
                    .compile("\\.(\\d{7,9})$").matcher(v);
            if (frac.find()) {
                v = v.substring(0, frac.start()) + "." + frac.group(1).substring(0, 6);
            }
            return new SqlExpr.TimestampLit(v);
        }
        if (type == Type.Primitive.STRING) {
            return new SqlExpr.StringLit(cell);
        }
        // A Variant cell is JSON TEXT (the TDS literal wraps it in quotes).
        if (PlatformTypes.isVariant(type)) {
            String json = cell.length() >= 2 && cell.startsWith("\"") && cell.endsWith("\"")
                    ? cell.substring(1, cell.length() - 1) : cell;
            return new SqlExpr.Cast(new SqlExpr.StringLit(json),
                    com.legend.sql.SqlType.Scalar.JSON);
        }
        throw new IllegalStateException(
                "no TDS cell rendering for Pure type " + type.typeName());
    }
    /** The DuckDB interval-constructor for a DurationUnit enum literal. */
    private static String intervalFn(com.legend.compiler.spec.typed.TypedSpec unit) {
        return switch (enumName(unit)) {
            case "YEARS" -> "to_years";
            case "MONTHS" -> "to_months";
            case "WEEKS" -> "to_weeks";
            case "DAYS" -> "to_days";
            case "HOURS" -> "to_hours";
            case "MINUTES" -> "to_minutes";
            case "SECONDS" -> "to_seconds";
            case "MILLISECONDS" -> "to_milliseconds";
            case "MICROSECONDS" -> "to_microseconds";
            default -> throw new IllegalStateException(
                    "unknown DurationUnit for interval arithmetic: " + enumName(unit));
        };
    }

    /** The date_diff part name for a DurationUnit enum literal. */
    private static String diffPart(com.legend.compiler.spec.typed.TypedSpec unit) {
        return switch (enumName(unit)) {
            case "YEARS" -> "year";
            case "MONTHS" -> "month";
            case "WEEKS" -> "week";
            case "DAYS" -> "day";
            case "HOURS" -> "hour";
            case "MINUTES" -> "minute";
            case "SECONDS" -> "second";
            case "MILLISECONDS" -> "millisecond";
            case "MICROSECONDS" -> "microsecond";
            default -> throw new IllegalStateException(
                    "unknown DurationUnit for dateDiff: " + enumName(unit));
        };
    }

    /**
     * A DATE-ARITHMETIC argument: partial-date LITERALS (year, year-month
     * — globally string-typed for the pinned string-comparison semantics)
     * pad to the first of their period as real DATE literals.
     */
    private static SqlExpr dateArg(com.legend.compiler.spec.typed.TypedSpec typed,
                                   SqlExpr lowered) {
        if (typed instanceof com.legend.compiler.spec.typed.TypedCDate d) {
            if (d.value() instanceof com.legend.values.PureDateLiteral.Year y) {
                return new SqlExpr.DateLit(y.toEngineString() + "-01-01");
            }
            if (d.value() instanceof com.legend.values.PureDateLiteral.YearMonth ym) {
                return new SqlExpr.DateLit(ym.toEngineString() + "-01");
            }
        }
        return lowered;
    }

    /**
     * {@code dateDiff} with REAL pure's per-unit semantics (PCT-pinned):
     * WEEKS counts SUNDAY-boundary crossings — {@code (d1, d2]} forward but
     * {@code [d2, d1)} backward (NOT the negation; the audit's asymmetry);
     * HOURS/MINUTES/SECONDS are TRUNCATED ELAPSED time (SQL date_diff
     * counts boundary crossings — a different number); the calendar parts
     * (year/month/day/ms) match SQL date_diff.
     */
    private static SqlExpr dateDiffExpr(String part, SqlExpr d1, SqlExpr d2) {
        switch (part) {
            case "week" -> {
                SqlExpr forward = SqlExpr.Call.of(SqlFn.MINUS,
                        sundayIndex(d2), sundayIndex(d1));
                SqlExpr backward = SqlExpr.Call.of(SqlFn.MINUS,
                        sundayIndex(backOneDay(d2)), sundayIndex(backOneDay(d1)));
                return new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                        SqlExpr.Call.of(SqlFn.LESS_EQUAL,
                                new SqlExpr.Call(SqlFn.DATE_DIFF, List.of(
                                        new SqlExpr.StringLit("day"), d2, d1)),
                                new SqlExpr.IntLit(0)),
                        forward)), backward);
            }
            case "hour" -> {
                return elapsed(d1, d2, 3_600_000L);
            }
            case "minute" -> {
                return elapsed(d1, d2, 60_000L);
            }
            case "second" -> {
                return elapsed(d1, d2, 1_000L);
            }
            default -> {
                return new SqlExpr.Call(SqlFn.DATE_DIFF, List.of(
                        new SqlExpr.StringLit(part), d1, d2));
            }
        }
    }

    /** Truncated elapsed time in {@code unitMs} chunks (Java toHours-style). */
    private static SqlExpr elapsed(SqlExpr d1, SqlExpr d2, long unitMs) {
        return SqlExpr.Call.of(SqlFn.INT_DIVIDE,
                SqlExpr.Call.of(SqlFn.MINUS,
                        new SqlExpr.Call(SqlFn.EPOCH_MS, List.of(d2)),
                        new SqlExpr.Call(SqlFn.EPOCH_MS, List.of(d1))),
                new SqlExpr.IntLit(unitMs));
    }

    /**
     * Floored week index counted from an ANCIENT Sunday epoch (0001-01-07,
     * proleptic Gregorian) — always positive for real dates, so DuckDB's
     * truncating {@code //} IS floor division (the audit's pre-1970 case).
     */
    private static SqlExpr sundayIndex(SqlExpr d) {
        return SqlExpr.Call.of(SqlFn.INT_DIVIDE,
                new SqlExpr.Call(SqlFn.DATE_DIFF, List.of(
                        new SqlExpr.StringLit("day"),
                        new SqlExpr.DateLit("0001-01-07"), d)),
                new SqlExpr.IntLit(7));
    }

    private static SqlExpr backOneDay(SqlExpr d) {
        return new SqlExpr.Call(SqlFn.ADD_INTERVAL, List.of(
                new SqlExpr.StringLit("to_days"),
                new SqlExpr.IntLit(-1), d));
    }

    /**
     * Pure's DISCRETE percentile (engine percentile.pure): over the sorted
     * data, {@code ip = floor(p*(n-1))}; pick {@code data[ip]} when
     * {@code (ip+1)/n > p}, else {@code data[ip+1]}. quantile_disc computes
     * a DIFFERENT rank at exact-rank points — the audit's divergence.
     */
    private static SqlExpr pureDiscretePercentile(SqlExpr list, SqlExpr p,
            boolean ascending) {
        SqlExpr sorted = new SqlExpr.Call(
                ascending ? SqlFn.LIST_SORT : SqlFn.LIST_SORT_DESC, List.of(list));
        SqlExpr n = new SqlExpr.Call(SqlFn.LIST_LENGTH, List.of(list));
        SqlExpr ip = new SqlExpr.Call(SqlFn.FLOOR, List.of(
                SqlExpr.Call.of(SqlFn.TIMES, p,
                        SqlExpr.Call.of(SqlFn.MINUS, n, new SqlExpr.IntLit(1)))));
        SqlExpr pick = new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.GREATER,
                        SqlExpr.Call.of(SqlFn.PLUS, ip, new SqlExpr.IntLit(1)),
                        SqlExpr.Call.of(SqlFn.TIMES, p, n)),
                SqlExpr.Call.of(SqlFn.PLUS, ip, new SqlExpr.IntLit(1)))),
                SqlExpr.Call.of(SqlFn.PLUS, ip, new SqlExpr.IntLit(2)));
        return new SqlExpr.Call(SqlFn.LIST_GET, List.of(sorted,
                new SqlExpr.Cast(pick, com.legend.sql.SqlType.Scalar.BIGINT)));
    }

    /**
     * Replace bare references to {@code name} with {@code replacement} across
     * an expression tree — how a 2-param comparator lambda closes over the
     * needle when squeezed into a 1-param SQL lambda. Inner lambdas that
     * rebind the name SHADOW (no substitution inside).
     */
    /** A comparator whose body is bare eq/equal over its two parameters. */
    private static boolean isEqualityComparator(com.legend.compiler.spec.typed.TypedSpec spec) {
        if (!(spec instanceof com.legend.compiler.spec.typed.TypedLambda cmp)
                || cmp.parameters().size() != 2 || cmp.body().size() != 1
                || !(cmp.body().get(0) instanceof TypedNativeCall cc)
                || cc.args().size() != 2) {
            return false;
        }
        String fqn = cc.callee().qualifiedName();
        return fqn.equals("meta::pure::functions::boolean::eq")
                || fqn.equals("meta::pure::functions::boolean::equal");
    }

    /**
     * The direction of a bare-compare comparator: {@code {x,y|$x->compare($y)}}
     * ascending, {@code {x,y|$y->compare($x)}} descending; anything richer
     * has no relational sort shape (null).
     */
    private static Boolean comparatorDirection(com.legend.compiler.spec.typed.TypedSpec spec) {
        if (!(spec instanceof com.legend.compiler.spec.typed.TypedLambda cmp)
                || cmp.parameters().size() != 2 || cmp.body().size() != 1
                || !(cmp.body().get(0) instanceof TypedNativeCall cc)
                || !cc.callee().qualifiedName().equals("meta::pure::functions::lang::compare")
                || cc.args().size() != 2
                || !(cc.args().get(0) instanceof com.legend.compiler.spec.typed.TypedVariable a)
                || !(cc.args().get(1) instanceof com.legend.compiler.spec.typed.TypedVariable b)) {
            return null;
        }
        String p0 = cmp.parameters().get(0);
        String p1 = cmp.parameters().get(1);
        if (a.name().equals(p0) && b.name().equals(p1)) {
            return Boolean.TRUE;
        }
        if (a.name().equals(p1) && b.name().equals(p0)) {
            return Boolean.FALSE;
        }
        return null;
    }

    private static SqlExpr substituteRef(SqlExpr e, String name, SqlExpr replacement) {
        return switch (e) {
            case SqlExpr.Column c when c.table() == null && name.equals(c.name()) -> replacement;
            case SqlExpr.Column c when name.equals(c.table()) ->
                    new SqlExpr.StructGet(replacement, c.name());   // $b.field over the needle
            case SqlExpr.Call c -> new SqlExpr.Call(c.fn(),
                    c.args().stream().map(a -> substituteRef(a, name, replacement)).toList());
            case SqlExpr.Cast c ->
                    new SqlExpr.Cast(substituteRef(c.value(), name, replacement), c.target());
            case SqlExpr.ArrayLit a -> new SqlExpr.ArrayLit(a.elements().stream()
                    .map(x -> substituteRef(x, name, replacement)).toList());
            case SqlExpr.StructLit s -> new SqlExpr.StructLit(s.fields().stream()
                    .map(fl -> new SqlExpr.StructLit.Field(fl.name(),
                            substituteRef(fl.value(), name, replacement))).toList());
            case SqlExpr.StructGet g ->
                    new SqlExpr.StructGet(substituteRef(g.source(), name, replacement), g.field());
            case SqlExpr.Case cs -> new SqlExpr.Case(
                    cs.whens().stream().map(w -> new SqlExpr.Case.When(
                            substituteRef(w.condition(), name, replacement),
                            substituteRef(w.then(), name, replacement))).toList(),
                    cs.otherwise() == null ? null
                            : substituteRef(cs.otherwise(), name, replacement));
            case SqlExpr.Lambda l -> l.params().contains(name)
                    ? l
                    : new SqlExpr.Lambda(l.params(), substituteRef(l.body(), name, replacement));
            default -> e;   // leaves and query-carrying nodes: no bare lambda refs inside
        };
    }

    /** Whether a type is an instance kind (a user class or parameterized class), not a primitive. */
    /**
     * Pure prints a Float via its MINIMAL decimal repr: DuckDB's shortest
     * round-trip VARCHAR cast already matches ('1.5', '2.0') EXCEPT where it
     * chooses exponent notation — those re-render plain through a
     * DECIMAL(38,18) cast with trailing zeros trimmed (and a bare trailing
     * dot restored to '.0'). Magnitudes outside DECIMAL(38,18) keep the
     * exponent form.
     */
    private static SqlExpr floatRepr(SqlExpr x) {
        SqlExpr s = new SqlExpr.Cast(x, com.legend.sql.SqlType.Scalar.VARCHAR);
        // FRACTION-FREE values render through HUGEINT — exact plain digits
        // for the whole [1e16, 1e38) band where the DECIMAL(38,18) cast
        // fabricates garbage (audit: 1e18 printed ...042.42...); every
        // double >= 2^53 is fraction-free, so all large magnitudes take
        // this branch.
        SqlExpr intPlain = SqlExpr.Call.of(SqlFn.CONCAT,
                new SqlExpr.Cast(new SqlExpr.Cast(x, com.legend.sql.SqlType.Scalar.HUGEINT),
                        com.legend.sql.SqlType.Scalar.VARCHAR),
                new SqlExpr.StringLit(".0"));
        SqlExpr plain = SqlExpr.Call.of(SqlFn.RTRIM,
                new SqlExpr.Cast(new SqlExpr.Cast(x, new com.legend.sql.SqlType.Decimal(38, 18)),
                        com.legend.sql.SqlType.Scalar.VARCHAR),
                new SqlExpr.StringLit("0"));
        SqlExpr fixed = new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.ENDS_WITH, plain, new SqlExpr.StringLit(".")),
                SqlExpr.Call.of(SqlFn.CONCAT, plain, new SqlExpr.StringLit("0")))),
                plain);
        SqlExpr hasExp = SqlExpr.Call.of(SqlFn.GREATER,
                SqlExpr.Call.of(SqlFn.STRPOS, s, new SqlExpr.StringLit("e")),
                new SqlExpr.IntLit(0));
        SqlExpr fractionFree = SqlExpr.Call.of(SqlFn.AND,
                SqlExpr.Call.of(SqlFn.EQUAL, x, SqlExpr.Call.of(SqlFn.FLOOR_RAW, x)),
                SqlExpr.Call.of(SqlFn.LESS,
                        SqlExpr.Call.of(SqlFn.ABS, x), new SqlExpr.FloatLit(1e38)));
        // The DECIMAL path stays only where the scale-18 cast is exact for
        // short-decimal values: fractional magnitudes in [1e-17, 2^53)
        // (below 1e-17 the scale rounds — 1.5e-18 gained a digit; audit).
        SqlExpr inRange = SqlExpr.Call.of(SqlFn.AND,
                SqlExpr.Call.of(SqlFn.GREATER_EQUAL,
                        SqlExpr.Call.of(SqlFn.ABS, x), new SqlExpr.FloatLit(1e-17)),
                SqlExpr.Call.of(SqlFn.LESS,
                        SqlExpr.Call.of(SqlFn.ABS, x), new SqlExpr.FloatLit(9.007199254740992e15)));
        return new SqlExpr.Case(List.of(
                new SqlExpr.Case.When(
                        SqlExpr.Call.of(SqlFn.AND, hasExp, fractionFree), intPlain),
                new SqlExpr.Case.When(
                        SqlExpr.Call.of(SqlFn.AND, hasExp, inRange), fixed)), s);
    }

    /**
     * Scans a LITERAL printf format string for the pure-only directives,
     * rewriting each to %s and wrapping the matching spread argument
     * (spread = [fmt, arg1, ...]; directive order maps to argument order).
     */
    private static void rewriteFormatDirectives(String fmt, List<SqlExpr> spread) {
        StringBuilder out = new StringBuilder();
        int argIdx = 1;
        int i = 0;
        while (i < fmt.length()) {
            char c = fmt.charAt(i);
            if (c != '%' || i + 1 >= fmt.length()) {
                out.append(c);
                i++;
                continue;
            }
            char d = fmt.charAt(i + 1);
            if (d == '%') {
                out.append("%%");
                i += 2;
                continue;
            }
            if (d == 't' && i + 2 < fmt.length() && fmt.charAt(i + 2) == '{') {
                int close = fmt.indexOf('}', i + 3);
                if (close < 0) {
                    throw new IllegalStateException("unterminated %t{ in format: " + fmt);
                }
                spread.set(argIdx, SqlExpr.Call.of(SqlFn.STRFTIME, spread.get(argIdx),
                        new SqlExpr.StringLit(javaDateToStrftime(fmt.substring(i + 3, close)))));
                out.append("%s");
                argIdx++;
                i = close + 1;
                continue;
            }
            if (d == 'f') {
                spread.set(argIdx, floatRepr(spread.get(argIdx)));
                out.append("%s");
                argIdx++;
                i += 2;
                continue;
            }
            out.append('%').append(d);
            argIdx++;
            i += 2;
        }
        spread.set(0, new SqlExpr.StringLit(out.toString()));
    }

    /**
     * Java SimpleDateFormat pattern -> strftime, longest token first. Values
     * are UTC throughout, so the ZONE directives are literals: Z prints the
     * +0000 offset, X the ISO 'Z'.
     */
    private static String javaDateToStrftime(String pattern) {
        StringBuilder out = new StringBuilder();
        int i = 0;
        while (i < pattern.length()) {
            if (pattern.charAt(i) == '"') {
                int close = pattern.indexOf('"', i + 1);
                if (close < 0) {
                    throw new IllegalStateException("unterminated quote in date pattern: " + pattern);
                }
                out.append(pattern, i + 1, close);
                i = close + 1;
                continue;
            }
            String rest = pattern.substring(i);
            String[][] tokens = {
                    {"yyyy", "%Y"}, {"SSS", "%g"}, {"MM", "%m"}, {"dd", "%d"},
                    {"HH", "%H"}, {"hh", "%I"}, {"mm", "%M"}, {"ss", "%S"},
                    {"h", "%-I"}, {"a", "%p"}, {"Z", "+0000"}, {"X", "Z"},
            };
            boolean matched = false;
            for (String[] t : tokens) {
                if (rest.startsWith(t[0])) {
                    out.append(t[1]);
                    i += t[0].length();
                    matched = true;
                    break;
                }
            }
            if (!matched) {
                char ch = pattern.charAt(i);
                // A pattern LETTER outside the token table would silently
                // pass through as literal text ('MMM' -> '03M'; audit) —
                // loud instead; punctuation/separators pass.
                if (Character.isLetter(ch)) {
                    throw new IllegalStateException("unsupported date-format"
                            + " token '" + ch + "' in pattern '" + pattern + "'");
                }
                out.append(ch);
                i++;
            }
        }
        return out.toString();
    }

    private static boolean isClassish(Type t) {
        return (t instanceof Type.ClassType && !PlatformTypes.isVariant(t)
                        && !PlatformTypes.isAny(t) && !PlatformTypes.isNil(t))
                || t instanceof Type.GenericType;
    }

    /** Partial-date-literal precision: 1 = year, 2 = year-month; null otherwise. */
    private static Integer partialPrecision(com.legend.compiler.spec.typed.TypedSpec t) {
        if (t instanceof com.legend.compiler.spec.typed.TypedCDate d) {
            if (d.value() instanceof com.legend.values.PureDateLiteral.Year) {
                return 1;
            }
            if (d.value() instanceof com.legend.values.PureDateLiteral.YearMonth) {
                return 2;
            }
        }
        return null;
    }

    /** A date type whose VALUES are always full-precision (columns, full literals). */
    private static boolean isFullPrecisionDate(Type t) {
        return t == Type.Primitive.STRICT_DATE || t == Type.Primitive.DATE_TIME
                || t == Type.Primitive.DATE;
    }

    /** Whether an argument's Pure multiplicity is at most one. */
    private static boolean isToOne(com.legend.compiler.spec.typed.TypedSpec arg) {
        var m = arg.info().multiplicity();
        return m instanceof com.legend.compiler.element.type.Multiplicity.Bounded b
                && Integer.valueOf(1).equals(b.upper());
    }

    /** A literal boolean argument; LOUD otherwise (never a silent default). */
    private static boolean boolLiteral(com.legend.compiler.spec.typed.TypedSpec arg,
            String what) {
        if (arg instanceof com.legend.compiler.spec.typed.TypedCBoolean b) {
            return b.value();
        }
        throw new IllegalStateException(what + " must be a literal boolean, got "
                + arg.getClass().getSimpleName());
    }

    /** The enum VALUE of a literal enum argument; loud on anything else. */
    /**
     * RegexpParameter enum values (single or list) as RE2 INLINE flag chars —
     * prepended to the pattern as {@code (?ims)}; DuckDB's option-argument
     * chars have different semantics, inline flags are the portable spelling.
     */
    private static String regexpFlags(com.legend.compiler.spec.typed.TypedSpec arg) {
        List<com.legend.compiler.spec.typed.TypedSpec> params =
                arg instanceof com.legend.compiler.spec.typed.TypedCollection c
                        ? c.elements() : List.of(arg);
        StringBuilder flags = new StringBuilder();
        for (var pm : params) {
            flags.append(switch (enumName(pm)) {
                case "CASE_SENSITIVE" -> "";   // the default
                case "CASE_INSENSITIVE" -> "i";
                case "MULTILINE" -> "m";
                case "NON_NEWLINE_SENSITIVE" -> "s";
                default -> throw new IllegalStateException(
                        "unknown RegexpParameter " + enumName(pm));
            });
        }
        return flags.toString();
    }

    /** {@code pattern} -> {@code '(?<flags>)' || pattern}; identity when no flags. */
    private static SqlExpr inlineFlags(SqlExpr pattern, String flags) {
        if (flags.isEmpty()) {
            return pattern;
        }
        String prefix = "(?" + flags + ")";
        return pattern instanceof SqlExpr.StringLit lit
                ? new SqlExpr.StringLit(prefix + lit.value())
                : SqlExpr.Call.of(SqlFn.CONCAT, new SqlExpr.StringLit(prefix), pattern);
    }

    /**
     * {@code regexp_extract_all(subject, pattern, group, flags)} for a regexp
     * call whose OPTIONAL group/params begin at {@code tailStart} in the
     * lowered args (group defaults 0; flags default '').
     */
    private static SqlExpr regexpAll(com.legend.compiler.spec.typed.TypedNativeCall n,
                                     List<SqlExpr> args, int tailStart) {
        SqlExpr group = new SqlExpr.IntLit(0);
        String flags = "";
        for (int i = tailStart; i < n.args().size(); i++) {
            if (args.get(i) instanceof SqlExpr.IntLit g) {
                group = g;
            } else {
                flags = regexpFlags(n.args().get(i));
            }
        }
        return new SqlExpr.Call(SqlFn.REGEXP_EXTRACT_ALL, List.of(
                args.get(0), inlineFlags(args.get(1), flags), group));
    }

    private static String enumName(com.legend.compiler.spec.typed.TypedSpec arg) {
        if (arg instanceof com.legend.compiler.spec.typed.TypedEnumValue ev) {
            return ev.value();
        }
        throw new IllegalStateException("a DurationUnit argument must be an enum"
                + " literal, got " + arg.getClass().getSimpleName());
    }

    /**
     * The precision RANK of a date argument (0=year .. 6=subsecond): a
     * LITERAL answers from its own written precision; a column from its
     * Pure type (StrictDate = day, DateTime = SQL TIMESTAMP = full); the
     * abstract Date is undecidable and refuses loudly.
     */
    private static int datePrecision(com.legend.compiler.spec.typed.TypedSpec arg) {
        if (arg instanceof com.legend.compiler.spec.typed.TypedCDate d) {
            return switch (d.value()) {
                case com.legend.values.PureDateLiteral.Year ignored -> 0;
                case com.legend.values.PureDateLiteral.YearMonth ignored -> 1;
                case com.legend.values.PureDateLiteral.StrictDate ignored -> 2;
                case com.legend.values.PureDateLiteral.DateWithHour ignored -> 3;
                case com.legend.values.PureDateLiteral.DateWithMinute ignored -> 4;
                case com.legend.values.PureDateLiteral.DateWithSecond ignored -> 5;
                default -> 6;
            };
        }
        // A date() CONSTRUCTOR call's precision is its ARITY — the static
        // return type says DateTime for every arity (audit: hasMinute of
        // date(y,mo,d,h) answered true).
        if (arg instanceof TypedNativeCall dc
                && dc.callee().qualifiedName().equals("meta::pure::functions::date::date")) {
            return switch (dc.args().size()) {
                case 1 -> 0;
                case 2 -> 1;
                case 3 -> 2;
                case 4 -> 3;
                case 5 -> 4;
                default -> dc.args().get(5).info().type()
                        == com.legend.compiler.element.type.Type.Primitive.FLOAT
                        || dc.args().get(5).info().type()
                                == com.legend.compiler.element.type.Type.Primitive.DECIMAL
                        ? 6 : 5;
            };
        }
        var t = arg.info().type();
        if (t == com.legend.compiler.element.type.Type.Primitive.DATE_TIME) {
            return 6;
        }
        if (t == com.legend.compiler.element.type.Type.Primitive.STRICT_DATE) {
            return 2;
        }
        throw new IllegalStateException("a date-precision predicate over the"
                + " abstract Date type is not statically decidable — declare"
                + " the value StrictDate or DateTime");
    }

}
