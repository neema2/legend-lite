package com.legend.lowering;

import com.legend.builtin.Pure;
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
        family(SqlFn.EQUAL, "equal");
        family(SqlFn.LESS, "lessThan");
        family(SqlFn.LESS_EQUAL, "lessThanEqual");
        family(SqlFn.GREATER, "greaterThan");
        family(SqlFn.GREATER_EQUAL, "greaterThanEqual");
        family(SqlFn.AND, "and");
        family(SqlFn.OR, "or");
        // not(equal(a,b)) renders as a <> b (lean SQL): the peephole keeps
        // SqlFn.NOT_EQUAL alive even though the QUERY spelling is not(equal)
        // (real pure has no notEqual — FQN_MIGRATION finding).
        for (String f : Pure.nativeKeysAt("not")) {
            RULES.put(f, (n, args) ->
                    args.get(0) instanceof SqlExpr.Call c && c.fn() == SqlFn.EQUAL
                            ? new SqlExpr.Call(SqlFn.NOT_EQUAL, c.args())
                            : new SqlExpr.Call(SqlFn.NOT, args));
        }
        family(SqlFn.PLUS, "plus");
        family(SqlFn.MINUS, "minus");
        family(SqlFn.TIMES, "times");
        family(SqlFn.DIVIDE, "divide");
        family(SqlFn.MOD, "mod");
        family(SqlFn.REM, "rem");
        family(SqlFn.ABS, "abs");
        family(SqlFn.IS_NULL, "isEmpty");
        family(SqlFn.IS_NOT_NULL, "isNotEmpty");
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
                Map.entry("round", SqlFn.ROUND), Map.entry("sign", SqlFn.SIGN),
                Map.entry("xor", SqlFn.XOR),
                Map.entry("bitAnd", SqlFn.BIT_AND), Map.entry("bitOr", SqlFn.BIT_OR),
                Map.entry("bitXor", SqlFn.BIT_XOR),
                Map.entry("bitShiftLeft", SqlFn.BIT_SHIFT_LEFT),
                Map.entry("bitShiftRight", SqlFn.BIT_SHIFT_RIGHT),
                // Strings — plain families first; index-shifted below.
                Map.entry("startsWith", SqlFn.STARTS_WITH),
                Map.entry("endsWith", SqlFn.ENDS_WITH),
                Map.entry("matches", SqlFn.MATCHES),
                Map.entry("left", SqlFn.LEFT), Map.entry("right", SqlFn.RIGHT),
                Map.entry("lpad", SqlFn.LPAD), Map.entry("rpad", SqlFn.RPAD),
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
                Map.entry("greatest", SqlFn.GREATEST),
                Map.entry("least", SqlFn.LEAST),
                // Temporal
                Map.entry("today", SqlFn.TODAY), Map.entry("now", SqlFn.NOW),
                Map.entry("datePart", SqlFn.DATE_TRUNC_DAY),
                // Lists / collections
                Map.entry("zip", SqlFn.LIST_ZIP),
                Map.entry("removeDuplicates", SqlFn.LIST_DISTINCT),
                Map.entry("add", SqlFn.LIST_APPEND),
                Map.entry("mean", SqlFn.LIST_AVG),
                Map.entry("average", SqlFn.LIST_AVG),
                Map.entry("median", SqlFn.LIST_MEDIAN),
                Map.entry("mode", SqlFn.LIST_MODE),
                Map.entry("tail", SqlFn.LIST_TAIL),
                Map.entry("init", SqlFn.LIST_INIT),
                Map.entry("range", SqlFn.RANGE_FN),
                Map.entry("toVariant", SqlFn.TO_VARIANT)).entrySet()) {
            familyIfPresent(e.getValue(), e.getKey());
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
        for (String f : Pure.nativeKeysAt("min")) {
            RULES.put(f, (n, args) -> args.size() == 1
                    ? new SqlExpr.Call(SqlFn.LIST_MIN, args) : new SqlExpr.Call(SqlFn.LEAST, args));
        }
        for (String f : Pure.nativeKeysAt("max")) {
            RULES.put(f, (n, args) -> args.size() == 1
                    ? new SqlExpr.Call(SqlFn.LIST_MAX, args) : new SqlExpr.Call(SqlFn.GREATEST, args));
        }
        for (String f : Pure.nativeKeysAt("sum")) {
            RULES.put(f, (n, args) -> new SqlExpr.Call(SqlFn.LIST_SUM, args));
        }
        for (String f : Pure.nativeKeysAt("first")) {
            RULES.put(f, (n, args) -> new SqlExpr.Call(SqlFn.LIST_GET,
                    List.of(args.get(0), new SqlExpr.IntLit(1))));
        }
        for (String f : Pure.nativeKeysAt("head")) {
            RULES.put(f, (n, args) -> new SqlExpr.Call(SqlFn.LIST_GET,
                    List.of(args.get(0), new SqlExpr.IntLit(1))));
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
            RULES.put(f, (n, args) -> new SqlExpr.Call(SqlFn.MINUS, List.of(
                    new SqlExpr.Call(SqlFn.STRPOS, args), new SqlExpr.IntLit(1))));
        }
        for (String f : Pure.nativeKeysAt("at")) {
            RULES.put(f, (n, args) -> new SqlExpr.Call(SqlFn.LIST_GET,
                    List.of(args.get(0), plusOne(args.get(1)))));
        }
        for (String f : Pure.nativeKeysAt("splitPart")) {
            RULES.put(f, (n, args) -> {
                List<SqlExpr> shifted = new ArrayList<>(args);
                shifted.set(2, plusOne(args.get(2)));
                return new SqlExpr.Call(SqlFn.SPLIT_PART, shifted);
            });
        }
        // contains on STRINGS: strpos > 0 (list contains stays LIST_CONTAINS
        // by overload identity — register string overloads specifically).
        for (String f : Pure.nativeKeysAt("contains")) {
            RULES.put(f, (n, args) ->
                    n.args().get(0).info().type() == Type.Primitive.STRING
                            ? new SqlExpr.Call(SqlFn.GREATER, List.of(
                                    new SqlExpr.Call(SqlFn.STRPOS, args), new SqlExpr.IntLit(0)))
                            : new SqlExpr.Call(SqlFn.LIST_CONTAINS, args));
        }
        // format('%s...', [args]) -> printf(fmt, args...): the array spreads.
        for (String f : Pure.nativeKeysAt("format")) {
            RULES.put(f, (n, args) -> {
                List<SqlExpr> spread = new ArrayList<>();
                spread.add(args.get(0));
                if (args.get(1) instanceof SqlExpr.ArrayLit arr) {
                    spread.addAll(arr.elements());
                } else {
                    spread.add(args.get(1));
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
        castFamily("toString", Type.Primitive.STRING);
        castFamily("parseInteger", Type.Primitive.INTEGER);
        castFamily("parseFloat", Type.Primitive.FLOAT);
        castFamily("toFloat", Type.Primitive.FLOAT);
        castFamily("parseDecimal", Type.Primitive.DECIMAL);
        castFamily("parseBoolean", Type.Primitive.BOOLEAN);
        castFamily("parseDate", Type.Primitive.DATE_TIME);
        // date(y,m,d[,h,mi,s]) constructors.
        for (String f : Pure.nativeKeysAt("date")) {
            RULES.put(f, (n, args) -> new SqlExpr.Call(
                    args.size() <= 3 ? SqlFn.MAKE_DATE : SqlFn.MAKE_TIMESTAMP, args));
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
            List<SqlExpr> flat = new ArrayList<>();
            flat.add(args.get(0));
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

    /** Literal cell of a TDS row → typed SQL literal, by the column's Pure type. */
    static SqlExpr tdsCell(String cell, Type type) {
        if (cell == null || cell.isEmpty()) {
            return new SqlExpr.NullLit();
        }
        if (type == Type.Primitive.INTEGER) {
            return new SqlExpr.IntLit(Long.parseLong(cell));
        }
        if (type == Type.Primitive.FLOAT || type == Type.Primitive.NUMBER
                || type == Type.Primitive.DECIMAL) {
            return new SqlExpr.DecimalLit(new java.math.BigDecimal(cell));
        }
        if (type == Type.Primitive.BOOLEAN) {
            return new SqlExpr.BoolLit(Boolean.parseBoolean(cell));
        }
        if (type == Type.Primitive.STRICT_DATE) {
            return new SqlExpr.DateLit(cell.startsWith("%") ? cell.substring(1) : cell);
        }
        if (type == Type.Primitive.DATE_TIME || type == Type.Primitive.DATE) {
            return new SqlExpr.TimestampLit(cell.startsWith("%") ? cell.substring(1) : cell);
        }
        if (type == Type.Primitive.STRING) {
            return new SqlExpr.StringLit(cell);
        }
        throw new IllegalStateException(
                "no TDS cell rendering for Pure type " + type.typeName());
    }
}
