package org.finos.legend.engine.plan;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Static registry of Pure function signatures.
 * Maps (functionName, targetType) → returnType for all scalar Pure functions.
 *
 * This is the PRIMARY source of return types for FunctionExpression.type().
 * Every function that goes through FunctionExpression SHOULD be registered here.
 * Unregistered functions return ANY (to be eliminated in Step D).
 *
 * Type rules:
 * - CONSTANT: always returns a fixed type (e.g., length → INTEGER)
 * - PASSTHROUGH: returns same type as target (e.g., abs(n) → n's type)
 * - NUMERIC_PASSTHROUGH: returns the numeric supertype of arguments
 */
public final class PureFunctionRegistry {

    public enum TypeRule {
        CONSTANT,
        PASSTHROUGH,
        NUMERIC_PASSTHROUGH
    }

    public record FunctionSig(TypeRule rule, GenericType returnType) {
        static FunctionSig constant(GenericType type) {
            return new FunctionSig(TypeRule.CONSTANT, type);
        }

        static FunctionSig passthrough() {
            return new FunctionSig(TypeRule.PASSTHROUGH, GenericType.Primitive.ANY);
        }

        static FunctionSig numericPassthrough() {
            return new FunctionSig(TypeRule.NUMERIC_PASSTHROUGH, GenericType.Primitive.NUMBER);
        }
    }

    private static final Map<String, FunctionSig> REGISTRY = new HashMap<>();

    static {
        // ===== String → String =====
        for (String fn : List.of(
                "toUpper", "toLower", "trim", "ltrim", "rtrim",
                "lpad", "rpad", "reverse", "reverseString",
                "substring", "substr", "repeatString", "replace",
                "toLowerFirstCharacter", "toUpperFirstCharacter",
                "left", "splitPart", "format")) {
            register(fn, FunctionSig.constant(GenericType.Primitive.STRING));
        }

        // ===== String → Integer =====
        for (String fn : List.of("length", "indexOf", "ascii", "levenshteinDistance")) {
            register(fn, FunctionSig.constant(GenericType.Primitive.INTEGER));
        }

        // ===== String → Boolean =====
        for (String fn : List.of("startsWith", "endsWith", "contains", "matches", "isEmpty")) {
            register(fn, FunctionSig.constant(GenericType.Primitive.BOOLEAN));
        }

        register("concat", FunctionSig.constant(GenericType.Primitive.STRING));
        register("joinStrings", FunctionSig.constant(GenericType.Primitive.STRING));

        // ===== Math =====
        for (String fn : List.of(
                "abs", "ceiling", "ceil", "floor",
                "sqrt", "cbrt", "exp", "log", "log10",
                "pow", "power", "rem", "mod")) {
            register(fn, FunctionSig.numericPassthrough());
        }

        for (String fn : List.of(
                "sin", "cos", "tan", "asin", "acos", "atan", "atan2",
                "sinh", "cosh", "tanh", "cot",
                "toRadians", "toDegrees")) {
            register(fn, FunctionSig.constant(GenericType.Primitive.FLOAT));
        }

        register("round", FunctionSig.numericPassthrough());
        register("sign", FunctionSig.constant(GenericType.Primitive.INTEGER));
        register("pi", FunctionSig.constant(GenericType.Primitive.FLOAT));

        // ===== Conversion =====
        register("toString", FunctionSig.constant(GenericType.Primitive.STRING));
        register("toOne", FunctionSig.passthrough());
        register("toMany", FunctionSig.passthrough());
        register("parseInteger", FunctionSig.constant(GenericType.Primitive.INTEGER));
        register("toInteger", FunctionSig.constant(GenericType.Primitive.INTEGER));
        register("parseFloat", FunctionSig.constant(GenericType.Primitive.FLOAT));
        register("toFloat", FunctionSig.constant(GenericType.Primitive.FLOAT));
        register("parseDecimal", FunctionSig.constant(GenericType.Primitive.DECIMAL));
        register("toDecimal", FunctionSig.constant(GenericType.Primitive.DECIMAL));
        register("parseBoolean", FunctionSig.constant(GenericType.Primitive.BOOLEAN));
        register("type", FunctionSig.constant(GenericType.Primitive.STRING));

        // ===== Encoding =====
        register("encodeBase64", FunctionSig.constant(GenericType.Primitive.STRING));
        register("decodeBase64", FunctionSig.constant(GenericType.Primitive.STRING));
        register("hash", FunctionSig.constant(GenericType.Primitive.STRING));
        register("hashCode", FunctionSig.constant(GenericType.Primitive.INTEGER));

        // ===== Date extraction → Integer =====
        for (String fn : List.of(
                "year", "yearNumber", "month", "monthNumber",
                "dayOfMonth", "dayNumber", "dayOfWeek", "dayOfYear",
                "hour", "hourNumber", "minute", "minuteNumber",
                "second", "secondNumber", "weekOfYear", "quarter")) {
            register(fn, FunctionSig.constant(GenericType.Primitive.INTEGER));
        }

        // ===== Date → Date (passthrough: adjust(StrictDate)→StrictDate) =====
        for (String fn : List.of("adjust", "datePart", "dateTrunc", "timeBucket")) {
            register(fn, FunctionSig.passthrough());
        }
        register("today", FunctionSig.constant(GenericType.Primitive.STRICT_DATE));
        register("now", FunctionSig.constant(GenericType.Primitive.DATE_TIME));
        register("parseDate", FunctionSig.constant(GenericType.Primitive.DATE_TIME));
        register("dateDiff", FunctionSig.constant(GenericType.Primitive.INTEGER));
        register("epoch", FunctionSig.constant(GenericType.Primitive.INTEGER));

        // ===== Aggregates =====
        register("sum", FunctionSig.numericPassthrough());
        register("avg", FunctionSig.constant(GenericType.Primitive.FLOAT));
        register("average", FunctionSig.constant(GenericType.Primitive.FLOAT));
        register("wavg", FunctionSig.constant(GenericType.Primitive.FLOAT));
        register("count", FunctionSig.constant(GenericType.Primitive.INTEGER));
        for (String fn : List.of("min", "max", "first", "last", "median", "mode", "percentile", "minBy", "maxBy")) {
            register(fn, FunctionSig.passthrough());
        }

        // ===== Statistical → Float =====
        for (String fn : List.of(
                "stdDev", "stdDevSample", "stdDevPopulation",
                "varianceSample", "variancePopulation", "variance",
                "covarSample", "covarPopulation", "corr",
                "cumulativeDistribution")) {
            register(fn, FunctionSig.constant(GenericType.Primitive.FLOAT));
        }
        register("denseRank", FunctionSig.constant(GenericType.Primitive.INTEGER));
        register("rank", FunctionSig.constant(GenericType.Primitive.INTEGER));
        register("rowNumber", FunctionSig.constant(GenericType.Primitive.INTEGER));

        // ===== Bit operations → Integer =====
        for (String fn : List.of("bitAnd", "bitOr", "bitXor", "bitNot", "bitShiftLeft", "bitShiftRight")) {
            register(fn, FunctionSig.constant(GenericType.Primitive.INTEGER));
        }

        // ===== Comparison/Logic =====
        for (String fn : List.of("lessThan", "greaterThan", "lessThanEqual", "greaterThanEqual", "equal", "eq")) {
            register(fn, FunctionSig.constant(GenericType.Primitive.BOOLEAN));
        }
        register("if", FunctionSig.passthrough());
        register("coalesce", FunctionSig.passthrough());

        // ===== Collection =====
        register("size", FunctionSig.constant(GenericType.Primitive.INTEGER));
        for (String fn : List.of("at", "distinct", "sort", "reverse", "head", "tail",
                "removeDuplicates", "removeDuplicatesBy")) {
            register(fn, FunctionSig.passthrough());
        }

        // ===== JSON/Variant =====
        register("toJson", FunctionSig.constant(GenericType.Primitive.JSON));
        register("toVariant", FunctionSig.constant(GenericType.Primitive.JSON));
        register("fromJson", FunctionSig.passthrough());
        register("get", FunctionSig.passthrough());

        // ===== Pure collection functions (renamed from DuckDB) =====
        register("sum", FunctionSig.numericPassthrough());
        register("min", FunctionSig.passthrough());
        register("max", FunctionSig.passthrough());
        register("average", FunctionSig.constant(GenericType.Primitive.FLOAT));
        register("removeDuplicates", FunctionSig.passthrough());
        register("reverse", FunctionSig.passthrough());
        register("and", FunctionSig.constant(GenericType.Primitive.BOOLEAN));
        register("or", FunctionSig.constant(GenericType.Primitive.BOOLEAN));
        register("add", FunctionSig.passthrough());
        register("concatenate", FunctionSig.passthrough());
        register("list", FunctionSig.passthrough());
        register("joinStrings", FunctionSig.constant(GenericType.Primitive.STRING));
        register("replace", FunctionSig.constant(GenericType.Primitive.STRING));
        register("date", FunctionSig.constant(GenericType.Primitive.STRICT_DATE));
        register("generate_series", FunctionSig.passthrough());

        // ===== DuckDB helpers still in IR (to be cleaned later) =====
        register("list_contains", FunctionSig.constant(GenericType.Primitive.BOOLEAN));
        register("list_slice", FunctionSig.passthrough());
        register("list_extract", FunctionSig.passthrough());
        register("list_aggr", FunctionSig.passthrough());
        register("listIndexOf", FunctionSig.constant(GenericType.Primitive.INTEGER));
        register("range", FunctionSig.passthrough());

        // ===== String/utility =====
        register("startsWith", FunctionSig.constant(GenericType.Primitive.BOOLEAN));
        register("endsWith", FunctionSig.constant(GenericType.Primitive.BOOLEAN));
        register("strftime", FunctionSig.constant(GenericType.Primitive.STRING));
        register("repeat", FunctionSig.constant(GenericType.Primitive.STRING));
        register("left", FunctionSig.constant(GenericType.Primitive.STRING));
        register("right", FunctionSig.constant(GenericType.Primitive.STRING));
        register("split", FunctionSig.constant(GenericType.Primitive.STRING));
        register("len", FunctionSig.constant(GenericType.Primitive.INTEGER));
        register("struct_extract", FunctionSig.passthrough());

        register("strpos", FunctionSig.constant(GenericType.Primitive.INTEGER));

        // ===== Math/date helpers =====
        register("greatest", FunctionSig.passthrough());
        register("least", FunctionSig.passthrough());
        register("to_days", FunctionSig.constant(GenericType.Primitive.INTEGER));
        register("to_weeks", FunctionSig.constant(GenericType.Primitive.INTEGER));
        register("to_years", FunctionSig.constant(GenericType.Primitive.INTEGER));
        register("to_months", FunctionSig.constant(GenericType.Primitive.INTEGER));
        register("to_hours", FunctionSig.constant(GenericType.Primitive.INTEGER));
        register("to_minutes", FunctionSig.constant(GenericType.Primitive.INTEGER));
        register("to_seconds", FunctionSig.constant(GenericType.Primitive.INTEGER));
        register("ln", FunctionSig.constant(GenericType.Primitive.FLOAT));
        register("sha1", FunctionSig.constant(GenericType.Primitive.STRING));
        register("sha256", FunctionSig.constant(GenericType.Primitive.STRING));
        register("md5", FunctionSig.constant(GenericType.Primitive.STRING));
        register("not", FunctionSig.constant(GenericType.Primitive.BOOLEAN));
        register("to_json", FunctionSig.constant(GenericType.Primitive.JSON));
        register("quantile_disc", FunctionSig.passthrough());

        // ===== Other =====
        register("char", FunctionSig.constant(GenericType.Primitive.STRING));
        register("chr", FunctionSig.constant(GenericType.Primitive.STRING));
        register("uuid", FunctionSig.constant(GenericType.Primitive.STRING));
        register("jaroWinklerSimilarity", FunctionSig.constant(GenericType.Primitive.FLOAT));
        register("cast", FunctionSig.passthrough());
    }

    private static void register(String functionName, FunctionSig sig) {
        REGISTRY.put(functionName.toLowerCase(), sig);
    }

    /**
     * Resolves the return type for a function call.
     * Primary source of truth for FunctionExpression.type().
     *
     * @return The resolved return type (never DEFERRED)
     */
    public static GenericType resolveReturnType(String functionName, GenericType targetType, List<GenericType> argTypes) {
        FunctionSig sig = REGISTRY.get(functionName.toLowerCase());
        if (sig == null) {
            throw new IllegalArgumentException(
                    "Unregistered function: '" + functionName + "'. Add it to PureFunctionRegistry.");
        }
        return switch (sig.rule()) {
            case CONSTANT -> sig.returnType();
            case PASSTHROUGH -> targetType;
            case NUMERIC_PASSTHROUGH -> resolveNumericType(targetType, argTypes);
        };
    }

    public static boolean isRegistered(String functionName) {
        return REGISTRY.containsKey(functionName.toLowerCase());
    }

    private static GenericType resolveNumericType(GenericType targetType, List<GenericType> argTypes) {
        boolean hasDecimal = targetType == GenericType.Primitive.DECIMAL;
        boolean hasFloat = targetType == GenericType.Primitive.FLOAT;
        boolean allInteger = targetType == GenericType.Primitive.INTEGER;

        for (GenericType t : argTypes) {
            if (t == GenericType.Primitive.DECIMAL) hasDecimal = true;
            if (t == GenericType.Primitive.FLOAT) hasFloat = true;
            if (t != GenericType.Primitive.INTEGER) allInteger = false;
        }

        if (hasDecimal) return GenericType.Primitive.DECIMAL;
        if (hasFloat) return GenericType.Primitive.FLOAT;
        if (allInteger) return GenericType.Primitive.INTEGER;
        if (targetType instanceof GenericType.Primitive p && p.isNumeric()) return targetType;
        return GenericType.Primitive.NUMBER;
    }
}
