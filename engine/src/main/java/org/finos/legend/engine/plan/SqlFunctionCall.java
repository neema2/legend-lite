package org.finos.legend.engine.plan;

import java.util.List;
import java.util.Objects;

/**
 * Represents a SQL function call.
 * 
 * Used for functions like UPPER(), LOWER(), TRIM(), etc.
 * 
 * @param functionName The SQL function name (lowercase)
 * @param target       The primary expression the function operates on
 * @param arguments    Additional arguments (if any)
 * @param returnType   The SQL type returned by this function
 */
public record SqlFunctionCall(
        String functionName,
        Expression target,
        List<Expression> arguments,
        SqlType returnType) implements Expression {

    public SqlFunctionCall {
        Objects.requireNonNull(functionName, "Function name cannot be null");
        Objects.requireNonNull(arguments, "Arguments cannot be null");
        arguments = List.copyOf(arguments);
        if (returnType == null) {
            returnType = SqlType.UNKNOWN;
        }
    }

    /**
     * Creates a function call with no additional arguments and unknown return type.
     * E.g., UPPER(column)
     */
    public static SqlFunctionCall of(String functionName) {
        return new SqlFunctionCall(functionName, null, List.of(), SqlType.UNKNOWN);
    }

    public static SqlFunctionCall of(String functionName, Expression target) {
        return new SqlFunctionCall(functionName, target, List.of(), SqlType.UNKNOWN);
    }

    /**
     * Creates a function call with type specified.
     */
    public static SqlFunctionCall of(String functionName, Expression target, SqlType returnType) {
        return new SqlFunctionCall(functionName, target, List.of(), returnType);
    }

    /**
     * Creates a function call with additional arguments.
     * E.g., SUBSTRING(column, 1, 10)
     */
    public static SqlFunctionCall of(String functionName, Expression target, Expression... args) {
        return new SqlFunctionCall(functionName, target, List.of(args), SqlType.UNKNOWN);
    }

    /**
     * Creates a function call with arguments and type.
     */
    public static SqlFunctionCall of(String functionName, Expression target, SqlType returnType, Expression... args) {
        return new SqlFunctionCall(functionName, target, List.of(args), returnType);
    }

    /**
     * @return The SQL function name to use for this function
     */
    public String sqlFunctionName() {
        // Map Pure function names to SQL function names
        // Normalize to lowercase for matching (Pure uses camelCase like toRadians)
        return switch (functionName.toLowerCase()) {
            // String functions
            case "toupper" -> "UPPER";
            case "tolower" -> "LOWER";
            case "trim" -> "TRIM";
            case "length" -> "LENGTH";
            case "reversestring", "reverse" -> "REVERSE";
            case "substring" -> "SUBSTRING";
            case "concat" -> "CONCAT";
            case "startswith" -> "STARTS_WITH";
            case "endswith" -> "ENDS_WITH";
            case "contains" -> "STRPOS"; // STRPOS returns position, need to check > 0
            case "indexof" -> "INSTR";
            case "matches" -> "REGEXP_MATCHES";
            case "levenshteindistance" -> "LEVENSHTEIN";
            case "jarowinklersimilarity" -> "JARO_WINKLER_SIMILARITY";

            // Math functions
            case "abs" -> "ABS";
            case "round" -> "ROUND";
            case "ceiling", "ceil" -> "CEIL";
            case "floor" -> "FLOOR";
            case "sqrt" -> "SQRT";
            case "exp" -> "EXP";
            case "log" -> "LN";
            case "log10" -> "LOG10";
            case "sin" -> "SIN";
            case "cos" -> "COS";
            case "tan" -> "TAN";
            case "asin" -> "ASIN";
            case "acos" -> "ACOS";
            case "atan" -> "ATAN";
            case "atan2" -> "ATAN2";
            case "toradians" -> "RADIANS";
            case "todegrees" -> "DEGREES";
            case "rem" -> "MOD";
            case "pow", "power" -> "POW";
            case "sign" -> "SIGN";

            // Type/Cast functions
            case "type" -> "TYPEOF";
            case "tostring" -> "CAST"; // Will need special handling
            case "tointeger", "parseinteger" -> "CAST"; // Will need special handling
            case "parsefloat" -> "CAST"; // CAST(x AS DOUBLE)
            case "parsedecimal" -> "CAST"; // CAST(x AS DECIMAL)
            case "parseboolean" -> "CAST"; // Will need special handling

            // Collection/null functions
            case "isempty" -> "COALESCE"; // Will need special handling
            case "size" -> "LENGTH";
            case "first" -> "FIRST";
            case "last" -> "LAST";

            // Bit operations - DuckDB uses operators, not functions. Need special handling
            // in SQLGenerator
            // These mappings don't help since DuckDB uses operators: & | ^ ~ << >>
            case "bitxor" -> "XOR"; // Need special handling: a ^ b
            case "bitor" -> "|"; // Need special handling: a | b (infix)
            case "bitand" -> "&"; // Need special handling: a & b (infix)
            case "bitnot", "bit_not" -> "~"; // Need special handling: ~a (prefix)
            case "bitshiftleft" -> "<<"; // Need special handling: a << b (infix)
            case "bitshiftright" -> ">>"; // Need special handling: a >> b (infix)

            // Hash functions
            case "hashcode", "hash" -> "HASH";

            // Date functions
            case "monthnumber", "month" -> "MONTH";
            case "daynumber", "day" -> "DAY";
            case "yearnumber", "year" -> "YEAR";
            case "hournumber", "hour" -> "HOUR";
            case "minutenumber", "minute" -> "MINUTE";
            case "secondnumber", "second" -> "SECOND";

            // Aggregate functions
            case "variancesample", "variance" -> "VAR_SAMP";
            case "variancepopulation" -> "VAR_POP";
            case "covarsample", "covariance" -> "COVAR_SAMP";
            case "covarpopulation" -> "COVAR_POP";
            case "stddevsample", "stddev" -> "STDDEV_SAMP";
            case "stddevpopulation" -> "STDDEV_POP";
            case "median" -> "MEDIAN";
            case "mode" -> "MODE";
            case "percentile" -> "QUANTILE_CONT";
            case "minby" -> "ARG_MIN";
            case "maxby" -> "ARG_MAX";

            // Variant/JSON functions - handled specially by SQLGenerator
            case "fromjson" -> "FROMJSON";
            case "tojson" -> "TOJSON";
            case "get" -> "GET";

            default -> functionName.toUpperCase();
        };
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visitFunctionCall(this);
    }

    @Override
    public SqlType type() {
        if (returnType != SqlType.UNKNOWN) {
            return returnType;
        }
        // Propagate DECIMAL type from target or any argument
        if (target != null && target.type() == SqlType.DECIMAL) {
            return SqlType.DECIMAL;
        }
        for (Expression arg : arguments) {
            if (arg.type() == SqlType.DECIMAL) {
                return SqlType.DECIMAL;
            }
        }
        return returnType;
    }

    @Override
    public String toString() {
        if (arguments.isEmpty()) {
            return functionName.toUpperCase() + "(" + target + ")";
        } else {
            return functionName.toUpperCase() + "(" + target + ", " + arguments + ")";
        }
    }
}
