package com.gs.legend.sqlgen;
import com.gs.legend.plan.*;
import com.gs.legend.model.m3.*;
/**
 * SQL dialect implementation for DuckDB.
 * DuckDB uses double quotes for identifiers and single quotes for strings.
 */
public final class DuckDBDialect implements SQLDialect {

    public static final DuckDBDialect INSTANCE = new DuckDBDialect();

    private DuckDBDialect() {
        // Singleton
    }

    @Override
    public String name() {
        return "DuckDB";
    }

    @Override
    public String quoteIdentifier(String identifier) {
        if (identifier == null)
            return "\"_unknown_\"";
        // DuckDB uses double quotes for identifiers
        // Escape any existing double quotes by doubling them
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    @Override
    public String quoteStringLiteral(String value) {
        // DuckDB uses single quotes for string literals
        // Escape any existing single quotes by doubling them
        return "'" + value.replace("'", "''") + "'";
    }

    @Override
    public String formatBoolean(boolean value) {
        // DuckDB supports TRUE/FALSE literals
        return value ? "TRUE" : "FALSE";
    }



    // ==================== Star Exclude ====================

    @Override
    public String renderStarExcept(java.util.List<String> columns) {
        return " EXCLUDE(" + String.join(", ", columns) + ")";
    }

    // ==================== Variant (JSON-based) ====================

    @Override
    public String renderVariantLiteral(String expr) {
        return expr + "::JSON";
    }

    @Override
    public String renderVariantAccess(String expr, String key) {
        if (isSimpleIdentifier(expr)) {
            return "(" + expr + "->" + quoteStringLiteral(key) + ")";
        }
        return "((" + expr + ")->" + quoteStringLiteral(key) + ")";
    }

    @Override
    public String renderVariantIndex(String expr, int index) {
        return "(" + expr + ")[" + index + "]";
    }

    @Override
    public String renderVariantTextAccess(String expr, String key) {
        if (isSimpleIdentifier(expr)) {
            return "(" + expr + "->>" + quoteStringLiteral(key) + ")";
        }
        return "((" + expr + ")->>" + quoteStringLiteral(key) + ")";
    }

    @Override
    public String renderToVariant(String expr) {
        return "CAST(" + expr + " AS JSON)";
    }

    @Override
    public String renderVariantArrayCast(String expr, String sqlType) {
        return "CAST(" + expr + " AS " + sqlType + "[])";
    }

    @Override
    public String renderVariantScalarCast(String expr, String sqlType) {
        return "CAST(" + expr + " AS " + sqlType + ")";
    }

    @Override
    public String renderVariantCast(String expr) {
        return expr + "::VARIANT";
    }

    private static boolean isSimpleIdentifier(String expr) {
        return expr.matches("^[a-zA-Z_][a-zA-Z0-9_]*$");
    }

    @Override
    public String renderStructLiteral(java.util.LinkedHashMap<String, String> fields) {
        // DuckDB struct syntax: {'name': 'ok', 'age': 30}
        return "{" + fields.entrySet().stream()
                .map(e -> "'" + e.getKey() + "': " + e.getValue())
                .collect(java.util.stream.Collectors.joining(", ")) + "}";
    }

    @Override
    public String renderArrayLiteral(java.util.List<String> elements) {
        // DuckDB array syntax: [1, 2, 3]
        return "[" + String.join(", ", elements) + "]";
    }

    @Override
    public String renderUnnestExpression(String arrayPath) {
        return "UNNEST(" + arrayPath + ")";
    }

    @Override
    public String renderListContains(String listExpr, String elemExpr) {
        return "LIST_CONTAINS(" + listExpr + ", " + elemExpr + ")";
    }

    @Override
    public String sqlTypeName(String pureTypeName) {
        // Parameterized types (e.g., "Decimal(18,0)") pass through as-is
        if (pureTypeName.contains("(")) return pureTypeName.toUpperCase();
        return switch (pureTypeName) {
            case "String" -> "VARCHAR";
            case "Integer" -> "BIGINT";
            case "Float", "Double" -> "DOUBLE";
            case "Decimal" -> "DECIMAL";
            case "Boolean" -> "BOOLEAN";
            case "Date", "StrictDate" -> "DATE";
            case "DateTime" -> "TIMESTAMP";
            case "TimestampNS" -> "TIMESTAMP_NS";
            case "TimestampTZ" -> "TIMESTAMPTZ";
            case "JSON" -> "JSON";
            case "Int128" -> "HUGEINT";
            default -> throw new IllegalArgumentException(
                    "DuckDB: unmapped Pure type name '" + pureTypeName + "' in sqlTypeName");
        };
    }

    @Override
    public String renderDateAdd(String dateExpr, String amount, String unit) {
        return "(" + dateExpr + " + (INTERVAL '1' " + unit + " * " + amount + "))";
    }

    @Override
    public String renderStartsWith(String str, String prefix) {
        return "STARTS_WITH(" + str + ", " + prefix + ")";
    }

    @Override
    public String renderEndsWith(String str, String suffix) {
        return "ENDS_WITH(" + str + ", " + suffix + ")";
    }

    @Override
    public String renderFunction(String pureName, java.util.List<String> args) {
        // Special rendering: functions that don't follow name(args) pattern
        switch (pureName) {
            // Date extraction → EXTRACT(X FROM y) form
            case "year":
                return "YEAR(" + args.get(0) + ")";
            case "month":
                return "MONTH(" + args.get(0) + ")";
            case "dayOfMonth":
                return "DAYOFMONTH(" + args.get(0) + ")";
            case "hour":
                return "HOUR(" + args.get(0) + ")";
            case "minute":
                return "EXTRACT(MINUTE FROM " + args.get(0) + ")";
            case "second":
                return "EXTRACT(SECOND FROM " + args.get(0) + ")";
            case "quarter":
                return "EXTRACT(QUARTER FROM " + args.get(0) + ")";
            case "quarterNumber":
                return "EXTRACT(QUARTER FROM " + args.get(0) + ")";
            case "dayOfWeekNumber":
                return "EXTRACT(ISODOW FROM " + args.get(0) + ")";
            // Date truncation → DATE_TRUNC('unit', x) form
            case "firstDayOfMonth":
                return "DATE_TRUNC('month', " + args.get(0) + ")";
            case "firstDayOfYear":
                return "DATE_TRUNC('year', " + args.get(0) + ")";
            case "firstDayOfQuarter":
                return "DATE_TRUNC('quarter', " + args.get(0) + ")";
            case "firstHourOfDay":
                return "DATE_TRUNC('day', " + args.get(0) + ")";
            case "firstMillisecondOfSecond":
                return "DATE_TRUNC('second', " + args.get(0) + ")";
            case "dayOfWeek":
                return "EXTRACT(ISODOW FROM " + args.get(0) + ")";
            case "dayOfYear":
                return "EXTRACT(DOY FROM " + args.get(0) + ")";
            case "weekOfYear":
                return "EXTRACT(WEEK FROM " + args.get(0) + ")";
            case "extractDow":
                return "EXTRACT(DOW FROM " + args.get(0) + ")";
            case "dateTruncDay":
                return "DATE_TRUNC('day', " + args.get(0) + ")";
            case "encodeBase64":
                return "TO_BASE64(CAST(" + args.get(0) + " AS BLOB))";
            // EXTRACT form for column-based date extraction
            case "extractYear":
                return "EXTRACT(YEAR FROM " + args.get(0) + ")";
            case "extractMonth":
                return "EXTRACT(MONTH FROM " + args.get(0) + ")";
            case "extractDay":
                return "EXTRACT(DAY FROM " + args.get(0) + ")";
            case "extractHour":
                return "EXTRACT(HOUR FROM " + args.get(0) + ")";
            case "timeBucket": {
                // args: [quantity, unitString, dateExpr]
                String qty = args.get(0);
                String unit = args.get(1);
                if (unit.startsWith("'") && unit.endsWith("'")) unit = unit.substring(1, unit.length() - 1);
                return "TIME_BUCKET(INTERVAL '" + qty + " " + unit + "', " + args.get(2) + ")";
            }
            case "format":
                return renderFormat(args);
            case "timeBucketScalar": {
                // args: [quantity, unit, dateExpr, castType (Pure type name)]
                String qty = args.get(0);
                String tbUnit = args.get(1);
                if (tbUnit.startsWith("'") && tbUnit.endsWith("'")) tbUnit = tbUnit.substring(1, tbUnit.length() - 1);
                String date = args.get(2);
                String castType = args.get(3);
                if (castType.startsWith("'") && castType.endsWith("'")) castType = castType.substring(1, castType.length() - 1);
                castType = sqlTypeName(castType); // Map Pure type name → SQL type
                String toFunc = tbUnit.equals("weeks") ? "TO_WEEKS" : "TO_DAYS";
                String origin = tbUnit.equals("weeks") ? "'1969-12-29'" : "'1970-01-01'";
                return "CAST(TIME_BUCKET(" + toFunc + "(" + qty + "), " + date +
                        ", CAST(" + origin + " AS TIMESTAMP)) AS " + castType + ")";
            }
            case "lpadSafe": {
                // args: [str, len, fill]
                // DuckDB LPAD requires INTEGER, not BIGINT — handle cast here
                // CASE WHEN LENGTH(str) >= len THEN LEFT(str, len)
                //      WHEN LENGTH(fill) = 0 THEN str
                //      ELSE LPAD(str, CAST(len AS INTEGER), fill) END
                String str = args.get(0);
                String len = args.get(1);
                String fill = args.get(2);
                return "CASE WHEN LENGTH(" + str + ") >= " + len
                        + " THEN LEFT(" + str + ", " + len + ")"
                        + " WHEN LENGTH(" + fill + ") = 0 THEN " + str
                        + " ELSE LPAD(" + str + ", CAST(" + len + " AS INTEGER), " + fill + ") END";
            }
            case "rpadSafe": {
                // args: [str, len, fill] — same pattern for RPAD
                String str = args.get(0);
                String len = args.get(1);
                String fill = args.get(2);
                return "CASE WHEN LENGTH(" + str + ") >= " + len
                        + " THEN LEFT(" + str + ", " + len + ")"
                        + " WHEN LENGTH(" + fill + ") = 0 THEN " + str
                        + " ELSE RPAD(" + str + ", CAST(" + len + " AS INTEGER), " + fill + ") END";
            }
            case "decodeBase64": {
                // Old pipeline: CAST(FROM_BASE64(RPAD(RTRIM(input, '='),
                //   CAST((CAST(((LENGTH(RTRIM(input, '=')) + 3) / 4) AS BIGINT) * 4) AS INTEGER), '=')) AS VARCHAR)
                String input = args.get(0);
                String rtrim = "RTRIM(" + input + ", '=')";
                String lenRtrim = "LENGTH(" + rtrim + ")";
                String padLen = "CAST((CAST(((" + lenRtrim + " + 3) / 4) AS BIGINT) * 4) AS INTEGER)";
                return "CAST(FROM_BASE64(RPAD(" + rtrim + ", " + padLen + ", '=')) AS VARCHAR)";
            }
            case "indexOfFrom": {
                // args: [str, search, fromIndex]
                // ((fromIndex + INSTR(SUBSTRING(str, (fromIndex + 1)), search)) - 1)
                return "((" + args.get(2) + " + INSTR(SUBSTRING(" + args.get(0) + ", (" + args.get(2) + " + 1)), " + args.get(1) + ")) - 1)";
            }
            case "joinStringsWithPrefixSuffix": {
                // args: [list, prefix, separator, suffix]
                // (prefix || COALESCE(ARRAY_TO_STRING(list, sep), '') || suffix)
                return "(" + args.get(1) + " || COALESCE(ARRAY_TO_STRING(" + args.get(0) + ", " + args.get(2) + "), '') || " + args.get(3) + ")";
            }
            case "wrapList":
                // Wrap a single value in list brackets: [value]
                return "[" + args.get(0) + "]";
            case "bitShiftRightSafe": {
                // args: [val, shift]
                // CASE WHEN shift > 62 THEN CAST(1 AS BIGINT) << shift
                //      ELSE val >> shift END
                return "(CASE WHEN " + args.get(1) + " > 62 THEN CAST(1 AS BIGINT) << " + args.get(1)
                        + " ELSE " + args.get(0) + " >> " + args.get(1) + " END)";
            }
            case "listSort":
                // args: [list] or [list, direction]
                if (args.size() > 1) {
                    return "LIST_SORT(" + args.get(0) + ", " + args.get(1) + ")";
                }
                return "LIST_SORT(" + args.get(0) + ")";
            case "listSortWithKey": {
                // args: [list, direction, paramName, keyBody]
                String p = args.get(2); // Identifier renders as raw name
                return "list_transform(LIST_SORT(list_transform(" + args.get(0)
                        + ", " + p + " -> {'k': " + args.get(3) + ", 'v': " + p + "}), " + args.get(1)
                        + "), _sv -> STRUCT_EXTRACT(_sv, 'v'))";
            }
            case "listFind": {
                // args: [list, filterPredicate]
                // LIST_EXTRACT(list_filter(list, predicate), 1)
                return "LIST_EXTRACT(list_filter(" + args.get(0) + ", " + args.get(1) + "), 1)";
            }
            case "listZip": {
                // args: [list1, list2]
                // list_transform(GENERATE_SERIES(1, LEAST(LEN(a), LEN(b))),
                //   _zip_i -> {'first': LIST_EXTRACT(a, _zip_i), 'second': LIST_EXTRACT(b, _zip_i)})
                String a = args.get(0);
                String b = args.get(1);
                return "list_transform(GENERATE_SERIES(1, LEAST(LEN(" + a + "), LEN(" + b + "))), "
                        + "_zip_i -> {'first': LIST_EXTRACT(" + a + ", _zip_i), "
                        + "'second': LIST_EXTRACT(" + b + ", _zip_i)})";
            }
            case "listReduce": {
                // args: [list, lambda, init]
                // LambdaExpr renders itself as ((acc, elem) -> body)
                return "list_reduce(" + args.get(0) + ", " + args.get(1) + ", " + args.get(2) + ")";
            }
            case "jsonGet": {
                // args: [variant, key]
                // DuckDB JSON access: (variant)->'key'
                return "((" + args.get(0) + ")->" + args.get(1) + ")";
            }

            // --- List aggregate functions using LIST_AGGR pattern ---
            case "listMedian":
                return "LIST_AGGR(" + args.get(0) + ", 'median')";
            case "listMode":
                return "LIST_AGGR(" + args.get(0) + ", 'mode')";
            case "listStdDevSample":
                return "LIST_AGGR(" + args.get(0) + ", 'stddev_samp')";
            case "listStdDevPopulation":
                return "LIST_AGGR(" + args.get(0) + ", 'stddev_pop')";
            case "listVarianceSample":
                return "LIST_AGGR(" + args.get(0) + ", 'var_samp')";
            case "listVariancePopulation":
                return "LIST_AGGR(" + args.get(0) + ", 'var_pop')";
            case "listCorr":
                return "(SELECT CORR(a, b) FROM (SELECT UNNEST(" + args.get(0) + ") AS a, UNNEST(" + args.get(1) + ") AS b))";
            case "listCovarSample":
                return "(SELECT COVAR_SAMP(a, b) FROM (SELECT UNNEST(" + args.get(0) + ") AS a, UNNEST(" + args.get(1) + ") AS b))";
            case "listCovarPopulation":
                return "(SELECT COVAR_POP(a, b) FROM (SELECT UNNEST(" + args.get(0) + ") AS a, UNNEST(" + args.get(1) + ") AS b))";
            case "listPercentileCont":
                return "LIST_AGGR(" + args.get(0) + ", 'quantile_cont', " + args.get(1) + ")";
            case "listPercentileDisc":
                return "LIST_AGGR(" + args.get(0) + ", 'quantile_disc', " + args.get(1) + ")";
            case "arrayToString":
                return "COALESCE(ARRAY_TO_STRING(" + args.get(0) + ", " + args.get(1) + "), '')";
            case "dateDiff": {
                // args: [unit, start, end]
                String unit = args.get(0);
                // Strip quotes from unit literal (e.g., 'WEEK' -> WEEK)
                if (unit.startsWith("'") && unit.endsWith("'")) unit = unit.substring(1, unit.length() - 1);
                if ("WEEK".equalsIgnoreCase(unit)) {
                    // DuckDB has no native WEEK diff — decompose:
                    // (DATE_DIFF('day', start, end) + CAST(EXTRACT(DOW FROM start) AS INTEGER)) // 7
                    String start = args.get(1);
                    String end = args.get(2);
                    return "(DATE_DIFF('day', " + start + ", " + end
                            + ") + CAST(EXTRACT(DOW FROM " + start + ") AS INTEGER)) // 7";
                }
                return "DATE_DIFF(" + String.join(", ", args) + ")";
            }
            case "listMinMaxBy": {
                // args: [list1, list2, aggFunc]
                // (SELECT ARG_MIN(a, b) FROM (SELECT UNNEST(list1) AS a, UNNEST(list2) AS b))
                String l1 = args.get(0);
                String l2 = args.get(1);
                String agg = args.get(2); // "minBy" or "maxBy"
                String sqlAgg = agg.equals("minBy") ? "ARG_MIN" : "ARG_MAX";
                return "(SELECT " + sqlAgg + "(a, b) FROM (SELECT UNNEST(" + l1 + ") AS a, UNNEST(" + l2 + ") AS b))";
            }
            case "listMinMaxByTopK": {
                // args: [list1, list2, limit, orderDir]
                // (SELECT LIST(sub.a) FROM (SELECT a FROM
                //   (SELECT UNNEST(l1) AS a, UNNEST(l2) AS b,
                //    UNNEST(generate_series(0, len(l1)-1)) AS rn)
                //   ORDER BY b orderDir, rn ASC LIMIT k) sub)
                String l1 = args.get(0);
                String l2 = args.get(1);
                String limit = args.get(2);
                String dir = args.get(3);
                return "(SELECT LIST(sub.a) FROM (SELECT a FROM "
                        + "(SELECT UNNEST(" + l1 + ") AS a, UNNEST(" + l2 + ") AS b, "
                        + "UNNEST(generate_series(0, len(" + l1 + ")-1)) AS rn) "
                        + "ORDER BY b " + dir + ", rn ASC LIMIT " + limit + ") sub)";
            }
            case "toUpperFirstCharacter": {
                // UPPER(LEFT(s,1)) || SUBSTRING(s FROM 2)
                String s = args.get(0);
                return "(UPPER(LEFT(" + s + ", 1)) || SUBSTRING(" + s + " FROM 2))";
            }
            case "toLowerFirstCharacter": {
                // LOWER(LEFT(s,1)) || SUBSTRING(s FROM 2)
                String s = args.get(0);
                return "(LOWER(LEFT(" + s + ", 1)) || SUBSTRING(" + s + " FROM 2))";
            }
        }

        String sqlName = switch (pureName) {
            // --- List ---
            case "listExtract" -> "LIST_EXTRACT";
            case "listSlice" -> "LIST_SLICE";
            case "listLength" -> "LEN";
            case "listConcat" -> "LIST_CONCAT";
            case "listAppend" -> "LIST_APPEND";
            case "listReverse" -> "LIST_REVERSE";
            case "listFilter" -> "list_filter";

            // --- List aggregate functions ---
            case "listSum" -> "LIST_SUM";
            case "listAvg" -> "LIST_AVG";
            case "listMin" -> "LIST_MIN";
            case "listMax" -> "LIST_MAX";
            case "listBoolAnd" -> "LIST_BOOL_AND";
            case "listBoolOr" -> "LIST_BOOL_OR";

            // --- String ---
            case "reverseString" -> "REVERSE";
            case "splitPart" -> "SPLIT_PART";
            case "joinStrings" -> "STRING_AGG";
            case "levenshteinDistance" -> "LEVENSHTEIN";
            case "hash" -> "HASH";

            // --- New string functions ---
            case "left" -> "LEFT";
            case "right" -> "RIGHT";
            case "ltrim" -> "LTRIM";
            case "rtrim" -> "RTRIM";
            case "split" -> "STRING_SPLIT";
            case "matches" -> "REGEXP_MATCHES";
            case "jaroWinklerSimilarity" -> "JARO_WINKLER_SIMILARITY";
            case "hashCode" -> "HASH";

            case "decodeBase64" -> "FROM_BASE64";
            case "indexOf" -> "LIST_POSITION";

            // --- Math ---
            case "roundHalfEven" -> "ROUND_EVEN";

            // --- Date ---
            case "makeDate" -> "MAKE_DATE";
            case "fromEpochValue" -> "TO_TIMESTAMP";
            case "toEpochValue" -> "EPOCH";

            // --- Bitwise ---
            case "bitXor" -> "XOR";

            // --- Aggregates / Statistics ---
            case "stdDev" -> "STDDEV";
            case "median" -> "MEDIAN";
            case "mode" -> "MODE";
            case "stdDevSample" -> "STDDEV_SAMP";
            case "stdDevPopulation" -> "STDDEV_POP";
            case "variance" -> "VARIANCE";
            case "varianceSample" -> "VAR_SAMP";
            case "variancePopulation" -> "VAR_POP";
            case "corr" -> "CORR";
            case "covarSample" -> "COVAR_SAMP";
            case "covarPopulation" -> "COVAR_POP";
            case "percentileCont" -> "QUANTILE_CONT";
            case "percentileDisc" -> "QUANTILE_DISC";

            // --- Analytical ---
            // Direct minBy/maxBy mapping for aggregate context (groupBy);
            // scalar list context uses listMinMaxBy/listMinMaxByTopK above.
            case "maxBy" -> "ARG_MAX";
            case "minBy" -> "ARG_MIN";

            // --- Standard SQL aggregates (semantic name → SQL name) ---
            case "sum" -> "SUM";
            case "avg" -> "AVG";
            case "count" -> "COUNT";
            case "min" -> "MIN";
            case "max" -> "MAX";

            // --- Ranking (semantic name → SQL name) ---
            case "rowNumber" -> "ROW_NUMBER";
            case "rank" -> "RANK";
            case "denseRank" -> "DENSE_RANK";
            case "percentRank" -> "PERCENT_RANK";
            case "cumulativeDistribution" -> "CUME_DIST";

            // --- Value functions (semantic name → SQL name) ---
            case "firstValue" -> "FIRST_VALUE";
            case "lastValue" -> "LAST_VALUE";
            case "lag" -> "LAG";
            case "lead" -> "LEAD";
            case "ntile" -> "NTILE";
            case "nthValue" -> "NTH_VALUE";

            // --- Math (semantic name → SQL name) ---
            case "round" -> "ROUND";
            case "abs" -> "ABS";
            case "ceil" -> "CEIL";
            case "floor" -> "FLOOR";
            case "truncate" -> "TRUNCATE";
            // cast pass-through: identity in aggregate context
            case "cast" -> "CAST";

            // --- New trig/math functions ---
            case "sinh" -> "SINH";
            case "cosh" -> "COSH";
            case "tanh" -> "TANH";
            case "cot" -> "COT";
            case "cbrt" -> "CBRT";

            // --- Collection ---
            case "removeDuplicates" -> "LIST_DISTINCT";
            case "unnest" -> "UNNEST";

            // --- List transform ---
            case "listTransform" -> "list_transform";

            // --- Misc ---
            case "generateGuid" -> "UUID";
            case "typeOf" -> "TYPEOF";

            // --- Functions moved from PlanGenerator hardcoding ---
            case "toJson" -> "TO_JSON";
            case "strPos" -> "STRPOS";
            case "instr" -> "INSTR";
            case "regexpReplace" -> "REGEXP_REPLACE";
            case "strftime" -> "STRFTIME";
            case "makeTimestamp" -> "MAKE_TIMESTAMP";

            // Standard SQL: pass through unchanged
            default -> pureName;
        };
        return sqlName + "(" + String.join(", ", args) + ")";
    }

    /** Strip surrounding single quotes from a rendered StringLiteral. */
    private static String stripQuotes(String s) {
        if (s.startsWith("'") && s.endsWith("'")) return s.substring(1, s.length() - 1);
        return s;
    }

    /**
     * DuckDB-specific rendering for Pure's format() function.
     * Converts Pure format specifiers to DuckDB PRINTF:
     * - %t{pattern} → %s + STRFTIME('duckdb_pattern', arg)
     * - %f (no modifiers) → %g (minimal float repr)
     * - %r → %s (generic repr)
     * - %% → %%
     */
    private String renderFormat(java.util.List<String> args) {
        if (args.isEmpty()) return "PRINTF()";
        String fmtLiteral = args.get(0);
        // Extract the actual format string from the SQL literal (strip quotes)
        String fmt = stripQuotes(fmtLiteral);

        StringBuilder newFmt = new StringBuilder();
        java.util.List<String> sqlArgs = new java.util.ArrayList<>();
        int argIdx = 1; // args[0] is the format string
        int i = 0;
        while (i < fmt.length()) {
            if (fmt.charAt(i) == '%' && i + 1 < fmt.length()) {
                char next = fmt.charAt(i + 1);
                if (next == 't' && i + 2 < fmt.length() && fmt.charAt(i + 2) == '{') {
                    int closeBrace = fmt.indexOf('}', i + 3);
                    if (closeBrace > 0) {
                        String duckdbPattern = convertDateFormat(fmt.substring(i + 3, closeBrace));
                        newFmt.append("%s");
                        if (argIdx < args.size()) {
                            sqlArgs.add("STRFTIME('" + duckdbPattern + "', " + args.get(argIdx) + ")");
                        }
                        argIdx++;
                        i = closeBrace + 1;
                        continue;
                    }
                } else if (next == 'r') {
                    newFmt.append("%s");
                    if (argIdx < args.size()) sqlArgs.add(args.get(argIdx));
                    argIdx++;
                    i += 2;
                    continue;
                } else if (next == '%') {
                    newFmt.append("%%");
                    i += 2;
                    continue;
                } else {
                    newFmt.append('%');
                    i++;
                    boolean hasModifiers = false;
                    while (i < fmt.length() && "0123456789.-+# ".indexOf(fmt.charAt(i)) >= 0) {
                        newFmt.append(fmt.charAt(i));
                        hasModifiers = true;
                        i++;
                    }
                    if (i < fmt.length()) {
                        char spec = fmt.charAt(i);
                        newFmt.append(!hasModifiers && spec == 'f' ? 'g' : spec);
                        i++;
                    }
                    if (argIdx < args.size()) sqlArgs.add(args.get(argIdx));
                    argIdx++;
                    continue;
                }
            }
            newFmt.append(fmt.charAt(i));
            i++;
        }
        StringBuilder result = new StringBuilder("PRINTF('");
        result.append(newFmt.toString().replace("'", "''"));
        result.append("'");
        for (String sa : sqlArgs) {
            result.append(", ").append(sa);
        }
        result.append(")");
        return result.toString();
    }

    /**
     * Converts a Java/Pure date format pattern to DuckDB STRFTIME format.
     * Examples:
     *   yyyy-MM-dd → %Y-%m-%d
     *   HH:mm:ss → %H:%M:%S
     *   EEE → %a (abbreviated day name)
     */
    private static String convertDateFormat(String javaPattern) {
        StringBuilder sb = new StringBuilder();
        int i = 0;
        while (i < javaPattern.length()) {
            char c = javaPattern.charAt(i);
            if (c == '"') { i++; continue; } // Skip quote chars
            if (c == '\'') {
                // Literal text in single quotes
                i++;
                while (i < javaPattern.length() && javaPattern.charAt(i) != '\'') {
                    sb.append(javaPattern.charAt(i));
                    i++;
                }
                if (i < javaPattern.length()) i++; // skip closing quote
                continue;
            }
            // Count repeated pattern chars
            int start = i;
            while (i < javaPattern.length() && javaPattern.charAt(i) == c) i++;
            int count = i - start;
            switch (c) {
                case 'y': sb.append(count <= 2 ? "%y" : "%Y"); break;
                case 'M': sb.append(count >= 3 ? "%b" : "%m"); break;
                case 'd': sb.append("%d"); break;
                case 'H': sb.append("%H"); break;
                case 'h': sb.append("%-I"); break; // 12-hour, no leading zero
                case 'm': sb.append("%M"); break;
                case 's': sb.append("%S"); break;
                case 'S': sb.append("%g"); break; // milliseconds → DuckDB %g (fractional secs)
                case 'a': sb.append("%p"); break; // AM/PM
                case 'E': sb.append(count >= 4 ? "%A" : "%a"); break;
                case 'Z': sb.append("%z00"); break; // timezone offset (+0000 format)
                case 'X': sb.append("Z"); break; // ISO timezone (Z for UTC)
                case 'z': sb.append("%Z"); break; // timezone name
                case 'G': sb.append("AD"); break; // era
                default: sb.append(c); break; // Pass through (T, -, :, etc.)
            }
        }
        return sb.toString();
    }
}
