package org.finos.legend.engine.transpiler;

import org.finos.legend.engine.transpiler.json.DuckDbJsonDialect;
import org.finos.legend.engine.transpiler.json.JsonSqlDialect;

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

    @Override
    public JsonSqlDialect getJsonDialect() {
        return DuckDbJsonDialect.INSTANCE;
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
        return switch (pureTypeName) {
            case "String" -> "VARCHAR";
            case "Integer" -> "BIGINT";
            case "Float", "Double" -> "DOUBLE";
            case "Decimal" -> "DECIMAL";
            case "Boolean" -> "BOOLEAN";
            case "Date", "StrictDate" -> "DATE";
            case "DateTime" -> "TIMESTAMP";
            case "TimestampTZ" -> "TIMESTAMPTZ";
            default -> "VARCHAR";
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
                // args: [quantity, unit, dateExpr, castType]
                String qty = args.get(0);
                String tbUnit = args.get(1);
                if (tbUnit.startsWith("'") && tbUnit.endsWith("'")) tbUnit = tbUnit.substring(1, tbUnit.length() - 1);
                String date = args.get(2);
                String castType = args.get(3);
                if (castType.startsWith("'") && castType.endsWith("'")) castType = castType.substring(1, castType.length() - 1);
                String toFunc = tbUnit.equals("weeks") ? "TO_WEEKS" : "TO_DAYS";
                String origin = tbUnit.equals("weeks") ? "'1969-12-29'" : "'1970-01-01'";
                return "CAST(TIME_BUCKET(" + toFunc + "(" + qty + "), " + date +
                        ", CAST(" + origin + " AS TIMESTAMP)) AS " + castType + ")";
            }
            case "lpadSafe": {
                // args: [str, len, lenCastInt, fill]
                // CASE WHEN LENGTH(str) >= len THEN LEFT(str, len)
                //      WHEN LENGTH(fill) = 0 THEN str
                //      ELSE LPAD(str, CAST(len AS INTEGER), fill) END
                return "CASE WHEN LENGTH(" + args.get(0) + ") >= " + args.get(1)
                        + " THEN LEFT(" + args.get(0) + ", " + args.get(1) + ")"
                        + " WHEN LENGTH(" + args.get(3) + ") = 0 THEN " + args.get(0)
                        + " ELSE LPAD(" + args.get(0) + ", " + args.get(2) + ", " + args.get(3) + ") END";
            }
            case "rpadSafe": {
                // Same pattern for RPAD
                return "CASE WHEN LENGTH(" + args.get(0) + ") >= " + args.get(1)
                        + " THEN LEFT(" + args.get(0) + ", " + args.get(1) + ")"
                        + " WHEN LENGTH(" + args.get(3) + ") = 0 THEN " + args.get(0)
                        + " ELSE RPAD(" + args.get(0) + ", " + args.get(2) + ", " + args.get(3) + ") END";
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
                // args: [list, direction]
                return "LIST_SORT(" + args.get(0) + ", " + args.get(1) + ")";
            case "listSortWithKey": {
                // args: [list, direction, keyExpr]
                // list_transform(LIST_SORT(list_transform(list, s -> {'k': keyExpr, 'v': s}), direction),
                //   _sv -> STRUCT_EXTRACT(_sv, 'v'))
                // Note: keyExpr is already rendered from the lambda body
                return "list_transform(LIST_SORT(list_transform(" + args.get(0)
                        + ", s -> {'k': " + args.get(2) + ", 'v': s}), " + args.get(1)
                        + "), _sv -> STRUCT_EXTRACT(_sv, 'v'))";
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
                return "LIST_AGGR(" + args.get(0) + ", 'corr')";
            case "listCovarSample":
                return "LIST_AGGR(" + args.get(0) + ", 'covar_samp')";
            case "listCovarPopulation":
                return "LIST_AGGR(" + args.get(0) + ", 'covar_pop')";
            case "listPercentileCont":
                return "QUANTILE_CONT(" + args.get(0) + ", " + args.get(1) + ")";
            case "listPercentileDisc":
                return "QUANTILE_DISC(" + args.get(0) + ", " + args.get(1) + ")";
            case "arrayToString":
                return "COALESCE(ARRAY_TO_STRING(" + args.get(0) + ", " + args.get(1) + "), '')";
        }

        String sqlName = switch (pureName) {
            // --- List ---
            case "listExtract" -> "LIST_EXTRACT";
            case "listSlice" -> "LIST_SLICE";
            case "listLength" -> "LEN";
            case "listConcat" -> "LIST_CONCAT";
            case "listAppend" -> "LIST_APPEND";

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
            case "decodeBase64" -> "FROM_BASE64";
            case "indexOf" -> "LIST_POSITION";

            // --- Math ---
            case "roundHalfEven" -> "ROUND_EVEN";

            // --- Date ---
            case "dateDiff" -> "DATE_DIFF";
            case "makeDate" -> "MAKE_DATE";
            case "fromEpochValue" -> "TO_TIMESTAMP";
            case "toEpochValue" -> "EPOCH";

            // --- Bitwise ---
            case "bitXor" -> "XOR";

            // --- Aggregates / Statistics ---
            case "median" -> "MEDIAN";
            case "mode" -> "MODE";
            case "stdDevSample" -> "STDDEV_SAMP";
            case "stdDevPopulation" -> "STDDEV_POP";
            case "varianceSample" -> "VAR_SAMP";
            case "variancePopulation" -> "VAR_POP";
            case "corr" -> "CORR";
            case "covarSample" -> "COVAR_SAMP";
            case "covarPopulation" -> "COVAR_POP";
            case "percentileCont" -> "PERCENTILE_CONT";
            case "percentileDisc" -> "PERCENTILE_DISC";

            // --- Analytical ---
            case "maxBy" -> "ARG_MAX";
            case "minBy" -> "ARG_MIN";

            // --- Misc ---
            case "generateGuid" -> "UUID";
            case "typeOf" -> "TYPEOF";

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
