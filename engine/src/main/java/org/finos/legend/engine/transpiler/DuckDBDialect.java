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
            case "encodeBase64" -> "BASE64";
            case "decodeBase64" -> "FROM_BASE64";
            case "format" -> "PRINTF";
            case "indexOf" -> "LIST_POSITION";

            // --- Math ---
            case "roundHalfEven" -> "ROUND_EVEN";

            // --- Date ---
            case "dateDiff" -> "DATE_DIFF";
            case "makeDate" -> "MAKE_DATE";
            case "timeBucket" -> "TIME_BUCKET";
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
}
