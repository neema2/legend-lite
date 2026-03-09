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
            case "Float", "Decimal" -> "DOUBLE";
            case "Boolean" -> "BOOLEAN";
            case "Date", "StrictDate" -> "DATE";
            case "DateTime" -> "TIMESTAMP";
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
        return str + " LIKE '%' || " + suffix;
    }

    @Override
    public String renderFunction(String pureName, java.util.List<String> args) {
        String sqlName = switch (pureName) {
            // --- List ---
            case "listExtract" -> "LIST_EXTRACT";
            case "listSlice" -> "LIST_SLICE";
            case "listLength" -> "LIST_LENGTH";
            case "listConcat" -> "LIST_CONCAT";
            case "listAppend" -> "LIST_APPEND";

            // --- String ---
            case "reverseString" -> "REVERSE";
            case "splitPart" -> "SPLIT_PART";
            case "joinStrings" -> "CONCAT_WS";
            case "levenshteinDistance" -> "LEVENSHTEIN";
            case "hash" -> "MD5";
            case "encodeBase64" -> "BASE64";
            case "decodeBase64" -> "FROM_BASE64";
            case "format" -> "FORMAT";

            // --- Date ---
            case "dayOfWeek" -> "DAYOFWEEK";
            case "dayOfYear" -> "DAYOFYEAR";
            case "weekOfYear" -> "WEEKOFYEAR";
            case "dateDiff" -> "DATE_DIFF";
            case "makeDate" -> "MAKE_DATE";
            case "timeBucket" -> "TIME_BUCKET";

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

            // --- Analytical ---
            case "maxBy" -> "MAX_BY";
            case "minBy" -> "MIN_BY";

            // --- Misc ---
            case "generateGuid" -> "UUID";
            case "typeOf" -> "TYPEOF";

            // Standard SQL: pass through unchanged
            default -> pureName;
        };
        return sqlName + "(" + String.join(", ", args) + ")";
    }
}
