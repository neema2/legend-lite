package org.finos.legend.engine.transpiler;

import org.finos.legend.engine.transpiler.json.JsonSqlDialect;

/**
 * Interface defining SQL dialect-specific behavior.
 * Implementations handle differences between database engines.
 */
public interface SQLDialect {

    /**
     * @return The dialect name (e.g., "DuckDB", "SQLite")
     */
    String name();

    /**
     * Quote an identifier (table name, column name, alias).
     * 
     * @param identifier The identifier to quote
     * @return The quoted identifier
     */
    String quoteIdentifier(String identifier);

    /**
     * Quote a string literal value.
     * 
     * @param value The string value to quote
     * @return The quoted string literal
     */
    String quoteStringLiteral(String value);

    /**
     * Format a boolean literal.
     * 
     * @param value The boolean value
     * @return The SQL boolean representation
     */
    String formatBoolean(boolean value);

    /**
     * Format a NULL literal.
     * 
     * @return The SQL NULL representation
     */
    default String formatNull() {
        return "NULL";
    }

    /**
     * Get the JSON dialect for JSON/VARIANT operations.
     * 
     * @return The JSON dialect, or null if not supported
     */
    default JsonSqlDialect getJsonDialect() {
        return null;
    }

    /**
     * Format a DATE literal from Pure's %YYYY-MM-DD format.
     * 
     * @param pureDate The Pure date string (e.g., "%2024-01-15" or
     *                 "%2024-01-15T10:30:00")
     * @return SQL date/timestamp literal (e.g., "DATE '2024-01-15'" or "TIMESTAMP
     *         '2024-01-15 10:30:00'")
     */
    default String formatDate(String pureDate) {
        // Strip the % prefix
        String dateValue = pureDate.startsWith("%") ? pureDate.substring(1) : pureDate;

        // If it contains 'T', it's a DateTime - render as TIMESTAMP
        if (dateValue.contains("T")) {
            int tIdx = dateValue.indexOf('T');
            String datePart = dateValue.substring(0, tIdx);
            String timePart = dateValue.substring(tIdx + 1);

            // Extract timezone suffix if present (e.g., +0000, -0500)
            String tz = "";
            int tzIdx = timePart.lastIndexOf('+');
            if (tzIdx < 0)
                tzIdx = timePart.lastIndexOf('-');
            // Only treat as timezone if it's after the time portion (not a negative sign in
            // time)
            if (tzIdx > 0 && tzIdx >= timePart.indexOf(':')) {
                tz = timePart.substring(tzIdx);
                timePart = timePart.substring(0, tzIdx);
            }

            // Pad partial times: "17" -> "17:00:00", "17:09" -> "17:09:00"
            String[] timeParts = timePart.split(":");
            String hours = timeParts.length > 0 ? timeParts[0] : "00";
            String minutes = timeParts.length > 1 ? timeParts[1] : "00";
            String seconds = timeParts.length > 2 ? timeParts[2] : "00";
            String fullTime = hours + ":" + minutes + ":" + seconds;

            return "TIMESTAMP '" + datePart + " " + fullTime + tz + "'";
        }

        // Handle partial dates - Pure supports YYYY and YYYY-MM formats
        // DuckDB requires full YYYY-MM-DD, so default missing parts
        String[] parts = dateValue.split("-");
        if (parts.length == 1) {
            // Year only: 2012 -> 2012-01-01
            dateValue = dateValue + "-01-01";
        } else if (parts.length == 2) {
            // Year-month only: 2012-03 -> 2012-03-01
            dateValue = dateValue + "-01";
        }

        return "DATE '" + dateValue + "'";
    }

    /**
     * Format a TIMESTAMP literal from Pure's DateTime format.
     * Delegates to formatDate which already handles DateTime values.
     */
    default String formatTimestamp(String pureDate) {
        return formatDate(pureDate);
    }

    /**
     * Format a TIME literal from Pure's %HH:MM:SS format.
     * 
     * @param pureTime The Pure time string (e.g., "%12:30:00")
     * @return SQL time literal (e.g., "TIME '12:30:00'")
     */
    default String formatTime(String pureTime) {
        // Strip the % prefix and return as TIME literal
        String timeValue = pureTime.startsWith("%") ? pureTime.substring(1) : pureTime;
        return "TIME '" + timeValue + "'";
    }

    // ==================== Struct / Array Rendering ====================

    /**
     * Render an inline struct literal from pre-rendered field name→value pairs.
     * Field values are already rendered SQL expressions (e.g., quoted strings,
     * numbers).
     *
     * @param fields Ordered map of field name → rendered SQL value
     * @return Dialect-specific struct literal (e.g., DuckDB: {'name': 'ok', 'age':
     *         30})
     */
    String renderStructLiteral(java.util.LinkedHashMap<String, String> fields);

    /**
     * Render an array literal from pre-rendered element values.
     *
     * @param elements List of rendered SQL expressions
     * @return Dialect-specific array literal (e.g., DuckDB: [1, 2, 3])
     */
    String renderArrayLiteral(java.util.List<String> elements);

    /**
     * Render the SQL expression that unnests (flattens) an array into rows.
     * SqlBuilder handles the JOIN structure (LEFT JOIN LATERAL);
     * this method provides only the dialect-specific unnest expression.
     *
     * @param arrayPath SQL expression for the array to unnest
     * @return Unnest expression (e.g., DuckDB: "UNNEST(path)")
     */
    String renderUnnestExpression(String arrayPath);

    // ==================== Scalar Function Rendering ====================

    /**
     * Render a list-contains check.
     *
     * @param listExpr The list/array expression
     * @param elemExpr The element to find
     * @return Dialect-specific contains expression (e.g., DuckDB:
     *         "LIST_CONTAINS(list, elem)")
     */
    String renderListContains(String listExpr, String elemExpr);

    /**
     * Map a Pure type name to the SQL type name for CAST expressions.
     *
     * @param pureTypeName Pure type name (e.g., "String", "Integer", "Float")
     * @return SQL type name (e.g., DuckDB: "VARCHAR", "BIGINT", "DOUBLE")
     */
    String sqlTypeName(String pureTypeName);

    /**
     * Render date arithmetic: date + amount * unit.
     *
     * @param dateExpr Compiled date expression
     * @param amount   Compiled amount expression
     * @param unit     SQL interval unit (e.g., "DAY", "MONTH", "YEAR")
     * @return Dialect-specific date add (e.g., DuckDB: "(date + (INTERVAL '1' DAY *
     *         amount))")
     */
    String renderDateAdd(String dateExpr, String amount, String unit);

    /**
     * Render starts-with check.
     *
     * @param str    String expression
     * @param prefix Prefix expression
     * @return Dialect-specific starts-with (e.g., DuckDB: "STARTS_WITH(str,
     *         prefix)")
     */
    String renderStartsWith(String str, String prefix);

    /**
     * Render ends-with check.
     *
     * @param str    String expression
     * @param suffix Suffix expression
     * @return Dialect-specific ends-with (e.g., DuckDB: "str LIKE '%' || suffix")
     */
    String renderEndsWith(String str, String suffix);

    /**
     * Render a function call. PlanGenerator uses Pure/abstract function names;
     * this method maps them to dialect-specific SQL.
     * Default: uppercase the name and use standard SQL call syntax: NAME(args).
     *
     * @param pureName Pure/abstract function name (e.g., "listExtract",
     *                 "levenshteinDistance")
     * @param args     Pre-rendered SQL argument expressions
     * @return Dialect-specific function call SQL
     */
    default String renderFunction(String pureName, java.util.List<String> args) {
        // Default: treat the name as a standard SQL function
        return pureName + "(" + String.join(", ", args) + ")";
    }

    /**
     * Render the SELECT * EXCLUDE/EXCEPT clause for excluding columns from star.
     * DuckDB uses EXCLUDE, PostgreSQL uses EXCEPT.
     *
     * @param columns Pre-rendered column names to exclude
     * @return Dialect-specific clause (e.g., " EXCLUDE(col1, col2)")
     */
    default String renderStarExcept(java.util.List<String> columns) {
        return " EXCLUDE(" + String.join(", ", columns) + ")";
    }

    // ==================== Variant Rendering ====================
    // Default implementations use DuckDB JSON semantics.
    // Future dialects (Snowflake, Databricks, DuckDB 1.5) override with native VARIANT.

    /** Mark a literal value as Variant. DuckDB: expr::JSON */
    default String renderVariantLiteral(String expr) {
        return expr + "::JSON";
    }

    /** Access a Variant field by key (returns Variant). DuckDB: (expr)->'key' */
    default String renderVariantAccess(String expr, String key) {
        // Simple identifiers (lambda params) skip inner parens: (i->'key')
        // Complex expressions get inner parens: (("PAYLOAD")->'key')
        if (isSimpleIdentifier(expr)) {
            return "(" + expr + "->" + quoteStringLiteral(key) + ")";
        }
        return "((" + expr + ")->" + quoteStringLiteral(key) + ")";
    }

    /** Access a Variant array element by index (returns Variant). DuckDB: (expr)[idx] */
    default String renderVariantIndex(String expr, int index) {
        return "(" + expr + ")[" + index + "]";
    }

    /** Extract text value from Variant by key (returns string). DuckDB: (expr)->>'key' */
    default String renderVariantTextAccess(String expr, String key) {
        if (isSimpleIdentifier(expr)) {
            return "(" + expr + "->>" + quoteStringLiteral(key) + ")";
        }
        return "((" + expr + ")->>" + quoteStringLiteral(key) + ")";
    }

    /**
     * Check if expr is a simple unquoted identifier (e.g. a lambda parameter).
     * Matches the old pipeline's DuckDBJsonDialect.isSimpleIdentifier logic.
     */
    private static boolean isSimpleIdentifier(String expr) {
        return expr.matches("^[a-zA-Z_][a-zA-Z0-9_]*$");
    }

    /** Convert a value to Variant. DuckDB: CAST(expr AS JSON) */
    default String renderToVariant(String expr) {
        return "CAST(" + expr + " AS JSON)";
    }

    /** Cast Variant to a typed array. DuckDB: CAST(expr AS type[]) */
    default String renderVariantArrayCast(String expr, String sqlType) {
        return "CAST(" + expr + " AS " + sqlType + "[])";
    }

    /** Cast Variant to a scalar type. DuckDB: CAST(expr AS type) */
    default String renderVariantScalarCast(String expr, String sqlType) {
        return "CAST(" + expr + " AS " + sqlType + ")";
    }

    /**
     * Render an interval unit literal for date diff functions.
     * DuckDB: 'DAY', PostgreSQL: 'day', etc.
     */
    default String renderIntervalUnit(String unit) {
        return "'" + unit + "'";
    }
}
