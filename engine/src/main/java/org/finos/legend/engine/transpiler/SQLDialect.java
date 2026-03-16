package org.finos.legend.engine.transpiler;

import org.finos.legend.engine.transpiler.json.JsonSqlDialect;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Interface defining SQL dialect-specific behavior.
 * Implementations handle differences between database engines.
 */
public interface SQLDialect {

    /**
     * Detects the SQL dialect from a live JDBC connection's metadata.
     *
     * @param conn A live JDBC connection
     * @return The appropriate SQLDialect
     */
    static SQLDialect forConnection(Connection conn) {
        try {
            String product = conn.getMetaData().getDatabaseProductName();
            return switch (product) {
                case "DuckDB" -> DuckDBDialect.INSTANCE;
                case "SQLite" -> SQLiteDialect.INSTANCE;
                default -> DuckDBDialect.INSTANCE;
            };
        } catch (SQLException e) {
            return DuckDBDialect.INSTANCE;
        }
    }

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
        // Standard SQL DATE literals require full YYYY-MM-DD
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
     * Field values are already rendered SQL expressions (e.g., quoted strings, numbers).
     *
     * @param fields Ordered map of field name → rendered SQL value
     * @return Dialect-specific struct literal
     */
    String renderStructLiteral(java.util.LinkedHashMap<String, String> fields);

    /**
     * Render an array literal from pre-rendered element values.
     *
     * @param elements List of rendered SQL expressions
     * @return Dialect-specific array literal
     */
    String renderArrayLiteral(java.util.List<String> elements);

    /**
     * Render the SQL expression that unnests (flattens) an array into rows.
     * SqlBuilder handles the JOIN structure (LEFT JOIN LATERAL);
     * this method provides only the dialect-specific unnest expression.
     *
     * @param arrayPath SQL expression for the array to unnest
     * @return Dialect-specific unnest expression
     */
    String renderUnnestExpression(String arrayPath);

    // ==================== Scalar Function Rendering ====================

    /**
     * Render a list-contains check.
     *
     * @param listExpr The list/array expression
     * @param elemExpr The element to find
     * @return Dialect-specific contains expression
     */
    String renderListContains(String listExpr, String elemExpr);

    /**
     * Map a Pure type name to the SQL type name for CAST expressions.
     *
     * @param pureTypeName Pure type name (e.g., "String", "Integer", "Float")
     * @return SQL type name (e.g., "VARCHAR", "BIGINT", "DOUBLE")
     */
    String sqlTypeName(String pureTypeName);

    /**
     * Render date arithmetic: date + amount * unit.
     *
     * @param dateExpr Compiled date expression
     * @param amount   Compiled amount expression
     * @param unit     SQL interval unit (e.g., "DAY", "MONTH", "YEAR")
     * @return Dialect-specific date add expression
     */
    String renderDateAdd(String dateExpr, String amount, String unit);

    /**
     * Render starts-with check.
     *
     * @param str    String expression
     * @param prefix Prefix expression
     * @return Dialect-specific starts-with expression
     */
    String renderStartsWith(String str, String prefix);

    /**
     * Render ends-with check.
     *
     * @param str    String expression
     * @param suffix Suffix expression
     * @return Dialect-specific ends-with expression
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
     * Dialect-specific: e.g., EXCLUDE(...) or EXCEPT(...).
     *
     * @param columns Pre-rendered column names to exclude
     * @return Dialect-specific clause
     */
    String renderStarExcept(java.util.List<String> columns);

    // ==================== Variant Rendering ====================
    // No defaults — each dialect must implement its own Variant semantics.

    /** Mark a literal value as Variant. */
    String renderVariantLiteral(String expr);

    /** Access a Variant field by key (returns Variant). */
    String renderVariantAccess(String expr, String key);

    /** Access a Variant array element by index (returns Variant). */
    String renderVariantIndex(String expr, int index);

    /** Extract text value from Variant by key (returns string). */
    String renderVariantTextAccess(String expr, String key);

    /** Convert a value to Variant. */
    String renderToVariant(String expr);

    /** Cast Variant to a typed array. */
    String renderVariantArrayCast(String expr, String sqlType);

    /** Cast Variant to a scalar type. */
    String renderVariantScalarCast(String expr, String sqlType);

    /** Cast a value to VARIANT type for type preservation in mixed-type lists. */
    String renderVariantCast(String expr);

    /**
     * Render an interval unit literal for date diff functions.
     *
     * @param unit The interval unit (e.g., "DAY", "MONTH")
     * @return Dialect-specific interval unit expression
     */
    default String renderIntervalUnit(String unit) {
        return "'" + unit + "'";
    }
}
