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
            // Replace T with space for SQL TIMESTAMP format
            String timestampValue = dateValue.replace("T", " ");
            return "TIMESTAMP '" + timestampValue + "'";
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
}
