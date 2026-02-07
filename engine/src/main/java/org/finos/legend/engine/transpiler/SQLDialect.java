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
            if (tzIdx < 0) tzIdx = timePart.lastIndexOf('-');
            // Only treat as timezone if it's after the time portion (not a negative sign in time)
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
}
