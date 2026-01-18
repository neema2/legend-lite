package org.finos.legend.engine.server;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Simple JSON serializer for JDBC ResultSet data.
 * 
 * This serializer is GraalVM native-image compatible because:
 * - No reflection used
 * - Uses JDBC ResultSetMetaData for column information
 * - Hand-written string building for JSON output
 * 
 * Output format:
 * {
 * "columns": ["col1", "col2"],
 * "rows": [
 * {"col1": "value1", "col2": 123},
 * {"col1": "value2", "col2": 456}
 * ]
 * }
 */
public final class ResultSetJsonSerializer {

    private ResultSetJsonSerializer() {
    }

    /**
     * Serializes a ResultSet to JSON.
     * 
     * @param rs The ResultSet to serialize (cursor should be before first row)
     * @return JSON string representation
     * @throws SQLException if database access fails
     */
    public static String serialize(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();

        StringBuilder json = new StringBuilder();
        json.append("{\"columns\":[");

        // Write column names
        String[] columnNames = new String[columnCount];
        for (int i = 1; i <= columnCount; i++) {
            if (i > 1)
                json.append(",");
            String name = meta.getColumnLabel(i);
            columnNames[i - 1] = name;
            json.append("\"").append(escapeJson(name)).append("\"");
        }
        json.append("],\"rows\":[");

        // Write rows
        boolean firstRow = true;
        while (rs.next()) {
            if (!firstRow)
                json.append(",");
            firstRow = false;

            json.append("{");
            for (int i = 1; i <= columnCount; i++) {
                if (i > 1)
                    json.append(",");
                json.append("\"").append(escapeJson(columnNames[i - 1])).append("\":");
                appendValue(json, rs, i, meta.getColumnType(i));
            }
            json.append("}");
        }

        json.append("]}");
        return json.toString();
    }

    /**
     * Serializes a ResultSet to a JSON array of objects (simpler format).
     * 
     * @param rs The ResultSet to serialize
     * @return JSON array string
     * @throws SQLException if database access fails
     */
    public static String serializeAsArray(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();

        // Collect column names
        String[] columnNames = new String[columnCount];
        for (int i = 1; i <= columnCount; i++) {
            columnNames[i - 1] = meta.getColumnLabel(i);
        }

        StringBuilder json = new StringBuilder();
        json.append("[");

        boolean firstRow = true;
        while (rs.next()) {
            if (!firstRow)
                json.append(",");
            firstRow = false;

            json.append("{");
            for (int i = 1; i <= columnCount; i++) {
                if (i > 1)
                    json.append(",");
                json.append("\"").append(escapeJson(columnNames[i - 1])).append("\":");
                appendValue(json, rs, i, meta.getColumnType(i));
            }
            json.append("}");
        }

        json.append("]");
        return json.toString();
    }

    private static void appendValue(StringBuilder json, ResultSet rs, int columnIndex, int sqlType)
            throws SQLException {
        Object value = rs.getObject(columnIndex);

        if (value == null || rs.wasNull()) {
            json.append("null");
            return;
        }

        switch (sqlType) {
            // Numeric types - no quotes
            case Types.INTEGER, Types.SMALLINT, Types.TINYINT, Types.BIGINT ->
                json.append(value);

            case Types.FLOAT, Types.DOUBLE, Types.REAL ->
                json.append(value);

            case Types.DECIMAL, Types.NUMERIC ->
                json.append(value);

            // Boolean
            case Types.BOOLEAN, Types.BIT ->
                json.append(Boolean.TRUE.equals(value) ? "true" : "false");

            // String types - quote and escape
            case Types.VARCHAR, Types.CHAR, Types.LONGVARCHAR, Types.NVARCHAR, Types.NCHAR ->
                json.append("\"").append(escapeJson(value.toString())).append("\"");

            // Date/Time types - as ISO strings
            case Types.DATE, Types.TIME, Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE ->
                json.append("\"").append(escapeJson(value.toString())).append("\"");

            // Default: treat as string
            default ->
                json.append("\"").append(escapeJson(value.toString())).append("\"");
        }
    }

    /**
     * Escapes special characters in a string for JSON.
     */
    private static String escapeJson(String s) {
        if (s == null)
            return "";

        StringBuilder result = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> result.append("\\\"");
                case '\\' -> result.append("\\\\");
                case '\b' -> result.append("\\b");
                case '\f' -> result.append("\\f");
                case '\n' -> result.append("\\n");
                case '\r' -> result.append("\\r");
                case '\t' -> result.append("\\t");
                default -> {
                    if (c < 0x20) {
                        // Control character - escape as unicode
                        result.append(String.format("\\u%04x", (int) c));
                    } else {
                        result.append(c);
                    }
                }
            }
        }
        return result.toString();
    }
}
