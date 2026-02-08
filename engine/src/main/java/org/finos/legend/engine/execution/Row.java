package org.finos.legend.engine.execution;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * A row of values in a query result.
 * 
 * GraalVM native-image compatible.
 */
public record Row(List<Object> values) {

    /**
     * Gets the value at the specified index.
     */
    public Object get(int index) {
        return values.get(index);
    }

    /**
     * Creates a Row from the current position of a ResultSet.
     */
    public static Row fromResultSet(ResultSet rs, int columnCount) throws SQLException {
        List<Object> values = new ArrayList<>(columnCount);
        for (int i = 1; i <= columnCount; i++) {
            Object val = rs.getObject(i);
            values.add(unwrapValue(val));
        }
        return new Row(values);
    }

    /**
     * Unwraps DuckDB-specific types to standard Java types.
     * Only converts JSON[] arrays (mixed-type lists with JsonNode elements)
     * to a List of native Java values. Leaves homogeneous arrays as-is.
     */
    private static Object unwrapValue(Object value) throws SQLException {
        if (value instanceof java.sql.Array sqlArray) {
            Object[] elements = (Object[]) sqlArray.getArray();
            // Only unwrap if the array contains JsonNode elements (JSON[] from mixed-type lists)
            if (elements.length > 0 && elements[0] != null
                    && "org.duckdb.JsonNode".equals(elements[0].getClass().getName())) {
                List<Object> result = new ArrayList<>(elements.length);
                for (Object elem : elements) {
                    result.add(unwrapJsonNode(elem));
                }
                return result;
            }
        }
        return value;
    }

    /**
     * Unwraps a DuckDB JsonNode to its native Java type.
     * JsonNode is used internally for JSON[] mixed-type lists.
     */
    private static Object unwrapJsonNode(Object value) {
        if (value == null) return null;
        // Only unwrap org.duckdb.JsonNode; leave native types (Integer, String, etc.) as-is
        if (!"org.duckdb.JsonNode".equals(value.getClass().getName())) return value;

        String text = value.toString();
        if (text.startsWith("\"") && text.endsWith("\"")) {
            return text.substring(1, text.length() - 1);
        }
        if ("true".equals(text)) return true;
        if ("false".equals(text)) return false;
        if ("null".equals(text)) return null;
        try { return Long.parseLong(text); } catch (NumberFormatException ignored) {}
        try { return Double.parseDouble(text); } catch (NumberFormatException ignored) {}
        return text;
    }
}
