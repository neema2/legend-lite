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
     * Converts JSON[] arrays (mixed-type lists with JsonNode elements)
     * to a List of native Java values. Converts DuckDB Structs to
     * LinkedHashMap preserving field order. Leaves other types as-is.
     */
    private static Object unwrapValue(Object value) throws SQLException {
        if (value instanceof java.sql.Struct struct) {
            return unwrapStruct(struct);
        }
        if (value instanceof java.sql.Array sqlArray) {
            Object[] elements = (Object[]) sqlArray.getArray();
            // Unwrap struct arrays (e.g., from zip → Pair structs)
            if (elements.length > 0 && elements[0] instanceof java.sql.Struct) {
                List<Object> result = new ArrayList<>(elements.length);
                for (Object elem : elements) {
                    if (elem instanceof java.sql.Struct s) result.add(unwrapStruct(s));
                    else result.add(elem);
                }
                return result;
            }
            // Unwrap JsonNode arrays (JSON[] from mixed-type lists)
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
     * Unwraps a DuckDB Struct to a LinkedHashMap with field names as keys.
     * Uses JDBC Struct metadata to extract field names and values.
     */
    private static java.util.Map<String, Object> unwrapStruct(java.sql.Struct struct) throws SQLException {
        Object[] attributes = struct.getAttributes();
        String typeName = struct.getSQLTypeName();
        // DuckDB struct type name is like: STRUCT(firstName VARCHAR, lastName VARCHAR)
        List<String> fieldNames = parseStructFieldNames(typeName);
        java.util.Map<String, Object> map = new java.util.LinkedHashMap<>();
        for (int i = 0; i < attributes.length && i < fieldNames.size(); i++) {
            Object val = attributes[i];
            // Recursively unwrap nested structs
            if (val instanceof java.sql.Struct nestedStruct) {
                val = unwrapStruct(nestedStruct);
            }
            map.put(fieldNames.get(i), val);
        }
        return map;
    }

    /**
     * Parses field names from a DuckDB STRUCT type name.
     * Input: "STRUCT(firstName VARCHAR, lastName VARCHAR)"
     * Output: ["firstName", "lastName"]
     */
    private static List<String> parseStructFieldNames(String typeName) {
        List<String> names = new ArrayList<>();
        if (typeName == null || !typeName.startsWith("STRUCT(")) {
            return names;
        }
        // Remove "STRUCT(" prefix and ")" suffix
        String inner = typeName.substring(7, typeName.length() - 1);
        // Parse field declarations, handling nested STRUCT types
        int depth = 0;
        int fieldStart = 0;
        for (int i = 0; i <= inner.length(); i++) {
            if (i == inner.length() || (inner.charAt(i) == ',' && depth == 0)) {
                String field = inner.substring(fieldStart, i).trim();
                // Field is like "firstName VARCHAR" or "\"first\" INTEGER" - take the first word, strip quotes
                int space = field.indexOf(' ');
                if (space > 0) {
                    String name = field.substring(0, space);
                    if (name.startsWith("\"") && name.endsWith("\"")) {
                        name = name.substring(1, name.length() - 1);
                    }
                    names.add(name);
                }
                fieldStart = i + 1;
            } else if (inner.charAt(i) == '(') {
                depth++;
            } else if (inner.charAt(i) == ')') {
                depth--;
            }
        }
        return names;
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
