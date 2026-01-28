package org.finos.legend.engine.execution;

import java.lang.reflect.RecordComponent;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds Java record instances from JDBC ResultSet rows.
 * 
 * Uses Java 16+ Record component metadata which is compile-time information,
 * making this GraalVM native-image compatible without additional reflection
 * config.
 * 
 * @param <T> The record type to build
 */
public class RecordResultBuilder<T extends Record> {

    private final Class<T> recordType;
    private final RecordComponent[] components;
    private final java.lang.reflect.Constructor<T> constructor;

    /**
     * Creates a builder for the specified record type.
     * 
     * @param recordType The record class
     * @throws IllegalArgumentException If the type is not a record
     */
    public RecordResultBuilder(Class<T> recordType) {
        if (!recordType.isRecord()) {
            throw new IllegalArgumentException("Type must be a record: " + recordType.getName());
        }
        this.recordType = recordType;
        this.components = recordType.getRecordComponents();

        // Get the canonical constructor
        Class<?>[] componentTypes = new Class<?>[components.length];
        for (int i = 0; i < components.length; i++) {
            componentTypes[i] = components[i].getType();
        }

        try {
            this.constructor = recordType.getDeclaredConstructor(componentTypes);
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException("Cannot find canonical constructor for " + recordType.getName(), e);
        }
    }

    /**
     * Builds a list of record instances from a ResultSet.
     * 
     * @param rs The ResultSet to read from
     * @return List of record instances
     * @throws SQLException If database access fails
     */
    public List<T> buildAll(ResultSet rs) throws SQLException {
        List<T> results = new ArrayList<>();
        ResultSetMetaData meta = rs.getMetaData();

        // Map column labels to indices
        Map<String, Integer> columnMap = buildColumnMap(meta);

        while (rs.next()) {
            results.add(buildOne(rs, columnMap));
        }

        return results;
    }

    /**
     * Builds a single record instance from the current ResultSet row.
     */
    private T buildOne(ResultSet rs, Map<String, Integer> columnMap) throws SQLException {
        Object[] args = new Object[components.length];

        for (int i = 0; i < components.length; i++) {
            RecordComponent component = components[i];
            String componentName = component.getName();
            Integer columnIndex = columnMap.get(componentName);

            if (columnIndex != null) {
                args[i] = getValue(rs, columnIndex, component.getType());
            } else {
                // Column not found - use null or default
                args[i] = getDefaultValue(component.getType());
            }
        }

        try {
            return constructor.newInstance(args);
        } catch (Exception e) {
            throw new SQLException("Failed to construct record: " + recordType.getName(), e);
        }
    }

    /**
     * Gets a value from the ResultSet, converting to the target type.
     */
    private Object getValue(ResultSet rs, int columnIndex, Class<?> targetType) throws SQLException {
        if (targetType == String.class) {
            return rs.getString(columnIndex);
        } else if (targetType == Integer.class || targetType == int.class) {
            int value = rs.getInt(columnIndex);
            return rs.wasNull() ? null : value;
        } else if (targetType == Long.class || targetType == long.class) {
            long value = rs.getLong(columnIndex);
            return rs.wasNull() ? null : value;
        } else if (targetType == Double.class || targetType == double.class) {
            double value = rs.getDouble(columnIndex);
            return rs.wasNull() ? null : value;
        } else if (targetType == Boolean.class || targetType == boolean.class) {
            boolean value = rs.getBoolean(columnIndex);
            return rs.wasNull() ? null : value;
        } else if (targetType == java.time.LocalDate.class) {
            java.sql.Date date = rs.getDate(columnIndex);
            return date != null ? date.toLocalDate() : null;
        } else if (targetType == java.time.LocalDateTime.class) {
            java.sql.Timestamp ts = rs.getTimestamp(columnIndex);
            return ts != null ? ts.toLocalDateTime() : null;
        } else {
            // Generic object fallback
            return rs.getObject(columnIndex);
        }
    }

    /**
     * Gets the default value for a type (null for references, 0 for primitives).
     */
    private Object getDefaultValue(Class<?> type) {
        if (type.isPrimitive()) {
            if (type == int.class)
                return 0;
            if (type == long.class)
                return 0L;
            if (type == double.class)
                return 0.0;
            if (type == boolean.class)
                return false;
            if (type == float.class)
                return 0.0f;
            if (type == short.class)
                return (short) 0;
            if (type == byte.class)
                return (byte) 0;
            if (type == char.class)
                return '\0';
        }
        return null;
    }

    /**
     * Builds a map from column labels to column indices.
     */
    private Map<String, Integer> buildColumnMap(ResultSetMetaData meta) throws SQLException {
        Map<String, Integer> map = new HashMap<>();
        int columnCount = meta.getColumnCount();

        for (int i = 1; i <= columnCount; i++) {
            String label = meta.getColumnLabel(i);
            map.put(label, i);
        }

        return map;
    }
}
