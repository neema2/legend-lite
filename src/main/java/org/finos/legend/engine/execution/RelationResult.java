package org.finos.legend.engine.execution;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents the result of a Relation query (tabular data).
 * 
 * This is the return type for Pure relation expressions like:
 * {@code #>store::PersonDb.T_PERSON}->project(...)
 * 
 * GraalVM native-image compatible.
 */
public record RelationResult(
        List<Column> columns,
        List<Row> rows) {

    /**
     * Creates a RelationResult from a JDBC ResultSet.
     */
    public static RelationResult fromResultSet(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();

        // Build column metadata
        List<Column> columns = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++) {
            columns.add(new Column(
                    meta.getColumnLabel(i),
                    meta.getColumnTypeName(i),
                    mapToJavaType(meta.getColumnType(i))));
        }

        // Build rows
        List<Row> rows = new ArrayList<>();
        while (rs.next()) {
            List<Object> values = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                values.add(rs.getObject(i));
            }
            rows.add(new Row(values));
        }

        return new RelationResult(columns, rows);
    }

    /**
     * Maps JDBC type to Java class name.
     */
    private static String mapToJavaType(int jdbcType) {
        return switch (jdbcType) {
            case java.sql.Types.VARCHAR, java.sql.Types.CHAR, java.sql.Types.LONGVARCHAR -> "String";
            case java.sql.Types.INTEGER, java.sql.Types.SMALLINT, java.sql.Types.TINYINT -> "Integer";
            case java.sql.Types.BIGINT -> "Long";
            case java.sql.Types.DOUBLE, java.sql.Types.FLOAT, java.sql.Types.REAL -> "Double";
            case java.sql.Types.DECIMAL, java.sql.Types.NUMERIC -> "BigDecimal";
            case java.sql.Types.BOOLEAN, java.sql.Types.BIT -> "Boolean";
            case java.sql.Types.DATE -> "LocalDate";
            case java.sql.Types.TIMESTAMP, java.sql.Types.TIMESTAMP_WITH_TIMEZONE -> "LocalDateTime";
            default -> "Object";
        };
    }

    /**
     * Gets the number of rows.
     */
    public int rowCount() {
        return rows.size();
    }

    /**
     * Gets the number of columns.
     */
    public int columnCount() {
        return columns.size();
    }

    /**
     * Gets a value at the specified row and column.
     */
    public Object getValue(int rowIndex, int columnIndex) {
        return rows.get(rowIndex).values().get(columnIndex);
    }

    /**
     * Gets a value at the specified row by column name.
     */
    public Object getValue(int rowIndex, String columnName) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).name().equals(columnName)) {
                return rows.get(rowIndex).values().get(i);
            }
        }
        throw new IllegalArgumentException("Column not found: " + columnName);
    }

    /**
     * Column metadata.
     */
    public record Column(String name, String sqlType, String javaType) {
    }

    /**
     * A row of values.
     */
    public record Row(List<Object> values) {
        public Object get(int index) {
            return values.get(index);
        }
    }
}
