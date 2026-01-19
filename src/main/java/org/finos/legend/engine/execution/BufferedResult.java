package org.finos.legend.engine.execution;

import org.finos.legend.engine.serialization.ResultSerializer;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

/**
 * A fully materialized, immutable query result.
 * 
 * All rows are loaded into memory, allowing random access and re-iteration.
 * Suitable for small to medium result sets or when results need to be processed
 * multiple times.
 * 
 * GraalVM native-image compatible.
 */
public record BufferedResult(
        List<Column> columns,
        List<Row> rows) implements Result {

    @Override
    public long rowCount() {
        return rows.size();
    }

    @Override
    public Iterator<Row> iterator() {
        return rows.iterator();
    }

    @Override
    public Stream<Row> stream() {
        return rows.stream();
    }

    @Override
    public void writeTo(OutputStream out, ResultSerializer serializer) throws IOException {
        serializer.serialize(this, out);
    }

    @Override
    public BufferedResult toBuffered() {
        return this;
    }

    @Override
    public void close() {
        // No-op: already materialized, no resources to release
    }

    @Override
    public int columnCount() {
        return columns.size();
    }

    @Override
    public Object getValue(int rowIndex, int columnIndex) {
        return rows.get(rowIndex).values().get(columnIndex);
    }

    @Override
    public Object getValue(int rowIndex, String columnName) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).name().equals(columnName)) {
                return rows.get(rowIndex).values().get(i);
            }
        }
        throw new IllegalArgumentException("Column not found: " + columnName);
    }

    /**
     * Serializes this result as a JSON array of objects.
     * Each row becomes a JSON object with column names as keys.
     */
    public String toJsonArray() {
        StringBuilder sb = new StringBuilder("[");

        for (int rowIdx = 0; rowIdx < rows.size(); rowIdx++) {
            if (rowIdx > 0) {
                sb.append(",");
            }
            sb.append("{");

            Row row = rows.get(rowIdx);
            for (int colIdx = 0; colIdx < columns.size(); colIdx++) {
                if (colIdx > 0) {
                    sb.append(",");
                }

                Column col = columns.get(colIdx);
                Object value = row.values().get(colIdx);

                sb.append("\"").append(escapeJson(col.name())).append("\":");
                sb.append(formatJsonValue(value));
            }
            sb.append("}");
        }

        sb.append("]");
        return sb.toString();
    }

    private static String formatJsonValue(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof Boolean) {
            return value.toString();
        }
        if (value instanceof Number) {
            return value.toString();
        }
        // String or other - escape and quote
        return "\"" + escapeJson(value.toString()) + "\"";
    }

    private static String escapeJson(String s) {
        if (s == null)
            return "";
        StringBuilder sb = new StringBuilder(s.length());
        for (char c : s.toCharArray()) {
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> sb.append(c);
            }
        }
        return sb.toString();
    }

    /**
     * Creates a BufferedResult from a JDBC ResultSet.
     * The ResultSet is fully consumed and can be closed after this call.
     */
    public static BufferedResult fromResultSet(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();

        // Build column metadata
        List<Column> columns = new ArrayList<>(columnCount);
        for (int i = 1; i <= columnCount; i++) {
            columns.add(new Column(
                    meta.getColumnLabel(i),
                    meta.getColumnTypeName(i),
                    Column.mapJdbcTypeToJava(meta.getColumnType(i))));
        }

        // Build rows
        List<Row> rows = new ArrayList<>();
        while (rs.next()) {
            rows.add(Row.fromResultSet(rs, columnCount));
        }

        return new BufferedResult(columns, rows);
    }
}
