package org.finos.legend.engine.execution;

import org.finos.legend.engine.serialization.ResultSerializer;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Result for constant/scalar queries that return a single value.
 * 
 * Used for Pure expressions like |1+1, |'hello', |abs(-5) that
 * evaluate to a single scalar value rather than a tabular result.
 * 
 * This is detected at IR level when the query compiles to a ConstantNode.
 */
public record ScalarResult(Object value, String sqlType) implements Result {

    public ScalarResult(Object value) {
        this(value, null);
    }

    @Override
    public List<Column> columns() {
        // Single "value" column for scalar results
        return List.of(new Column("value", getTypeName(), getJavaTypeName()));
    }

    @Override
    public long rowCount() {
        return 1;
    }

    @Override
    public Iterator<Row> iterator() {
        return List.of(new Row(List.of(value))).iterator();
    }

    @Override
    public Stream<Row> stream() {
        return Stream.of(new Row(List.of(value)));
    }

    @Override
    public void writeTo(OutputStream out, ResultSerializer serializer) throws IOException {
        // Serialize as single-cell TDS
        serializer.serialize(toBuffered(), out);
    }

    @Override
    public BufferedResult toBuffered() {
        return new BufferedResult(columns(), List.of(new Row(List.of(value))));
    }

    @Override
    public void close() {
        // No-op - no resources to release
    }

    @Override
    public int columnCount() {
        return 1;
    }

    @Override
    public Object getValue(int rowIndex, int columnIndex) {
        if (rowIndex != 0 || columnIndex != 0) {
            throw new IndexOutOfBoundsException("ScalarResult only has value at [0,0]");
        }
        return value;
    }

    @Override
    public Object getValue(int rowIndex, String columnName) {
        if (rowIndex != 0 || !"value".equals(columnName)) {
            throw new IllegalArgumentException("ScalarResult only has column 'value' at row 0");
        }
        return value;
    }

    /**
     * Returns true if the value is a scalar (not TDS).
     * Useful for PCT integration to know when to unwrap.
     */
    public boolean isScalar() {
        return true;
    }

    private String getTypeName() {
        if (value == null)
            return "String";
        if (value instanceof Integer || value instanceof Long)
            return "INTEGER";
        if (value instanceof Double || value instanceof Float)
            return "DOUBLE";
        if (value instanceof Boolean)
            return "BOOLEAN";
        return "VARCHAR";
    }

    private String getJavaTypeName() {
        if (value == null)
            return "String";
        if (value instanceof Integer)
            return "Integer";
        if (value instanceof Long)
            return "Long";
        if (value instanceof Double)
            return "Double";
        if (value instanceof Float)
            return "Float";
        if (value instanceof Boolean)
            return "Boolean";
        return "String";
    }
}
