package com.legend.warehouse.client.jdbc;

import com.legend.warehouse.sqlapi.DuckType;
import java.sql.Array;
import java.sql.SQLException;
import com.legend.Nullable;
import java.util.List;

/** A list value, as DuckDB's driver hands one back: its elements, and its printed form. */
final class WhArray implements Array {

    private final DuckType element;
    private final List<@Nullable Object> items;

    WhArray(DuckType element, List<@Nullable Object> items) {
        this.element = element;
        this.items = items;
    }

    @Override
    public String getBaseTypeName() {
        return switch (element) {
            // DuckDB's driver names the element by its DuckDBColumnType constant: TIMESTAMP_WITH_TIME_ZONE
            case DuckType.Scalar s -> s.base().replace(' ', '_');
            case DuckType.ListOf l -> "LIST";
            case DuckType.StructOf st -> "STRUCT";
            case DuckType.MapOf m -> "MAP";
        };
    }

    @Override
    public int getBaseType() {
        return Carriers.jdbcType(element);
    }

    @Override
    public Object getArray() {
        return items.toArray();
    }

    /** DuckDB's own spelling: {@code [1, 2, null]}. */
    @Override
    public String toString() {
        return items.toString();
    }

    @Override
    public Object getArray(java.util.Map<String, Class<?>> map) throws SQLException {
        throw Unsupported.of("Array.getArray(Map)");
    }

    @Override
    public Object getArray(long index, int count) {
        return items.subList((int) index - 1, (int) index - 1 + count).toArray();
    }

    @Override
    public Object getArray(long index, int count, java.util.Map<String, Class<?>> map) throws SQLException {
        throw Unsupported.of("Array.getArray(long, int, Map)");
    }

    @Override
    public java.sql.ResultSet getResultSet() throws SQLException {
        throw Unsupported.of("Array.getResultSet");
    }

    @Override
    public java.sql.ResultSet getResultSet(java.util.Map<String, Class<?>> map) throws SQLException {
        throw Unsupported.of("Array.getResultSet");
    }

    @Override
    public java.sql.ResultSet getResultSet(long index, int count) throws SQLException {
        throw Unsupported.of("Array.getResultSet");
    }

    @Override
    public java.sql.ResultSet getResultSet(long index, int count, java.util.Map<String, Class<?>> map) throws SQLException {
        throw Unsupported.of("Array.getResultSet");
    }

    @Override
    public void free() {
    }
}
