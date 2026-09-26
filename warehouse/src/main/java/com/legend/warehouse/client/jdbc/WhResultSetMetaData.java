package com.legend.warehouse.client.jdbc;

import com.legend.warehouse.sqlapi.DuckType;
import com.legend.warehouse.sqlapi.SqlApi.Column;
import java.sql.SQLException;
import java.util.List;

/** A result's columns, reported as DuckDB's driver reports them: its type names and codes. */
final class WhResultSetMetaData implements java.sql.ResultSetMetaData {

    private final List<Column> columns;
    private final List<DuckType> types;

    WhResultSetMetaData(List<Column> columns, List<DuckType> types) {
        this.columns = columns;
        this.types = types;
    }

    /** The columns a described statement will return. */
    static WhResultSetMetaData of(List<Column> columns) {
        List<DuckType> types = new java.util.ArrayList<>(columns.size());
        for (Column c : columns) types.add(DuckType.parse(c.type()));
        return new WhResultSetMetaData(columns, types);
    }

    private DuckType type(int i) throws SQLException {
        if (i < 1 || i > types.size()) throw new SQLException("no column " + i);
        return types.get(i - 1);
    }

    @Override
    public int getColumnCount() {
        return columns.size();
    }

    @Override
    public String getColumnName(int i) throws SQLException {
        type(i);
        return columns.get(i - 1).name();
    }

    @Override
    public String getColumnLabel(int i) throws SQLException {
        return getColumnName(i);
    }

    @Override
    public String getColumnTypeName(int i) throws SQLException {
        type(i);
        return columns.get(i - 1).type();
    }

    @Override
    public int getColumnType(int i) throws SQLException {
        return Carriers.jdbcType(type(i));
    }

    @Override
    public String getColumnClassName(int i) throws SQLException {
        return Carriers.className(type(i));
    }

    @Override
    public int isNullable(int i) {
        return columnNullable;   // as DuckDB's driver reports every column
    }

    @Override
    public boolean isAutoIncrement(int i) {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int i) {
        return true;
    }

    @Override
    public boolean isSearchable(int i) {
        return true;
    }

    @Override
    public boolean isCurrency(int i) {
        return false;
    }

    @Override
    public boolean isSigned(int i) throws SQLException {
        return Carriers.signed(type(i));
    }

    @Override
    public int getColumnDisplaySize(int i) {
        return 0;
    }

    @Override
    public String getSchemaName(int i) {
        return "";
    }

    @Override
    public int getPrecision(int i) throws SQLException {
        return Carriers.precision(type(i));
    }

    @Override
    public int getScale(int i) throws SQLException {
        return Carriers.scale(type(i));
    }

    @Override
    public String getTableName(int i) {
        return "";
    }

    @Override
    public String getCatalogName(int i) {
        return "";
    }

    @Override
    public boolean isReadOnly(int i) {
        return true;
    }

    @Override
    public boolean isWritable(int i) {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int i) {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw Unsupported.of("unwrap: this driver wraps nothing");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return false;
    }
}
