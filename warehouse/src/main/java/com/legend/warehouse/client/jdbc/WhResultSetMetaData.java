package com.legend.warehouse.client.jdbc;

import com.legend.warehouse.sqlapi.DuckType;
import java.sql.SQLException;

/** A result's columns, reported as DuckDB's driver reports them: its type names and codes. */
final class WhResultSetMetaData implements java.sql.ResultSetMetaData {

    private final WhResultSet rs;

    WhResultSetMetaData(WhResultSet rs) {
        this.rs = rs;
    }

    private DuckType type(int i) throws SQLException {
        if (i < 1 || i > rs.types().size()) throw new SQLException("no column " + i);
        return rs.types().get(i - 1);
    }

    @Override
    public int getColumnCount() {
        return rs.columns().size();
    }

    @Override
    public String getColumnName(int i) throws SQLException {
        type(i);
        return rs.columns().get(i - 1).name();
    }

    @Override
    public String getColumnLabel(int i) throws SQLException {
        return getColumnName(i);
    }

    @Override
    public String getColumnTypeName(int i) throws SQLException {
        type(i);
        return rs.columns().get(i - 1).type();
    }

    @Override
    public int getColumnType(int i) throws SQLException {
        return Carriers.jdbcType(type(i));
    }

    @Override
    public String getColumnClassName(int i) throws SQLException {
        return switch (type(i)) {
            case DuckType.ListOf l -> "java.sql.Array";
            case DuckType.StructOf s -> "java.sql.Struct";
            case DuckType.MapOf m -> "java.util.Map";
            case DuckType.Scalar s -> "java.lang.Object";
        };
    }

    @Override
    public int isNullable(int i) {
        return columnNullableUnknown;
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
    public boolean isSigned(int i) {
        return true;
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
    public int getPrecision(int i) {
        return 0;
    }

    @Override
    public int getScale(int i) {
        return 0;
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
