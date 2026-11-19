package com.legend.warehouse.client.jdbc;

import java.sql.SQLException;

/**
 * A prepared statement's parameters as the warehouse describes them: how many values it binds, as DuckDB's own
 * prepare counts them (the describe's {@code parameterCount}; track Q2, 2026-11-19). What the warehouse does not
 * report -- a parameter's type, its mode, whether it may be null -- is refused by name, never answered by a guess.
 */
final class WhParameterMetaData implements java.sql.ParameterMetaData {

    private final int count;

    WhParameterMetaData(int count) {
        this.count = count;
    }

    @Override
    public int getParameterCount() {
        return count;
    }

    @Override
    public int isNullable(int param) throws SQLException {
        throw Unsupported.of("ParameterMetaData.isNullable");
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        throw Unsupported.of("ParameterMetaData.isSigned");
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        throw Unsupported.of("ParameterMetaData.getPrecision");
    }

    @Override
    public int getScale(int param) throws SQLException {
        throw Unsupported.of("ParameterMetaData.getScale");
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        throw Unsupported.of("ParameterMetaData.getParameterType");
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        throw Unsupported.of("ParameterMetaData.getParameterTypeName");
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        throw Unsupported.of("ParameterMetaData.getParameterClassName");
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
        throw Unsupported.of("ParameterMetaData.getParameterMode");
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
