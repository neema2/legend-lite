package com.legend.warehouse.client.jdbc;

import com.legend.warehouse.client.WarehouseClient;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

/**
 * A connection to one catalog of a warehouse: ONE server-side session, so
 * what a statement leaves behind (USE, SET, temp tables, a transaction) is
 * there for the next, as on a local DuckDB connection. The signed-in user's
 * identity is set by the server before every statement.
 */
final class WhConnection implements java.sql.Connection {

    final WarehouseClient client;
    final String catalog;
    final String session;
    private final String url;
    private volatile boolean closed;
    private boolean autoCommit = true;

    WhConnection(WarehouseClient client, String catalog, String session, String url) {
        this.client = client;
        this.catalog = catalog;
        this.session = session;
        this.url = url;
    }

    private void run(String sql) throws SQLException {
        try (Statement s = createStatement()) {
            s.execute(sql);
        }
    }

    private void open() throws SQLException {
        if (closed) throw new SQLException("the connection is closed");
    }

    @Override
    public Statement createStatement() throws SQLException {
        open();
        return new WhStatement(this, null);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        open();
        return new WhStatement(this, sql);
    }

    @Override
    public boolean getAutoCommit() {
        return autoCommit;
    }

    /** Off: a transaction is begun on the session; commit and rollback end it and begin the next. */
    @Override
    public void setAutoCommit(boolean on) throws SQLException {
        open();
        if (on == autoCommit) return;
        if (on) {
            run("COMMIT");
        } else {
            run("BEGIN TRANSACTION");
        }
        autoCommit = on;
    }

    @Override
    public void commit() throws SQLException {
        open();
        if (autoCommit) throw new SQLException("commit with autocommit on");
        run("COMMIT");
        run("BEGIN TRANSACTION");
    }

    @Override
    public void rollback() throws SQLException {
        open();
        if (autoCommit) throw new SQLException("rollback with autocommit on");
        run("ROLLBACK");
        run("BEGIN TRANSACTION");
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        try {
            client.closeSession(session);
        } catch (java.io.IOException | RuntimeException gone) {
            // the session expires on the server anyway
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public boolean isValid(int timeout) {
        return !closed;
    }

    @Override
    public String getCatalog() {
        return catalog;
    }

    @Override
    public @com.legend.Nullable SQLWarning getWarnings() {
        return null;
    }

    @Override
    public void clearWarnings() {
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public int getTransactionIsolation() {
        return TRANSACTION_SERIALIZABLE;
    }

    @Override
    public String toString() {
        return "warehouse connection " + url.replaceAll("password=[^&]*", "password=***");
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw Unsupported.of("unwrap: this driver wraps nothing");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return false;
    }

    // -- not supported: each says so, loudly --------------------------------

    @Override
    public void setReadOnly(boolean p0) throws java.sql.SQLException {
        throw Unsupported.of("Connection.setReadOnly");
    }

    @Override
    public void abort(java.util.concurrent.Executor p0) throws java.sql.SQLException {
        throw Unsupported.of("Connection.abort");
    }

    @Override
    public java.sql.Statement createStatement(int p0, int p1) throws java.sql.SQLException {
        throw Unsupported.of("Connection.createStatement");
    }

    @Override
    public java.sql.Statement createStatement(int p0, int p1, int p2) throws java.sql.SQLException {
        throw Unsupported.of("Connection.createStatement");
    }

    @Override
    public java.sql.PreparedStatement prepareStatement(java.lang.String p0, int p1, int p2) throws java.sql.SQLException {
        throw Unsupported.of("Connection.prepareStatement");
    }

    @Override
    public java.sql.PreparedStatement prepareStatement(java.lang.String p0, int p1, int p2, int p3) throws java.sql.SQLException {
        throw Unsupported.of("Connection.prepareStatement");
    }

    @Override
    public java.sql.PreparedStatement prepareStatement(java.lang.String p0, int p1) throws java.sql.SQLException {
        throw Unsupported.of("Connection.prepareStatement");
    }

    @Override
    public java.sql.PreparedStatement prepareStatement(java.lang.String p0, java.lang.String[] p1) throws java.sql.SQLException {
        throw Unsupported.of("Connection.prepareStatement");
    }

    @Override
    public java.sql.PreparedStatement prepareStatement(java.lang.String p0, int[] p1) throws java.sql.SQLException {
        throw Unsupported.of("Connection.prepareStatement");
    }

    @Override
    public java.sql.CallableStatement prepareCall(java.lang.String p0, int p1, int p2, int p3) throws java.sql.SQLException {
        throw Unsupported.of("Connection.prepareCall");
    }

    @Override
    public java.sql.CallableStatement prepareCall(java.lang.String p0, int p1, int p2) throws java.sql.SQLException {
        throw Unsupported.of("Connection.prepareCall");
    }

    @Override
    public java.sql.CallableStatement prepareCall(java.lang.String p0) throws java.sql.SQLException {
        throw Unsupported.of("Connection.prepareCall");
    }

    @Override
    public java.lang.String nativeSQL(java.lang.String p0) throws java.sql.SQLException {
        throw Unsupported.of("Connection.nativeSQL");
    }

    @Override
    public void rollback(java.sql.Savepoint p0) throws java.sql.SQLException {
        throw Unsupported.of("Connection.rollback");
    }

    @Override
    public java.sql.DatabaseMetaData getMetaData() throws java.sql.SQLException {
        throw Unsupported.of("Connection.getMetaData");
    }

    @Override
    public void setCatalog(java.lang.String p0) throws java.sql.SQLException {
        throw Unsupported.of("Connection.setCatalog");
    }

    @Override
    public java.util.Map<java.lang.String, java.lang.Class<?>> getTypeMap() throws java.sql.SQLException {
        throw Unsupported.of("Connection.getTypeMap");
    }

    @Override
    public void setTypeMap(java.util.Map<java.lang.String, java.lang.Class<?>> p0) throws java.sql.SQLException {
        throw Unsupported.of("Connection.setTypeMap");
    }

    @Override
    public void setHoldability(int p0) throws java.sql.SQLException {
        throw Unsupported.of("Connection.setHoldability");
    }

    @Override
    public int getHoldability() throws java.sql.SQLException {
        throw Unsupported.of("Connection.getHoldability");
    }

    @Override
    public java.sql.Savepoint setSavepoint() throws java.sql.SQLException {
        throw Unsupported.of("Connection.setSavepoint");
    }

    @Override
    public java.sql.Savepoint setSavepoint(java.lang.String p0) throws java.sql.SQLException {
        throw Unsupported.of("Connection.setSavepoint");
    }

    @Override
    public void releaseSavepoint(java.sql.Savepoint p0) throws java.sql.SQLException {
        throw Unsupported.of("Connection.releaseSavepoint");
    }

    @Override
    public java.sql.Clob createClob() throws java.sql.SQLException {
        throw Unsupported.of("Connection.createClob");
    }

    @Override
    public java.sql.Blob createBlob() throws java.sql.SQLException {
        throw Unsupported.of("Connection.createBlob");
    }

    @Override
    public java.sql.NClob createNClob() throws java.sql.SQLException {
        throw Unsupported.of("Connection.createNClob");
    }

    @Override
    public java.sql.SQLXML createSQLXML() throws java.sql.SQLException {
        throw Unsupported.of("Connection.createSQLXML");
    }

    @Override
    public void setClientInfo(java.util.Properties p0) {
        throw new UnsupportedOperationException("the warehouse JDBC driver does not support Connection.setClientInfo");
    }

    @Override
    public void setClientInfo(java.lang.String p0, java.lang.String p1) {
        throw new UnsupportedOperationException("the warehouse JDBC driver does not support Connection.setClientInfo");
    }

    @Override
    public java.lang.String getClientInfo(java.lang.String p0) throws java.sql.SQLException {
        throw Unsupported.of("Connection.getClientInfo");
    }

    @Override
    public java.util.Properties getClientInfo() throws java.sql.SQLException {
        throw Unsupported.of("Connection.getClientInfo");
    }

    @Override
    public java.sql.Array createArrayOf(java.lang.String p0, java.lang.Object[] p1) throws java.sql.SQLException {
        throw Unsupported.of("Connection.createArrayOf");
    }

    @Override
    public java.sql.Struct createStruct(java.lang.String p0, java.lang.Object[] p1) throws java.sql.SQLException {
        throw Unsupported.of("Connection.createStruct");
    }

    @Override
    public void setSchema(java.lang.String p0) throws java.sql.SQLException {
        throw Unsupported.of("Connection.setSchema");
    }

    @Override
    public java.lang.String getSchema() throws java.sql.SQLException {
        throw Unsupported.of("Connection.getSchema");
    }

    @Override
    public void setNetworkTimeout(java.util.concurrent.Executor p0, int p1) throws java.sql.SQLException {
        throw Unsupported.of("Connection.setNetworkTimeout");
    }

    @Override
    public int getNetworkTimeout() throws java.sql.SQLException {
        throw Unsupported.of("Connection.getNetworkTimeout");
    }

    @Override
    public void setTransactionIsolation(int p0) throws java.sql.SQLException {
        throw Unsupported.of("Connection.setTransactionIsolation");
    }
}
