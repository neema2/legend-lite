package com.legend.warehouse.client.jdbc;

import com.legend.Nullable;
import com.legend.warehouse.client.WarehouseClient;
import com.legend.warehouse.sqlapi.SqlApi.StatementRequest;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;

/**
 * A statement, and a prepared one: the text goes to the warehouse, which
 * prepares it there (W1a). Parameters are not carried yet, so a prepared
 * statement is only its text.
 */
final class WhStatement implements java.sql.PreparedStatement {

    private final WhConnection conn;
    private final @Nullable String prepared;
    private @Nullable WhResultSet results;
    private long updateCount = -1;
    private int timeoutSeconds;
    private int maxRows;
    private volatile @Nullable String inFlight;
    private boolean closed;

    WhStatement(WhConnection conn, @Nullable String prepared) {
        this.conn = conn;
        this.prepared = prepared;
    }

    private boolean run(String sql) throws SQLException {
        results = null;
        updateCount = -1;
        WarehouseClient.Result r = call(sql, false);
        // A statement with no columns reported an update count, not rows
        // (DDL: -1, as DuckDB's driver reports it; a write: its row count).
        if (r.meta().columns().isEmpty()) {
            updateCount = r.meta().rowCount();
            return false;
        }
        results = new WhResultSet(this, r.meta(), maxRows > 0 && r.rows().size() > maxRows
                ? r.rows().subList(0, maxRows) : r.rows());
        return true;
    }

    /** One request: the statement run, or (describe) only prepared and its columns reported. */
    private WarehouseClient.Result call(String sql, boolean describe) throws SQLException {
        if (closed) throw new SQLException("the statement is closed");
        long timeoutMs = timeoutSeconds > 0 ? timeoutSeconds * 1000L : StatementRequest.DEFAULT_TIMEOUT_MS;
        WarehouseClient.Result r;
        try {
            StatementRequest request = new StatementRequest(sql, conn.catalog, timeoutMs,
                    StatementRequest.DEFAULT_WAIT_MS, StatementRequest.DEFAULT_ROWS_PER_CHUNK, conn.session);
            r = conn.client.execute(describe ? request.describe() : request, id -> inFlight = id);
        } catch (WarehouseClient.Failure f) {
            throw new SQLException(f.error().message(), f.error().code().name(), f);
        } catch (java.io.IOException e) {
            throw new SQLException("the warehouse could not be reached: " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("interrupted", e);
        } finally {
            inFlight = null;
        }
        return r;
    }

    private String text() throws SQLException {
        String s = prepared;
        if (s == null) throw new SQLException("not a prepared statement");
        return s;
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        return run(sql);
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        if (!run(sql)) throw new SQLException("the statement did not return rows");
        return getResultSet();
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        if (run(sql)) throw new SQLException("the statement returned rows");
        return (int) Math.max(0, updateCount);
    }

    @Override
    public boolean execute() throws SQLException {
        return run(text());
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return executeQuery(text());
    }

    @Override
    public int executeUpdate() throws SQLException {
        return executeUpdate(text());
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        WhResultSet r = results;
        if (r == null) throw new SQLException("no result set");
        return r;
    }

    @Override
    public int getUpdateCount() {
        return results != null ? -1 : (int) updateCount;
    }

    @Override
    public long getLargeUpdateCount() {
        return results != null ? -1 : updateCount;
    }

    @Override
    public boolean getMoreResults() {
        results = null;
        updateCount = -1;
        return false;
    }

    @Override
    public void cancel() throws SQLException {
        String id = inFlight;
        if (id == null) return;
        try {
            conn.client.cancel(id);
        } catch (java.io.IOException e) {
            throw new SQLException("could not cancel: " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void setQueryTimeout(int seconds) {
        timeoutSeconds = seconds;
    }

    @Override
    public int getQueryTimeout() {
        return timeoutSeconds;
    }

    @Override
    public void setMaxRows(int max) {
        maxRows = max;
    }

    @Override
    public int getMaxRows() {
        return maxRows;
    }

    @Override
    public void setFetchSize(int rows) {
        // chunks are the warehouse's; fetch size is only a hint
    }

    @Override
    public int getFetchSize() {
        return StatementRequest.DEFAULT_ROWS_PER_CHUNK;
    }

    @Override
    public java.sql.Connection getConnection() {
        return conn;
    }

    @Override
    public @com.legend.Nullable SQLWarning getWarnings() {
        return null;
    }

    @Override
    public void clearWarnings() {
    }

    @Override
    public void close() {
        closed = true;
        results = null;
    }

    @Override
    public boolean isClosed() {
        return closed;
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
    public void setBoolean(int p0, boolean p1) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setBoolean");
    }

    @Override
    public void setByte(int p0, byte p1) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setByte");
    }

    @Override
    public void setShort(int p0, short p1) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setShort");
    }

    @Override
    public void setInt(int p0, int p1) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setInt");
    }

    @Override
    public void setLong(int p0, long p1) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setLong");
    }

    @Override
    public void setFloat(int p0, float p1) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setFloat");
    }

    @Override
    public void setDouble(int p0, double p1) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setDouble");
    }

    @Override
    public void setURL(int p0, java.net.URL p1) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setURL");
    }

    @Override
    public void setArray(int p0, java.sql.Array p1) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setArray");
    }

    @Override
    public void setTime(int p0, java.sql.Time p1, java.util.Calendar p2) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setTime");
    }

    @Override
    public void setTime(int p0, java.sql.Time p1) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setTime");
    }

    @Override
    public void setDate(int p0, java.sql.Date p1, java.util.Calendar p2) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setDate");
    }

    @Override
    public void setDate(int p0, java.sql.Date p1) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setDate");
    }

    @Override
    public void setNull(int p0, int p1) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setNull");
    }

    @Override
    public void setNull(int p0, int p1, java.lang.String p2) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setNull");
    }

    @Override
    public void setBigDecimal(int p0, java.math.BigDecimal p1) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setBigDecimal");
    }

    @Override
    public void setString(int p0, java.lang.String p1) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setString");
    }

    @Override
    public void setBytes(int p0, byte[] p1) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setBytes");
    }

    @Override
    public void setTimestamp(int p0, java.sql.Timestamp p1, java.util.Calendar p2) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setTimestamp");
    }

    @Override
    public void setTimestamp(int p0, java.sql.Timestamp p1) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setTimestamp");
    }

    @Override
    public void setAsciiStream(int p0, java.io.InputStream p1) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setAsciiStream");
    }

    @Override
    public void setAsciiStream(int p0, java.io.InputStream p1, int p2) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setAsciiStream");
    }

    @Override
    public void setAsciiStream(int p0, java.io.InputStream p1, long p2) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setAsciiStream");
    }

    @Override
    public void setUnicodeStream(int p0, java.io.InputStream p1, int p2) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setUnicodeStream");
    }

    @Override
    public void setBinaryStream(int p0, java.io.InputStream p1) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setBinaryStream");
    }

    @Override
    public void setBinaryStream(int p0, java.io.InputStream p1, long p2) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setBinaryStream");
    }

    @Override
    public void setBinaryStream(int p0, java.io.InputStream p1, int p2) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setBinaryStream");
    }

    @Override
    public void clearParameters() throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.clearParameters");
    }

    @Override
    public void setObject(int p0, java.lang.Object p1) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setObject");
    }

    @Override
    public void setObject(int p0, java.lang.Object p1, int p2, int p3) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setObject");
    }

    @Override
    public void setObject(int p0, java.lang.Object p1, int p2) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setObject");
    }

    @Override
    public void addBatch() throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.addBatch");
    }

    @Override
    public void setCharacterStream(int p0, java.io.Reader p1, int p2) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setCharacterStream");
    }

    @Override
    public void setCharacterStream(int p0, java.io.Reader p1) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setCharacterStream");
    }

    @Override
    public void setCharacterStream(int p0, java.io.Reader p1, long p2) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setCharacterStream");
    }

    @Override
    public void setRef(int p0, java.sql.Ref p1) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setRef");
    }

    @Override
    public void setBlob(int p0, java.sql.Blob p1) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setBlob");
    }

    @Override
    public void setBlob(int p0, java.io.InputStream p1) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setBlob");
    }

    @Override
    public void setBlob(int p0, java.io.InputStream p1, long p2) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setBlob");
    }

    @Override
    public void setClob(int p0, java.sql.Clob p1) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setClob");
    }

    @Override
    public void setClob(int p0, java.io.Reader p1, long p2) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setClob");
    }

    @Override
    public void setClob(int p0, java.io.Reader p1) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setClob");
    }

    @Override
    public java.sql.ResultSetMetaData getMetaData() throws java.sql.SQLException {
        WhResultSet r = results;
        if (r != null) return r.getMetaData();
        return WhResultSetMetaData.of(call(text(), true).meta().columns());
    }

    @Override
    public void setRowId(int p0, java.sql.RowId p1) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setRowId");
    }

    @Override
    public void setNString(int p0, java.lang.String p1) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setNString");
    }

    @Override
    public void setNClob(int p0, java.sql.NClob p1) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setNClob");
    }

    @Override
    public void setNClob(int p0, java.io.Reader p1) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setNClob");
    }

    @Override
    public void setNClob(int p0, java.io.Reader p1, long p2) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setNClob");
    }

    @Override
    public void setSQLXML(int p0, java.sql.SQLXML p1) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setSQLXML");
    }

    @Override
    public java.sql.ParameterMetaData getParameterMetaData() throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.getParameterMetaData");
    }

    @Override
    public void setNCharacterStream(int p0, java.io.Reader p1, long p2) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setNCharacterStream");
    }

    @Override
    public void setNCharacterStream(int p0, java.io.Reader p1) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setNCharacterStream");
    }

    @Override
    public boolean execute(java.lang.String p0, int p1) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.execute");
    }

    @Override
    public boolean execute(java.lang.String p0, java.lang.String[] p1) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.execute");
    }

    @Override
    public boolean execute(java.lang.String p0, int[] p1) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.execute");
    }

    @Override
    public int executeUpdate(java.lang.String p0, int p1) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.executeUpdate");
    }

    @Override
    public int executeUpdate(java.lang.String p0, java.lang.String[] p1) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.executeUpdate");
    }

    @Override
    public int executeUpdate(java.lang.String p0, int[] p1) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.executeUpdate");
    }

    @Override
    public void addBatch(java.lang.String p0) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.addBatch");
    }

    @Override
    public int getMaxFieldSize() throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.getMaxFieldSize");
    }

    @Override
    public void setMaxFieldSize(int p0) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setMaxFieldSize");
    }

    @Override
    public void setCursorName(java.lang.String p0) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setCursorName");
    }

    @Override
    public boolean getMoreResults(int p0) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.getMoreResults");
    }

    @Override
    public void setFetchDirection(int p0) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setFetchDirection");
    }

    @Override
    public int getFetchDirection() throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.getFetchDirection");
    }

    @Override
    public int getResultSetType() throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.getResultSetType");
    }

    @Override
    public void clearBatch() throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.clearBatch");
    }

    @Override
    public int[] executeBatch() throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.executeBatch");
    }

    @Override
    public java.sql.ResultSet getGeneratedKeys() throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.getGeneratedKeys");
    }

    @Override
    public void setPoolable(boolean p0) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setPoolable");
    }

    @Override
    public boolean isPoolable() throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.isPoolable");
    }

    @Override
    public void closeOnCompletion() throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.closeOnCompletion");
    }

    @Override
    public void setEscapeProcessing(boolean p0) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setEscapeProcessing");
    }

    @Override
    public int getResultSetConcurrency() throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.getResultSetConcurrency");
    }

    @Override
    public int getResultSetHoldability() throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.getResultSetHoldability");
    }

    @Override
    public boolean isCloseOnCompletion() throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.isCloseOnCompletion");
    }
}
