package com.legend.warehouse.client.jdbc;

import com.legend.base.Nullable;
import com.legend.warehouse.client.WarehouseClient;
import com.legend.warehouse.sqlapi.SqlApi;
import com.legend.warehouse.sqlapi.SqlApi.StatementRequest;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.List;

/**
 * A statement, and a prepared one: the text goes to the warehouse, which
 * prepares it there (W1a). A prepared statement's parameters go beside its
 * text as the API's values ({@link StatementRequest#parameters}), and the
 * warehouse binds them to its {@code ?}s at run: a value is never written
 * into the text (AGENTS.md invariant 3; the orchestrator's ruling (a),
 * 2026-11-17). The values the warehouse binds are a String, a Long, a Double,
 * a Boolean and null, each as DuckDB's own driver binds the same Java value
 * (VARCHAR, BIGINT, DOUBLE, BOOLEAN, an untyped NULL); any other value is
 * refused by name, never converted (track Q2, 2026-11-19).
 */
final class WhStatement implements java.sql.PreparedStatement {

    /** A parameter index never set: told apart from a value set to null. */
    private static final Object UNSET = new Object();

    private final WhConnection conn;
    private final @Nullable String prepared;
    /** The prepared statement's values, by index - 1 ({@link #UNSET} where none was set). */
    private final List<@Nullable Object> parameters = new ArrayList<>();
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

    private boolean run(String sql, List<@Nullable Object> values) throws SQLException {
        results = null;
        updateCount = -1;
        WarehouseClient.Result r = call(sql, false, values);
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

    /** One request: the statement run with {@code values} bound to its {@code ?}s in order, or (describe) only
     *  prepared and its columns reported, nothing bound. */
    private WarehouseClient.Result call(String sql, boolean describe, List<@Nullable Object> values)
            throws SQLException {
        if (closed) throw new SQLException("the statement is closed");
        long timeoutMs = timeoutSeconds > 0 ? timeoutSeconds * 1000L : StatementRequest.DEFAULT_TIMEOUT_MS;
        WarehouseClient.Result r;
        try {
            StatementRequest request = new StatementRequest(sql, conn.catalog, timeoutMs,
                    StatementRequest.DEFAULT_WAIT_MS, StatementRequest.DEFAULT_ROWS_PER_CHUNK, conn.session);
            // Arrow (with DuckDB's text for nested cells, for getString) unless the URL chose JSON
            if (conn.format == SqlApi.ResultFormat.ARROW) request = request.as(SqlApi.ResultFormat.ARROW).withCellText();
            if (!values.isEmpty()) request = request.withParameters(values);
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

    /** The prepared statement's values in index order; an index below the highest set one that was never set is
     *  refused by name, never sent as a null. */
    private List<@Nullable Object> bound() throws SQLException {
        List<@Nullable Object> out = new ArrayList<>(parameters.size());
        for (int i = 0; i < parameters.size(); i++) {
            Object v = parameters.get(i);
            if (v == UNSET) {
                throw new SQLException("parameter " + (i + 1) + " was not set (parameters up to "
                        + parameters.size() + " were): the warehouse binds no value it was not given");
            }
            out.add(v);
        }
        return out;
    }

    /** {@code value} held for parameter {@code index}: a String, a Long, a Double, a Boolean or null -- what the
     *  warehouse binds -- or refused by name ({@code setter} the call that gave it). */
    private void hold(int index, @Nullable Object value, String setter) throws SQLException {
        if (closed) throw new SQLException("the statement is closed");
        if (prepared == null) throw new SQLException("not a prepared statement: " + setter);
        if (index < 1) throw new SQLException("parameter index " + index + " is not a parameter (they count from 1)");
        if (value != null && !(value instanceof String || value instanceof Long || value instanceof Double
                || value instanceof Boolean)) {
            throw new SQLFeatureNotSupportedException("the warehouse JDBC driver does not bind a "
                    + value.getClass().getName() + " (" + setter + " of parameter " + index + "): the warehouse"
                    + " binds a String, a Long, a Double, a Boolean or null");
        }
        if (value instanceof Double d && (d.isNaN() || d.isInfinite())) {
            throw new SQLFeatureNotSupportedException("the warehouse JDBC driver does not bind the double " + d + " ("
                    + setter + " of parameter " + index + "): the warehouse's values travel as JSON, which has no "
                    + d);
        }
        while (parameters.size() < index) parameters.add(UNSET);
        parameters.set(index - 1, value);
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        return run(sql, List.of());
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        return query(sql, List.of());
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        return update(sql, List.of());
    }

    private ResultSet query(String sql, List<@Nullable Object> values) throws SQLException {
        if (!run(sql, values)) throw new SQLException("the statement did not return rows");
        return getResultSet();
    }

    private int update(String sql, List<@Nullable Object> values) throws SQLException {
        if (run(sql, values)) throw new SQLException("the statement returned rows");
        return (int) Math.max(0, updateCount);
    }

    @Override
    public boolean execute() throws SQLException {
        return run(text(), bound());
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return query(text(), bound());
    }

    @Override
    public int executeUpdate() throws SQLException {
        return update(text(), bound());
    }

    @Override
    public void setString(int index, @Nullable String value) throws SQLException {
        hold(index, value, "setString");
    }

    /** {@inheritDoc} A String, a Long, a Double, a Boolean or null; any other value is refused by name. */
    @Override
    public void setObject(int index, @Nullable Object value) throws SQLException {
        hold(index, value, "setObject");
    }

    /** {@inheritDoc} A NULL, untyped, as DuckDB's own driver binds it whatever {@code sqlType} says. */
    @Override
    public void setNull(int index, int sqlType) throws SQLException {
        hold(index, null, "setNull");
    }

    @Override
    public void clearParameters() throws SQLException {
        if (closed) throw new SQLException("the statement is closed");
        parameters.clear();
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
    public @com.legend.base.Nullable SQLWarning getWarnings() {
        return null;
    }

    @Override
    public void clearWarnings() {
    }

    @Override
    public void close() {
        closed = true;
        results = null;
        parameters.clear();
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
    public void setNull(int p0, int p1, java.lang.String p2) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setNull");
    }

    @Override
    public void setBigDecimal(int p0, java.math.BigDecimal p1) throws java.sql.SQLException {
        throw Unsupported.of("PreparedStatement.setBigDecimal");
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
        return WhResultSetMetaData.of(call(text(), true, List.of()).meta().columns());
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

    /** {@inheritDoc} The statement described by the warehouse (prepared there, nothing run or bound), its parameter
     *  count DuckDB's own; a warehouse that reports none is refused by name, never answered 0. */
    @Override
    public java.sql.ParameterMetaData getParameterMetaData() throws java.sql.SQLException {
        Integer count = call(text(), true, List.of()).meta().parameterCount();
        if (count == null) {
            throw new SQLException("the warehouse's describe of the statement reported no parameter count (its"
                    + " parameterCount): how many values it binds is unknown");
        }
        return new WhParameterMetaData(count);
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
