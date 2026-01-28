package org.finos.legend.engine.execution;

import org.finos.legend.engine.serialization.ResultSerializer;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A lazy, streaming query result that holds an open JDBC cursor.
 * 
 * Rows are fetched on-demand, minimizing memory usage for large result sets.
 * The underlying JDBC resources (ResultSet, Statement, Connection) are held
 * open
 * until this result is closed.
 * 
 * IMPORTANT: This result MUST be closed after use to release database
 * resources.
 * Use try-with-resources for safe resource management.
 * 
 * GraalVM native-image compatible.
 */
public final class StreamingResult implements Result {

    private final List<Column> columns;
    private final ResultSet resultSet;
    private final Statement statement;
    private final Connection connection;
    private final int columnCount;
    private final boolean ownsConnection;

    private boolean closed = false;
    private boolean consumed = false;

    private StreamingResult(
            List<Column> columns,
            ResultSet resultSet,
            Statement statement,
            Connection connection,
            int columnCount,
            boolean ownsConnection) {
        this.columns = columns;
        this.resultSet = resultSet;
        this.statement = statement;
        this.connection = connection;
        this.columnCount = columnCount;
        this.ownsConnection = ownsConnection;
    }

    @Override
    public List<Column> columns() {
        return columns;
    }

    @Override
    public long rowCount() {
        return -1; // Unknown until fully consumed
    }

    @Override
    public Iterator<Row> iterator() {
        if (consumed) {
            throw new IllegalStateException("Streaming result can only be iterated once");
        }
        consumed = true;
        return new ResultSetIterator();
    }

    @Override
    public Stream<Row> stream() {
        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(iterator(), Spliterator.ORDERED),
                false).onClose(this::close);
    }

    @Override
    public void writeTo(OutputStream out, ResultSerializer serializer) throws IOException {
        serializer.serializeStreaming(this, out);
    }

    @Override
    public BufferedResult toBuffered() {
        List<Row> rows = new ArrayList<>();
        iterator().forEachRemaining(rows::add);
        close();
        return new BufferedResult(columns, rows);
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            try {
                if (resultSet != null)
                    resultSet.close();
            } catch (SQLException ignored) {
            }
            try {
                if (statement != null)
                    statement.close();
            } catch (SQLException ignored) {
            }
            if (ownsConnection) {
                try {
                    if (connection != null)
                        connection.close();
                } catch (SQLException ignored) {
                }
            }
        }
    }

    /**
     * Creates a StreamingResult from JDBC resources.
     * 
     * @param rs        The ResultSet to stream from
     * @param stmt      The Statement that created the ResultSet
     * @param conn      The Connection (will NOT be closed by this result)
     * @param fetchSize Hint for JDBC driver batch size
     * @return A new StreamingResult
     */
    public static StreamingResult fromResultSet(
            ResultSet rs,
            Statement stmt,
            Connection conn,
            int fetchSize) throws SQLException {

        rs.setFetchSize(fetchSize);

        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();

        List<Column> columns = new ArrayList<>(columnCount);
        for (int i = 1; i <= columnCount; i++) {
            columns.add(new Column(
                    meta.getColumnLabel(i),
                    meta.getColumnTypeName(i),
                    Column.mapJdbcTypeToJava(meta.getColumnType(i))));
        }

        return new StreamingResult(columns, rs, stmt, conn, columnCount, false);
    }

    /**
     * Creates a StreamingResult that owns and will close the connection.
     */
    public static StreamingResult fromResultSetOwningConnection(
            ResultSet rs,
            Statement stmt,
            Connection conn,
            int fetchSize) throws SQLException {

        rs.setFetchSize(fetchSize);

        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();

        List<Column> columns = new ArrayList<>(columnCount);
        for (int i = 1; i <= columnCount; i++) {
            columns.add(new Column(
                    meta.getColumnLabel(i),
                    meta.getColumnTypeName(i),
                    Column.mapJdbcTypeToJava(meta.getColumnType(i))));
        }

        return new StreamingResult(columns, rs, stmt, conn, columnCount, true);
    }

    /**
     * Iterator implementation that wraps a JDBC ResultSet.
     */
    private class ResultSetIterator implements Iterator<Row> {

        private Boolean hasNext = null;

        @Override
        public boolean hasNext() {
            if (closed) {
                return false;
            }
            if (hasNext == null) {
                try {
                    hasNext = resultSet.next();
                    if (!hasNext) {
                        close();
                    }
                } catch (SQLException e) {
                    close();
                    throw new RuntimeException("Error reading result set", e);
                }
            }
            return hasNext;
        }

        @Override
        public Row next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            try {
                Row row = Row.fromResultSet(resultSet, columnCount);
                hasNext = null; // Reset for next call to hasNext()
                return row;
            } catch (SQLException e) {
                close();
                throw new RuntimeException("Error reading result set", e);
            }
        }
    }
}
