package org.finos.legend.engine.execution;

import org.finos.legend.engine.serialization.ResultSerializer;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Sealed interface representing query results.
 * 
 * Supports both buffered (fully materialized) and streaming (lazy cursor)
 * modes.
 * Use {@link BufferedResult} for small results or when re-processing is needed.
 * Use {@link StreamingResult} for large results to minimize memory usage.
 * 
 * GraalVM native-image compatible.
 */
public sealed interface Result extends AutoCloseable
        permits BufferedResult, StreamingResult {

    /**
     * Gets the column metadata for this result.
     * Available immediately for both buffered and streaming results.
     */
    List<Column> columns();

    /**
     * Gets the number of rows.
     * Returns -1 for streaming results where the count is unknown until consumed.
     */
    long rowCount();

    /**
     * Returns an iterator over the rows.
     * For streaming results, this consumes the underlying cursor.
     */
    Iterator<Row> iterator();

    /**
     * Returns a stream over the rows.
     * For streaming results, the stream should be closed when done.
     */
    Stream<Row> stream();

    /**
     * Serializes this result to the given output stream using the specified
     * serializer.
     */
    void writeTo(OutputStream out, ResultSerializer serializer) throws IOException;

    /**
     * Converts this result to a BufferedResult.
     * For already-buffered results, returns itself.
     * For streaming results, materializes all remaining rows.
     */
    BufferedResult toBuffered();

    /**
     * Closes this result and releases any underlying resources.
     * For streaming results, this closes the JDBC cursor.
     * For buffered results, this is a no-op.
     */
    @Override
    void close();

    /**
     * Gets the number of columns.
     */
    default int columnCount() {
        return columns().size();
    }

    /**
     * Gets a value at the specified row and column index.
     * Only supported for buffered results.
     */
    default Object getValue(int rowIndex, int columnIndex) {
        throw new UnsupportedOperationException("Random access not supported for streaming results");
    }

    /**
     * Gets a value at the specified row by column name.
     * Only supported for buffered results.
     */
    default Object getValue(int rowIndex, String columnName) {
        throw new UnsupportedOperationException("Random access not supported for streaming results");
    }
}
