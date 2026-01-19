package org.finos.legend.engine.serialization;

import org.finos.legend.engine.execution.BufferedResult;
import org.finos.legend.engine.execution.StreamingResult;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Interface for serializing query results to various output formats.
 * 
 * Implementations can support both buffered results (all data in memory)
 * and streaming results (lazy iteration).
 * 
 * GraalVM native-image compatible.
 */
public interface ResultSerializer {

    /**
     * Returns the format identifier (e.g., "json", "csv", "arrow").
     */
    String formatId();

    /**
     * Returns the MIME content type for HTTP responses.
     */
    String contentType();

    /**
     * Serializes a buffered result to the output stream.
     */
    void serialize(BufferedResult result, OutputStream out) throws IOException;

    /**
     * Serializes a streaming result to the output stream.
     * 
     * Default implementation buffers the result first.
     * Override for true streaming serialization.
     */
    default void serializeStreaming(StreamingResult result, OutputStream out) throws IOException {
        serialize(result.toBuffered(), out);
    }

    /**
     * Returns true if this serializer supports true streaming
     * (can write incrementally without buffering the entire result).
     */
    default boolean supportsStreaming() {
        return false;
    }
}
