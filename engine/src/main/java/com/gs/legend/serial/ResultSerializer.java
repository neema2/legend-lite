package com.gs.legend.serial;

import com.gs.legend.exec.ExecutionResult;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Interface for serializing query results to various output formats.
 *
 * GraalVM native-image compatible.
 */
public interface ResultSerializer {

    /**
     * Returns the format identifier (e.g., "json", "csv").
     */
    String formatId();

    /**
     * Returns the MIME content type for HTTP responses.
     */
    String contentType();

    /**
     * Serializes an ExecutionResult to the output stream.
     */
    void serialize(ExecutionResult result, OutputStream out) throws IOException;

    /**
     * Returns true if this serializer supports true streaming
     * (can write incrementally without buffering the entire result).
     */
    default boolean supportsStreaming() {
        return false;
    }
}
