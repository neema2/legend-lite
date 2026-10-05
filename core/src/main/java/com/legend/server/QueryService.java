package com.legend.server;

import com.legend.exec.ExecutionResult;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

/**
 * Stateless query execution: parse → compile → plan → execute — the CORE
 * pipeline end to end (engine-lite deletion; the A/B settled). Results are
 * core records ({@link ExecutionResult}); this class owns orchestration and
 * connection resolution only.
 *
 * <p>Two orthogonal concerns, two method families:
 *
 * <h2>Snapshot ({@code execute}) — materializes all rows in memory</h2>
 * <ul>
 *   <li>{@link #execute(String, String, String, Connection)} — returns a
 *       typed {@link ExecutionResult} the caller can introspect.</li>
 *   <li>{@link #execute(String, String, String)} — same, auto-resolves
 *       the JDBC connection from the Runtime.</li>
 *   <li>{@link #execute(String, String, String, Connection, OutputStream, OutputFormat)}
 *       — materializes then writes serialized bytes to an OutputStream in
 *       the chosen {@link OutputFormat} (JSON, CSV).</li>
 *   <li>{@link #execute(String, String, String, OutputStream, OutputFormat)}
 *       — same, auto-resolves the connection.</li>
 * </ul>
 *
 * <h2>Streaming ({@code stream}) — writes JSON row-by-row, no materialization</h2>
 * <ul>
 *   <li>{@link #stream(String, String, String, Connection, OutputStream)} —
 *       iterates the JDBC ResultSet lazily and emits each row directly to the
 *       OutputStream ({@code Compiler.executeStreaming} →
 *       {@code Executor.stream}). Memory footprint is O(one row) regardless
 *       of result size. JSON only.</li>
 *   <li>{@link #stream(String, String, String, OutputStream)} — same,
 *       auto-resolves the connection.</li>
 * </ul>
 *
 * <p>There is no raw-SQL entry point: {@code executeSql} and its {@code /engine/sql} route were deleted
 * (execution plan W0.1, 2026-09-29); tests seed tables in-process.
 */
public class QueryService {

    /**
     * Parse → compile → generate plan → execute with typed result.
     * Mappings are auto-discovered from the registry.
     */
    public ExecutionResult execute(String pureSource, String query, String runtimeName,
            Connection connection) throws SQLException {
        return execute(pureSource, query, runtimeName, connection,
                com.legend.ExecuteOptions.NONE);
    }

    /** With the caller's execute OPTIONS (the PCT adapter's wire render). */
    public ExecutionResult execute(String pureSource, String query, String runtimeName,
            Connection connection, com.legend.ExecuteOptions options) throws SQLException {
        return Objects.requireNonNull(
                com.legend.Compiler.execute(pureSource, query, null, runtimeName, connection,
                        options),
                "query produced no result");
    }

    /**
     * Convenience: resolves connection from Runtime, then executes.
     * Mappings are auto-discovered from the registry.
     */
    public ExecutionResult execute(String pureSource, String query, String runtimeName)
            throws SQLException {

        return Objects.requireNonNull(com.legend.Compiler.execute(pureSource, query, runtimeName,
                ConnectionResolver.SOURCE, com.legend.ExecuteOptions.NONE), "query produced no result");
    }

    /**
     * Snapshot write-to-output: parse → compile → plan → execute → serialize
     * in the requested {@link OutputFormat} to the caller's OutputStream.
     *
     * <p>All rows are materialized into an {@link ExecutionResult} in memory
     * before serialization begins. Use {@link #stream} for true streaming
     * without materialization (JSON only).
     *
     * <p>This method does NOT close {@code out}. Caller owns lifecycle.
     */
    public void execute(String pureSource, String query, String runtimeName,
            Connection connection, OutputStream out, OutputFormat format)
            throws SQLException, IOException {

        // E5: the wire text is PLAN-RENDERED — the database composes the
        // CSV/JSON bytes (Compiler.executeWire); the registry stays the
        // format-metadata surface (id/contentType/streaming capability)
        Writer writer = new OutputStreamWriter(out, StandardCharsets.UTF_8);
        com.legend.Compiler.executeWire(pureSource, query, runtimeName,
                connection,
                format == OutputFormat.CSV
                        ? com.legend.lowering.WireRender.Format.CSV
                        : com.legend.lowering.WireRender.Format.JSON,
                writer);
        writer.flush();
    }

    /**
     * Upstream {@code pure/v1/execution/execute} (E8): the runtime's connection leased, the
     * already-parsed query run on it ({@code Compiler.executeWire}), the database's JSON rows
     * written to {@code rows}. A database failure is a {@link com.legend.error.DataError}.
     */
    public com.legend.plan.QueryPlan executeUpstream(String model,
            com.legend.protocol.spec.ValueSpecification query, String runtimeName, Writer rows) {
        try {
            return com.legend.Compiler.executeWire(model, query, runtimeName, ConnectionResolver.SOURCE, rows);
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    /**
     * Convenience: auto-resolves the JDBC connection from the Runtime,
     * then calls {@link #execute(String, String, String, Connection, OutputStream, OutputFormat)}.
     */
    public void execute(String pureSource, String query, String runtimeName,
            OutputStream out, OutputFormat format)
            throws SQLException, IOException {

        Writer writer = new OutputStreamWriter(out, StandardCharsets.UTF_8);
        com.legend.Compiler.executeWire(pureSource, query, runtimeName, ConnectionResolver.SOURCE,
                format == OutputFormat.CSV
                        ? com.legend.lowering.WireRender.Format.CSV
                        : com.legend.lowering.WireRender.Format.JSON,
                writer);
        writer.flush();
    }

    /**
     * True streaming JSON path — no {@code ExecutionResult} materialization.
     *
     * <p>For Tabular query plans, rows are pulled from the JDBC ResultSet one
     * at a time and written directly to {@code out} as they arrive. Memory
     * footprint is O(one row) regardless of result size.
     *
     * <p>Internally wraps {@code out} in a UTF-8 {@link OutputStreamWriter}
     * and delegates to {@code Compiler.executeStreaming} (the streaming
     * lowering's per-row {@code json_object} root pushed through
     * {@code Executor.stream}). After each row the writer is flushed, so HTTP
     * response bodies, sockets, and other downstream consumers observe
     * incremental delivery.
     *
     * <p>This method does NOT close {@code out}. Caller owns lifecycle.
     */
    public void stream(String pureSource, String query, String runtimeName,
            Connection connection, OutputStream out)
            throws SQLException, IOException {

        Writer writer = new OutputStreamWriter(out, StandardCharsets.UTF_8);
        com.legend.Compiler.executeStreaming(pureSource, query, runtimeName,
                connection, writer);
        writer.flush();
    }

    /**
     * Convenience: auto-resolves the JDBC connection from the Runtime,
     * then calls {@link #stream(String, String, String, Connection, OutputStream)}.
     */
    public void stream(String pureSource, String query, String runtimeName,
            OutputStream out)
            throws SQLException, IOException {

        Writer writer = new OutputStreamWriter(out, StandardCharsets.UTF_8);
        com.legend.Compiler.executeStreaming(pureSource, query, runtimeName, ConnectionResolver.SOURCE, writer);
        writer.flush();
    }

}
