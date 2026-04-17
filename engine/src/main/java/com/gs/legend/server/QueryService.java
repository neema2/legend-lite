package com.gs.legend.server;

import com.gs.legend.exec.ExecutionResult;
import com.gs.legend.exec.PlanExecutor;
import com.gs.legend.model.PureModelBuilder;
import com.gs.legend.plan.PlanGenerator;
import com.gs.legend.plan.SingleExecutionPlan;
import com.gs.legend.serial.ResultSerializer;
import com.gs.legend.serial.SerializerRegistry;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Stateless query execution: parse → compile → plan → execute.
 *
 * <p>QueryService is the mapping entry point. It orchestrates the full pipeline
 * and threads mapping information explicitly to MappingResolver.
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
 *       OutputStream. Memory footprint is O(one row) regardless of result
 *       size. JSON only.</li>
 *   <li>{@link #stream(String, String, String, OutputStream)} — same,
 *       auto-resolves the connection.</li>
 * </ul>
 *
 * <p>Raw-SQL escape hatch:
 * <ul>
 *   <li>{@link #executeSql(String, String, String)} — runs arbitrary SQL
 *       against the Runtime's connection (no Pure parsing, no plan).</li>
 * </ul>
 *
 * <pre>
 * // Typed snapshot
 * ExecutionResult result = svc.execute(model, query, runtime);
 *
 * // Snapshot written to an HTTP response body in CSV
 * svc.execute(model, query, runtime, conn, response.getOutputStream(), OutputFormat.CSV);
 *
 * // Streaming JSON to an HTTP response body
 * svc.stream(model, query, runtime, conn, response.getOutputStream());
 * </pre>
 */
public class QueryService {

    /**
     * Parse → compile → generate plan → execute with typed result.
     * Mappings are auto-discovered from the registry.
     */
    public ExecutionResult execute(String pureSource, String query, String runtimeName,
            Connection connection) throws SQLException {

        PureModelBuilder model = new PureModelBuilder().addSource(pureSource);
        SingleExecutionPlan plan = PlanGenerator.generate(model, query, runtimeName, PlanGenerator.Mode.SNAPSHOT);

        System.out.println("Pure Query: " + query);
        System.out.println("Generated SQL: " + plan.sql());

        return PlanExecutor.execute(plan, connection);
    }

    /**
     * Convenience: resolves connection from Runtime, then executes.
     * Mappings are auto-discovered from the registry.
     */
    public ExecutionResult execute(String pureSource, String query, String runtimeName)
            throws SQLException {

        PureModelBuilder model = new PureModelBuilder().addSource(pureSource);
        Connection conn = model.resolveConnection(runtimeName);
        SingleExecutionPlan plan = PlanGenerator.generate(model, query, runtimeName, PlanGenerator.Mode.SNAPSHOT);

        System.out.println("Pure Query: " + query);
        System.out.println("Generated SQL: " + plan.sql());

        return PlanExecutor.execute(plan, conn);
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

        PureModelBuilder model = new PureModelBuilder().addSource(pureSource);
        SingleExecutionPlan plan = PlanGenerator.generate(model, query, runtimeName, PlanGenerator.Mode.SNAPSHOT);

        System.out.println("Pure Query: " + query);
        System.out.println("Generated SQL: " + plan.sql());

        ExecutionResult result = PlanExecutor.execute(plan, connection);
        ResultSerializer serializer = SerializerRegistry.get(format.id());
        serializer.serialize(result, out);
    }

    /**
     * Convenience: auto-resolves the JDBC connection from the Runtime,
     * then calls {@link #execute(String, String, String, Connection, OutputStream, OutputFormat)}.
     */
    public void execute(String pureSource, String query, String runtimeName,
            OutputStream out, OutputFormat format)
            throws SQLException, IOException {

        PureModelBuilder model = new PureModelBuilder().addSource(pureSource);
        Connection conn = model.resolveConnection(runtimeName);
        execute(pureSource, query, runtimeName, conn, out, format);
    }

    /**
     * Execute raw SQL against the connection from the Runtime.
     */
    public ExecutionResult executeSql(String pureSource, String sql, String runtimeName)
            throws SQLException {

        PureModelBuilder model = new PureModelBuilder().addSource(pureSource);
        Connection conn = model.resolveConnection(runtimeName);

        try (java.sql.Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            return ExecutionResult.empty();
        }
    }

    /**
     * True streaming JSON path — no {@code ExecutionResult} materialization.
     *
     * <p>For Tabular query plans, rows are pulled from the JDBC ResultSet one
     * at a time and written directly to {@code out} as they arrive. Memory
     * footprint is O(one row) regardless of result size.
     *
     * <p>Internally wraps {@code out} in a UTF-8 {@link OutputStreamWriter}
     * and delegates to {@link PlanExecutor#streamJson}. After each row is
     * emitted the writer is flushed, which propagates bytes through the
     * OutputStreamWriter buffer to the underlying {@code out} — so HTTP
     * response bodies, sockets, and other downstream consumers observe
     * incremental delivery.
     *
     * <p>This method does NOT close {@code out}. Caller owns lifecycle.
     *
     * @see com.gs.legend.exec.PlanExecutor#streamJson
     */
    public void stream(String pureSource, String query, String runtimeName,
            Connection connection, OutputStream out)
            throws SQLException, IOException {

        PureModelBuilder model = new PureModelBuilder().addSource(pureSource);
        SingleExecutionPlan plan = PlanGenerator.generate(model, query, runtimeName, PlanGenerator.Mode.STREAMING);

        System.out.println("Pure Query: " + query);
        System.out.println("Generated SQL: " + plan.sql());

        Writer writer = new OutputStreamWriter(out, StandardCharsets.UTF_8);
        PlanExecutor.streamJson(plan, connection, writer);
        writer.flush();
    }

    /**
     * Convenience: auto-resolves the JDBC connection from the Runtime,
     * then calls {@link #stream(String, String, String, Connection, OutputStream)}.
     */
    public void stream(String pureSource, String query, String runtimeName,
            OutputStream out)
            throws SQLException, IOException {

        PureModelBuilder model = new PureModelBuilder().addSource(pureSource);
        Connection conn = model.resolveConnection(runtimeName);
        stream(pureSource, query, runtimeName, conn, out);
    }

}
