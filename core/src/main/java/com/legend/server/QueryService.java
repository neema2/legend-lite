package com.legend.server;


import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * Stateless query execution: parse → compile → plan → execute — the CORE
 * pipeline end to end (engine-lite deletion; the A/B settled); this class owns
 * orchestration and connection resolution only.
 *
 * <ul>
 *   <li>(The materialized {@code execute} entries, the old door's snapshot of a query's answer, were deleted
 *       2026-10-29, the result boundary program's track D: a product answer is a plan, written at the boundary.)</li>
 *   <li>{@link #execute(String, String, String, Connection, OutputStream, OutputFormat)}
 *       and {@link #stream} — the query planned as typed rows, run by the plan's
 *       runner, its rows written in batches as they arrive by the format's writer
 *       ({@code Execution.run}): lite's CSV or its JSON rows, in memory of one
 *       batch whatever the result's size.</li>
 *   <li>{@link #executeUpstream} — {@code pure/v1} execute's runner, written by
 *       the engine's writer the API hands it.</li>
 * </ul>
 *
 * <p>There is no raw-SQL entry point: {@code executeSql} and its {@code /engine/sql} route were deleted
 * (execution plan W0.1, 2026-09-29); tests seed tables in-process.
 */
public class QueryService {

    /**
     * The query run on {@code connection}, its answer written to {@code out} in {@code format} as its rows arrive
     * (docs/RESULT_BOUNDARY_PROGRAM_2026_10_26.md, the end state of 2026-10-27): planned as typed rows, run by the
     * plan's runner, each cell spelled by the format's writer -- lite's CSV ({@link com.legend.result.CsvWriter}) or
     * its JSON rows ({@link com.legend.result.JsonRowsWriter}). A program that writes keeps its change on the caller's
     * connection. This method does NOT close {@code out}.
     */
    public void execute(String pureSource, String query, String runtimeName,
            Connection connection, OutputStream out, OutputFormat format)
            throws SQLException, IOException {
        Writer writer = new OutputStreamWriter(out, StandardCharsets.UTF_8);
        com.legend.Execution.run(pureSource, query, runtimeName, com.legend.exec.Sessions.given(connection),
                com.legend.exec.PlanSessions.given(connection), plan -> writer(plan, format, writer));
        writer.flush();
    }

    /** {@link #execute(String, String, String, Connection, OutputStream, OutputFormat)} on the connection the runtime
     *  declares, leased for the query; a program that writes runs on a database of its own (decision A). */
    public void execute(String pureSource, String query, String runtimeName,
            OutputStream out, OutputFormat format)
            throws SQLException, IOException {
        Writer writer = new OutputStreamWriter(out, StandardCharsets.UTF_8);
        execute(pureSource, query, runtimeName, plan -> writer(plan, format, writer));
        writer.flush();
    }

    /** The route of {@link #execute(String, String, String, OutputStream, OutputFormat)} with the caller's own sink:
     *  the query run on the connection the runtime declares, leased from the server's resolver, the plan sessions
     *  shared, its rows handed to {@code sinkOf}'s sink as they arrive. Returns the plan that ran. */
    public com.legend.executionplan.ExecutionPlan execute(String pureSource, String query, String runtimeName,
            java.util.function.Function<com.legend.executionplan.ExecutionPlan, com.legend.result.ResultSink> sinkOf)
            throws IOException {
        return com.legend.Execution.run(pureSource, query, runtimeName, ConnectionResolver.SOURCE,
                com.legend.exec.PlanSessions.shared(), sinkOf);
    }

    /** The JSON rows of {@link #execute(String, String, String, Connection, OutputStream, OutputFormat)}: written row
     *  batch by row batch, each flushed as it is written, in memory of one batch whatever the result's size. */
    public void stream(String pureSource, String query, String runtimeName,
            Connection connection, OutputStream out)
            throws SQLException, IOException {
        stream(pureSource, query, runtimeName, connection, out, com.legend.result.Batcher.DEFAULT_CAPACITY);
    }

    /** {@link #stream(String, String, String, Connection, OutputStream)} in batches of up to {@code batchRows}: the
     *  stream's granularity, each batch written and flushed as it arrives. */
    public void stream(String pureSource, String query, String runtimeName,
            Connection connection, OutputStream out, int batchRows)
            throws SQLException, IOException {
        Writer writer = new OutputStreamWriter(out, StandardCharsets.UTF_8);
        com.legend.Execution.run(pureSource, query, runtimeName, com.legend.exec.Sessions.given(connection),
                com.legend.exec.PlanSessions.given(connection), plan -> writer(plan, OutputFormat.JSON, writer),
                batchRows);
        writer.flush();
    }

    /** {@link #stream(String, String, String, Connection, OutputStream)} on the connection the runtime declares. */
    public void stream(String pureSource, String query, String runtimeName,
            OutputStream out)
            throws SQLException, IOException {
        execute(pureSource, query, runtimeName, out, OutputFormat.JSON);
    }

    /**
     * Upstream {@code pure/v1/execution/execute} (E8), the {@link com.legend.server.PureV1Api.Runner} the server hands
     * the API: a query that reads runs on its runtime's connection, leased, established first as legend-engine
     * establishes it on every acquisition (its declared test data run on it); a program that writes runs from its plan
     * on the plan's sessions (docs/WRITES_DESIGN_2026_10_18.md §8, leg 7: an in-memory database they share is never
     * changed, the write runs on one of its own; a database a URL reaches keeps it). A database failure is a
     * {@link com.legend.error.DataError}.
     */
    public void executeUpstream(com.legend.TypedQuery query, String runtimeName,
            java.util.function.Function<com.legend.executionplan.ExecutionPlan, com.legend.result.ResultSink> sinkOf)
            throws IOException {
        com.legend.Execution.run(query, runtimeName, ConnectionResolver.SOURCE, com.legend.exec.PlanSessions.shared(),
                com.legend.Execution.Establish.DECLARED_SETUP, sinkOf);
    }

    /** {@code format}'s writer of {@code plan}'s answer onto {@code out}. */
    private static com.legend.result.ResultSink writer(com.legend.executionplan.ExecutionPlan plan,
            OutputFormat format, Writer out) {
        return switch (format) {
            case CSV -> new com.legend.result.CsvWriter(out, plan.answerShape());
            case JSON -> new com.legend.result.JsonRowsWriter(out, plan.answerShape());
        };
    }
}
