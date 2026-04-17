package com.gs.legend.exec;

import com.gs.legend.plan.GenericType;
import com.gs.legend.plan.ResultFormat;
import com.gs.legend.plan.SingleExecutionPlan;
import com.gs.legend.util.Json;

import java.io.IOException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * Executes a {@link SingleExecutionPlan} against a JDBC connection.
 *
 * <p>The plan's SQL is fully self-contained — JSON sources are inlined
 * as VARIANT subqueries in the FROM clause. No temp tables, no parameters.
 *
 * <pre>
 * var result = PlanExecutor.execute(plan, connection);
 * var tabular = result.asTabular();
 * </pre>
 *
 * <p>Two execution modes:
 * <ul>
 *   <li>{@link #execute} — materializes the full JDBC ResultSet into an
 *       {@link ExecutionResult} record. Typed access, all rows in memory.</li>
 *   <li>{@link #streamJson} — for Tabular and Graph plans, iterates the JDBC
 *       ResultSet lazily and writes JSON directly to a {@link Writer}. Rows
 *       are emitted as they are fetched; no materialization. This is the
 *       streaming path used for large query results over HTTP or for
 *       memory-bounded consumers. Graph plans must be generated with
 *       {@link com.gs.legend.plan.PlanGenerator.Mode#STREAMING} for the
 *       streaming path to produce the expected flat SQL.</li>
 * </ul>
 */
public class PlanExecutor {

    public static ExecutionResult execute(SingleExecutionPlan plan, Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(plan.sql())) {
            return ExecutionResult.fromResultSet(plan, rs);
        }
    }

    /**
     * Stream the plan's result as a JSON array directly to a {@link Writer},
     * iterating the JDBC ResultSet lazily.
     *
     * <p>For {@link ResultFormat.Tabular} plans, rows are emitted one at a
     * time to {@code out} as they arrive from JDBC — no {@code List<Row>} is
     * ever built. The total memory footprint is O(one row) regardless of the
     * result size, making this safe for arbitrarily large tables.
     *
     * <p>For {@link ResultFormat.Graph} plans, each JDBC row carries a pre-built
     * {@code json_object(...)} string (the plan was generated with
     * {@code PlanGenerator.Mode.STREAMING} — see {@link #streamGraph}). Each
     * row's JSON is written verbatim separated by commas inside an enclosing
     * array. Memory stays at O(one row).
     *
     * <p>For {@link ResultFormat.Scalar} and {@code CollectionResult} cases
     * the result is already bounded in size (a single value or a small list),
     * so this method falls back to the materialized path and writes the
     * resulting {@code toJsonArray()} string.
     *
     * @param plan the execution plan
     * @param conn the JDBC connection
     * @param out  the Writer to stream into; NOT closed by this method
     */
    public static void streamJson(SingleExecutionPlan plan, Connection conn, Writer out)
            throws SQLException, IOException {
        if (plan.resultFormat() instanceof ResultFormat.Tabular) {
            streamTabular(plan, conn, out);
        } else if (plan.resultFormat() instanceof ResultFormat.Graph) {
            streamGraph(plan, conn, out);
        } else {
            // Scalar / Collection: bounded size, materialize is fine.
            ExecutionResult result = execute(plan, conn);
            out.write(result.toJsonArray());
        }
    }

    private static void streamTabular(SingleExecutionPlan plan, Connection conn, Writer out)
            throws SQLException, IOException {
        GenericType returnType = plan.expressionType().type();
        if (!(returnType instanceof GenericType.Relation rel)) {
            throw new IllegalStateException(
                    "Tabular format requires Relation type — got " + returnType);
        }
        GenericType.Relation.Schema schema = rel.schema();

        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(plan.sql())) {
            ResultSetMetaData meta = rs.getMetaData();
            int colCount = meta.getColumnCount();

            // Column resolution identical to the materialized path — compiler schema
            // for static columns, hybrid lookup for pivot results.
            List<Column> columns = schema.dynamicPivotColumns().isEmpty()
                    ? ExecutionResult.columnsFromSchema(schema)
                    : ExecutionResult.columnsHybrid(schema, meta, colCount);

            Json.Writer w = Json.compactWriter(out);
            w.beginArray();
            while (rs.next()) {
                w.beginObject();
                for (int i = 0; i < columns.size(); i++) {
                    w.name(columns.get(i).name());
                    ExecutionResult.writeJsonValue(w, rs.getObject(i + 1));
                }
                w.endObject();
                // Flush hint: encourage intermediate buffers (OutputStreamWriter,
                // HttpExchange, ...) to release bytes to the downstream reader as
                // each row completes. Safe no-op for unbuffered writers.
                out.flush();
            }
            w.endArray();
            out.flush();
        }
    }

    /**
     * Stream a Graph plan (graphFetch / bare ClassType) as a JSON array of
     * pre-built objects.
     *
     * <p>Requires the plan to have been generated with
     * {@link com.gs.legend.plan.PlanGenerator.Mode#STREAMING} so the SQL emits
     * one {@code json_object(...)} per JDBC row (instead of DB-side aggregating
     * into a single row). Each row's first column is a complete, well-formed
     * JSON object — the engine concatenates them with {@code ','} separators
     * inside {@code '['...']'} without parsing, re-escaping, or buffering.
     *
     * <p>Memory stays at O(one row) regardless of result size — Java never
     * holds more than a single row's JSON string at a time.
     */
    private static void streamGraph(SingleExecutionPlan plan, Connection conn, Writer out)
            throws SQLException, IOException {
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(plan.sql())) {
            out.write('[');
            out.flush();
            boolean first = true;
            while (rs.next()) {
                if (!first) {
                    out.write(',');
                }
                first = false;
                String rowJson = rs.getString(1);
                // NULL-valued row: emit JSON null to keep the array well-formed.
                out.write(rowJson != null ? rowJson : "null");
                // Flush per row so downstream buffers (OutputStreamWriter,
                // HttpExchange, sockets) release bytes as rows arrive — this is
                // what makes the path observably streaming end-to-end.
                out.flush();
            }
            out.write(']');
            out.flush();
        }
    }
}
