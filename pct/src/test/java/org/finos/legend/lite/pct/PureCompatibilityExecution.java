package org.finos.legend.lite.pct;

import org.finos.legend.engine.execution.BufferedResult;
import org.finos.legend.engine.server.QueryService;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Executes Pure code through Legend-Lite's QueryService for PCT validation.
 * 
 * This class bridges between legend-engine's PCT framework and legend-lite's
 * execution engine. It takes Pure grammar text, executes it through
 * QueryService,
 * and returns results in a format the PCT framework can validate.
 */
public class PureCompatibilityExecution {

    private final QueryService queryService;

    public PureCompatibilityExecution() {
        this.queryService = new QueryService();
    }

    /**
     * Result of executing Pure code through Legend-Lite.
     */
    public static class ExecutionResult {
        private final boolean success;
        private final List<Map<String, Object>> rows;
        private final List<String> columns;
        private final String error;

        private ExecutionResult(boolean success, List<String> columns,
                List<Map<String, Object>> rows, String error) {
            this.success = success;
            this.columns = columns;
            this.rows = rows;
            this.error = error;
        }

        public static ExecutionResult success(List<String> columns, List<Map<String, Object>> rows) {
            return new ExecutionResult(true, columns, rows, null);
        }

        public static ExecutionResult failure(String error) {
            return new ExecutionResult(false, List.of(), List.of(), error);
        }

        public boolean isSuccess() {
            return success;
        }

        public List<Map<String, Object>> getRows() {
            return rows;
        }

        public List<String> getColumns() {
            return columns;
        }

        public String getError() {
            return error;
        }

        public int getRowCount() {
            return rows.size();
        }

        public Object getValue(int row, String column) {
            if (row >= rows.size())
                return null;
            return rows.get(row).get(column);
        }
    }

    /**
     * Executes a complete Pure source file with embedded query.
     * 
     * @param pureSource  The full Pure source with model, mapping, runtime, and
     *                    query
     * @param query       The Pure expression to execute
     * @param runtimeName The qualified runtime name
     * @return ExecutionResult with rows/columns or error message
     */
    public ExecutionResult execute(String pureSource, String query, String runtimeName) {
        try {
            BufferedResult result = queryService.execute(pureSource, query, runtimeName);
            return convertResult(result);
        } catch (SQLException e) {
            return ExecutionResult.failure("SQL Error: " + e.getMessage());
        } catch (Exception e) {
            return ExecutionResult.failure("Execution Error: " + e.getMessage());
        }
    }

    /**
     * Compiles Pure code without executing (for validation).
     * 
     * @param pureSource  The full Pure source
     * @param query       The Pure expression
     * @param runtimeName The runtime name
     * @return true if compilation succeeds
     */
    public boolean compile(String pureSource, String query, String runtimeName) {
        try {
            queryService.compile(pureSource, query, runtimeName);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Executes a simple Pure expression (for testing basic functionality).
     * Uses a minimal in-memory model.
     */
    public ExecutionResult executeSimple(String pureExpression) {
        String minimalSource = """
                ###Pure
                Class test::TestClass {
                    value: Integer[1];
                }

                ###Mapping
                Mapping test::TestMapping ()

                ###Connection
                test::TestConnection {
                    store: test::TestStore;
                    specification: DuckDB { };
                }

                ###Runtime
                test::TestRuntime {
                    mappings: [test::TestMapping];
                    connections: [test::TestConnection];
                }
                """;

        return execute(minimalSource, pureExpression, "test::TestRuntime");
    }

    private ExecutionResult convertResult(BufferedResult result) {
        List<String> columnNames = new ArrayList<>();
        for (var col : result.columns()) {
            columnNames.add(col.name());
        }

        List<Map<String, Object>> rows = new ArrayList<>();
        for (var row : result.rows()) {
            Map<String, Object> rowMap = new HashMap<>();
            List<Object> values = row.values();
            for (int i = 0; i < columnNames.size() && i < values.size(); i++) {
                rowMap.put(columnNames.get(i), values.get(i));
            }
            rows.add(rowMap);
        }

        return ExecutionResult.success(columnNames, rows);
    }
}
