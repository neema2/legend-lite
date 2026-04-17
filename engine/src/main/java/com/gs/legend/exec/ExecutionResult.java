package com.gs.legend.exec;

import com.gs.legend.plan.GenericType;
import com.gs.legend.util.Json;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Unified result from executing a Pure query.
 *
 * <p>
 * Each variant carries the {@link GenericType} return type from the plan,
 * enabling consumers to handle results with full type information —
 * no heuristics, no string parsing, no null checks.
 *
 * <p>
 * All variants support {@link #columns()}, {@link #rows()},
 * {@link #rowCount()},
 * and {@link #toJsonArray()} so callers can treat any result uniformly.
 *
 * <p>
 * GraalVM native-image compatible.
 */
public sealed interface ExecutionResult {

    /** The Pure-level return type of this result. Never null. */
    GenericType returnType();

    /** Column metadata. Every variant provides this. */
    List<Column> columns();

    /** Row data. Every variant provides this. */
    List<Row> rows();

    /** Number of rows. */
    default int rowCount() {
        return rows().size();
    }

    /** Number of columns. */
    default int columnCount() {
        return columns().size();
    }

    /**
     * Serializes this result as a JSON array of objects.
     * Each row becomes a JSON object with column names as keys.
     *
     * <p>Uses {@link Json.Writer} internally so escaping is RFC 8259 compliant
     * (the previous implementation missed {@code \b}, {@code \f}, and {@code \}u00xx
     * control-char escapes).
     */
    default String toJsonArray() {
        Json.Writer w = Json.compactWriter();
        w.beginArray();
        List<Column> cols = columns();
        for (Row row : rows()) {
            w.beginObject();
            for (int colIdx = 0; colIdx < cols.size(); colIdx++) {
                w.name(cols.get(colIdx).name());
                writeJsonValue(w, row.get(colIdx));
            }
            w.endObject();
        }
        w.endArray();
        return w.toString();
    }

    /**
     * Emit one cell value into a {@link Json.Writer}. Handles nulls,
     * booleans, integral and floating-point numbers, and other JDBC types
     * (Timestamp, LocalDate, byte[], …) by calling {@code toString()}.
     *
     * <p>Package-private so {@link PlanExecutor#streamJson} can emit values
     * lazily from a JDBC ResultSet without materializing rows.
     */
    static void writeJsonValue(Json.Writer w, Object v) {
        if (v == null) {
            w.writeNull();
        } else if (v instanceof Boolean b) {
            w.writeBool(b);
        } else if (v instanceof Number n) {
            if (n instanceof Long || n instanceof Integer || n instanceof Short || n instanceof Byte) {
                w.writeLong(n.longValue());
            } else {
                w.writeDouble(n.doubleValue());
            }
        } else {
            w.writeString(v.toString());
        }
    }

    // ===== Typed accessors — no cast, clear error =====

    default ScalarResult asScalar() {
        throw new IllegalStateException(
                "Expected ScalarResult but got " + getClass().getSimpleName());
    }

    default CollectionResult asCollection() {
        throw new IllegalStateException(
                "Expected CollectionResult but got " + getClass().getSimpleName());
    }

    default TabularResult asTabular() {
        throw new IllegalStateException(
                "Expected TabularResult but got " + getClass().getSimpleName());
    }

    default GraphResult asGraph() {
        throw new IllegalStateException(
                "Expected GraphResult but got " + getClass().getSimpleName());
    }

    // ===== Result variants =====

    /**
     * Single scalar value: |1+1 → Integer, |'hello' → String.
     */
    record ScalarResult(Object value, GenericType returnType) implements ExecutionResult {
        public ScalarResult {
            Objects.requireNonNull(returnType, "returnType must not be null");
        }

        @Override
        public ScalarResult asScalar() {
            return this;
        }

        @Override
        public List<Column> columns() {
            return List.of(new Column("value", returnType.typeName(), returnType.typeName()));
        }

        @Override
        public List<Row> rows() {
            return List.of(new Row(java.util.Collections.singletonList(value)));
        }
    }

    /**
     * Collection of values: filter(...).legalName → String[*].
     * SQL returns N rows × 1 column; values are extracted into a flat list.
     */
    record CollectionResult(List<Object> values, GenericType returnType) implements ExecutionResult {
        public CollectionResult {
            Objects.requireNonNull(values, "values must not be null");
            Objects.requireNonNull(returnType, "returnType must not be null");
        }

        @Override
        public CollectionResult asCollection() {
            return this;
        }

        @Override
        public List<Column> columns() {
            // Return type is already the element type (e.g., Integer for Integer[*])
            return List.of(new Column("value", returnType.typeName(), returnType.typeName()));
        }

        @Override
        public List<Row> rows() {
            return values.stream()
                    .map(v -> new Row(java.util.Collections.singletonList(v)))
                    .toList();
        }
    }

    /**
     * Tabular/relational result: Person.all()->project(...) → Relation<schema>.
     * Full rows × columns with schema metadata from the compiler.
     */
    record TabularResult(
            List<Column> columns,
            List<Row> rows,
            GenericType.Relation.Schema schema,
            GenericType returnType) implements ExecutionResult {
        public TabularResult {
            Objects.requireNonNull(columns, "columns must not be null");
            Objects.requireNonNull(rows, "rows must not be null");
            Objects.requireNonNull(returnType, "returnType must not be null");
        }

        @Override
        public TabularResult asTabular() {
            return this;
        }
    }

    /**
     * Graph/tree result from graphFetch: serialized JSON tree.
     */
    record GraphResult(String json, GenericType returnType) implements ExecutionResult {
        public GraphResult {
            Objects.requireNonNull(returnType, "returnType must not be null");
        }

        @Override
        public GraphResult asGraph() {
            return this;
        }

        @Override
        public List<Column> columns() {
            return List.of(new Column("json", "JSON", "String"));
        }

        @Override
        public List<Row> rows() {
            return List.of(new Row(List.of(json != null ? json : "")));
        }

        /**
         * The {@code json} field is already a well-formed JSON array (built by the DB
         * via {@code json_group_array(json_object(...))} for snapshot-mode graph plans).
         * Return it verbatim instead of the default {@code columns()/rows()} loop which
         * would double-wrap it into {@code [{"json":"<escaped>"}]}.
         */
        @Override
        public String toJsonArray() {
            return json != null ? json : "[]";
        }
    }

    // ===== Factory =====

    /**
     * Builds Column metadata from the compiler's GenericType.Relation.Schema schema.
     * This is the source of truth for non-pivot queries.
     *
     * <p>Package-private so {@link PlanExecutor#streamJson} can resolve columns
     * without materializing an ExecutionResult.
     */
    static List<Column> columnsFromSchema(GenericType.Relation.Schema schema) {
        List<Column> cols = new ArrayList<>(schema.size());
        for (var entry : schema.columns().entrySet()) {
            String typeName = entry.getValue().typeName();
            cols.add(new Column(entry.getKey(), typeName, typeName));
        }
        return cols;
    }

    /**
     * Hybrid column resolution for pivot results.
     * Static group-by columns use compiler types; dynamic pivot columns
     * (whose names are data-dependent) are matched by SEPARATOR suffix
     * to compiler-derived aggregate return types.
     *
     * <p>Package-private so {@link PlanExecutor#streamJson} can resolve columns
     * without materializing an ExecutionResult.
     */
    static List<Column> columnsHybrid(
            GenericType.Relation.Schema schema, ResultSetMetaData meta, int colCount) throws SQLException {
        // Build lookup: alias suffix → Pure return type
        var dynamicLookup = new java.util.HashMap<String, GenericType>();
        for (var dpc : schema.dynamicPivotColumns()) {
            dynamicLookup.put(dpc.aliasSuffix(), dpc.returnType());
        }

        List<Column> cols = new ArrayList<>(colCount);
        for (int i = 1; i <= colCount; i++) {
            String name = meta.getColumnLabel(i);

            // Try compiler schema first (static group-by columns)
            GenericType compilerType = schema.getColumnType(name);
            if (compilerType != null) {
                String typeName = compilerType.typeName();
                cols.add(new Column(name, typeName, typeName));
                continue;
            }

            // Dynamic pivot column: parse suffix after SEPARATOR
            String sep = GenericType.Relation.Schema.DynamicPivotColumn.SEPARATOR;
            int sepIdx = name.lastIndexOf(sep);
            if (sepIdx >= 0) {
                String suffix = name.substring(sepIdx + sep.length());
                GenericType aggType = dynamicLookup.get(suffix);
                if (aggType != null) {
                    String typeName = aggType.typeName();
                    cols.add(new Column(name, typeName, typeName));
                    continue;
                }
            }

            throw new IllegalStateException(
                    "Pivot column '" + name
                            + "' could not be resolved from compiler schema or DynamicPivotColumn lookup");
        }
        return cols;
    }

    /**
     * Reads a JDBC ResultSet and creates the right ExecutionResult variant
     * based on the plan's {@link com.gs.legend.plan.ResultFormat}.
     *
     * <p>Dispatch is format-based (set by PlanGenerator), not type-based
     * (set by Compiler). Column types still come from the compiler's schema.
     * Exception: pivot results use JDBC metadata for column names since pivot
     * output columns are data-dependent and unknown at compile time.
     */
    static ExecutionResult fromResultSet(com.gs.legend.plan.SingleExecutionPlan plan, ResultSet rs)
            throws SQLException {
        var exprType = plan.expressionType();
        java.util.Objects.requireNonNull(exprType, "exprType must not be null");

        GenericType returnType = exprType.type();
        ResultSetMetaData meta = rs.getMetaData();
        int colCount = meta.getColumnCount();

        // Materialize rows (data only — types come from compiler)
        List<Row> rows = new ArrayList<>();
        while (rs.next()) {
            rows.add(Row.fromResultSet(rs, colCount));
        }

        return switch (plan.resultFormat()) {
            case com.gs.legend.plan.ResultFormat.Graph g -> {
                // JSON-wrapped class instances or serialize output — single JSON string.
                String json = rows.isEmpty() ? "[]"
                        : (rows.get(0).get(0) != null ? rows.get(0).get(0).toString() : "[]");
                yield new GraphResult(json, returnType);
            }
            case com.gs.legend.plan.ResultFormat.Tabular t -> {
                if (!(returnType instanceof GenericType.Relation r)) {
                    throw new IllegalStateException(
                            "Tabular format requires Relation type — got " + returnType);
                }
                List<Column> columns;
                if (r.schema().dynamicPivotColumns().isEmpty()) {
                    columns = columnsFromSchema(r.schema());
                } else {
                    columns = columnsHybrid(r.schema(), meta, colCount);
                }
                yield new TabularResult(columns, rows, r.schema(), returnType);
            }
            case com.gs.legend.plan.ResultFormat.Scalar s -> {
                if (exprType.isMany()) {
                    List<Object> values = rows.stream().map(r -> r.get(0)).toList();
                    yield new CollectionResult(values, returnType);
                }
                Object value = rows.isEmpty() ? null : rows.get(0).get(0);
                yield new ScalarResult(value, returnType);
            }
        };
    }

    /**
     * Returns an empty result (no columns, no rows).
     * Use for DDL/DML operations that don't return a result set.
     */
    static ExecutionResult empty() {
        GenericType.Relation.Schema schema = GenericType.Relation.Schema.empty();
        return new TabularResult(List.of(), List.of(), schema, new GenericType.Relation(schema));
    }

}
