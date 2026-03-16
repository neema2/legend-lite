package org.finos.legend.engine.execution;

import org.finos.legend.engine.plan.GenericType;
import org.finos.legend.engine.plan.RelationType;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Unified result from executing a Pure query.
 *
 * <p>Each variant carries the {@link GenericType} return type from the plan,
 * enabling consumers to handle results with full type information —
 * no heuristics, no string parsing, no null checks.
 *
 * <p>All variants support {@link #columns()}, {@link #rows()}, {@link #rowCount()},
 * and {@link #toJsonArray()} so callers can treat any result uniformly.
 *
 * <p>GraalVM native-image compatible.
 */
public sealed interface ExecutionResult {

    /** The Pure-level return type of this result. Never null. */
    GenericType returnType();

    /** Column metadata. Every variant provides this. */
    List<Column> columns();

    /** Row data. Every variant provides this. */
    List<Row> rows();

    /** Number of rows. */
    default int rowCount() { return rows().size(); }

    /** Number of columns. */
    default int columnCount() { return columns().size(); }

    /**
     * Serializes this result as a JSON array of objects.
     * Each row becomes a JSON object with column names as keys.
     */
    default String toJsonArray() {
        StringBuilder sb = new StringBuilder("[");
        List<Column> cols = columns();
        List<Row> rowList = rows();

        for (int rowIdx = 0; rowIdx < rowList.size(); rowIdx++) {
            if (rowIdx > 0) sb.append(",");
            sb.append("{");
            Row row = rowList.get(rowIdx);
            for (int colIdx = 0; colIdx < cols.size(); colIdx++) {
                if (colIdx > 0) sb.append(",");
                sb.append("\"").append(escapeJson(cols.get(colIdx).name())).append("\":");
                sb.append(formatJsonValue(row.get(colIdx)));
            }
            sb.append("}");
        }

        sb.append("]");
        return sb.toString();
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

        @Override public ScalarResult asScalar() { return this; }

        @Override public List<Column> columns() {
            return List.of(new Column("value", returnType.typeName(), returnType.typeName()));
        }

        @Override public List<Row> rows() {
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

        @Override public CollectionResult asCollection() { return this; }

        @Override public List<Column> columns() {
            // Element type from List<T> → T
            GenericType elemType = returnType.elementType();
            return List.of(new Column("value", elemType.typeName(), elemType.typeName()));
        }

        @Override public List<Row> rows() {
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
            RelationType schema,
            GenericType returnType
    ) implements ExecutionResult {
        public TabularResult {
            Objects.requireNonNull(columns, "columns must not be null");
            Objects.requireNonNull(rows, "rows must not be null");
            Objects.requireNonNull(returnType, "returnType must not be null");
        }

        @Override public TabularResult asTabular() { return this; }
    }

    /**
     * Graph/tree result from graphFetch: serialized JSON tree.
     */
    record GraphResult(String json, GenericType returnType) implements ExecutionResult {
        public GraphResult {
            Objects.requireNonNull(returnType, "returnType must not be null");
        }

        @Override public GraphResult asGraph() { return this; }

        @Override public List<Column> columns() {
            return List.of(new Column("json", "JSON", "String"));
        }

        @Override public List<Row> rows() {
            return List.of(new Row(List.of(json != null ? json : "")));
        }
    }

    // ===== Factory =====

    /**
     * Builds Column metadata from the compiler's RelationType schema.
     * This is the source of truth for non-pivot queries.
     */
    private static List<Column> columnsFromSchema(RelationType schema) {
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
     */
    private static List<Column> columnsHybrid(
            RelationType schema, ResultSetMetaData meta, int colCount) throws SQLException {
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
            String sep = RelationType.DynamicPivotColumn.SEPARATOR;
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
                    "Pivot column '" + name + "' could not be resolved from compiler schema or DynamicPivotColumn lookup");
        }
        return cols;
    }

    /**
     * Reads a JDBC ResultSet and creates the right ExecutionResult variant
     * based on the compiler-provided returnType. The ResultSet is fully consumed.
     *
     * Column types come from the compiler's GenericType/RelationType — NOT JDBC metadata.
     * Exception: pivot results use JDBC metadata for column names since pivot
     * output columns are data-dependent and unknown at compile time.
     */
    static ExecutionResult fromResultSet(GenericType returnType, ResultSet rs) throws SQLException {
        Objects.requireNonNull(returnType, "returnType must not be null");

        ResultSetMetaData meta = rs.getMetaData();
        int colCount = meta.getColumnCount();

        // Materialize rows (data only — types come from compiler)
        List<Row> rows = new ArrayList<>();
        while (rs.next()) {
            rows.add(Row.fromResultSet(rs, colCount));
        }

        // Build the right variant using compiler types
        return switch (returnType) {
            case GenericType.Parameterized p when p.isList() -> {
                // ONE multiplicity = single row with DuckDB array (list(), fold())
                // MANY or null = N scalar rows (UNNEST'd collection expressions)
                boolean isSingleArray = p.multiplicity() != null && !p.multiplicity().isMany();
                @SuppressWarnings("unchecked")
                List<Object> values = isSingleArray
                        // Single row with array value (list(), fold()) → Row already unwrapped to List
                        ? rows.stream().flatMap(r -> ((List<Object>) r.get(0)).stream()).toList()
                        // N scalar rows (UNNEST'd) → each row is an element
                        : rows.stream().map(r -> r.get(0)).toList();
                yield new CollectionResult(values, returnType);
            }
            case GenericType.Relation r -> {
                List<Column> columns;
                if (r.schema().dynamicPivotColumns().isEmpty()) {
                    // Non-pivot: compiler schema is the complete source of truth
                    columns = columnsFromSchema(r.schema());
                } else {
                    // Pivot: hybrid resolution — static cols from compiler, dynamic cols from JDBC
                    columns = columnsHybrid(r.schema(), meta, colCount);
                }
                yield new TabularResult(columns, rows, r.schema(), returnType);
            }
            // Primitive, EnumType, PrecisionDecimal, ClassType (future: GraphResult)
            default -> {
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
        RelationType schema = new RelationType(java.util.Map.of(), java.util.List.of());
        return new TabularResult(List.of(), List.of(), schema, new GenericType.Relation(schema));
    }

    // ===== JSON helpers =====

    private static String formatJsonValue(Object value) {
        if (value == null) return "null";
        if (value instanceof Boolean) return value.toString();
        if (value instanceof Number) return value.toString();
        return "\"" + escapeJson(value.toString()) + "\"";
    }

    private static String escapeJson(String s) {
        if (s == null) return "";
        StringBuilder sb = new StringBuilder(s.length());
        for (char c : s.toCharArray()) {
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> sb.append(c);
            }
        }
        return sb.toString();
    }
}
