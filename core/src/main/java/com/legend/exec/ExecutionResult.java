package com.legend.exec;

import com.legend.compiler.element.type.Type;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * The typed execution result — a sealed quartet, one variant per
 * {@link ResultShape}. Representation-identical to the engine module's
 * result types by design (PHASE_K_EXECUTION.md): the bridge re-wraps
 * field-by-field and owns no rules.
 */
public sealed interface ExecutionResult {

    /** The Pure-level result type. Never null. */
    Type returnType();

    List<Column> columns();

    List<Row> rows();

    default int rowCount() {
        return rows().size();
    }

    /** Single scalar value: {@code 1 + 1} → 2. */
    record Scalar(Object value, Type returnType) implements ExecutionResult {
        public Scalar {
            Objects.requireNonNull(returnType, "returnType");
        }

        @Override
        public List<Column> columns() {
            return List.of(new Column("value", null, returnType.typeName()));
        }

        @Override
        public List<Row> rows() {
            return List.of(new Row(Collections.singletonList(value)));
        }
    }

    /** A flat collection: N rows × 1 column flattened; {@code returnType} is the ELEMENT type. */
    record Collection(List<Object> values, Type returnType) implements ExecutionResult {
        public Collection {
            Objects.requireNonNull(values, "values");
            Objects.requireNonNull(returnType, "returnType");
        }

        @Override
        public List<Column> columns() {
            return List.of(new Column("value", null, returnType.typeName()));
        }

        @Override
        public List<Row> rows() {
            return values.stream().map(v -> new Row(Collections.singletonList(v))).toList();
        }
    }

    /** Tabular result: full rows × typed columns (the relation surface). */
    record Tabular(List<Column> columns, List<Row> rows, Type returnType)
            implements ExecutionResult {
        public Tabular {
            Objects.requireNonNull(columns, "columns");
            Objects.requireNonNull(rows, "rows");
            Objects.requireNonNull(returnType, "returnType");
        }
    }

    /** Graph result: the json IS a well-formed JSON array built by the database. */
    record Graph(String json, Type returnType) implements ExecutionResult {
        public Graph {
            Objects.requireNonNull(returnType, "returnType");
        }

        @Override
        public List<Column> columns() {
            return List.of(new Column("json", "JSON", "String"));
        }

        @Override
        public List<Row> rows() {
            return List.of(new Row(List.of(json != null ? json : "")));
        }
    }
}
