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

    default int columnCount() {
        return columns().size();
    }

    /** THE RESULT-ENVELOPE ARITY (One-Platform Plan Phase 3 — the K pin
     * retires into the model): the engine's {@code Result.values} for a
     * relation-rooted query holds ONE TabularDataSet carrier; for a
     * class or scalar root, {@code values} IS the collection
     * ({@code flatSize} — the produced value count, a carriage fact).
     * This is the arity rule the harness's {@code tds ? 1 : size}
     * branch encoded; the MODEL owns it now. */
    default long envelopeCarriers(long flatSize) {
        boolean tds = this instanceof Tabular tb
                && (com.legend.compiler.element.type.Type
                        .schemaView(tb.returnType()) != null
                        || com.legend.compiler.element.type.PlatformTypes
                                .isTdsType(tb.returnType()));
        return tds ? 1L : flatSize;
    }

    // ===== Typed accessors (ported engine surface) — no cast, clear error =====

    default Scalar asScalar() {
        throw new IllegalStateException(
                "Expected Scalar but got " + getClass().getSimpleName());
    }

    default Collection asCollection() {
        throw new IllegalStateException(
                "Expected Collection but got " + getClass().getSimpleName());
    }

    default Tabular asTabular() {
        throw new IllegalStateException(
                "Expected Tabular but got " + getClass().getSimpleName());
    }

    default Graph asGraph() {
        throw new IllegalStateException(
                "Expected Graph but got " + getClass().getSimpleName());
    }

    /** Single scalar value: {@code 1 + 1} → 2. */
    record Scalar(@com.legend.base.Nullable Object value, Type returnType)
            implements ExecutionResult {
        public Scalar {
            Objects.requireNonNull(returnType, "returnType");
        }

        @Override
        public Scalar asScalar() {
            return this;
        }

        @Override
        public List<Column> columns() {
            return List.of(new Column("value", returnType));
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
        public Collection asCollection() {
            return this;
        }

        @Override
        public List<Column> columns() {
            return List.of(new Column("value", returnType));
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

        @Override
        public Tabular asTabular() {
            return this;
        }
    }

    /** The PCT wire render of a relation-rooted query (the {@code pctRender}
     * execute option): the PLAN emitted the TDS text (Lowerer PCT-TDS root
     * mode, PctTdsWrap) and the adapter hands it over verbatim — an option's
     * result, never an ordinary scalar. */
    record TdsText(String text, Type returnType) implements ExecutionResult {
        public TdsText {
            Objects.requireNonNull(text, "text");
            Objects.requireNonNull(returnType, "returnType");
        }

        @Override
        public List<Column> columns() {
            return List.of(new Column("value", returnType));
        }

        @Override
        public List<Row> rows() {
            return List.of(new Row(Collections.singletonList(text)));
        }
    }

    /** Graph result: the json IS a well-formed JSON array built by the database. */
    record Graph(String json, Type returnType) implements ExecutionResult {
        public Graph {
            Objects.requireNonNull(returnType, "returnType");
        }

        @Override
        public Graph asGraph() {
            return this;
        }

        @Override
        public List<Column> columns() {
            return List.of(new Column("json", Type.Primitive.STRING));
        }

        @Override
        public List<Row> rows() {
            return List.of(new Row(List.of(json != null ? json : "")));
        }
    }
}
