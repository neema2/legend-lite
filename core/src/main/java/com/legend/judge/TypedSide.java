package com.legend.judge;

import com.legend.compiler.element.type.Type;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * THE JUDGES' COLLECTED SIDE of a statement (the result boundary program's track D, 2026-10-29; the old door's
 * {@code ExecutionResult} until then, whose other role -- the product's materialized answer -- is gone: a product
 * answer is a plan, written at the boundary): a sealed quartet, one variant per {@link ResultShape}, which the body
 * driver's judges build to compare an assert's two sides and the SQL-text referee verifies; a side the driver holds at
 * a body's end is written to the caller's sink as host rows ({@code JudgePlans.host}, door 1, 2026-10-29). Never what
 * a caller of the product receives.
 */
public sealed interface TypedSide {

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

    /** Single scalar value: {@code 1 + 1} → 2. */
    record Scalar(@com.legend.base.Nullable Object value, Type returnType)
            implements TypedSide {
        public Scalar {
            Objects.requireNonNull(returnType, "returnType");
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
    record Collection(List<Object> values, Type returnType) implements TypedSide {
        public Collection {
            Objects.requireNonNull(values, "values");
            Objects.requireNonNull(returnType, "returnType");
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
            implements TypedSide {
        public Tabular {
            Objects.requireNonNull(columns, "columns");
            Objects.requireNonNull(rows, "rows");
            Objects.requireNonNull(returnType, "returnType");
        }
    }

    /** Graph result: the json IS a well-formed JSON array built by the database. */
    record Graph(String json, Type returnType) implements TypedSide {
        public Graph {
            Objects.requireNonNull(returnType, "returnType");
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
