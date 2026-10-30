// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.result;

import com.legend.executionplan.ExecutionPlan;
import com.legend.json.Json;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

/**
 * legend-engine's {@code pure/v1} execute answer, written at the edge (docs/RESULT_BOUNDARY_PROGRAM_2026_10_26.md, the
 * LEGACY writer of D6; 2026-10-27): a relation as the engine's TDS result, byte for byte in its streaming serializer's
 * layout -- {@code builder} and {@code activities} as the caller made them, then the columns' names and each row's
 * {@code values} in the engine's tokens ({@link CellText#engine}); a graph fetch's objects as the engine's JSON
 * result, the array or the one object bare. The rows stream as they arrive; nothing is buffered but a graph fetch's
 * first object (whether it is alone decides the layout).
 */
public final class PureV1Writer implements ResultSink {

    /** A relation's envelope: the TDS builder's JSON (each column's name, Pure type and relational spelling), the
     *  activities' JSON (the relational activity carrying the SQL), and the columns' names in the engine's order, the
     *  order each row's values are written in. */
    public record Envelope(String builder, String activities, List<String> names) {
        public Envelope {
            names = List.copyOf(names);
        }
    }

    private final Writer out;
    private final ExecutionPlan.TdsResult.Shape shape;
    private final java.util.function.Function<List<ExecutionPlan.Column>, Envelope> envelope;
    private List<ExecutionPlan.Column> columns = List.of();
    private int[] order = new int[0];
    private long rows;
    private @com.legend.base.Nullable String firstObject;

    private PureV1Writer(Writer out, ExecutionPlan.TdsResult.Shape shape,
            java.util.function.Function<List<ExecutionPlan.Column>, Envelope> envelope) {
        this.out = out;
        this.shape = shape;
        this.envelope = envelope;
    }

    /** A relation's answer, its envelope made from the columns the stream announces (a dynamic pivot's columns are
     *  known only once its keys are read). */
    public static PureV1Writer relation(Writer out,
            java.util.function.Function<List<ExecutionPlan.Column>, Envelope> envelope) {
        return new PureV1Writer(out, ExecutionPlan.TdsResult.Shape.RELATION, envelope);
    }

    /** A graph fetch's answer: the objects, no builder of columns. */
    public static PureV1Writer objects(Writer out) {
        return new PureV1Writer(out, ExecutionPlan.TdsResult.Shape.OBJECTS, columns -> {
            throw new IllegalStateException("not reached: a graph fetch's answer has no envelope of columns");
        });
    }

    @Override
    public void columns(List<ExecutionPlan.Column> columns) throws IOException {
        this.columns = List.copyOf(columns);
        if (shape == ExecutionPlan.TdsResult.Shape.OBJECTS) {
            return;
        }
        Envelope e = envelope.apply(this.columns);
        // each row's values in the engine's column order, read from the plan's columns by the name the engine labels
        // each with (CellText.presentedName: a pivot's generated column quoted)
        List<String> names = e.names();
        order = new int[names.size()];
        for (int i = 0; i < names.size(); i++) {
            order[i] = -1;
            for (int j = 0; j < columns.size(); j++) {
                if (CellText.presentedName(columns.get(j).name()).equals(names.get(i))) {
                    order[i] = j;
                }
            }
            if (order[i] < 0) {
                throw new IllegalStateException("the engine's column '" + names.get(i) + "' is none of the plan's "
                        + columns.stream().map(ExecutionPlan.Column::name).toList());
            }
        }
        out.write("{\"builder\": " + e.builder() + ", \"activities\": " + e.activities()
                + ", \"result\" : {\"columns\" : " + Json.toCompact(names) + ", \"rows\" : [");
    }

    @Override
    public void batch(Batch batch) throws IOException {
        if (shape == ExecutionPlan.TdsResult.Shape.OBJECTS) {
            for (int r = 0; r < batch.size(); r++) {
                if (!(batch.get(r, 0) instanceof String object)) {
                    throw new IllegalStateException("a graph fetch's row holds no object's JSON text: " + batch.get(r, 0));
                }
                if (rows == 0) {
                    firstObject = object;
                } else {
                    if (rows == 1) {
                        out.write("{\"builder\":{\"_type\":\"json\"},\"values\":[" + firstObject);
                    }
                    out.write("," + object);
                }
                rows++;
            }
            out.flush();
            return;
        }
        StringBuilder b = new StringBuilder();
        for (int r = 0; r < batch.size(); r++) {
            List<Json.Node> values = new ArrayList<>(order.length);
            for (int i : order) {
                values.add(CellText.engine(batch.get(r, i), columns.get(i)));
            }
            b.append(rows == 0 ? "" : ",").append("{\"values\": ").append(Json.toCompact(values)).append('}');
            rows++;
        }
        out.write(b.toString());
        out.flush();
    }

    @Override
    public void end() throws IOException {
        if (shape == ExecutionPlan.TdsResult.Shape.OBJECTS) {
            if (rows == 0) {
                out.write("{\"builder\":{\"_type\":\"json\"},\"values\":[]}");
            } else if (rows == 1) {
                // one object, bare (as Pure's serialize writes a one-element collection)
                out.write("{\"builder\":{\"_type\":\"json\"},\"values\":"
                        + Json.toCompact(Json.parse(java.util.Objects.requireNonNull(firstObject))) + "}");
            } else {
                out.write("]}");
            }
            out.flush();
            return;
        }
        out.write("]}}");
        out.flush();
    }
}
