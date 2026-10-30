// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.result;

import com.legend.executionplan.ExecutionPlan;
import com.legend.json.Json;

import java.io.IOException;
import java.io.Writer;
import java.util.List;

/**
 * legend-engine's {@code PURE_TDSOBJECT} serialization (the service test runner's format; the doors, 2026-10-29): a
 * relation as a JSON array of objects, one per row, each keyed by the column's presented name
 * ({@link CellText#presentedName}) with the cell in legend-engine's token ({@link CellText#engine}); a value of one
 * column as the cell itself where its column is to-one ({@code null} for no row), as the array of its cells
 * otherwise. A graph fetch's objects are {@link PureV1Writer#objects}'. Until this writer the runner assembled the
 * same tree in Java from a materialized side; now the body's last statement streams through it as any answer does.
 * A writer: it spells, decides nothing.
 */
public final class TdsObjectsWriter implements ResultSink {

    private final Writer out;
    private final ExecutionPlan.TdsResult.Shape shape;
    private List<ExecutionPlan.Column> columns = List.of();
    private boolean toOne;
    private long rows;

    public TdsObjectsWriter(Writer out, ExecutionPlan.TdsResult.Shape shape) {
        if (shape == ExecutionPlan.TdsResult.Shape.OBJECTS) {
            throw new IllegalArgumentException("a graph fetch's objects are PureV1Writer.objects' to write");
        }
        this.out = out;
        this.shape = shape;
    }

    @Override
    public void columns(List<ExecutionPlan.Column> columns) throws IOException {
        this.columns = List.copyOf(columns);
        if (shape == ExecutionPlan.TdsResult.Shape.VALUE) {
            if (columns.size() != 1) {
                throw new IllegalStateException("a value answers one column; got " + columns.size());
            }
            Integer upper = columns.get(0).multiplicity().upper();
            toOne = upper != null && upper <= 1;
        }
        if (!toOne) {
            out.write('[');
        }
    }

    @Override
    public void batch(Batch batch) throws IOException {
        StringBuilder b = new StringBuilder();
        for (int r = 0; r < batch.size(); r++) {
            if (toOne) {
                if (rows > 0) {
                    throw new IllegalStateException("a to-one value answered more than one row");
                }
                b.append(Json.toCompact(CellText.engine(batch.get(r, 0), columns.get(0))));
            } else if (shape == ExecutionPlan.TdsResult.Shape.VALUE) {
                b.append(rows == 0 ? "" : ",")
                        .append(Json.toCompact(CellText.engine(batch.get(r, 0), columns.get(0))));
            } else {
                b.append(rows == 0 ? "" : ",").append('{');
                for (int i = 0; i < columns.size(); i++) {
                    ExecutionPlan.Column c = columns.get(i);
                    b.append(i == 0 ? "" : ",").append('"').append(Json.escape(CellText.presentedName(c.name())))
                            .append("\":").append(Json.toCompact(CellText.engine(batch.get(r, i), c)));
                }
                b.append('}');
            }
            rows++;
        }
        out.write(b.toString());
        out.flush();
    }

    @Override
    public void end() throws IOException {
        if (toOne) {
            if (rows == 0) {
                out.write("null");
            }
        } else {
            out.write(']');
        }
        out.flush();
    }
}
