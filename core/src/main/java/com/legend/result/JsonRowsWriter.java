// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.result;

import com.legend.executionplan.ExecutionPlan;
import com.legend.json.Json;

import java.io.IOException;
import java.io.Writer;
import java.util.List;

/**
 * Lite's own JSON answer, written at the edge (docs/RESULT_BOUNDARY_PROGRAM_2026_10_26.md R3, R4; 2026-10-27): a JSON
 * array of one object per row, each member a column's name as legend-engine labels it and its cell's token in Pure's
 * text ({@link CellText#pureJson}); a value's elements as the one-column relation {@code value}; a graph fetch's
 * objects as the array of the objects the database built, each passed on as its text. Each batch is written and
 * flushed as it arrives, so a stream of rows reaches its reader row by row; the array's punctuation is the writer's.
 */
public final class JsonRowsWriter implements ResultSink {

    private final Writer out;
    private final ExecutionPlan.TdsResult.Shape shape;
    private List<ExecutionPlan.Column> columns = List.of();
    private boolean first = true;

    public JsonRowsWriter(Writer out, ExecutionPlan.TdsResult.Shape shape) {
        this.out = out;
        this.shape = shape;
    }

    @Override
    public void columns(List<ExecutionPlan.Column> columns) throws IOException {
        this.columns = List.copyOf(columns);
        out.write('[');
    }

    @Override
    public void batch(Batch batch) throws IOException {
        StringBuilder b = new StringBuilder();
        for (int r = 0; r < batch.size(); r++) {
            if (!first) {
                b.append(',');
            }
            first = false;
            if (shape == ExecutionPlan.TdsResult.Shape.OBJECTS) {
                // the object as the database built it: its JSON text, passed on
                Object object = batch.get(r, 0);
                if (!(object instanceof String text)) {
                    throw new IllegalStateException("a graph fetch's row holds no object's JSON text: " + object);
                }
                b.append(text);
                continue;
            }
            b.append('{');
            for (int i = 0; i < columns.size(); i++) {
                ExecutionPlan.Column c = columns.get(i);
                b.append(i == 0 ? "" : ",").append('"').append(Json.escape(CellText.presentedName(c.name())))
                        .append("\":").append(Json.toCompact(CellText.pureJson(batch.get(r, i), c)));
            }
            b.append('}');
        }
        out.write(b.toString());
        out.flush();
    }

    @Override
    public void end() throws IOException {
        out.write(']');
        out.flush();
    }
}
