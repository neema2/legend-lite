// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.result;

import com.legend.executionplan.ExecutionPlan;

import java.io.IOException;
import java.io.Writer;
import java.util.List;

/**
 * Lite's own CSV answer, written at the edge (docs/RESULT_BOUNDARY_PROGRAM_2026_10_26.md R3, R4; 2026-10-27), RFC 4180:
 * a header of the columns' names as legend-engine labels them, then each row, every line ended by CRLF (a result of no
 * row is its header alone); a cell is its value in Pure's text ({@link CellText#pure}), quoted when it holds a comma,
 * a quote, a line feed or a carriage return (a quote doubled), and a null is empty. A graph fetch's objects have no
 * CSV, refused by name.
 */
public final class CsvWriter implements ResultSink {

    private final Writer out;
    private List<ExecutionPlan.Column> columns = List.of();

    public CsvWriter(Writer out, ExecutionPlan.TdsResult.Shape shape) {
        if (shape == ExecutionPlan.TdsResult.Shape.OBJECTS) {
            throw new com.legend.error.NotImplementedException("a graph fetch's objects have no CSV");
        }
        this.out = out;
    }

    @Override
    public void columns(List<ExecutionPlan.Column> columns) throws IOException {
        this.columns = List.copyOf(columns);
        if (columns.isEmpty()) {
            throw new IllegalStateException("a CSV of a relation of no column");
        }
        StringBuilder header = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            header.append(i == 0 ? "" : ",").append(escape(CellText.presentedName(columns.get(i).name())));
        }
        out.write(header.append("\r\n").toString());
    }

    @Override
    public void batch(Batch batch) throws IOException {
        StringBuilder b = new StringBuilder();
        for (int r = 0; r < batch.size(); r++) {
            for (int i = 0; i < columns.size(); i++) {
                Object cell = batch.get(r, i);
                b.append(i == 0 ? "" : ",").append(cell == null ? "" : escape(CellText.pure(cell, columns.get(i))));
            }
            b.append("\r\n");
        }
        out.write(b.toString());
        out.flush();
    }

    @Override
    public void end() throws IOException {
        out.flush();
    }

    /** RFC 4180's escape: quoted when the text holds a comma, a quote, a line feed or a carriage return, each quote
     *  doubled; else as it is. */
    static String escape(String s) {
        if (s.indexOf(',') < 0 && s.indexOf('"') < 0 && s.indexOf('\n') < 0 && s.indexOf('\r') < 0) {
            return s;
        }
        return '"' + s.replace("\"", "\"\"") + '"';
    }
}
