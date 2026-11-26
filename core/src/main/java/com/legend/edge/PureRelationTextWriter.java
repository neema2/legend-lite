// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.executionplan.ExecutionPlan;
import com.legend.result.Batch;
import com.legend.result.ResultSink;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * A RELATION'S ROWS PRINTED AS LEGEND-PURE'S TEXT, A RUN'S RECEIVER (stage B's B2, B2b S1, 2026-11-24): the typed rows a
 * plan's run streams -- the relation a program's last {@code toString()}/{@code toCSV()} printed, that call removed by
 * the door, or since the edition program's step 8 (2026-11-26) the one value a last {@code toString()} printed
 * ({@link Form#VALUE}, {@link PureValueText}) -- collected in their order and, at the end, written by
 * {@link PureRelationText} in one of its forms and handed to {@code receiver} as one text. What the program computes
 * over that text afterwards (a print's tail) is not this writer's: the door that lifted the print applies it
 * ({@code PrintedRelations.Lifted.answer}; the independent audit of step 8's slice 2, 2026-11-26). The text exists only
 * once the result is whole (a header names columns before any row, and a CSV's last newline follows the last row), so
 * the rows are held, as legend-engine's own TDS holds them. A writer: it spells, decides nothing, and holds no state
 * between runs (one instance per run).
 */
public final class PureRelationTextWriter implements ResultSink {

    /** Which of legend-pure's texts of a relation the program asked for. */
    public enum Form {
        /** {@code toString(rel)}: the {@code #TDS} text. */
        TO_STRING,
        /** legend-engine's {@code toCSV(tds)}. */
        TO_CSV,
        /** legend-engine's {@code toCSV(tds, true)}: an empty cell {@code TDSNull}. */
        TO_CSV_TDS_NULL,
        /** a TDS's {@code csv} property. */
        TDS_CSV,
        /** a value's {@code toString} (the edition program's step 8, 2026-11-26): the answer's one cell, legend-pure's
         *  print of the value ({@link PureValueText#value}). */
        VALUE
    }

    private final Form form;
    private final Consumer<String> receiver;
    private @com.legend.base.Nullable List<ExecutionPlan.Column> columns;
    private final List<List<Object>> rows = new ArrayList<>();
    private boolean ended;

    /** {@code receiver} is handed the text, and the text alone: what a program computes over it after the print (a
     *  print's tail, {@link PrintTail}) is the door's to apply, never this writer's ({@link PrintedRelations.Lifted#answer},
     *  {@link PrintedRelations.Side#answer}; the independent audit of step 8's slice 2, 2026-11-26: a writer writes). */
    public PureRelationTextWriter(Form form, Consumer<String> receiver) {
        this.form = java.util.Objects.requireNonNull(form, "form");
        this.receiver = java.util.Objects.requireNonNull(receiver, "receiver");
    }

    /** A relation's rows answer this receiver, or a value under {@link Form#VALUE}; any other answer is refused by
     *  name before any statement is sent. */
    @Override
    public void answers(ExecutionPlan.TdsResult.Shape shape) {
        ExecutionPlan.TdsResult.Shape expected = form == Form.VALUE ? ExecutionPlan.TdsResult.Shape.VALUE
                : ExecutionPlan.TdsResult.Shape.RELATION;
        if (shape != expected) {
            throw new IllegalStateException("legend-pure's " + form + " text is of " + (form == Form.VALUE
                    ? "a value" : "a relation's rows") + "; the answer is " + shape);
        }
    }

    /** The columns, each one the printer writes, or the run refused by name before any row is read. */
    @Override
    public void columns(List<ExecutionPlan.Column> columns) {
        if (form != Form.VALUE) {
            PureRelationText.requirePrintable(columns);
        }
        this.columns = List.copyOf(columns);
    }

    @Override
    public void batch(Batch batch) {
        int width = announced().size();
        for (int r = 0; r < batch.size(); r++) {
            List<Object> row = new ArrayList<>(width);
            for (int i = 0; i < width; i++) {
                row.add(batch.get(r, i));
            }
            rows.add(row);
        }
    }

    @Override
    public void end() {
        if (ended) {
            throw new IllegalStateException("a result ends once");
        }
        ended = true;
        List<ExecutionPlan.Column> cols = announced();
        String text = switch (form) {
            case TO_STRING -> PureRelationText.tds(cols, rows);
            case TO_CSV -> PureRelationText.csv(cols, rows);
            case TO_CSV_TDS_NULL -> PureRelationText.csv(cols, rows, true);
            case TDS_CSV -> PureRelationText.tdsCsvProperty(cols, rows);
            case VALUE -> one(cols);
        };
        receiver.accept(text);
    }

    /** The one value a {@link Form#VALUE} answer carries, printed: a {@code toString}'s operand is one value, so any
     *  other count is refused by name. */
    private String one(List<ExecutionPlan.Column> cols) {
        if (cols.size() != 1 || rows.size() != 1) {
            throw new IllegalStateException("a value's toString prints one value; the answer holds " + rows.size()
                    + " row(s) of " + cols.size() + " column(s)");
        }
        if (rows.get(0).get(0) == null) {
            throw new IllegalStateException("a value's toString prints one value; the answer is empty");
        }
        return PureValueText.value(rows.get(0).get(0), cols.get(0));
    }

    /** The columns the run announced: a result always announces them before its rows and its end. */
    private List<ExecutionPlan.Column> announced() {
        List<ExecutionPlan.Column> announced = columns;
        if (announced == null) {
            throw new IllegalStateException("a result's columns are announced before its rows and its end");
        }
        return announced;
    }
}
