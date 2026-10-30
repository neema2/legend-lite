// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.result;

import com.legend.executionplan.ExecutionPlan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A result collected whole into memory (docs/RESULT_BOUNDARY_PROGRAM_2026_10_26.md R8): a writer like any other, for
 * the judges, the test harnesses and the tests -- never a production route's answer, which streams to its writer.
 */
public final class Collector implements ResultSink {

    private @com.legend.base.Nullable List<ExecutionPlan.Column> columns;
    private final List<List<Object>> rows = new ArrayList<>();
    private boolean ended;

    @Override
    public void columns(List<ExecutionPlan.Column> columns) {
        if (this.columns != null) {
            throw new IllegalStateException("a result's columns come once");
        }
        this.columns = List.copyOf(columns);
    }

    @Override
    public void batch(Batch batch) {
        if (columns == null || ended) {
            throw new IllegalStateException(ended ? "a batch after the end" : "a batch before the columns");
        }
        for (int r = 0; r < batch.size(); r++) {
            rows.add(batch.row(r));
        }
    }

    @Override
    public void end() {
        if (columns == null || ended) {
            throw new IllegalStateException(ended ? "a result ends once" : "an end before the columns");
        }
        ended = true;
    }

    /** The whole result, once it has ended. */
    public Collected collected() {
        if (!ended || columns == null) {
            throw new IllegalStateException("the result has not ended");
        }
        return new Collected(columns, Collections.unmodifiableList(rows));
    }

    /** A whole result: the columns with their three facts, and each row's cells in column order. */
    public record Collected(List<ExecutionPlan.Column> columns, List<List<Object>> rows) {
        public Collected {
            columns = List.copyOf(columns);
        }
    }
}
