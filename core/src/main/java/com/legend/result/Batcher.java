// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.result;

import com.legend.executionplan.ExecutionPlan;

import java.io.IOException;
import java.util.List;

/**
 * Rows into batches (docs/RESULT_BOUNDARY_PROGRAM_2026_10_26.md R8): the session's typed rows, one at a time, gathered
 * into batches of up to {@code capacity} rows and handed to a sink when a batch is full and at the end. Memory is
 * bounded by one batch; a source of one row at a time with a capacity of one is a tick per row. The columns go to the
 * sink first ({@link #start}); {@link #end} hands the last batch, if any, and ends the result.
 */
public final class Batcher {

    /** The rows a batch holds when the caller says nothing: the size at which a columnar writer amortizes its framing,
     *  small enough that a slow row driver's first rows are not held long. */
    public static final int DEFAULT_CAPACITY = 1024;

    private final List<ExecutionPlan.Column> columns;
    private final ResultSink sink;
    private final int capacity;
    private Batch.Builder builder;
    private boolean started;
    private boolean ended;

    public Batcher(List<ExecutionPlan.Column> columns, ResultSink sink, int capacity) {
        this.columns = List.copyOf(columns);
        this.sink = sink;
        this.capacity = capacity;
        this.builder = Batch.builder(this.columns, capacity);
    }

    /** The columns, to the sink, before any row. */
    public void start() throws IOException {
        if (started) {
            throw new IllegalStateException("a result is started once");
        }
        started = true;
        sink.columns(columns);
    }

    /** One row, its cells in column order. */
    public void row(List<?> cells) throws IOException {
        if (!started || ended) {
            throw new IllegalStateException(started ? "a row after the result's end" : "a row before the columns");
        }
        builder.add(cells);
        if (builder.full()) {
            sink.batch(builder.build());
            builder = Batch.builder(columns, capacity);
        }
    }

    /** The last rows, if any, then the end. */
    public void end() throws IOException {
        if (!started || ended) {
            throw new IllegalStateException(started ? "a result is ended once" : "an end before the columns");
        }
        ended = true;
        if (builder.size() > 0) {
            sink.batch(builder.build());
        }
        sink.end();
    }
}
