// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.result;

import com.legend.executionplan.ExecutionPlan;

import java.io.IOException;
import java.util.List;

/**
 * Where a plan's TYPED RESULT goes (docs/RESULT_BOUNDARY_PROGRAM_2026_10_26.md R8, ruled 2026-10-26): a stream of
 * batches of typed rows. The runner hands the columns once, with their three type facts, then each batch in the
 * result's order, then the end; a writer at the edge spells what it receives in its consumer's layout and computes
 * nothing (R3). A sink is called on one thread, in that order, and never after {@link #end}; a failure is the
 * runner's exception, after which nothing more is called.
 */
public interface ResultSink {

    /** The result's columns, in order, before any batch. */
    void columns(List<ExecutionPlan.Column> columns) throws IOException;

    /**
     * A DYNAMIC result's columns (a pivot whose columns its data names: its keys read first), in order, before any
     * batch, with each one's type as the database reported it once the result opened ({@code reported}, in the columns'
     * order: the driver's own type name, JDBC's {@code getColumnTypeName}; null where the database reports none,
     * MongoDB's documents) -- what legend-engine types a dynamic result's header by (D18 F, the user 2026-11-03). A sink
     * that writes no such header takes the columns alone.
     */
    default void columns(List<ExecutionPlan.Column> columns, @com.legend.base.Nullable List<@com.legend.base.Nullable String> reported)
            throws IOException {
        columns(columns);
    }

    /** Whether this sink is written from the database's report of the result's columns (legend-engine's ARROW_IPC
     *  answer, typed from the JDBC report alone: {@link EngineArrowWriter}): a run then reads the report and hands it
     *  over ({@link #reported}) before the columns. Every other sink is spared the read. */
    default boolean wantsReport() {
        return false;
    }

    /** The database's report of the result's columns ({@link ColumnReport}), in the result's order, once the result
     *  has opened and before {@link #columns}, to a sink that {@link #wantsReport}; null where the database reports
     *  none (MongoDB's documents). */
    default void reported(@com.legend.base.Nullable List<ColumnReport> report) throws IOException {
    }

    /** The next rows of the result, in its order; a batch holds at least one row. */
    void batch(Batch batch) throws IOException;

    /** The result is whole: no batch follows. */
    void end() throws IOException;
}
