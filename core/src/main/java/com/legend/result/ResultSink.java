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

    /** The next rows of the result, in its order; a batch holds at least one row. */
    void batch(Batch batch) throws IOException;

    /** The result is whole: no batch follows. */
    void end() throws IOException;
}
