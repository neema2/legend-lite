// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.ExecutionPlan;

import java.io.IOException;
import java.util.List;

/**
 * What a session does with a plan's text (docs/MONGODB_PLAN_2026_10_16.md §5.2, D12; 2026-10-17): the runner holds
 * the plan -- it checks the caller's values, fills the type holes, writes the result's punctuation -- and the session's
 * statements carry text in and text out. JDBC's are {@link JdbcStatements}, over the session's connection; a database
 * that speaks no JDBC gives its own through its connector ({@link DatabaseConnector}), and the runner never knows
 * which it holds. Since the plan path program's step 7 (2026-10-18) a session answers typed rows too
 * ({@link #rows}): text in, rows out, a row a text or the plan's typed cells (D12 amended by
 * docs/PLAN_AND_RESULT_MODEL_2026_10_18.md §3).
 */
public interface Statements {

    /** The session is the database {@code target} names and, where the statements' spelling depends on the server's
     *  version, of a version they are written for; anything else is refused by name, never run. */
    void check(ExecutionPlan.Target target);

    /** A statement run for its effect: a session statement, a setup statement. */
    void execute(String statement);

    /**
     * The plan's statement, each slot bound to its parameter's checked value as the slot says, and each row's one text
     * (what the database wrote: carried, never read as a value, Charter C1.2) handed to {@code rows} in order.
     */
    void query(String statement, List<ExecutionPlan.Slot> slots, List<PlanParameters.Checked> values, Rows rows)
            throws IOException;

    /** Where a query's rows go. */
    @FunctionalInterface
    interface Rows {
        void row(@com.legend.base.Nullable String text) throws IOException;
    }

    /**
     * The plan's statement, each slot bound as {@link #query} binds it, and each row's TYPED CELLS handed to
     * {@code cells} in order, one list per row, each cell read by the plan's column (the typed method of
     * docs/PLAN_AND_RESULT_MODEL_2026_10_18.md §3; the decode of its §4, slot first; the plan path program's step 7,
     * 2026-10-18): the statement answers {@code columns} in their order, and a cell is the carrier its column's slot
     * and Pure type say -- an integral slot a {@code long} (a {@code BigInteger} past 63 bits), a floating slot a
     * {@code double}, a decimal slot an exact {@code BigDecimal}, a date or timestamp slot a {@code PureDateLiteral},
     * a time slot a {@code PureTimeLiteral}, a text slot a {@code String}, a structure its map or list, a null a
     * null. The session CONVERTS the driver's value to the carrier and computes nothing (Charter C1.2; the Z2
     * ruling's carrier-decode bridge, docs/TENET_CHARTER.md Clause 2c); a cell no rule reads is refused by name,
     * never guessed (AGENTS.md invariant 4).
     */
    void rows(String statement, List<ExecutionPlan.Slot> slots, List<PlanParameters.Checked> values,
            List<ExecutionPlan.Column> columns, Cells cells) throws IOException;

    /**
     * {@code work} with every statement it sends seeing one state of the data, as {@code snapshot} says (a keyed plan's
     * keys probe and its body: the keys are the body's; 2026-10-20): a transaction at that isolation, committed when
     * the work is done and rolled back when it fails, the session's own setting restored; {@code NONE} runs it as it
     * is, where the database has no transaction across statements.
     */
    void inSnapshot(ExecutionPlan.Keyed.Snapshot snapshot, Work work) throws IOException;

    /** Work a session runs inside a snapshot. */
    @FunctionalInterface
    interface Work {
        void run() throws IOException;
    }

    /** Where a query's typed rows go. */
    @FunctionalInterface
    interface Cells {
        void row(List<Object> cells) throws IOException;
    }
}
