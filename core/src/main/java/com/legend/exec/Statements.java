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

    /** The execution-trace comment the next query this session sends will carry, minted now (the test-is-the-product
     *  plan's M3f, R17 C4: a statement's stamp a plan binds, {@code ExecutionPlan.Sql.stamp}); asked again before that
     *  query is sent, the same comment. */
    String nextStamp();

    /** A statement run for its effect -- a session statement, a setup statement, an effect a body sends
     *  ({@link ExecutionPlan.Effect}): whether it answered rows, which are not read. */
    boolean execute(String statement);

    /**
     * The plan's statement, each slot bound to its parameter's checked value as the slot says, and each row's one text
     * (what the database wrote: carried, never read as a value, Charter C1.2) handed to {@code rows} in order.
     */
    void query(String statement, List<ExecutionPlan.Slot> slots, List<PlanParameters.Checked> values, Rows rows)
            throws IOException;

    /** Where a query's rows go. */
    /** {@code statement} run for the change it makes, {@code slots} bound to {@code values}: the count of rows the
     *  database reports (a write plan's step, docs/WRITES_DESIGN_2026_10_18.md §8, leg 7; merged 2026-10-26). */
    long update(String statement, List<ExecutionPlan.Slot> slots, List<PlanParameters.Checked> values)
            throws IOException;

    /** {@code work} inside one transaction of this session (a caller's open one joined, {@link Sessions#inTransaction}):
     *  committed when it returns, rolled back when it throws. */
    <T> T inTransaction(java.util.function.Supplier<T> work);

    /** Whether this session's database takes a transaction at all (its driver's fact, {@link
     *  DatabaseConnector.Decoding#transactions}: ClickHouse's takes none). A runner that has nothing to bracket but a
     *  session's own tables asks it; every other bracket is {@link #inTransaction}'s, which such a database refuses. */
    default boolean takesTransactions() {
        return true;
    }

    /** Whether the session commits each statement itself, so a transaction is its holder's to open
     *  ({@link Sessions#ownsTransaction}): a caller that turned autocommit off holds it, and an effect script's bracket
     *  must not nest inside (the body driver asks through the seam, B1b, 2026-10-29). */
    boolean ownsTransaction();

    /**
     * The columns {@code statement} answers: the statement run and its result's columns read, no row read (the
     * caller's text is a {@code LIMIT 0} probe: a schema read, never values -- the E1 probe discipline). The body
     * driver's three schema probes (a raw grid's roster, a deferred relation's columns, a wire-decided kind's
     * reported type) and the static pivot's key typing read through this, so the driver holds no JDBC itself (track B
     * of the result boundary program, B1a, 2026-10-29; until then each probe ran its own JDBC statement, and the
     * wire-type reader a prepare, whose metadata a driver may withhold).
     */
    List<Reported> columns(String statement);

    /** The columns {@code statement} would answer, as the database DESCRIBES it before running it (a JDBC session:
     *  the prepared statement's metadata), none where the driver withholds them: the wire-type reader's reading
     *  ({@code WireTypes}: a prepare, no execution, since leg 3.3), kept as it was when it moved onto the seam
     *  (B1a); {@link #columns} is the probe that runs. */
    List<Reported> describe(String statement);

    /** A column a statement reports: its label; the driver's own name for its type (what a dialect's typing reads,
     *  {@code SqlDialect.typingWireName}); the SQL vocabulary's type where JDBC's type code names one (null
     *  otherwise); whether it may be null. */
    record Reported(String name, String driverType, com.legend.sql.@com.legend.base.Nullable SqlType type,
            boolean nullable) {
    }

    /** {@code rows} loaded through the session's database's bulk loader where it has one ({@link BulkLoad}): false
     *  when it has none, and the caller sends the one INSERT its dialect writes instead. A body's seeds and the system
     *  database's rows load so (B1a). */
    boolean load(ExecutionPlan.LoadStep.Rows rows);

    /** A file's load the database READS (docs/DATA_IN_THE_PLAN_2026_11_09.md 2.5, 2026-11-09): {@code statement}'s one
     *  {@code ?} bound to {@code staged}, the path of the runner's own copy of the verified bytes. */
    void readsFile(String statement, String staged);

    /** A file's load the runner STREAMS (2.5): {@code insert} sent once per row of {@code rows} in one batch, each
     *  row's cells bound to its {@code ?}s in order as text (a null cell SQL NULL), the database casting each. */
    void streams(String insert, List<List<String>> rows);

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
     * {@code statement}'s rows with the columns its RESULT reports (the planner-driver seam program's S3c, 2026-10-30):
     * a raw grid's, whose columns the program never named, read from the real run as the engine reads a raw result
     * set. {@code columns} receives them before any row -- each its label, the Pure type the wire table names for the
     * SQL type JDBC reports (Any where it names none, the cell then the driver's own kind), that type's slot, nullable
     * as reported -- and each row's cells are read by them as {@link #rows} reads a plan's. Nothing is read at
     * planning; the plan declared no column ({@code ExecutionPlan.TdsResult}: a relation of no column).
     */
    void reportedRows(String statement, List<ExecutionPlan.Slot> slots, List<PlanParameters.Checked> values,
            ReportedColumns columns, Cells cells) throws IOException;

    /** Where a result's reported columns go, before its rows. */
    @FunctionalInterface
    interface ReportedColumns {
        void columns(List<ExecutionPlan.Column> columns) throws IOException;
    }

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

    /** The statement origin this session's statements are counted under ({@link StatementOrigin}): the mark the
     *  runner gave them for what it runs now -- explicit, carried by the statements it hands down (until slice 5,
     *  2026-11-09, a thread-scoped mark the senders read). */
    default StatementOrigin mark() {
        return StatementOrigin.OTHER;
    }

    /** {@code st} counted under {@code mark}: the same statements, the same session. */
    static Statements marked(Statements st, StatementOrigin mark) {
        if (st.mark() == mark) {
            return st;
        }
        return new Marked(st instanceof Marked m ? m.inner() : st, mark);
    }

    /** A session's statements under a mark of the runner's: every call the inner statements', the mark this one's. */
    record Marked(Statements inner, StatementOrigin mark) implements Statements {
        @Override
        public void check(ExecutionPlan.Target target) {
            inner.check(target);
        }

        @Override
        public String nextStamp() {
            return inner.nextStamp();
        }

        @Override
        public boolean execute(String statement) {
            return inner.execute(statement);
        }

        @Override
        public void query(String statement, List<ExecutionPlan.Slot> slots, List<PlanParameters.Checked> values,
                Rows rows) throws IOException {
            inner.query(statement, slots, values, rows);
        }

        @Override
        public long update(String statement, List<ExecutionPlan.Slot> slots, List<PlanParameters.Checked> values)
                throws IOException {
            return inner.update(statement, slots, values);
        }

        @Override
        public <T> T inTransaction(java.util.function.Supplier<T> work) {
            return inner.inTransaction(work);
        }

        @Override
        public boolean ownsTransaction() {
            return inner.ownsTransaction();
        }

        @Override
        public List<Reported> columns(String statement) {
            return inner.columns(statement);
        }

        @Override
        public List<Reported> describe(String statement) {
            return inner.describe(statement);
        }

        @Override
        public boolean load(ExecutionPlan.LoadStep.Rows rows) {
            return inner.load(rows);
        }

        @Override
        public void readsFile(String statement, String staged) {
            inner.readsFile(statement, staged);
        }

        @Override
        public void streams(String insert, List<List<String>> rows) {
            inner.streams(insert, rows);
        }

        @Override
        public void rows(String statement, List<ExecutionPlan.Slot> slots, List<PlanParameters.Checked> values,
                List<ExecutionPlan.Column> columns, Cells cells) throws IOException {
            inner.rows(statement, slots, values, columns, cells);
        }

        @Override
        public void reportedRows(String statement, List<ExecutionPlan.Slot> slots,
                List<PlanParameters.Checked> values, ReportedColumns columns, Cells cells) throws IOException {
            inner.reportedRows(statement, slots, values, columns, cells);
        }

        @Override
        public void inSnapshot(ExecutionPlan.Keyed.Snapshot snapshot, Work work) throws IOException {
            inner.inSnapshot(snapshot, work);
        }
    }
}
