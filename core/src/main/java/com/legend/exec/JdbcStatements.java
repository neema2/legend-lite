// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.ExecutionPlan;
import com.legend.executionplan.PlannedSql;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.List;

/**
 * A JDBC session's {@link Statements} (2026-10-17, out of {@code PlanRunner}, whose JDBC this was): the statement
 * prepared, each slot bound to its value as the slot says (a date-time in a cast passed as its text), and each row's
 * one text read -- the one {@code getString} of the plan path, carriage (Charter C1.2) -- or, since the plan path
 * program's step 7 (2026-10-18), each row's typed cells read from the driver by the plan's columns
 * ({@link JdbcDecode}: one carrier conversion per cell, nothing computed).
 */
final class JdbcStatements implements Statements {

    private final Connection c;
    /** The session's state (its memo of the session statements run, cleared by a rollback: D66); the owning
     *  session's, handed with the connection (D70). */
    private final SessionState state;

    /** The session's driver facts the typed rows are read by (its database's: a timestamp re-read as
     *  {@code java.time}, a decimal's trailing zeros dropped, a failing statement's result opened with no columns),
     *  known once {@link #check} has checked the session against the plan's target; null before. */
    private DatabaseConnector.@com.legend.base.Nullable Decoding driver;

    /** The connector of the database the plan's target declares, known once {@link #check} has checked the session
     *  against it; null before (the checkpoint tenets audit's 2, 2026-11-10: the facts a run acts on are the target's
     *  connector's, never re-derived from the driver's product name). */
    private @com.legend.base.Nullable DatabaseConnector declared;

    JdbcStatements(Connection c, SessionState state) {
        this.c = c;
        this.state = state;
    }

    /** The JDBC type number of {@code one}'s null: the plan's null types are {@code java.sql.JDBCType} names (the
     *  plan format's own vocabulary, which every session reads; the runner checks every null's type by this before it
     *  opens a session), or the plan is refused by name. The seam's, since D67 (2026-10-30): the runner names no JDBC
     *  type. */
    static int nullType(ExecutionPlan.Binding.One one, ExecutionPlan.Slot slot) {
        try {
            return java.sql.JDBCType.valueOf(one.nullType()).getVendorTypeNumber();
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("slot of '" + slot.parameter() + "': its null type '" + one.nullType()
                    + "' is no JDBC type", e);
        }
    }

    /** One act of this session's: a run's (THE WALL's rule 3), on a session that holds no raise its reset failed to
     *  clear ({@link SessionState#refuseIfRaiseHeld}, the re-audit's note 8, 2026-11-12). */
    private void act() {
        com.legend.base.RunScope.statement("a statement");
        state.refuseIfRaiseHeld();
    }

    /** The error a failed statement is told as: its raise where the error carries one, else the raise the session
     *  holds ({@link #held}, read then alone -- the audit of D65, 2026-10-29: read before the error's own text was
     *  looked at, every failure paid the session's metadata read), else the database's own text
     *  ({@link RaisedErrors#dataError(SQLException, java.util.function.Supplier)}). */
    private com.legend.error.DataError failed(SQLException e) {
        com.legend.error.DataError failure = RaisedErrors.dataError(e, () -> heldBy(e));
        if (state.inTurn() && aborts()) {
            // a statement failed inside a run's turn on the catalog (§3 rule 3, A4) on a database that has aborted the
            // turn's whole transaction (DuckDB), refusing every statement until a rollback: the turn rolls back now and
            // a new transaction starts with the run's next statement; what the run registered there is forgotten, and
            // establishes again at its next step (the memo cleared)
            state.rolledBack();
            try {
                c.rollback();
            } catch (SQLException rollback) {
                failure.addSuppressed(rollback);
            }
        }
        return failure;
    }

    /**
     * {@code failure}, the database's own refusal of a statement that binds a {@code data:} payload, told under the
     * payload's label (the JSON pass, 2026-11-11): the database splits the payload itself, so its refusal -- a malformed
     * element, a connection's statement or packet limit (ClickHouse's {@code max_query_size}, MySQL's and MariaDB's
     * {@code max_allowed_packet}, Trino's {@code query.max-length}) -- reaches the caller saying which payload it was,
     * as a load's failure names what it loads. The label is the plan's ({@link ExecutionPlan.Binding.Payload#label}), and
     * a raise is told by its type ({@link RaisedErrors.Raised}: pure's own text, kept exactly), never by comparing texts
     * (the B checkpoint's tenets audit, LOW 4, 2026-11-11); a statement binding no payload is told as it is.
     */
    private static com.legend.error.DataError naming(SQLException e, com.legend.error.DataError failure,
            List<ExecutionPlan.Slot> slots) {
        if (failure.getCause() instanceof RaisedErrors.Raised) {
            return failure;   // a raise, its text pure's own
        }
        for (ExecutionPlan.Slot slot : slots) {
            if (slot.binding() instanceof ExecutionPlan.Binding.Payload payload) {
                com.legend.error.DataError named = new com.legend.error.DataError(payload.label() + " was refused: "
                        + failure.getMessage(), failure.getCause() != null ? failure.getCause() : e);
                for (Throwable suppressed : failure.getSuppressed()) {
                    named.addSuppressed(suppressed);
                }
                return named;
            }
        }
        return failure;
    }

    /** Whether the target's database aborts its transaction on a failed statement ({@link
     *  DatabaseConnector#abortsTransactionOnError}): asked inside a run's turn alone, whose statements a plan target has
     *  checked ({@link #check}); a turn's statement on a session none checked is a defect, said by name (until the
     *  checkpoint tenets audit's 2, 2026-11-10, the driver's product name was read and an unreadable one answered no). */
    private boolean aborts() {
        if (declared == null) {
            throw new IllegalStateException("a statement failed inside a run's turn on a session no plan target"
                    + " checked: which database aborts its transaction is the target's to say");
        }
        return declared.abortsTransactionOnError();
    }

    /** The raise {@code c}'s session holds after a failed COMMIT (a bracket's: {@code Sessions.inTransaction}), read back
     *  by the statements the plan's target declared when it checked this session ({@code SessionState.raiseReadBack}:
     *  MariaDB's and MySQL's, the charter's A3(c), C2 2026-11-12 -- until then the driver's connector declared them,
     *  outside every plan). Null for a session whose target declared none, or that holds none; a read that fails is kept
     *  beside the statement's {@code failure}, never dropped (the A delta audit's 6, 2026-11-11). */
    static @com.legend.base.Nullable String held(Connection c, SessionState state, SQLException failure) {
        try {
            return held(c, state.raiseReadBack(), failure, state);
        } catch (SQLException unreadable) {
            failure.addSuppressed(unreadable);
            return null;
        }
    }

    /** The raise this session holds after {@code e}, read by the statements the plan's target declares (checked:
     *  {@link #check}), else -- a statement on a session its target has not checked here -- the ones the session's last
     *  checked target declared; a read that fails is kept beside {@code e}, never dropped. */
    private @com.legend.base.Nullable String heldBy(SQLException e) {
        return held(c, state, e);
    }

    /** The raise held, by {@code readBack}'s statements -- exactly the plan's, sent as written: the read's one text (its
     *  first row's first column; null for no row), then the reset, sent WHATEVER the read did, so a raise held now never
     *  attaches to a later failure (a reset that fails after a failed read is kept beside it). A reset that fails after
     *  a read that answered keeps the text read -- the raise, the error raised -- and its own failure beside the
     *  statement's {@code failure} (the C final audit's J8, 2026-11-12: until then the text was lost). Each counted as a
     *  session's statement ({@link StatementOrigin#SESSION}; the C checkpoint's code audit, 2026-11-12: until then the
     *  read-back was sent uncounted and its reset skipped on a failed read). One {@code getString}: the database's own
     *  text passed on unread. A run's act (THE WALL's rule 3). */
    static @com.legend.base.Nullable String held(Connection c,
            ExecutionPlan.@com.legend.base.Nullable RaiseReadBack readBack, SQLException failure) throws SQLException {
        return held(c, readBack, failure, new SessionState());
    }

    /** {@link #held(Connection, ExecutionPlan.RaiseReadBack, SQLException)} on the session whose {@code state} it
     *  marks where the reset fails: that session may hold the raise still, and refuses every further statement by name
     *  (the re-audit's note 8, 2026-11-12: until then it went on, and a later failure read the stale raise). */
    private static @com.legend.base.Nullable String held(Connection c,
            ExecutionPlan.@com.legend.base.Nullable RaiseReadBack readBack, SQLException failure, SessionState state)
            throws SQLException {
        if (readBack == null) {
            return null;
        }
        com.legend.base.RunScope.statement("a declared error read-back");   // the charter's A3(c), a run's act
        PlannedSql read = readBack.plannedRead();
        PlannedSql reset = readBack.plannedReset();
        String held;
        try {
            StatementOrigin.sent(StatementOrigin.SESSION, read.text());
            List<String> texts = send(c, read, Send.TEXT_QUERY, Bind.NOTHING, a -> texts(rowsOf(a)));
            held = texts.isEmpty() ? null : texts.get(0);
        } catch (SQLException unread) {
            try {
                StatementOrigin.sent(StatementOrigin.SESSION, reset.text());
                send(c, reset, Send.TEXT_EXECUTE, Bind.NOTHING, JdbcStatements::executedOf);
            } catch (SQLException unreset) {
                unread.addSuppressed(unreset);
                state.raiseUnreset(String.valueOf(unreset.getMessage()));
            }
            throw unread;
        }
        StatementOrigin.sent(StatementOrigin.SESSION, reset.text());
        try {
            send(c, reset, Send.TEXT_EXECUTE, Bind.NOTHING, JdbcStatements::executedOf);
        } catch (SQLException unreset) {
            failure.addSuppressed(unreset);
            state.raiseUnreset(String.valueOf(unreset.getMessage()));
        }
        return held;
    }

    @Override
    public void check(ExecutionPlan.Target target) {
        act();   // THE WALL's rule 3, per act (2026-11-06)
        Sessions.check(c, target.database().type());
        declared = Connectors.of(target.database().type());
        driver = declared.decoding();   // the driver's facts are its connector's (D72)
        // the error read-back the plan's target declares, kept with the session for a failure on it (C2)
        state.declareRaiseReadBack(target.raiseReadBack());
        // the settings the target's database must have: written where this platform opened it, else read back once;
        // a target that declares none where its database must have them (H2's), or some where it takes none, is
        // refused by name (the C final audit's J4, 2026-11-12: until then an H2 target with none ran in H2's defaults)
        ExecutionPlan.DatabaseSettings settings = target.settings();
        if (declared.takesSettings() != (settings != null)) {
            throw new com.legend.error.NotImplementedException("a plan's target on " + target.database().type()
                    + (settings == null ? " declares the settings its database must have, and this one declares none"
                            : " declares no settings, and this one declares " + settings.required())
                    + ": never run on other semantics");
        }
        // a declared database run as it is (the third re-audit's MUST-FIX 2, 2026-11-12): nothing written, nothing
        // read back
        if (settings != null && !settings.theDatabasesOwn() && !state.holdsSettings(settings)) {
            verifySettings(c, settings);
            state.settingsHeld(settings);
        }
    }

    /**
     * {@code settings}, the ones the plan's target says its database must have, READ BACK from a session this platform
     * did not open (a caller's, a borrowed one, a URL's; the C checkpoint's SHOULD-FIX 3, the orchestrator's ruling of
     * 2026-11-12): the target's own read-back statement sent as written and counted as the session's. It answers one
     * text for each setting the database has otherwise -- the DATABASE compares; exec carries the texts unread -- and
     * any row refuses the session by name: the plan is never run with other semantics (H2's null ordering would order
     * its answer otherwise).
     */
    static void verifySettings(Connection c, ExecutionPlan.DatabaseSettings settings) {
        PlannedSql read = settings.plannedReadBack();
        List<String> otherwise;
        StatementOrigin.sent(StatementOrigin.SESSION, read.text());
        try {
            otherwise = send(c, read, Send.TEXT_QUERY, Bind.NOTHING, a -> texts(rowsOf(a)));
        } catch (SQLException e) {
            throw RaisedErrors.dataError(e);
        }
        if (!otherwise.isEmpty()) {
            throw new com.legend.error.NotImplementedException("the session's database does not have the settings the"
                    + " plan's target declares (it has " + String.join(", ", otherwise) + "; the target declares "
                    + String.join(", ", settings.required().stream().map(s -> s.name() + "=" + s.value()).toList())
                    + "): a session this platform did not open is read back, and never runs a plan with other"
                    + " settings");
        }
    }

    /** Each row's one text, in order: the database's own, passed on unread (a plan's read-back answers texts). */
    private static List<String> texts(ResultSet rs) throws SQLException {
        List<String> out = new java.util.ArrayList<>();
        while (rs.next()) {
            out.add(rs.getString(1));
        }
        return out;
    }

    @Override
    public boolean execute(PlannedSql statement) {
        act();   // THE WALL's rule 3, per act (2026-11-06)
        try {
            return send(c, statement, Send.TEXT_EXECUTE, Bind.NOTHING, JdbcStatements::executedOf);
        } catch (SQLException e) {
            throw failed(e);
        }
    }

    @Override
    public long update(PlannedSql statement, List<ExecutionPlan.Slot> slots, List<PlanParameters.Checked> values)
            throws IOException {
        act();   // THE WALL's rule 3, per act (2026-11-06)
        try {
            return send(c, statement, Send.UPDATE, st -> bindAll(st, slots, values), JdbcStatements::countOf);
        } catch (SQLException e) {
            throw naming(e, failed(e), slots);
        }
    }

    @Override
    public List<Reported> columns(PlannedSql statement) {
        act();   // THE WALL's rule 3, per act (2026-11-06)
        try {
            return send(c, statement, Send.TEXT_QUERY, Bind.NOTHING, a -> reported(rowsOf(a).getMetaData()));
        } catch (SQLException e) {
            throw failed(e);
        }
    }

    @Override
    public List<Reported> describe(PlannedSql statement) {
        act();   // THE WALL's rule 3, per act (2026-11-06)
        try {
            return send(c, statement, Send.DESCRIBE, Bind.NOTHING, a -> {
                java.sql.ResultSetMetaData md = describedOf(a);
                return md == null ? List.<Reported>of() : reported(md);
            });
        } catch (SQLException e) {
            throw failed(e);
        }
    }

    private static List<Reported> reported(java.sql.ResultSetMetaData md) throws SQLException {
        List<Reported> out = new java.util.ArrayList<>(md.getColumnCount());
        for (int i = 1; i <= md.getColumnCount(); i++) {
            out.add(new Reported(md.getColumnLabel(i), md.getColumnTypeName(i),
                    WireTypes.sqlTypeOf(md.getColumnType(i), md.getPrecision(i), md.getScale(i)),
                    md.isNullable(i) != java.sql.ResultSetMetaData.columnNoNulls));
        }
        return out;
    }

    @Override
    public void readsFile(PlannedSql statement, String path) {
        act();   // THE WALL's rule 3, per act (2026-11-06)
        try {
            com.legend.diagnostics.Census.inc(com.legend.diagnostics.Census.Key.SQL_ROUND_TRIPS);
            send(c, statement, Send.EXECUTE, st -> {
                if (st.getParameterMetaData().getParameterCount() == 1) {
                    st.setString(1, path);
                }
            }, JdbcStatements::executedOf);
        } catch (SQLException e) {
            throw failed(e);
        }
    }

    @Override
    public <T> T inTransaction(java.util.function.Supplier<T> work) {
        com.legend.base.RunScope.opening("a transaction bracket");   // the owner's act: refused while planning
        if (!takesTransactions()) {
            // a driver that takes no transaction (ClickHouse): the work's statements run each their own
            return work.get();
        }
        return Sessions.inTransaction(c, state, work);
    }

    /** Whether the session's database takes a transaction ({@link DatabaseConnector#takesTransactions}): the plan
     *  target's connector's ({@link #check}); for an owner's bracket opened before any statement named its target
     *  ({@code Sessions.Session.inTransaction}), the connector its driver reports, a session no built connector
     *  reports refused by name -- never bracketed by default (the checkpoint tenets audit's 2, 2026-11-10). Both
     *  runners' brackets come here, so a program of steps runs each its own where the database takes no transaction
     *  (ClickHouse; docs/DATA_DELIVERY_DESIGN_2026_11_10.md §3 rule 1), as upstream runs them: PR #81's refusal of a
     *  write there in the query runner alone went, the user's ruling of 2026-11-13 ("agree", after the correctness
     *  audit found the two runners answering otherwise). */
    private boolean takesTransactions() {
        if (declared != null) {
            return declared.takesTransactions();
        }
        DatabaseConnector connector;
        try {
            connector = Connectors.of(c);
        } catch (SQLException unreadable) {
            throw new com.legend.error.DataError("the session's database cannot be read: "
                    + unreadable.getMessage(), unreadable);
        }
        if (connector == null) {
            throw new IllegalStateException("a transaction bracket on a session whose database no plan target checked"
                    + " and no built connector reports: whether it takes a transaction is unknown");
        }
        return connector.takesTransactions();
    }

    @Override
    public boolean ownsTransaction() {
        com.legend.base.RunScope.opening("a transaction bracket");   // the owner's act: refused while planning
        return Sessions.ownsTransaction(c);
    }

    @Override
    public void query(PlannedSql statement, List<ExecutionPlan.Slot> slots, List<PlanParameters.Checked> values,
            Rows rows) throws IOException {
        act();   // THE WALL's rule 3, per act (2026-11-06)
        try {
            send(c, statement, Send.QUERY, st -> bindAll(st, slots, values), a -> {
                ResultSet rs = rowsOf(a);
                while (rs.next()) {
                    rows.row(rs.getString(1));
                }
                return Boolean.TRUE;   // the rows went to the act's sink
            });
        } catch (SQLException e) {
            throw naming(e, failed(e), slots);
        }
    }

    /** {@inheritDoc} Each row's cells read from the driver by the plan's columns ({@link JdbcDecode}), the i-th
     *  column from the i-th result column; a statement answering another number of columns than the plan declares
     *  is refused by name before a row is read. */
    @Override
    public void rows(PlannedSql statement, List<ExecutionPlan.Slot> slots, List<PlanParameters.Checked> values,
            List<ExecutionPlan.Column> columns, Cells cells) throws IOException {
        act();   // THE WALL's rule 3, per act (2026-11-06)
        DatabaseConnector.Decoding driver = this.driver;
        if (driver == null) {
            throw new IllegalStateException("typed rows read on a session no plan target was checked against:"
                    + " how its driver answers is its database's fact");
        }
        try {
            send(c, statement, Send.QUERY, st -> bindAll(st, slots, values), a -> {
                ResultSet rs = rowsOf(a);
                int answered = rs.getMetaData().getColumnCount();
                if (answered == 0 && driver.failedWithoutColumns()) {
                    // a failing statement's result opened with no columns: the first read raises its own error (the
                    // database's message, never the column count's; 2026-10-27, measured on the Trino lane)
                    rs.next();
                }
                int declared = com.legend.result.RowDecode.physicalColumns(columns);
                if (answered != declared) {
                    throw new IllegalStateException("the statement answers " + answered + " columns, and the plan"
                            + " declares " + declared + " " + columns.stream().map(ExecutionPlan.Column::name)
                            .toList() + ": a typed-rows statement answers the plan's columns in their order"
                            + (declared == columns.size() ? "" : " (a tagged column's slots beside it)"));
                }
                // a column whose kind is the wire's (a raw grid's, read by name) is read as its result reports it
                List<ExecutionPlan.Column> read = columns;
                if (columns.stream().anyMatch(c -> c.slot() == null
                        && com.legend.result.RowDecode.ANY.equals(c.type()))) {
                    List<Reported> reported = reported(rs.getMetaData());
                    read = new java.util.ArrayList<>(columns.size());
                    int at = 0;   // the physical column (a tagged column answers its slots beside it)
                    for (ExecutionPlan.Column c : columns) {
                        read.add(WireTypes.decodedAs(c, reported.get(at)));
                        at += com.legend.result.RowDecode.physicalColumns(List.of(c));
                    }
                }
                // the decode kind of each column read once (main's product-path tier 1), over the columns as read
                JdbcDecode.Reader reader = new JdbcDecode.Reader(read, driver);
                while (rs.next()) {
                    cells.row(reader.row(rs));
                }
                return Boolean.TRUE;   // the rows went to the act's sink
            });
        } catch (SQLException e) {
            throw naming(e, failed(e), slots);
        }
    }

    /** {@inheritDoc} Each reported column named by the wire table ({@link WireTypes#column}). */
    @Override
    public void reportedRows(PlannedSql statement, List<ExecutionPlan.Slot> slots, List<PlanParameters.Checked> values,
            ReportedColumns columns, Cells cells) throws IOException {
        act();   // THE WALL's rule 3, per act (2026-11-06)
        DatabaseConnector.Decoding driver = this.driver;
        if (driver == null) {
            throw new IllegalStateException("typed rows read on a session no plan target was checked against:"
                    + " how its driver answers is its database's fact");
        }
        try {
            send(c, statement, Send.QUERY, st -> bindAll(st, slots, values), a -> {
                ResultSet rs = rowsOf(a);
                List<ExecutionPlan.Column> reported = new java.util.ArrayList<>();
                for (Reported r : reported(rs.getMetaData())) {
                    reported.add(WireTypes.column(r));
                }
                columns.columns(reported);
                JdbcDecode.Reader reader = new JdbcDecode.Reader(reported, driver);
                while (rs.next()) {
                    cells.row(reader.row(rs));
                }
                return Boolean.TRUE;   // the rows went to the act's sink
            });
        } catch (SQLException e) {
            throw naming(e, failed(e), slots);
        }
    }

    /** {@inheritDoc} JDBC: auto-commit off for the work, at the snapshot's isolation (the database's own kept for
     *  {@code TRANSACTION}), committed after it, rolled back if it fails, the connection's settings restored. A
     *  transaction a caller holds (a run's turn on the system catalog, L4b; a harness's attempt) is joined as it is,
     *  and is the snapshot then -- never committed here (the runners' rule, {@code PlanRunner}'s sequence). */
    @Override
    public void inSnapshot(ExecutionPlan.Keyed.Snapshot snapshot, Work work) throws IOException {
        act();   // THE WALL's rule 3, per act (2026-11-06)
        if (snapshot == ExecutionPlan.Keyed.Snapshot.NONE || !ownsTransaction()) {
            work.run();
            return;
        }
        boolean autoCommit;
        int isolation;
        try {
            autoCommit = c.getAutoCommit();
            isolation = c.getTransactionIsolation();
            c.setAutoCommit(false);
            switch (snapshot) {
                case REPEATABLE_READ -> c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
                case SERIALIZABLE -> c.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
                case TRANSACTION, NONE -> {
                }
            }
        } catch (SQLException e) {
            throw failed(e);
        }
        // the seam's own transaction: the session statements are remembered inside it (J6); its rollback clears them
        state.ownTransactionBegan();
        try {
            try {
                work.run();
                c.commit();
            } catch (IOException | RuntimeException | Error e) {
                // the work's own failure is the one told -- an Error too (the final audit, 2026-11-12: a
                // RunScope.Refused or an OutOfMemoryError while a keyed snapshot streamed left a kept session in the open
                // transaction, autocommit off and the isolation set, and every later request joined it; as
                // Sessions.inTransaction catches it) -- the session rolled back and set back as it can be
                try {
                    restore(snapshot, autoCommit, isolation, true);
                } catch (SQLException restore) {
                    e.addSuppressed(restore);
                }
                throw e;
            } catch (SQLException e) {
                // the commit failed: rolled back and set back as it can be, the commit's failure told
                RuntimeException failure = failed(e);
                try {
                    restore(snapshot, autoCommit, isolation, true);
                } catch (SQLException restore) {
                    failure.addSuppressed(restore);
                }
                throw failure;
            }
            try {
                restore(snapshot, autoCommit, isolation, false);
            } catch (SQLException e) {
                throw failed(e);
            }
        } finally {
            state.ownTransactionEnded();
        }
    }

    /** The connection set back after a snapshot: rolled back first when the work failed, its isolation (where the
     *  snapshot set one) and its auto-commit as they were. */
    private void restore(ExecutionPlan.Keyed.Snapshot snapshot, boolean autoCommit, int isolation, boolean rollback)
            throws SQLException {
        if (rollback) {
            // a session statement run inside the transaction may be undone with it (Postgres's SET is transactional):
            // the session's memo forgets them, and the next statement runs them again (D66) -- before the rollback,
            // whose own failure would leave the transaction's fate unknown
            state.rolledBack();
            c.rollback();
        }
        if (snapshot != ExecutionPlan.Keyed.Snapshot.TRANSACTION) {
            c.setTransactionIsolation(isolation);
        }
        c.setAutoCommit(autoCommit);
    }

    /** How {@link #send} sends a statement: PREPARED, its slots bound -- a query's rows, an update's count, an
     *  execution, a description -- or as its TEXT alone, unprepared -- a statement binding nothing that each act sent so
     *  before the choke point (an effect's, which may hold several; a column read's; a read-back's; a settings check's). */
    private enum Send { QUERY, UPDATE, EXECUTE, DESCRIBE, TEXT_QUERY, TEXT_EXECUTE }

    /** What a send answered, handed to its answer while the result is open. */
    private sealed interface Answered permits Answered.Rows, Answered.Count, Answered.Executed, Answered.Described {
        /** A query's open result. */
        record Rows(ResultSet rows) implements Answered {
        }

        /** An update's count. */
        record Count(long count) implements Answered {
        }

        /** Whether an execution answered a result. */
        record Executed(boolean results) implements Answered {
        }

        /** A prepared statement's description of its result; null where the driver gives none. */
        record Described(java.sql.@com.legend.base.Nullable ResultSetMetaData description) implements Answered {
        }
    }

    /** A prepared statement's slots bound: setters only, nothing sent. */
    @FunctionalInterface
    private interface Bind {
        void bind(PreparedStatement st) throws SQLException;

        /** Binds nothing. */
        Bind NOTHING = st -> { };
    }

    /** What an act reads of a send's answer. */
    @FunctionalInterface
    private interface Answer<T, X extends Exception> {
        T of(Answered answered) throws SQLException, X;
    }

    /**
     * THE ONE PLACE A JDBC SESSION IS SENT A STATEMENT (the third re-audit's MUST-FIX 1, 2026-11-12, as MongoDB's
     * {@code MongoDbStatements.send}): the plan's statement as the plan holds it ({@link PlannedSql}), prepared and bound
     * by {@code bind} or sent as its text ({@link Send}), its answer read by {@code answer} while it is open. Every
     * {@code Connection.createStatement}, {@code prepareStatement} and {@code prepareCall}, and every
     * {@code Statement.execute*} and {@code addBatch}, of the product is here and nowhere else
     * ({@code ArchitectureTest.jdbcSendsAtOneChokePoint}): until then each act prepared and executed for itself, so a
     * helper beside them could have sent text of its own with every guard green.
     */
    private static <T, X extends Exception> T send(Connection c, PlannedSql statement, Send how, Bind bind,
            Answer<T, X> answer) throws SQLException, X {
        if (how == Send.TEXT_QUERY || how == Send.TEXT_EXECUTE) {
            try (Statement st = c.createStatement()) {
                if (how == Send.TEXT_EXECUTE) {
                    return answer.of(new Answered.Executed(st.execute(statement.text())));
                }
                try (ResultSet rs = st.executeQuery(statement.text())) {
                    return answer.of(new Answered.Rows(rs));
                }
            }
        }
        try (PreparedStatement st = c.prepareStatement(statement.text())) {
            bind.bind(st);
            switch (how) {
                case QUERY -> {
                    try (ResultSet rs = st.executeQuery()) {
                        return answer.of(new Answered.Rows(rs));
                    }
                }
                case UPDATE -> {
                    return answer.of(new Answered.Count(st.executeUpdate()));
                }
                case EXECUTE -> {
                    return answer.of(new Answered.Executed(st.execute()));
                }
                case DESCRIBE -> {
                    return answer.of(new Answered.Described(st.getMetaData()));
                }
                case TEXT_QUERY, TEXT_EXECUTE -> throw new IllegalStateException("not reached: sent as its text above");
            }
            throw new IllegalStateException("not reached: every send is an arm above");
        }
    }

    /** A query's open result; any other answer is a defect, said by name. */
    private static ResultSet rowsOf(Answered a) {
        if (a instanceof Answered.Rows r) {
            return r.rows();
        }
        throw new IllegalStateException("a query's send answered " + a);
    }

    /** An update's count. */
    private static long countOf(Answered a) {
        if (a instanceof Answered.Count n) {
            return n.count();
        }
        throw new IllegalStateException("an update's send answered " + a);
    }

    /** Whether an execution answered a result. */
    private static boolean executedOf(Answered a) {
        if (a instanceof Answered.Executed e) {
            return e.results();
        }
        throw new IllegalStateException("an execution's send answered " + a);
    }

    /** A description. */
    private static java.sql.@com.legend.base.Nullable ResultSetMetaData describedOf(Answered a) {
        if (a instanceof Answered.Described d) {
            return d.description();
        }
        throw new IllegalStateException("a description's send answered " + a);
    }

    /** Every slot bound to its value, in order. */
    private void bindAll(PreparedStatement st, List<ExecutionPlan.Slot> slots, List<PlanParameters.Checked> values)
            throws SQLException {
        for (int i = 0; i < slots.size(); i++) {
            bind(st, i + 1, slots.get(i), values.get(i));
        }
    }

    /** Slot {@code index} bound to its parameter's checked value, as the slot says: one value (an absent one a null of
     *  the slot's type), or a list as one array of the slot's element type (an absent one an empty array). */
    private void bind(PreparedStatement st, int index, ExecutionPlan.Slot slot, PlanParameters.Checked value)
            throws SQLException {
        switch (slot.binding()) {
            case ExecutionPlan.Binding.Array array -> {
                List<Object> items = switch (value) {
                    case PlanParameters.Many many -> many.values();
                    case PlanParameters.None none -> List.of();
                    case PlanParameters.One one -> List.of(one.value());
                };
                Integer kept = array.fraction();
                if (kept != null) {
                    // an element finer than its database keeps is refused, never cut or rounded by the driver (S181:
                    // the plan says the digits kept; the A delta audit's 2, 2026-11-11)
                    for (Object item : items) {
                        if (item instanceof LocalDateTime t && t.getNano() % divisor(9 - kept) != 0) {
                            throw new com.legend.error.DataError("the list parameter '" + slot.parameter() + "' holds "
                                    + t + ", which has more fraction digits than its database keeps (" + kept
                                    + "): refused, never cut (S181)");
                        }
                    }
                }
                st.setArray(index, c.createArrayOf(array.elementSqlType(),
                        items.stream().map(JdbcStatements::arrayElement).toArray()));
            }
            case ExecutionPlan.Binding.One one -> {
                switch (value) {
                    case PlanParameters.One v -> {
                        ExecutionPlan.AsText asText = one.asText();
                        if (asText != null && v.value() instanceof LocalDateTime t) {
                            // a date-time in its declared type's cast is passed as its text: the drivers pass no digit
                            // finer than a microsecond alike (DuckDB's cuts it, Postgres's rounds it;
                            // probes/timestamp-results.txt)
                            st.setString(index, PlanRunner.dateTimeText(t));
                        } else if (asText != null && v.value() instanceof java.time.LocalDate d) {
                            // a StrictDate value of a Date parameter: its text, a midnight timestamp in the cast (D1)
                            st.setString(index, d.toString());
                        } else if (asText != null) {
                            // a slot bound as text holds a date or a date-time: anything else is the plan's defect,
                            // refused by name, never bound as the driver would take it (invariant 4)
                            throw new IllegalStateException("slot " + index + " of '" + slot.parameter() + "' binds a"
                                    + " date or a date-time as its text, given a " + v.value().getClass().getSimpleName());
                        } else {
                            st.setObject(index, bindable(v.value()));
                        }
                    }
                    case PlanParameters.None none -> st.setNull(index, nullType(one, slot));
                    case PlanParameters.Many many -> throw new IllegalStateException("slot " + index + " binds one"
                            + " value of '" + slot.parameter() + "', given a list");
                }
            }
            // a key's cell in a copy of a per-key region: that key's text, as the runner's expansion gives it (J7,
            // 2026-11-12: the plan's own slot, told apart by its type)
            case ExecutionPlan.Binding.Key key -> {
                if (!(value instanceof PlanParameters.One one && one.value() instanceof String text)) {
                    throw new IllegalStateException("slot " + index + " binds a key's cell given no key's text");
                }
                st.setString(index, text);
            }
            // a data: payload's one JSON array, the plan's own value (its statement's content, PlanRunner.value), as its
            // text: the database splits it
            case ExecutionPlan.Binding.Payload payload -> {
                if (!(value instanceof PlanParameters.One one && one.value() instanceof String text)) {
                    throw new IllegalStateException("slot " + index + " binds the payload " + payload.digest()
                            + " given no text");
                }
                st.setString(index, text);
            }
            case ExecutionPlan.Binding.Source src -> {
                // the path the run admitted for the source's location (PlanRunner.admitted), as its text
                if (!(value instanceof PlanParameters.One one && one.value() instanceof String path)) {
                    throw new IllegalStateException("slot " + index + " reads the source '" + src.location()
                            + "' given no admitted path");
                }
                st.setString(index, path);
            }
        }
    }

    /** Ten to the power {@code digits}: a nanosecond count's divisor for its digits past the kept ones. */
    private static int divisor(int digits) {
        int d = 1;
        for (int i = 0; i < digits; i++) {
            d *= 10;
        }
        return d;
    }

    /** A value as this session's driver binds it: a date and a date-time as {@code java.sql.Date} and
     *  {@code java.sql.Timestamp} where the driver binds no {@code java.time} value (its connector's fact,
     *  {@link DatabaseConnector.Decoding#bindsJavaTime}), every other value as it is. */
    private Object bindable(Object v) {
        if (!(v instanceof java.time.LocalDate || v instanceof LocalDateTime)) {
            return v;
        }
        DatabaseConnector.Decoding driver = this.driver;
        if (driver == null) {
            throw new IllegalStateException("a date bound on a session no plan target was checked against: how its"
                    + " driver binds one is its database's fact");
        }
        if (driver.bindsJavaTime()) {
            return v;
        }
        return v instanceof LocalDateTime t ? java.sql.Timestamp.valueOf(t)
                : java.sql.Date.valueOf((java.time.LocalDate) v);
    }

    /** An array element as the drivers take it: a timestamp as {@code java.sql.Timestamp} (measured,
     *  probes/list-results.txt), every other value as it is. */
    private static Object arrayElement(Object v) {
        return v instanceof LocalDateTime t ? java.sql.Timestamp.valueOf(t) : v;
    }
}
