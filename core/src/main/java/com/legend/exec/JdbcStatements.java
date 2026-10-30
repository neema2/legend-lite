// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.ExecutionPlan;

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

    /** The execution trace a query's stamp is published to (the body driver's, whose activity rows record the comment
     *  the statement carried); null for a session no trace reads ({@link ExecutionTrace#stampOnly}). */
    private final @com.legend.base.Nullable ExecutionTrace trace;

    /** The session's driver facts the typed rows are read by (its database's: a timestamp re-read as
     *  {@code java.time}, a decimal's trailing zeros dropped, a failing statement's result opened with no columns),
     *  known once {@link #check} has checked the session against the plan's target; null before. */
    private DatabaseConnector.@com.legend.base.Nullable Decoding driver;

    JdbcStatements(Connection c, SessionState state) {
        this(c, state, null);
    }

    JdbcStatements(Connection c, SessionState state, @com.legend.base.Nullable ExecutionTrace trace) {
        this.c = c;
        this.state = state;
        this.trace = trace;
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

    /** The error a failed statement is told as: its raise where the error carries one, else the raise the session
     *  holds ({@link #held}, read then alone -- the audit of D65, 2026-10-29: read before the error's own text was
     *  looked at, every failure paid the session's metadata read), else the database's own text
     *  ({@link RaisedErrors#dataError(SQLException, java.util.function.Supplier)}). */
    private com.legend.error.DataError failed(SQLException e) {
        return RaisedErrors.dataError(e, () -> held(c));
    }

    /** The raise {@code c}'s session holds after a statement failed, read back and cleared by its connector's two
     *  declared statements ({@link DatabaseConnector#raiseRecovery}: MariaDB's and MySQL's, whose errors cannot carry
     *  a raise's text), by the seam (D65, 2026-10-29: until then the connector ran them itself). Null for a session
     *  whose database declares none, holds none, or cannot be read (a failure of the read is not the statement's). */
    static @com.legend.base.Nullable String held(Connection c) {
        try {
            DatabaseConnector connector = Connectors.of(c);
            return connector == null ? null : held(c, connector.raiseRecovery());
        } catch (SQLException unreadable) {
            return null;
        }
    }

    /** {@link #held(Connection)} by {@code recovery}'s statements: the read's one text (its first row's first column;
     *  null for no row), then the reset. One {@code getString}: the database's own text passed on unread. */
    static @com.legend.base.Nullable String held(Connection c,
            DatabaseConnector.@com.legend.base.Nullable RaiseRecovery recovery) throws SQLException {
        if (recovery == null) {
            return null;
        }
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(recovery.read())) {
            String held = rs.next() ? rs.getString(1) : null;
            st.execute(recovery.reset());
            return held;
        }
    }

    /** A query as the database receives it: the engine's execution-trace comment line, then the statement
     *  ({@link ExecutionTrace}: legend-engine's RelationalExecutor stamps every query it runs, and the activity records
     *  the comment). The old executor stamped at its one JDBC boundary; the plan runner's is this one (2026-10-27, the
     *  result boundary program's phase 5: with the old executor deleted no query carried it, and the corpus's
     *  testSQLComments read an activity comment no statement had). */
    private String stamped(String statement) {
        return trace != null ? trace.stamp(statement) : ExecutionTrace.stampOnly(statement);
    }

    @Override
    public void check(ExecutionPlan.Target target) {
        Sessions.check(c, target.database().type());
        driver = Connectors.of(target.database().type()).decoding();   // the driver's facts are its connector's (D72)
        switch (target.servers()) {
            case ExecutionPlan.Servers.Every every -> {
                // the spelling depends on no version
            }
            case ExecutionPlan.Servers.Versions versions -> {
                String version = Sessions.version(c);
                if (versions.prefixes().stream().noneMatch(version::startsWith)) {
                    throw new com.legend.error.NotImplementedException("the plan's statements are written for "
                            + target.database().type() + " " + versions.prefixes() + ", and the session is "
                            + target.database().type() + " " + version + ": not run");
                }
            }
        }
    }

    @Override
    public boolean execute(String statement) {
        try (Statement st = c.createStatement()) {
            return st.execute(statement);
        } catch (SQLException e) {
            throw failed(e);
        }
    }

    @Override
    public long update(String statement, List<ExecutionPlan.Slot> slots, List<PlanParameters.Checked> values)
            throws IOException {
        try (PreparedStatement st = c.prepareStatement(statement)) {
            for (int i = 0; i < slots.size(); i++) {
                bind(st, i + 1, slots.get(i), values.get(i));
            }
            return st.executeUpdate();
        } catch (SQLException e) {
            throw failed(e);
        }
    }

    @Override
    public List<Reported> columns(String statement) {
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(statement)) {
            return reported(rs.getMetaData());
        } catch (SQLException e) {
            throw failed(e);
        }
    }

    @Override
    public List<Reported> describe(String statement) {
        try (PreparedStatement st = c.prepareStatement(statement)) {
            java.sql.ResultSetMetaData md = st.getMetaData();
            return md == null ? List.of() : reported(md);
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
    public boolean load(ExecutionPlan.SetupStep.Rows rows) {
        try {
            BulkLoad bulk = BulkLoads.of(c);
            if (bulk == null) {
                return false;
            }
            BulkLoads.load(bulk, c, rows);
            return true;
        } catch (SQLException e) {
            throw failed(e);
        }
    }

    @Override
    public <T> T inTransaction(java.util.function.Supplier<T> work) {
        return Sessions.inTransaction(c, state, work);
    }

    @Override
    public boolean ownsTransaction() {
        return Sessions.ownsTransaction(c);
    }

    @Override
    public void query(String statement, List<ExecutionPlan.Slot> slots, List<PlanParameters.Checked> values,
            Rows rows) throws IOException {
        try (PreparedStatement st = c.prepareStatement(stamped(statement))) {
            for (int i = 0; i < slots.size(); i++) {
                bind(st, i + 1, slots.get(i), values.get(i));
            }
            try (ResultSet rs = st.executeQuery()) {
                while (rs.next()) {
                    rows.row(rs.getString(1));
                }
            }
        } catch (SQLException e) {
            throw failed(e);
        }
    }

    /** {@inheritDoc} Each row's cells read from the driver by the plan's columns ({@link JdbcDecode}), the i-th
     *  column from the i-th result column; a statement answering another number of columns than the plan declares
     *  is refused by name before a row is read. */
    @Override
    public void rows(String statement, List<ExecutionPlan.Slot> slots, List<PlanParameters.Checked> values,
            List<ExecutionPlan.Column> columns, Cells cells) throws IOException {
        try (PreparedStatement st = c.prepareStatement(stamped(statement))) {
            for (int i = 0; i < slots.size(); i++) {
                bind(st, i + 1, slots.get(i), values.get(i));
            }
            DatabaseConnector.Decoding driver = this.driver;
            if (driver == null) {
                throw new IllegalStateException("typed rows read on a session no plan target was checked against:"
                        + " how its driver answers is its database's fact");
            }
            try (ResultSet rs = st.executeQuery()) {
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
                while (rs.next()) {
                    cells.row(JdbcDecode.row(rs, columns, driver));
                }
            }
        } catch (SQLException e) {
            throw failed(e);
        }
    }

    /** {@inheritDoc} JDBC: auto-commit off for the work, at the snapshot's isolation (the database's own kept for
     *  {@code TRANSACTION}), committed after it, rolled back if it fails, the connection's settings restored. */
    @Override
    public void inSnapshot(ExecutionPlan.Keyed.Snapshot snapshot, Work work) throws IOException {
        if (snapshot == ExecutionPlan.Keyed.Snapshot.NONE) {
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
        try {
            work.run();
            c.commit();
        } catch (IOException | RuntimeException e) {
            // the work's own failure is the one told; the session is rolled back and set back as it can be
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
                st.setArray(index, c.createArrayOf(array.elementSqlType(),
                        items.stream().map(JdbcStatements::arrayElement).toArray()));
            }
            case ExecutionPlan.Binding.One one -> {
                switch (value) {
                    case PlanParameters.One v -> {
                        ExecutionPlan.TypeHole hole = one.hole();
                        if (hole != null && v.value() instanceof LocalDateTime t) {
                            // a date-time in a cast is passed as its text: the drivers pass no digit finer than a
                            // microsecond alike (DuckDB's cuts it, Postgres's rounds it; probes/timestamp-results.txt)
                            st.setString(index, PlanRunner.dateTimeText(t,
                                    PlanRunner.spelling(hole, PlanRunner.kindOf(t, slot), slot).fraction()));
                        } else {
                            st.setObject(index, v.value());
                        }
                    }
                    case PlanParameters.None none -> st.setNull(index, nullType(one, slot));
                    case PlanParameters.Many many -> throw new IllegalStateException("slot " + index + " binds one"
                            + " value of '" + slot.parameter() + "', given a list");
                }
            }
            case ExecutionPlan.Binding.Key key -> throw new IllegalStateException("slot " + index + " binds a key's"
                    + " cell unexpanded: the runner writes a keyed body for its keys before it is sent");
        }
    }

    /** An array element as the drivers take it: a timestamp as {@code java.sql.Timestamp} (measured,
     *  probes/list-results.txt), every other value as it is. */
    private static Object arrayElement(Object v) {
        return v instanceof LocalDateTime t ? java.sql.Timestamp.valueOf(t) : v;
    }
}
