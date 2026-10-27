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

    /** Whether the session's driver re-reads a timestamp cell as {@code java.time}: its database's fact, known once
     *  {@link #check} has checked the session against the plan's target; null before. */
    private @com.legend.base.Nullable Boolean javaTime;

    JdbcStatements(Connection c) {
        this.c = c;
    }

    @Override
    public void check(ExecutionPlan.Target target) {
        Sessions.check(c, target.database().type());
        javaTime = com.legend.database.Databases.dialect(target.database().type()).refetchesTimestampAsJavaTime();
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
    public void execute(String statement) {
        try (Statement st = c.createStatement()) {
            st.execute(statement);
        } catch (SQLException e) {
            throw PlanSessions.dataError(e, c);
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
            throw PlanSessions.dataError(e, c);
        }
    }

    @Override
    public <T> T inTransaction(java.util.function.Supplier<T> work) {
        return Executor.inTransaction(c, work);
    }

    @Override
    public void query(String statement, List<ExecutionPlan.Slot> slots, List<PlanParameters.Checked> values,
            Rows rows) throws IOException {
        try (PreparedStatement st = c.prepareStatement(statement)) {
            for (int i = 0; i < slots.size(); i++) {
                bind(st, i + 1, slots.get(i), values.get(i));
            }
            try (ResultSet rs = st.executeQuery()) {
                while (rs.next()) {
                    rows.row(rs.getString(1));
                }
            }
        } catch (SQLException e) {
            throw PlanSessions.dataError(e, c);
        }
    }

    /** {@inheritDoc} Each row's cells read from the driver by the plan's columns ({@link JdbcDecode}), the i-th
     *  column from the i-th result column; a statement answering another number of columns than the plan declares
     *  is refused by name before a row is read. */
    @Override
    public void rows(String statement, List<ExecutionPlan.Slot> slots, List<PlanParameters.Checked> values,
            List<ExecutionPlan.Column> columns, Cells cells) throws IOException {
        try (PreparedStatement st = c.prepareStatement(statement)) {
            for (int i = 0; i < slots.size(); i++) {
                bind(st, i + 1, slots.get(i), values.get(i));
            }
            try (ResultSet rs = st.executeQuery()) {
                int answered = rs.getMetaData().getColumnCount();
                int declared = RowDecode.physicalColumns(columns);
                if (answered != declared) {
                    throw new IllegalStateException("the statement answers " + answered + " columns, and the plan"
                            + " declares " + declared + " " + columns.stream().map(ExecutionPlan.Column::name)
                            .toList() + ": a typed-rows statement answers the plan's columns in their order"
                            + (declared == columns.size() ? "" : " (a tagged column's slots beside it)"));
                }
                Boolean javaTime = this.javaTime;
                if (javaTime == null) {
                    throw new IllegalStateException("typed rows read on a session no plan target was checked against:"
                            + " the driver's timestamp re-read is its database's fact");
                }
                while (rs.next()) {
                    cells.row(JdbcDecode.row(rs, columns, javaTime));
                }
            }
        } catch (SQLException e) {
            throw PlanSessions.dataError(e, c);
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
            throw PlanSessions.dataError(e, c);
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
            RuntimeException failure = PlanSessions.dataError(e, c);
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
            throw PlanSessions.dataError(e, c);
        }
    }

    /** The connection set back after a snapshot: rolled back first when the work failed, its isolation (where the
     *  snapshot set one) and its auto-commit as they were. */
    private void restore(ExecutionPlan.Keyed.Snapshot snapshot, boolean autoCommit, int isolation, boolean rollback)
            throws SQLException {
        if (rollback) {
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
                    case PlanParameters.None none -> st.setNull(index, PlanRunner.nullType(one, slot));
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
