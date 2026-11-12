// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.DatabaseType;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * One database's sessions (the dialect program's Phase 5, docs/DIALECT_LEAK_CENSUS_AND_PLAN_2026_10_12.md): the product
 * name its JDBC driver reports, how a connection declared on it is opened, whether it has a private in-process
 * instance, and what its sessions hold that the platform reads back. The execution side's counterpart of a dialect:
 * each database's knowledge in a class of its own ({@code DuckDbConnector}, {@code H2Connector} ...), and
 * {@link Connectors} the one place a declared type finds its connector. {@link Sessions} decides nothing by a database
 * of its own; it asks the connector.
 */
public interface DatabaseConnector {

    /** The database type this connector opens sessions on. */
    DatabaseType type();

    /** The product name its JDBC driver reports ({@code DatabaseMetaData.getDatabaseProductName}): what a session
     *  handed to execution is checked against. */
    String jdbcProduct();

    /** How {@code def}, a connection declared on this database, is opened; a specification this database is not opened
     *  from is refused by name ({@link #refused}). Its authentication is checked before
     *  ({@link Sessions#openingFor}). */
    Sessions.Opening opening(com.legend.executionplan.Connection def);

    /** Whether it has a private in-process instance ({@link #openPrivate}): the engines the process's catalog is made
     *  on (B4, 2026-11-11: a server has none, and its statements reading the system store run on the platform's own
     *  engine's catalog). Each connector answers: a default would make a catalog on a server. */
    boolean inProcess();

    /** A PRIVATE in-memory instance, gone when the connection closes (the system database, the platform's engine);
     *  refused by name for a database with none ({@link #inProcess} false). */
    default Connection openPrivate() throws SQLException {
        throw new com.legend.error.NotImplementedException("no in-memory instance of database type '" + type() + "'");
    }

    // (raiseRecovery, the two statements that read back a raise a session holds: the PLAN's since C2, 2026-11-12 --
    // SqlDialect.raiseReadBack, carried by ExecutionPlan.Target.raiseReadBack and sent by the seam after a failure, the
    // charter's A3(c); a connector declares no statement)

    /** Whether this database ABORTS its whole transaction when a statement in it fails, refusing every statement after
     *  it until a rollback (DuckDB, Postgres), where another rolls back the failed statement alone (H2, SQLite): a run's
     *  turn on the catalog is rolled back at once after a failure where it aborts, so the run's next statement runs
     *  (docs/DATA_DELIVERY_DESIGN_2026_11_10.md §3 rule 3, A4). Each connector answers. */
    boolean abortsTransactionOnError();

    /** Whether this database's driver TAKES a transaction the platform opens across statements (autocommit off, then a
     *  commit or a rollback). ClickHouse's refuses one ("setAutoCommit(false) is not supported", measured 2026-11-07 on
     *  the timeseries line, PR #81): there a load's steps and a program's statements run unbracketed, each its own, and
     *  a load's no-leftovers rule is its drop first and its drop at release (docs/DATA_DELIVERY_DESIGN_2026_11_10.md §3
     *  rule 1, the orchestrator's check of 2026-11-10). Each connector answers. */
    boolean takesTransactions();

    /** The facts of this database's JDBC driver that reading a row back needs and no plan column's slot can say
     *  (D72, 2026-10-30; until then four {@code SqlDialect} hooks the runtime read through the plan side -- a driver's
     *  fact is the connector's, the execution side's counterpart of a dialect): whether the driver re-fetches a
     *  timestamp and a date cell as {@code java.time} (the BC-faithful carriers the reader reads every timestamp and
     *  date as; measured 2026-10-17 on pgjdbc, DuckDB's and H2's drivers, the other drivers' JDBC 4.2 support of
     *  LocalDate first measured in their cloud lanes), whether it hands a decimal back with its trailing zeros
     *  dropped (padded back to the column's declared scale, never rounded), whether it opens the result of a
     *  statement that fails before its columns are known with none (its error on the first read: a result with no
     *  columns is then no claim to check), and whether the database's JSON writes a double that is not finite as a
     *  bare token ({@code NaN}, {@code Infinity}, {@code -Infinity}: read as that double, written back as read,
     *  {@link NonFiniteJson}). Default: re-fetches, keeps the zeros, reports its columns, writes JSON alone. */
    default Decoding decoding() {
        return new Decoding(true, false, false, false);
    }

    /** {@link #decoding}'s four facts, in the order the reader reads them (the connector's own record since the D72
     *  audit: the decoder's package-private class had held it, a type no caller outside exec could name). */
    record Decoding(boolean javaTime, boolean dropsDecimalZeros, boolean failedWithoutColumns,
            boolean nonFiniteJson) {
    }

    /** Whether its sessions are its own and not JDBC's (MongoDB's, docs/MONGODB_PLAN_2026_10_16.md D12, 2026-10-17):
     *  {@link #open} opens one on a declared connection, its credential read by the connector and carried no further,
     *  and nothing reads a JDBC product, an opening or a held raise from it. */
    default boolean ownSessions() {
        return false;
    }

    /** A session of its own on {@code def} ({@link #ownSessions}), owned by the caller: closing it releases it. A
     *  specification or an authentication the connector does not open is refused by name. */
    default Sessions.Session open(com.legend.executionplan.Connection def) {
        throw new IllegalStateException(type() + "'s sessions are JDBC's, opened by Sessions.openingFor (connection '"
                + def.qualifiedName() + "')");
    }

    /** The specification and authentication a storeless runtime declares for this database's in-process session
     *  ({@code testbackend.StorelessRuntime}, the tests'), in the model's connection grammar. */
    default String storelessDeclaration() {
        throw new IllegalArgumentException("no storeless runtime on " + type()
                + ": the platform does not execute on it");
    }

    /** An in-memory database that outlives its connections by NAME ({@link Sessions.Opening.Named}): a connector whose
     *  database keeps one by name. */
    interface NamedMemory {

        /** A connection to the named in-memory database {@code name}, which outlives it. */
        Connection openNamed(String name) throws SQLException;

        /** A connection to the named in-memory database {@code name}, which lives only while a connection to it is
         *  open: its opener keeps this one, and when that one closes with no other open, the database is gone. */
        Connection openKept(String name) throws SQLException;

        /** Another connection to the kept database {@code name} ({@link #openKept}), which must still be open: one
         *  whose keeper closed is gone, and connecting fails by name rather than making an empty database of that
         *  name. */
        Connection joinKept(String name) throws SQLException;
    }

    /** {@code def}'s specification, refused: this database is not opened from it. */
    static Sessions.Opening refused(com.legend.executionplan.Connection def) {
        throw new com.legend.error.NotImplementedException("a " + def.databaseType() + " connection with a "
                + def.specification().getClass().getSimpleName() + " specification is not implemented (connection '"
                + def.qualifiedName() + "')");
    }

    /** {@code def}, refused: the platform opens no declared connection to its database yet (its sessions are the
     *  test harnesses' or a host's). */
    static Sessions.Opening notOpened(com.legend.executionplan.Connection def) {
        throw new com.legend.error.NotImplementedException("connections to database type '" + def.databaseType()
                + "' are not implemented (connection '" + def.qualifiedName() + "')");
    }
}
