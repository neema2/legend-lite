// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testbackend;

import com.legend.database.Databases;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.sql.dialect.SqlDialect;
import com.legend.test.StorelessRuntime;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * One database as the tests run on it (the dialect program's Phase 6, docs/DIALECT_LEAK_CENSUS_AND_PLAN_2026_10_12.md):
 * a session of its own, the storeless runtime a model declares for that session, and, for a server, the system
 * connection the platform's system database lives on. The tests' counterpart of a connector: each database's harness
 * knowledge in a class of its own ({@code DuckDbBackend}, {@code PostgresBackend} ...), {@link TestBackends} the one
 * place a test finds it. A test names no database: it takes {@link TestBackends#platform()}, the backend a lane names,
 * or every one ({@link TestBackends#all()}).
 */
public interface TestBackend {

    /** The database type its sessions are on. */
    DatabaseType type();

    /** Its name as a lane spells it ({@code LEGENDLITE_PCT_BACKEND}): lower case. */
    String name();

    /** A session on it, for one test: in-process, a private in-memory instance; a server, a new session on the test
     *  server's database; a cloud, the suite's one session (closing it is a no-op). */
    Connection open() throws SQLException;

    /** {@link #open()} as a session the runner owns: closing it closes the connection (D68, 2026-10-30: the test
     *  runners hold sessions, not connections); a connection the driver refuses is a {@code DataError} (a backend
     *  that opens no JDBC session refuses by its own name, as before). */
    default com.legend.exec.Sessions.Session openSession() {
        try {
            return TestSessions.owning(open());
        } catch (SQLException e) {
            throw new com.legend.error.DataError(String.valueOf(e.getMessage()), e);
        }
    }

    /** Whether it runs in-process (the platform opens its own private instances of it), or is a server. */
    boolean inProcess();

    /** The statements a test runs on a fresh session of this backend so the suite's answers are deterministic
     *  (the platform's: its clock in UTC); none for a backend whose session takes the launcher's one zone (track T,
     *  T-d, 2026-10-29: the decision is the backend's, never a test's). */
    default java.util.List<String> sessionSetUp() {
        return java.util.List.of();
    }

    /** A statement answering the year of this backend's own clock, as text (a test of {@code now()} reads it either
     *  side of its query); ANSI's {@code EXTRACT}, which SQLite has not (T-d2, 2026-10-29). */
    default String yearOfNowSql() {
        return "SELECT CAST(EXTRACT(YEAR FROM CURRENT_TIMESTAMP) AS VARCHAR)";
    }

    /** A NEW session for the platform's system database on this server (closing it closes it). An in-process backend
     *  has none: the platform opens its own private instance. */
    Connection openSystem() throws SQLException;

    /** The suite's one held session ({@link #open}'s: closing it is a no-op, the JVM's exit closes it), where a login is
     *  dear (a cloud), or null where a new session per use costs nothing worth holding (the servers in this process's
     *  reach; an in-process backend opens its own). The PCT harness runs channel A's plans on it where they make no
     *  table of the session's own, as the merge-base's route ran every case on it. 2026-10-21: a login per plan cost
     *  every test on the Snowflake lane a second or more, its suites two to three times the merge-base's. 2026-10-22:
     *  the held SYSTEM session that replaced it was BigQuery's with {@code EnableSession}, which doubles each query's
     *  latency (216 ms against 421 ms for {@code SELECT 1}): BigQuery's channel A was 1.8 to 2.4 times the
     *  merge-base's, all of it inside BigQuery (measured on the lane at 78bd5da3). */
    default @com.legend.base.Nullable Connection held() throws SQLException {
        return null;
    }

    /** The specification and authentication a connection declares for the session {@link #open} opens (a
     *  {@code RelationalDatabaseConnection}'s body after its {@code type}): an in-process database's in-memory
     *  specification, a server's coordinates, a cloud's coordinates with its credential by reference. */
    String specificationAndAuth();

    /** A connection's body for the session {@link #open} opens, as a {@code RelationalDatabaseConnection} declares it:
     *  its type, specification and authentication, each ending in {@code ;}. */
    default String connectionBody() {
        return "type: " + type() + ";\n  " + specificationAndAuth() + ";";
    }

    /** {@code model} with the storeless runtime on this backend declared beside it: the session {@link #open} opens. */
    default String withStorelessRuntime(String model) {
        return StorelessRuntime.declaring(model, type(), specificationAndAuth());
    }

    /** The dialect its queries are written in. */
    default SqlDialect dialect() {
        return Databases.dialect(type());
    }

    /** A corpus WORKSPACE on it (the relational corpus's package session): a session whose tables are its own, beside
     *  other workspaces, and gone when it closes. Refused by name where none is built. */
    default Connection openWorkspace() throws SQLException {
        throw new com.legend.error.NotImplementedException("no corpus workspace on the test backend '" + name() + "'");
    }

    /** Sets a clashing fixture ASIDE in {@code workspace}, a connection {@link #openWorkspace} opened: the fixture's
     *  tables apart from the workspace's own, read after them once it has run. Refused by name where this backend
     *  cannot isolate one: no fixture runs unisolated over a workspace's tables (the plan's defect 5). */
    default FixtureAside setFixtureAside(Connection workspace) throws SQLException {
        throw new com.legend.error.NotImplementedException("no fixture isolation on the test backend '" + name()
                + "': an on-demand fixture would run over the workspace's own tables");
    }

    /** A second reader of {@code session}'s data, a connection of its own (the referee reads our side's rows while
     *  {@code session} may hold an open result). Refused, as an {@link SQLException} the referee declines by, where
     *  this backend has none. */
    default Connection readerOn(Connection session) throws SQLException {
        throw new SQLException("no second reader of a session on the test backend '" + name() + "'");
    }
}
