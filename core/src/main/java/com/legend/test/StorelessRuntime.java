// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.test;

import com.legend.model.ConnectionDefinition.DatabaseType;

/**
 * The runtime a STORELESS query executes on -- an expression over literals, which reads no store
 * (the PCT suites, Channel B, the language-semantics tests). A query executes on a runtime's
 * declared connection ({@code Compiler.executesOn}), so a storeless one is given a runtime the way
 * legend-engine's PCT adapter gives it one ({@code testAdapterForRelationalExecution}: an empty
 * {@code MyDatabase} bound to {@code getTestConnection(DatabaseType.X)} in a {@code Runtime}): an
 * empty database, bound to a connection declaring the database the caller's session is on.
 *
 * <p>The connection's specification describes that session; the caller opens it and hands it to
 * the execution entry, which checks it against the declared type.
 */
public final class StorelessRuntime {

    /** The runtime's name, to execute with. */
    public static final String RUNTIME = "storeless::Runtime";

    private StorelessRuntime() {
    }

    /** {@code model} with the storeless runtime for {@code type}, an in-process database, declared beside it. */
    public static String with(String model, DatabaseType type) {
        return model + "\n" + declaration(type);
    }

    /** {@code model} with the storeless runtime on the {@code type} SERVER at {@code host}:{@code port}, database
     *  {@code database} — the session the caller opens, declared as it is (C3b: a server's coordinates are real). */
    public static String onServer(String model, DatabaseType type, String host, int port, String database) {
        return model + "\n" + runtime(type, "specification: Static { name: '" + database + "'; host: '" + host
                + "'; port: " + port + "; };\n  auth: Test");
    }

    /** The empty database, its connection on the in-process {@code type}, and the runtime binding them. */
    public static String declaration(DatabaseType type) {
        return runtime(type, specificationAndAuth(type));
    }

    private static String runtime(DatabaseType type, String specificationAndAuth) {
        return """
                ###Relational
                Database storeless::Store ( )
                ###Connection
                RelationalDatabaseConnection storeless::Connection
                {
                  store: storeless::Store;
                  type: %s;
                  %s;
                }
                ###Runtime
                Runtime storeless::Runtime
                {
                  mappings: [];
                  connections: [ storeless::Store: [ session: storeless::Connection ] ];
                }
                """.formatted(type, specificationAndAuth);
    }

    /** The connection's specification and authentication, as the in-process session on {@code type} is opened. */
    private static String specificationAndAuth(DatabaseType type) {
        return switch (type) {
            case DuckDB -> "specification: DuckDB { };\n  auth: Test";
            case H2 -> "specification: LocalH2 { };\n  auth: DefaultH2";
            case SQLite -> "specification: SQLite { };\n  auth: Test";
            case Postgres -> throw new IllegalArgumentException("Postgres is a server: its storeless runtime"
                    + " declares the session's coordinates (onServer)");
            case DB2, MemSQL, Sybase, SybaseIQ, Composite, SqlServer, Hive, Snowflake, Presto, Trino, BigQuery,
                 Redshift, Databricks, Spanner, Athena, Oracle, ClickHouse, Aurora ->
                    throw new IllegalArgumentException("no storeless runtime on " + type
                            + ": the platform does not execute on it");
        };
    }
}
