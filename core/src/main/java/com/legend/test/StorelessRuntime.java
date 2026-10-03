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

    /** {@code model} with the storeless runtime for {@code type} declared beside it. */
    public static String with(String model, DatabaseType type) {
        return model + "\n" + declaration(type);
    }

    /** The empty database, its connection on {@code type}, and the runtime binding them. */
    public static String declaration(DatabaseType type) {
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
                """.formatted(type, specificationAndAuth(type));
    }

    /** The connection's specification and authentication, as the session on {@code type} is opened. */
    private static String specificationAndAuth(DatabaseType type) {
        return switch (type) {
            case DuckDB -> "specification: DuckDB { };\n  auth: Test";
            case H2 -> "specification: LocalH2 { };\n  auth: DefaultH2";
            case Postgres -> "specification: Static { name: 'storeless'; host: '127.0.0.1'; port: 5432; };\n  auth: Test";
            case SQLite -> "specification: SQLite { };\n  auth: Test";
            case DB2, MemSQL, Sybase, SybaseIQ, Composite, SqlServer, Hive, Snowflake, Presto, Trino, BigQuery,
                 Redshift, Databricks, Spanner, Athena, Oracle, ClickHouse, Aurora ->
                    throw new IllegalArgumentException("no storeless runtime on " + type
                            + ": the platform does not execute on it");
        };
    }
}
