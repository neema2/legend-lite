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

    /** {@code model} with the storeless runtime on {@code type}, its connection declared by
     *  {@code specificationAndAuth} -- a cloud warehouse's coordinates and its credential's vault REFERENCE, never
     *  the secret -- for the session the caller opens (the cloud PCT lanes; the session is checked against
     *  {@code type}, as every session is). */
    public static String declaring(String model, DatabaseType type, String specificationAndAuth) {
        return model + "\n" + runtime(type, specificationAndAuth);
    }

    /** The empty database, its connection on the in-process {@code type}, and the runtime binding them. */
    public static String declaration(DatabaseType type) {
        return runtime(type, specificationAndAuth(type));
    }

    /** {@code model} with the storeless runtime over an empty store of the section {@code storeSection}, its connection
     *  {@code connection}'s declaration (a store and a connection that are not relational: a MongoDB's, whose test
     *  backend owns their text since main's merge, 2026-10-22, docs/MONGODB_PLAN_2026_10_16.md D4). The connection is
     *  named {@code storeless::Connection} and reads {@code storeless::Store}. */
    public static String declaringConnection(String model, String storeSection, String connection) {
        return model + "\n###" + storeSection + "\nDatabase storeless::Store ( )\n###Connection\n" + connection + """

                ###Runtime
                Runtime storeless::Runtime
                {
                  mappings: [];
                  connections: [ storeless::Store: [ session: storeless::Connection ] ];
                }
                """;
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

    /** The connection's specification and authentication, as the in-process session on {@code type} is opened: its
     *  connector's ({@code exec.DatabaseConnector.storelessDeclaration}). */
    private static String specificationAndAuth(DatabaseType type) {
        return com.legend.exec.Connectors.of(type).storelessDeclaration();
    }
}
