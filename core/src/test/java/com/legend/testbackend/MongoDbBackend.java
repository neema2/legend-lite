// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testbackend;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.test.StorelessRuntime;
import com.legend.testing.EmbeddedMongoDb;

import java.sql.Connection;

/**
 * MongoDB's sessions (docs/MONGODB_PLAN_2026_10_16.md §6; a backend of its own since main's dialect program Phase 6
 * merged in, 2026-10-22, the PCT harness's lane until then): this JVM's embedded MongoDB 8.0, reached by its connector
 * alone. It has no JDBC session and no system database: a query on it runs through its plan, on the session the
 * connector opens from the storeless runtime's {@code MongoDBConnection}, whose password is the system property
 * {@link #PASSWORD_PROPERTY} (the engine's {@code SystemPropertiesSecret}).
 */
final class MongoDbBackend implements TestBackend {

    static final MongoDbBackend INSTANCE = new MongoDbBackend();

    /** The system property the storeless runtime's connection reads its password from. */
    static final String PASSWORD_PROPERTY = "legend.mongodb.pct.password";

    private MongoDbBackend() {
    }

    @Override
    public DatabaseType type() {
        return DatabaseType.MongoDB;
    }

    @Override
    public String name() {
        return "mongodb";
    }

    /** Its name: what a parameterised test shows. */
    @Override
    public String toString() {
        return name();
    }

    /** None: a MongoDB speaks no JDBC (D12). */
    @Override
    public Connection open() {
        throw new IllegalStateException("the MongoDB backend has no JDBC session: a query on it runs through its plan,"
                + " on the session its connector opens");
    }

    @Override
    public boolean inProcess() {
        return false;
    }

    /** None: the metamodel store on MongoDB is refused by name (D13). */
    @Override
    public Connection openSystem() {
        throw new IllegalStateException("the MongoDB backend has no system database: the metamodel store on MongoDB is"
                + " refused by name (docs/MONGODB_PLAN_2026_10_16.md D13)");
    }

    /** None: its connection is legend-engine's {@code MongoDBConnection}, no {@code RelationalDatabaseConnection}'s
     *  body ({@link #withStorelessRuntime} declares it). */
    @Override
    public String specificationAndAuth() {
        throw new IllegalStateException("a MongoDB connection is a MongoDBConnection, with no relational"
                + " specification: its storeless runtime declares the server's coordinates (withStorelessRuntime)");
    }

    /** {@inheritDoc} The embedded server's coordinates in a {@code MongoDBConnection}, its user's password set as
     *  {@link #PASSWORD_PROPERTY}. */
    @Override
    public String withStorelessRuntime(String model) {
        System.setProperty(PASSWORD_PROPERTY, EmbeddedMongoDb.PASSWORD);
        // legend-engine's own MongoDBConnection naming the server, its database and a user whose password the system
        // property holds (the engine's SystemPropertiesSecret; a MongoDBConnection always declares an authentication)
        return StorelessRuntime.declaringConnection(model, "MongoDB", String.format(java.util.Locale.ROOT, """
                MongoDBConnection storeless::Connection
                {
                  database: %s;
                  store: storeless::Store;
                  serverURLs: [127.0.0.1:%d];
                  authentication: # UserPassword {
                    username: '%s';
                    password: SystemPropertiesSecret
                    {
                      systemPropertyName: '%s';
                    };
                  }#;
                }""", EmbeddedMongoDb.DATABASE, EmbeddedMongoDb.shared().port(), EmbeddedMongoDb.USER,
                PASSWORD_PROPERTY));
    }
}
