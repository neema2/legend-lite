// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.model.AuthenticationSpec;
import com.legend.model.ConnectionDefinition;
import com.legend.model.ConnectionSpecification;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import java.sql.Connection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * MongoDB's connector (docs/MONGODB_PLAN_2026_10_16.md §5.2, D12; 2026-10-17): beside the JDBC drivers, found as they
 * are ({@link Connectors}, by {@link java.util.ServiceLoader}), it opens a session on a {@code MongoDBConnection} -- its one server, its database,
 * its user and the password the connection's vault reference names -- whose statements ({@link MongoDbStatements})
 * run the pipelines the MongoDb dialect writes. The driver is the client library alone: the platform's own code
 * decides nothing by it, and core compiles against none of it (this source set rides with {@code //core:drivers}).
 */
public final class MongoDbConnector implements DatabaseConnector {

    public MongoDbConnector() {
    }

    @Override
    public ConnectionDefinition.DatabaseType type() {
        return ConnectionDefinition.DatabaseType.MongoDB;
    }

    /** {@inheritDoc} Yes: a MongoDB speaks no JDBC. */
    @Override
    public boolean ownSessions() {
        return true;
    }

    /** {@inheritDoc} None (D12, D14): a JDBC session handed for MongoDB is refused here by name. */
    @Override
    public String jdbcProduct() {
        throw new com.legend.error.NotImplementedException("sessions on MongoDB are not JDBC: its connector opens them,"
                + " through the plan runner");
    }

    /** {@inheritDoc} None: no JDBC opening reaches a MongoDB ({@link #open} does). */
    @Override
    public Sessions.Opening opening(ConnectionDefinition def) {
        throw new com.legend.error.NotImplementedException("connection '" + def.qualifiedName() + "' declares MongoDB,"
                + " which no JDBC opening reaches: its connector opens it, through the plan runner");
    }

    /** {@inheritDoc} No: a MongoDB is a server. */
    @Override
    public boolean inProcess() {
        return false;
    }

    /** {@inheritDoc} Never asked: no JDBC session is a MongoDB's ({@link #ownSessions}). */
    @Override
    public @com.legend.base.Nullable String raisedMessage(Connection session) {
        throw new IllegalStateException("a MongoDB session holds no JDBC raise");
    }

    /** {@inheritDoc} None: a server, whose connection is the engine's MongoDBConnection. */
    @Override
    public String storelessDeclaration() {
        throw new IllegalArgumentException(type() + " is a server, and its connection is the engine's"
                + " MongoDBConnection: its storeless runtime declares the session's coordinates (onMongo)");
    }

    @Override
    public Sessions.Session open(ConnectionDefinition connection) {
        if (!(connection.specification() instanceof ConnectionSpecification.StaticDatasource(String host, int port,
                String database))) {
            throw new com.legend.error.NotImplementedException("connection '" + connection.qualifiedName()
                    + "' names its MongoDB by a " + connection.specification().getClass().getSimpleName()
                    + ": only a server (host, port, database) is opened");
        }
        MongoCredential credential = switch (connection.authentication()) {
            case AuthenticationSpec.UsernamePassword up -> MongoCredential.createCredential(up.username(), database,
                    secret(connection, up.passwordVaultRef()).toCharArray());
            default -> throw new com.legend.error.NotImplementedException("connection '" + connection.qualifiedName()
                    + "' authenticates by " + connection.authentication().getClass().getSimpleName()
                    + ", which the MongoDB connector does not open (a user name and a password are)");
        };
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyToClusterSettings(b -> b.hosts(List.of(new ServerAddress(host, port)))
                        .serverSelectionTimeout(30, TimeUnit.SECONDS))
                .credential(credential)
                .build();
        MongoClient client = MongoClients.create(settings);
        return new MongoDbSession(client, new MongoDbStatements(client.getDatabase(database)));
    }

    /** The password a vault reference names: {@code systemproperties:<name>} reads the JVM's system property, set by
     *  the host; any other vault is refused by name. The value is handed to the driver and never carried further. */
    private static String secret(ConnectionDefinition connection, String reference) {
        int colon = reference.indexOf(':');
        String kind = colon < 0 ? reference : reference.substring(0, colon);
        if (!"systemproperties".equals(kind)) {
            throw new com.legend.error.NotImplementedException("connection '" + connection.qualifiedName()
                    + "' keeps its password in a '" + kind + "' vault, which is not read (a system property is)");
        }
        String name = reference.substring(colon + 1);
        String value = System.getProperty(name);
        if (value == null) {
            throw new IllegalStateException("connection '" + connection.qualifiedName() + "' reads its password from"
                    + " the system property '" + name + "', which is not set");
        }
        return value;
    }

    /** A session on a MongoDB: no JDBC connection, its statements the connector's; closing it closes the client. */
    private static final class MongoDbSession implements Sessions.Session {
        private final MongoClient client;
        private final MongoDbStatements statements;

        MongoDbSession(MongoClient client, MongoDbStatements statements) {
            this.client = client;
            this.statements = statements;
        }

        @Override
        public Connection connection() {
            throw new IllegalStateException("a MongoDB session has no JDBC connection: it runs the plan's commands"
                    + " through its own statements (Sessions.Session.statements)");
        }

        @Override
        public Statements statements() {
            return statements;
        }

        @Override
        public void close() {
            client.close();
        }
    }
}
