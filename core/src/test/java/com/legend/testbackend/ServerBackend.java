// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testbackend;

import com.legend.exec.Sessions;
import com.legend.model.AuthenticationSpec;
import com.legend.model.ConnectionDefinition;
import com.legend.model.ConnectionSpecification;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/** A database the tests run on an embedded server of this JVM ({@code com.legend.testing}): each session a new one on
 *  the server's test database, the system connection a session of the same server, and the storeless runtime the
 *  server's coordinates. */
abstract class ServerBackend implements TestBackend {

    /** The address every embedded server of this JVM listens on. */
    static final String HOST = "127.0.0.1";

    /** The server's port (starting the server on first use). */
    abstract int port();

    /** The test database a session is on. */
    abstract String database();

    /** A new session on the server's database {@code database}. */
    abstract Connection session(String database) throws SQLException;

    /** The platform's system connection: by default a session like any other. */
    Connection system() throws SQLException {
        return session(database());
    }

    /**
     * A session on {@code database} opened as the PLATFORM opens one: at the URL this database's connector makes from a
     * static datasource on this server ({@code exec.Sessions.openingFor}), with only the launcher's {@code settings}
     * (its user, its own options) beside it. The coordinates a model declares for this backend
     * ({@link #specificationAndAuth}) and the session a test runs on are then one route, by construction: a connector
     * whose URL left the server would fail every session here (the dialect program's Phase 6, 2026-10-17: each
     * backend opened its launcher's own URL, a second route the declared coordinates were never checked against).
     */
    final Connection openedAsThePlatformOpens(String database, Properties settings) throws SQLException {
        ConnectionDefinition declared = new ConnectionDefinition("test::" + name(), null, type(),
                new ConnectionSpecification.StaticDatasource(HOST, port(), database), new AuthenticationSpec.TestAuth());
        if (!(Sessions.openingFor(declared) instanceof Sessions.Opening.Url url)) {
            throw new IllegalStateException(type() + "'s connector opens no URL for a static datasource");
        }
        return DriverManager.getConnection(url.url(), settings);
    }

    /** Its name: what a parameterised test shows. */
    @Override
    public final String toString() {
        return name();
    }

    @Override
    public final Connection open() throws SQLException {
        return session(database());
    }

    @Override
    public final boolean inProcess() {
        return false;
    }

    @Override
    public final Connection openSystem() throws SQLException {
        return system();
    }

    /** The server's coordinates, as they are (C3b): a static datasource on this JVM's server. */
    @Override
    public String specificationAndAuth() {
        return "specification: Static { name: '" + database() + "'; host: '" + HOST + "'; port: " + port() + "; };\n"
                + "  auth: Test";
    }
}
