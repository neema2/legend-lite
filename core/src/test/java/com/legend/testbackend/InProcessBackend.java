// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testbackend;

import com.legend.exec.Connectors;
import com.legend.exec.Sessions;

import java.sql.Connection;
import java.sql.SQLException;

/** A database the platform runs in-process: each session a private in-memory instance, as its connector opens one
 *  ({@code Sessions.openPrivate}), and the storeless runtime its in-memory specification. */
abstract class InProcessBackend implements TestBackend {

    /** Its name: what a parameterised test shows. */
    @Override
    public final String toString() {
        return name();
    }

    /** A private instance, its session as a plan's target opens one: its dialect's session statements run first (the
     *  C checkpoint's SHOULD-FIX 3, 2026-11-12: H2's settings are plan statements, no longer on its connector's URL, and
     *  a harness that seeds the session by its own text needs them as a plan's run does; idempotent, a plan's first
     *  statement runs them again). */
    @Override
    public final Connection open() throws SQLException {
        return withSession(com.legend.exec.Connectors.of(com.legend.planner.ConnectionPlans.type(type())).openPrivate(),
                dialect());
    }

    /** {@code c} with {@code dialect}'s session statements run on it; closed if one fails. */
    static Connection withSession(Connection c, com.legend.sql.dialect.SqlDialect dialect) throws SQLException {
        try (java.sql.Statement st = c.createStatement()) {
            for (String s : dialect.sessionSetup()) {
                com.legend.exec.StatementOrigin.count(com.legend.exec.StatementOrigin.SESSION);
                st.execute(s);
            }
        } catch (SQLException e) {
            c.close();
            throw e;
        }
        return c;
    }

    @Override
    public final boolean inProcess() {
        return true;
    }

    @Override
    public final Connection openSystem() {
        throw new IllegalStateException(name() + " is in-process: the platform's system database is its own private"
                + " instance, on no system connection");
    }

    /** Its connector's in-memory specification ({@code exec.DatabaseConnector.storelessDeclaration}). */
    @Override
    public final String specificationAndAuth() {
        return Connectors.of(com.legend.planner.ConnectionPlans.type(type())).storelessDeclaration();
    }
}
