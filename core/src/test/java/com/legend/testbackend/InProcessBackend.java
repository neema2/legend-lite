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

    /** A private in-memory instance made with the settings a plan's target on it declares (H2's: the C checkpoint's
     *  SHOULD-FIX 3, 2026-11-12), and the dialect's session statements run on it, as a plan's run runs them first --
     *  counted as the session's: a test's own text sent on it reads the session a plan would. */
    @Override
    public final Connection open() throws SQLException {
        com.legend.executionplan.DatabaseType t = com.legend.planner.ConnectionPlans.type(type());
        com.legend.executionplan.ExecutionPlan.Target target = com.legend.planner.Targets.of(
                new com.legend.executionplan.ExecutionPlan.Database.Platform(t, null), dialect());
        Connection c = Connectors.of(t).openPrivate(target.settings());
        try (java.sql.Statement st = c.createStatement()) {
            for (String statement : target.session()) {
                com.legend.exec.StatementOrigin.count(com.legend.exec.StatementOrigin.SESSION);
                st.execute(statement);
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
