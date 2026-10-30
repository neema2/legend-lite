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

    @Override
    public final Connection open() throws SQLException {
        return com.legend.exec.Connectors.of(type()).openPrivate();
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
        return Connectors.of(type()).storelessDeclaration();
    }
}
