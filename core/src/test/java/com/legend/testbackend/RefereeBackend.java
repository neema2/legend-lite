// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testbackend;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * The database the SQL-text replay referee runs on ({@code Databases.REPLAY_REFEREE}, {@link TestBackends#referee}):
 * legend-engine's golden SQL is its text, run on sessions with the engine's own settings and extension functions, beside
 * a corpus session on any database. Its duties beyond a backend's: the referee's own sessions, and a mirror's replay of
 * a fixture the corpus set aside.
 */
public interface RefereeBackend extends TestBackend {

    /** Whether its driver is on this classpath; without it the referee declines every replay, by name. */
    boolean available();

    /** A NEW referee session: a private in-memory instance named {@code name} (each mirror and replay names its own,
     *  so that none sees another's tables), with the engine's session settings and extension functions. */
    Connection openReferee(String name) throws SQLException;
}
