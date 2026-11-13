// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Classes PLANTED in exec's package to send a JDBC statement of their own, for one test alone (the third re-audit's
 * MUST-FIX 1, 2026-11-12): {@code ArchitectureTest.theJdbcRuleBitesAPlantedSend} shows
 * {@code ArchitectureTest.jdbcSendsAtOneChokePoint} fails on each. Test classes: the rule's real check reads the
 * product's classes, never these.
 */
public final class PlantedJdbcSend {

    private PlantedJdbcSend() {
    }

    /** Makes a statement and executes text exec wrote: the version check's shape, on JDBC. */
    public static final class Executes {
        private Executes() {
        }

        public static boolean sent(Connection c) throws SQLException {
            return c.createStatement().execute("SET search_path TO x");
        }
    }

    /** Prepares text exec wrote. */
    public static final class Prepares {
        private Prepares() {
        }

        public static java.sql.PreparedStatement prepared(Connection c) throws SQLException {
            return c.prepareStatement("SELECT 'exec wrote this'");
        }
    }

    /** Names a statement's execution by reference. */
    public static final class ByReference {
        private ByReference() {
        }

        /** A function that runs text on a statement. */
        public interface Runs {
            boolean run(java.sql.Statement st, String text) throws SQLException;
        }

        public static Runs runs() {
            return java.sql.Statement::execute;
        }
    }
}
