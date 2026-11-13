// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

/**
 * Classes PLANTED in exec's package, outside the session seam, for one test alone (the third re-audit, 2026-11-12):
 * {@code ArchitectureTest.theDoorsRuleBitesAPlantedCaller} shows
 * {@code ArchitectureTest.theRunnerIsTheOneCallerOfTheSessionsStatements} fails on each. Test classes: the rule's real
 * check reads the product's classes, never these.
 */
public final class PlantedSessionCaller {

    private PlantedSessionCaller() {
    }

    /** Sends through a session's statements, past the runner. */
    public static final class Sends {
        private Sends() {
        }

        public static boolean sent(Statements st, com.legend.executionplan.PlannedSql statement) {
            return st.execute(statement);
        }
    }

    /** Takes a session's statements. */
    public static final class Takes {
        private Takes() {
        }

        public static Statements taken(Sessions.Session session) {
            return session.statements();
        }
    }
}
