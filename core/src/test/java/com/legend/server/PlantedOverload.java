// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

/**
 * Classes PLANTED in the server's package for one test alone (the C4 final fix round's T3, 2026-11-14):
 * {@code ArchitectureTest.PLANNER_ENTRIES_ONLY} lists each entry of the planners by its full signature with its callers,
 * so it fails on each -- an overload of a listed name that is not listed for its caller, and a listed entry reached
 * from a class that is not its caller. Test classes: the rule's real checks read the product's classes and the hosts'
 * doors, never these.
 */
public final class PlantedOverload {

    private PlantedOverload() {
    }

    /** QueryDoor's refusal of a typed body's executeInDb read, the root Compiler's overload, from the server's package
     *  (the test-data routes' entry is the typed query's overload). */
    public static final class AnotherOverload {
        private AnotherOverload() {
        }

        public static void refused() {
            com.legend.planner.QueryDoor.refuseRawSql(java.util.List.of(), null);
        }
    }

    /** The test-data routes' listed entry, from a class that is not the routes. */
    public static final class AnotherCaller {
        private AnotherCaller() {
        }

        public static void refused(com.legend.TypedQuery query) {
            com.legend.planner.QueryDoor.refuseRawSql(query);
        }
    }
}
