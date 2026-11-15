// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

/**
 * A class PLANTED to catch and continue past an IllegalStateException, for one test alone (C5-J item 4, 2026-11-15):
 * {@code ArchitectureTest.broadCatches} names it. A test class, never a product planner.
 */
public final class PlantedBroadCatch {

    private PlantedBroadCatch() {
    }

    public static boolean tolerates(Runnable step) {
        try {
            step.run();
            return true;
        } catch (IllegalStateException e) {
            return false;
        }
    }
}
