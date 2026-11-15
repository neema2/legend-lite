// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

/**
 * A host door PLANTED for one test alone (the C4 audit's M3, 2026-11-13): a {@code jsonPlan} that mints its execution's
 * id twice -- once by a call, once by a method reference -- which {@code ArchitectureTest.theDoorMintCountSeesAMethodReference}
 * shows the one-mint count reads as two. A test class: the rule's real run reads the product's classes alone.
 */
public final class PlantedDoorMints {

    private PlantedDoorMints() {
    }

    /** Two ids for one plan: the defect the count names. */
    public static String jsonPlan() {
        java.util.function.Supplier<java.util.UUID> again = java.util.UUID::randomUUID;
        return java.util.UUID.randomUUID() + " " + again.get();
    }
}
