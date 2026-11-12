// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.result;

/**
 * A class PLANTED in the result library's package to mint an id, for one test alone (the C final audit's J5, 2026-11-12): an
 * {@code ArchitectureTest} rule fails on it. A test class: the rule's real check reads the product's classes, never
 * this one.
 */
public final class PlantedWriterValue {

    private PlantedWriterValue() {
    }

    /** An id minted outside a door: the defect the rule refuses. */
    public static java.util.UUID minted() {
        return java.util.UUID.randomUUID();
    }
}
