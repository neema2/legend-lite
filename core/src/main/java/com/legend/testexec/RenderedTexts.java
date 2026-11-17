// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

/**
 * WHETHER A RENDERED TEXT THAT DIFFERS FROM ITS GOLDEN IS HELD (census C1, the orchestrator's ruling of 2026-11-17:
 * the run hook decides, never the rules): asked of the run's hearing ({@link EdgeHearing#renderedTexts}). Lite's own
 * runs answer {@link Product#STRINGS}: the two strings are the verdict (the user, 2026-11-05: after a verdict the
 * harness may only explain); the edge's form compares upstream's ruled way (S131, S132).
 */
public interface RenderedTexts {

    /** Lite's form: two strings that differ are never held. */
    enum Product implements RenderedTexts {
        STRINGS;

        @Override
        public boolean held(String expected, String actual, String form, boolean sorted) {
            return false;
        }
    }

    /** Whether {@code expected} and {@code actual}, two texts of render {@code form} that differ, are held; {@code sorted}:
     *  the chain's rows have an order. */
    boolean held(String expected, String actual, String form, boolean sorted);
}
