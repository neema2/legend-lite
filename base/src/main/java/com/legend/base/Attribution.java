// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.base;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The test a measurement is attributed to: the one label a census's samples carry, named by the harness that runs the
 * test ({@link #to}). Measurement only, never read by a decision. It lives in base so every census reads the same label
 * whatever library it measures in (lowering's stamp census, the decision probes, the canonical instruments) -- where
 * five censuses each held a hook of their own (2026-11-09, no mutable static state but the register's:
 * {@code ArchitectureTest.staticStateIsConstantOrRegistered}, kind MEASUREMENT).
 */
public final class Attribution {

    private static final AtomicReference<String> LABEL = new AtomicReference<>("<unattributed>");

    private Attribution() {
    }

    /** The harness names the test it runs now. */
    public static void to(String test) {
        LABEL.set(Objects.requireNonNull(test, "test"));
    }

    /** The test the measurement is attributed to now. */
    public static String now() {
        return LABEL.get();
    }
}
