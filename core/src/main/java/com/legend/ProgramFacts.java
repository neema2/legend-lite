// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

/**
 * What the platform STATES about a resolved program before running it —
 * the facts a test runner needs and must never derive by reading the
 * program's text itself: whether its statements carry effects (store
 * writes, executions, test-data generation), whether its execution
 * contexts seed inline CSV test data (a session-isolation fact), and
 * whether it calls a verdict function (an assert the statement channel
 * adjudicates). Computed by {@link Compiler#programFacts} in ONE typing
 * pass.
 */
public record ProgramFacts(boolean effects, boolean seedsInlineCsv, boolean verdicts,
        java.util.Set<String> seedsStores, String shape) {

    public ProgramFacts(boolean effects, boolean seedsInlineCsv, boolean verdicts,
            java.util.Set<String> seedsStores) {
        this(effects, seedsInlineCsv, verdicts, seedsStores, "");
    }

    /** The body's SHAPE, one letter per statement in order (block-compiler
     * homework 2026-09-21): {@code F} a let bound to an execute (a frame),
     * {@code L} another let, {@code A} a verdict call, {@code X} an assertError
     * (a verdict over a raise), {@code E} an effect (a raw statement, DDL, a
     * test-data generator), {@code O} anything else. */
    public String shape() {
        return shape;
    }

    /** The stores (Database FQNs) the program seeds through a typed
     * element reference ({@link com.legend.compiler.spec.SeededStores}):
     * the corpus runner's fixture-on-demand index. */
    public ProgramFacts {
        seedsStores = java.util.Collections.unmodifiableSet(new java.util.LinkedHashSet<>(seedsStores));
    }
}
