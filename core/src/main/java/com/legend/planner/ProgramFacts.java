// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

/**
 * What the platform STATES about a resolved program before running it —
 * the facts a test runner needs and must never derive by reading the
 * program's text itself: whether its statements carry effects (store
 * writes, executions, test-data generation), whether its execution
 * contexts seed inline CSV test data (a session-isolation fact), and
 * whether it calls a verdict function (an assert the statement channel
 * adjudicates). Computed by {@link #of} in ONE typing pass, for the
 * compiler's effect question ({@link Compiler#hasStatementEffects}) and the
 * test planner ({@code testplan.TestPlanner.facts}, whose record the test
 * plan carries: C5-J item 2, 2026-11-15; the compiler's public door to them
 * all, {@code Compiler.programFacts}, went with C5-J's last round, A3, once
 * no caller was left).
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

    /** The platform's facts about a program, in one typing pass: {@code body} its statements as {@code specs} typed them
     *  (moved here from {@code Compiler.programFacts}, C5-J item 2, 2026-11-15: the compiler and the test planner
     *  state them alike). */
    public static ProgramFacts of(java.util.List<com.legend.compiler.spec.typed.TypedSpec> body,
            com.legend.compiler.spec.SpecCompiler specs) {
        java.util.Map<com.legend.model.FunctionId, Boolean> memo = new java.util.HashMap<>();
        java.util.Map<com.legend.model.FunctionId, Boolean> verdictMemo = new java.util.HashMap<>();
        java.util.Map<com.legend.model.FunctionId, java.util.Set<String>> storeMemo = new java.util.HashMap<>();
        boolean effects = false;
        boolean seeds = false;
        boolean verdicts = false;
        java.util.Set<String> stores = new java.util.LinkedHashSet<>();
        StringBuilder shape = new StringBuilder(body.size());
        for (int i = 0; i < body.size(); i++) {
            com.legend.compiler.spec.typed.TypedSpec s = body.get(i);
            java.util.List<com.legend.compiler.spec.typed.TypedSpec> preceding = body.subList(0, i);
            boolean effect = com.legend.compiler.spec.StatementEffects.containsEffect(s, specs, memo)
                    || com.legend.compiler.spec.StatementEffects.containsTdgGenerator(s);
            boolean verdict = com.legend.compiler.spec.StatementEffects.callsVerdict(s, specs, verdictMemo);
            shape.append(statementKind(s, effect, verdict));
            effects |= effect;
            stores.addAll(com.legend.compiler.spec.SeededStores.of(s, specs, storeMemo));
            // the ONE reader of runtime shapes: inline CSV test data anywhere
            // in the statement (a from(), an execute's runtime argument, a
            // let-bound connection copy) is a bound-context fact; values the
            // statement names chase its preceding lets
            seeds |= !com.legend.compiler.spec.typed.ExecutionContext.reader()
                    .bind(v -> com.legend.compiler.spec.typed.Lets.bound(v, preceding))
                    .read(java.util.Optional.empty(), s).csvSetups().isEmpty();
            verdicts |= verdict;
        }
        return new ProgramFacts(effects, seeds, verdicts, stores, shape.toString());
    }

    /** One statement's letter in {@link ProgramFacts#shape()}. */
    private static char statementKind(com.legend.compiler.spec.typed.TypedSpec s, boolean effect, boolean verdict) {
        if (s instanceof com.legend.compiler.spec.typed.TypedNativeCall c
                && com.legend.builtin.NativeFn.ContextOwner.of(c.callee().id()).isPresent()) {
            return 'X';
        }
        if (s instanceof com.legend.compiler.spec.typed.TypedLet l) {
            com.legend.compiler.spec.typed.TypedSpec v = l.value();
            while (v instanceof com.legend.compiler.spec.typed.TypedFrom f) {
                v = f.source();
            }
            if (v instanceof com.legend.compiler.spec.typed.TypedNativeCall ec
                    && com.legend.builtin.NativeFn.Handle.isExecute(ec.callee().id())) {
                return 'F';
            }
            return effect ? 'E' : 'L';
        }
        return verdict ? 'A' : effect ? 'E' : 'O';
    }

    /** The stores (Database FQNs) the program seeds through a typed
     * element reference ({@link com.legend.compiler.spec.SeededStores}):
     * the corpus runner's fixture-on-demand index. */
    public ProgramFacts {
        seedsStores = java.util.Collections.unmodifiableSet(new java.util.LinkedHashSet<>(seedsStores));
    }
}
