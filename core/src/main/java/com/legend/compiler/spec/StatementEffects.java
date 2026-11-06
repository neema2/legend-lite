// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.compiler.spec.typed.TypedCopyInstance;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedNewInstance;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedUserCall;

/**
 * What a TYPED program will do when it runs, read before it runs: whether a statement writes (reaches a database
 * effect, transitively through compiled user-function bodies), whether it generates test data, whether it reaches a
 * verdict. These are compile-time facts over the typed tree, so they live with the compiler; the execution side and
 * the driver's program facts ask here (C1, docs/PLAN_EXECUTION_SPLIT_AND_DATABASE_OWNER_2026_10_03.md: they lived in
 * {@code BodyDriver} and {@code Compiler}, the plan side's one reach into execution). Moved verbatim; the
 * effect scan's catch of an un-typeable callee is the rebuild's B3.
 */
public final class StatementEffects {

    private StatementEffects() {
    }

    /** A SUBSUMED engine program ({@link com.legend.builtin.Subsumed}): its call stands as an opaque typed value on
     *  every road, its body never compiled or run -- the one stop of the three scans ({@link #containsEffect},
     *  {@link #reaches}, {@link #writesStore}). */
    private static boolean subsumed(TypedUserCall uc) {
        return com.legend.builtin.Subsumed.of(uc.callee().qualifiedName()).isPresent();
    }

    /**
     * Does this expression (transitively, through user calls) reach the
     * {@code executeInDb} K-native? Memoized per callee signature; a cycle
     * scores the in-progress callee non-effectful — real recursion is
     * caught loudly at execution time.
     */
    public static boolean containsEffect(TypedSpec node, SpecCompiler specs,
            java.util.Map<com.legend.model.FunctionId, Boolean> memo) {
        if (node instanceof TypedNativeCall nc
                && com.legend.builtin.NativeFn.Effect.isDbEffect(nc.callee().id())) {
            return true;
        }
        if (node instanceof TypedUserCall uc && subsumed(uc)) {
            // a SUBSUMED engine program's body is never compiled or run
            // here (Subsumed.java): no effect
            return false;
        }
        if (node instanceof TypedUserCall uc) {
            com.legend.model.FunctionId key = uc.callee().id();
            Boolean known = memo.get(key);
            if (known == null) {
                memo.put(key, false);   // in-progress: cycles score false
                boolean effectful = false;
                java.util.List<TypedSpec> calleeBody;
                try {
                    calleeBody = specs.compile(uc.callee()).body();
                } catch (TypeInferenceException e) {
                    // an UN-TYPEABLE callee (a dead match arm's library
                    // closure — toPostgresModel's SemiStructuredObjectNavigation
                    // arm reaching sqlQueryToString's string recursion) cannot
                    // execute in EITHER channel: this over-approximating
                    // reachability scan must not decide the test on it — the
                    // SQL channel's inliner walls the LIVE arm loudly if it is
                    // ever reached (WORLD_MAP rule 5)
                    memo.put(key, false);
                    return false;
                }
                for (TypedSpec stmt : calleeBody) {
                    if (containsEffect(stmt, specs, memo)) {
                        effectful = true;
                        break;
                    }
                }
                memo.put(key, effectful);
                known = effectful;
            }
            if (known) {
                return true;
            }
        }
        // post-processor CONFIG properties carry plan-time SQL-rewrite
        // hooks, never DDL/executeInDb effects — compiling them drags in
        // relational-metamodel vocabulary the prelude does not declare
        // (ledger cluster 63)
        if (node instanceof com.legend.compiler.spec.typed
                .TypedNewInstance ni9) {
            for (var pe : ni9.properties().entrySet()) {
                if (!com.legend.compiler.element.type.PlatformTypes
                                .isPostProcessorConfigProperty(pe.getKey())
                        && containsEffect(pe.getValue(), specs, memo)) {
                    return true;
                }
            }
            return false;
        }
        if (node instanceof com.legend.compiler.spec.typed
                .TypedCopyInstance cp9) {
            if (containsEffect(cp9.source(), specs, memo)) {
                return true;
            }
            for (var pe : cp9.overrides().entrySet()) {
                if (!com.legend.compiler.element.type.PlatformTypes
                                .isPostProcessorConfigProperty(pe.getKey())
                        && containsEffect(pe.getValue(), specs, memo)) {
                    return true;
                }
            }
            return false;
        }
        for (TypedSpec c : node.children()) {
            if (containsEffect(c, specs, memo)) {
                return true;
            }
        }
        return false;
    }


    /** Does the program REACH a verdict function — directly, or through
     * the compiled body of a user function it calls (the same descent as
     * {@link #containsEffect}: memoized by signature,
     * cycles and un-typeable callees score false)? Phase 0.3: a test
     * whose program reaches no verdict is no pass — the runner classifies
     * it SKIPPED (no assertion reachable) instead of scoring a body that
     * merely did not throw. */
    public static boolean callsVerdict(TypedSpec n, SpecCompiler specs,
            java.util.Map<com.legend.model.FunctionId, Boolean> memo) {
        return reaches(n, StatementEffects::isVerdictCall, specs, memo);
    }

    /** Is {@code n} itself a call of a verdict function ({@code PlatformTypes.isVerdictFunction}: the asserts
     *  package, {@code fail} among them)? */
    public static boolean isVerdictCall(TypedSpec n) {
        String callee = n instanceof TypedNativeCall nc
                ? nc.callee().qualifiedName()
                : n instanceof TypedUserCall uc
                        ? uc.callee().qualifiedName() : null;
        return callee != null && com.legend.compiler.element.type.PlatformTypes.isVerdictFunction(callee);
    }

    /** Does {@code n} REACH a node {@code p} accepts -- itself, a node under it, or a node of the compiled body of a
     * user function it calls ({@link #callsVerdict}'s descent: memoized by signature in {@code memo}, which belongs
     * to one {@code p}; cycles and un-typeable callees score false)? A SUBSUMED engine program's body is never
     * compiled or run on any road ({@link com.legend.builtin.Subsumed}: the call stands as an opaque typed value), so
     * its body is not reached -- as {@link #containsEffect} already read it; its arguments are (the test-is-the-product
     * plan's M10c item 7c: the engine's {@code createDbConfig} body asserts, and the assert tagged every body that
     * named it). */
    public static boolean reaches(TypedSpec n, java.util.function.Predicate<TypedSpec> p, SpecCompiler specs,
            java.util.Map<com.legend.model.FunctionId, Boolean> memo) {
        if (p.test(n)) {
            return true;
        }
        if (n instanceof TypedUserCall uc && !subsumed(uc)) {
            com.legend.model.FunctionId key = uc.callee().id();
            Boolean known = memo.get(key);
            if (known == null) {
                memo.put(key, false);   // in-progress: cycles score false
                boolean reaches = false;
                try {
                    for (TypedSpec stmt : specs.compile(uc.callee()).body()) {
                        if (reaches(stmt, p, specs, memo)) {
                            reaches = true;
                            break;
                        }
                    }
                } catch (TypeInferenceException e) {
                    // an un-typeable callee cannot execute in either
                    // channel; this reachability scan does not decide on it
                }
                memo.put(key, reaches);
                known = reaches;
            }
            if (known) {
                return true;
            }
        }
        for (TypedSpec c : n.children()) {
            if (reaches(c, p, specs, memo)) {
                return true;
            }
        }
        return false;
    }

    /** {@link #reaches} over what RUNNING {@code n} runs: a lambda stored as a constructed instance's property
     *  ({@code ^Conn(sqlQueryPostProcessors = [{q | ...}])}, a {@code ^$c(...)} copy's override) is a value the
     *  construction holds, not code the statement runs -- the platform runs no stored lambda as Pure (a runtime's SQL
     *  post-processors are recognized shapes it rewrites its own SQL tree by: {@code lowering/SqlPostProcessors}), so
     *  a verdict inside one is none of the statement's (the walk deletion, 2026-11-06: the corpus's post-processor
     *  tests, whose library helpers guard with asserts). Every other child is walked as {@link #reaches} walks it. */
    public static boolean reachesRunning(TypedSpec n, java.util.function.Predicate<TypedSpec> p, SpecCompiler specs,
            java.util.Map<com.legend.model.FunctionId, Boolean> memo) {
        if (p.test(n)) {
            return true;
        }
        if (n instanceof TypedUserCall uc && !subsumed(uc)) {
            com.legend.model.FunctionId key = uc.callee().id();
            Boolean known = memo.get(key);
            if (known == null) {
                memo.put(key, false);   // in-progress: cycles score false
                boolean reaches = false;
                try {
                    for (TypedSpec stmt : specs.compile(uc.callee()).body()) {
                        if (reachesRunning(stmt, p, specs, memo)) {
                            reaches = true;
                            break;
                        }
                    }
                } catch (TypeInferenceException e) {
                    // an un-typeable callee cannot execute in either channel; this scan does not decide on it
                }
                memo.put(key, reaches);
                known = reaches;
            }
            if (known) {
                return true;
            }
        }
        java.util.Collection<TypedSpec> stored = n instanceof com.legend.compiler.spec.typed.TypedNewInstance ni
                ? ni.properties().values()
                : n instanceof com.legend.compiler.spec.typed.TypedCopyInstance ci ? ci.overrides().values()
                : java.util.List.of();
        for (TypedSpec c : n.children()) {
            if (stored.contains(c) && storedLambdas(c)) {
                continue;
            }
            if (reachesRunning(c, p, specs, memo)) {
                return true;
            }
        }
        return false;
    }

    /** Whether {@code value}, a constructed instance's property, is lambdas alone: one, or a collection of them. */
    private static boolean storedLambdas(TypedSpec value) {
        if (value instanceof com.legend.compiler.spec.typed.TypedLambda) {
            return true;
        }
        return value instanceof com.legend.compiler.spec.typed.TypedCollection c && !c.elements().isEmpty()
                && c.elements().stream().allMatch(e -> e instanceof com.legend.compiler.spec.typed.TypedLambda);
    }

    /** Is {@code n} itself a write into a store table: {@code rel->write(#>{db.t}#)}, or lite's {@code delete},
     *  {@code update} or {@code merge} (each over a store table by its checker)? The statement the execution runs as
     *  the database's DML (BodyDriver's store-write arm). */
    public static boolean isStoreWrite(TypedSpec n) {
        return (n instanceof com.legend.compiler.spec.typed.TypedWrite w && w.destination().isPresent()
                && w.destination().get() instanceof com.legend.compiler.spec.typed.TypedTableReference)
                || n instanceof com.legend.compiler.spec.typed.TypedStoreWrite
                // a class write (L2), which the store resolver writes as L1's statement
                || n instanceof com.legend.compiler.spec.typed.TypedNativeCall nc
                        && com.legend.builtin.NativeFn.ResolverForm.of(nc.callee().id())
                                .map(com.legend.builtin.NativeFn.ResolverForm::classWrite).orElse(false);
    }

    /** Does the program WRITE A STORE TABLE — a store write ({@link #isStoreWrite}; docs/WRITES_DESIGN_2026_10_18.md §4, L1),
     * directly or through the compiled body of a user function it calls (the descent of {@link #callsVerdict}:
     * memoized by signature, cycles and un-typeable callees score false)? The execution runs such a program in one
     * transaction. */
    public static boolean writesStore(TypedSpec n, SpecCompiler specs,
            java.util.Map<com.legend.model.FunctionId, Boolean> memo) {
        if (isStoreWrite(n)) {
            return true;
        }
        if (n instanceof TypedUserCall uc && !subsumed(uc)) {
            com.legend.model.FunctionId key = uc.callee().id();
            Boolean known = memo.get(key);
            if (known == null) {
                memo.put(key, false);   // in-progress: cycles score false
                boolean writes = false;
                try {
                    for (TypedSpec stmt : specs.compile(uc.callee()).body()) {
                        if (writesStore(stmt, specs, memo)) {
                            writes = true;
                            break;
                        }
                    }
                } catch (TypeInferenceException e) {
                    // an un-typeable callee cannot execute; this reachability scan does not decide on it
                }
                memo.put(key, writes);
                known = writes;
            }
            if (known) {
                return true;
            }
        }
        for (TypedSpec c : n.children()) {
            if (writesStore(c, specs, memo)) {
                return true;
            }
        }
        return false;
    }

    public static boolean containsTdgGenerator(TypedSpec n) {
        if (n instanceof TypedNativeCall nc
                && (com.legend.compiler.element.type.PlatformTypes
                        .GENERATE_TEST_DATA.equals(
                                nc.callee().qualifiedName())
                    || com.legend.compiler.element.type.PlatformTypes
                        .GENERATE_SEED_DATA_STRING.equals(
                                nc.callee().qualifiedName()))) {
            return true;
        }
        for (TypedSpec c : n.children()) {
            if (containsTdgGenerator(c)) {
                return true;
            }
        }
        return false;
    }
}
