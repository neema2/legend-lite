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
 * {@code StatementExecutor} and {@code Compiler}, the plan side's one reach into execution). Moved verbatim; the
 * effect scan's catch of an un-typeable callee is the rebuild's B3.
 */
public final class StatementEffects {

    private StatementEffects() {
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
        if (node instanceof TypedUserCall uc
                && com.legend.builtin.Subsumed.of(uc.callee().qualifiedName()).isPresent()) {
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
        String callee = n instanceof TypedNativeCall nc
                ? nc.callee().qualifiedName()
                : n instanceof TypedUserCall uc
                        ? uc.callee().qualifiedName() : null;
        if (callee != null && com.legend.compiler.element.type.PlatformTypes
                .isVerdictFunction(callee)) {
            return true;
        }
        if (n instanceof TypedUserCall uc) {
            com.legend.model.FunctionId key = uc.callee().id();
            Boolean known = memo.get(key);
            if (known == null) {
                memo.put(key, false);   // in-progress: cycles score false
                boolean reaches = false;
                try {
                    for (TypedSpec stmt : specs.compile(uc.callee()).body()) {
                        if (callsVerdict(stmt, specs, memo)) {
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
            if (callsVerdict(c, specs, memo)) {
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
