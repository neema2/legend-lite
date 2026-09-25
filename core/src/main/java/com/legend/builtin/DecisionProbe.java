// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.builtin;

import com.legend.Nullable;
import com.legend.model.Function;

import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Stream;

/**
 * THE TWO DECISION POINTS, observable (platform architecture untangle, step 3
 * — a probe, deleted at step 4): the overload set the typer chooses from, and
 * the implementation lowering picks for a resolved function. The compiler and
 * the lowering report each decision here; the provider that compares them with
 * the declaration and implementation tables lives above both (it reads their
 * registries), so it is bound by {@link ServiceLoader}, never imported — the
 * package layering stays acyclic. Nothing is installed unless {@code LL_SHADOW}
 * is set, and every call is then a no-op.
 */
public interface DecisionProbe {

    /** The overload set today's merge returns at {@code fqn}, with the model it was read from. */
    void onOverloads(String fqn, List<Function> today, Object model, Stream<Function> modelFunctions);

    /** What lowering picked for the resolved {@code definition}; {@code today} names the pick. */
    void onPick(@Nullable Function definition, String today);

    /** A language form dispatched on the spelled {@code name}. */
    void onForm(String name, String form);

    /** The candidate declarations the typer considers for a call spelled {@code name}
     *  (a null is a candidate with no source definition — a test convenience). */
    void onCandidates(String name, String source, Stream<@Nullable Function> candidates);

    /** The installed probe, or null. The binding (META-INF/services) is a TEST-LANE
     *  resource (//core:shadow_binding on the suites' libraries), never the product
     *  jar's: a planner build carries this interface and the Shadow class, never
     *  the binding, so its loader is empty. */
    @Nullable DecisionProbe INSTALLED = installed();

    private static @Nullable DecisionProbe installed() {
        if (System.getenv("LL_SHADOW") == null) {
            return null;
        }
        java.util.Iterator<DecisionProbe> provided = ServiceLoader.load(DecisionProbe.class).iterator();
        if (!provided.hasNext()) {
            throw new IllegalStateException("LL_SHADOW set, no DecisionProbe bound (//core:shadow_binding)");
        }
        return provided.next();
    }

    static void overloads(String fqn, List<Function> today, Object model, Stream<Function> modelFunctions) {
        if (INSTALLED != null) {
            INSTALLED.onOverloads(fqn, today, model, modelFunctions);
        }
    }

    static void pick(@Nullable Function definition, String today) {
        if (INSTALLED != null) {
            INSTALLED.onPick(definition, today);
        }
    }

    static void form(String name, String form) {
        if (INSTALLED != null) {
            INSTALLED.onForm(name, form);
        }
    }

    /** {@code source}: {@code node} when the resolver left the candidates on the
     *  call, {@code bare} when the typer's bare-name rule supplied them. */
    static void candidates(String name, String source, Stream<@Nullable Function> candidates) {
        if (INSTALLED != null) {
            INSTALLED.onCandidates(name, source, candidates);
        }
    }
}
