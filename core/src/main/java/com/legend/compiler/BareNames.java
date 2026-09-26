// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler;

import com.legend.builtin.EngineHandlers;
import com.legend.builtin.Pure;
import com.legend.model.NativeFunctionDefinition;
import com.legend.platform.CoreFn;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/**
 * THE ONE RULE for a call spelled BARE that the resolver could not qualify
 * (untangle 4b.2, study §14). The resolver's own tiers (the file's imports, its
 * package, the core import group) run first and leave their candidates on the
 * node; what reaches the typer bare is served by exactly three tiers, in this
 * order, each a declaration the platform can point at:
 * <ol>
 *   <li>the ENGINE SURFACE — the bare names legend-engine's {@code Handlers.java}
 *       registers, each resolved to the FQNs the platform declares
 *       ({@link EngineHandlers}, generated from the pinned checkout);</li>
 *   <li>the CORE GROUP — the packages real pure imports implicitly
 *       ({@link NameResolver#CORE_IMPORTS}, generated from the engine's
 *       {@code CompileContext}), the name spelled under each;</li>
 *   <li>the FORM's OWNED FQNs — a language form the typer implements dispatches
 *       by the declarations it owns ({@link CoreFn#ownedFqns()}).</li>
 * </ol>
 * The catalog's bare-name index ({@code FN_BY_BARE}: every native by short name,
 * from any package, unverified) served this until 2026-09-25; it is gone from
 * resolution. What stays bare after this rule is a property probe or a name the
 * platform does not declare, and the typer says so.
 *
 * <p>The lite partition holds here as it did in the index: a lite-package FQN is
 * reachable from user text only when its bare name is on the product surface
 * ({@link Pure#userResolvableFunctionFqns()}).
 */
public final class BareNames {

    private BareNames() {
    }

    /** One tier's answer for a bare name: the tier's name (ENGINE, CORE, FORM)
     *  and an FQN it spells. The same FQN may come from several tiers (a core
     *  package's function the engine also registers a handler for): the probe
     *  counts each, so a name served by ONE tier alone is visible. */
    public record TierFqn(String tier, String fqn) {
    }

    /** Every (tier, FQN) a bare {@code name} may denote, in tier order, before
     *  de-duplication across tiers; the lite partition applied. */
    public static List<TierFqn> tiered(String name) {
        List<TierFqn> out = new ArrayList<>();
        for (String fqn : EngineHandlers.fqnsOf(name)) {
            out.add(new TierFqn("ENGINE", fqn));
        }
        for (String pkg : NameResolver.CORE_IMPORTS) {
            out.add(new TierFqn("CORE", pkg + "::" + name));
        }
        // the form's owned declarations SPELLED LIKE THE CALL (a form owning
        // select and newTDSRelationAccessor dispatches both; a call spelled
        // select denotes only the former)
        CoreFn form = CoreFn.parseNames().get(name);
        if (form != null) {
            for (String fqn : new java.util.TreeSet<>(form.ownedFqns())) {
                if (fqn.substring(fqn.lastIndexOf("::") + 2).equals(name)) {
                    out.add(new TierFqn("FORM", fqn));
                }
            }
        }
        // the lite partition: a lite-package declaration is reachable from a
        // bare spelling only on the product surface (a form may own a
        // lite-internal FQN — joinSlot — that user text must never reach)
        var userResolvable = Pure.userResolvableFunctionFqns();
        out.removeIf(t -> t.fqn().startsWith(Pure.Lite.PKG) && !userResolvable.contains(t.fqn()));
        return out;
    }

    /** The FQNs a bare {@code name} may denote, in tier order, distinct
     *  ({@code name} is bare: a qualified spelling is its own declaration and
     *  never asks). */
    public static List<String> fqns(String name) {
        LinkedHashSet<String> out = new LinkedHashSet<>();
        for (TierFqn t : tiered(name)) {
            out.add(t.fqn());
        }
        return List.copyOf(out);
    }

    /** The catalog natives at each (tier, FQN) of {@link #tiered} — the
     *  resolver's prelude merge reads this so the probe can name the tier that
     *  supplied each native (execution plan step 3 homework, 2026-09-26). */
    public static List<Map.Entry<TierFqn, List<NativeFunctionDefinition>>> catalogTiered(String name) {
        List<Map.Entry<TierFqn, List<NativeFunctionDefinition>>> out = new ArrayList<>();
        for (TierFqn t : tiered(name)) {
            out.add(Map.entry(t, Pure.nativeFunctionsAt(t.fqn())));
        }
        return out;
    }

    /** The catalog natives a bare {@code name} may denote — the overloads at
     *  every tier FQN. */
    public static List<NativeFunctionDefinition> catalog(String name) {
        List<NativeFunctionDefinition> out = new ArrayList<>();
        java.util.Set<String> seen = new java.util.HashSet<>();
        for (var tier : catalogTiered(name)) {
            if (seen.add(tier.getKey().fqn())) {
                out.addAll(tier.getValue());
            }
        }
        return out;
    }
}
