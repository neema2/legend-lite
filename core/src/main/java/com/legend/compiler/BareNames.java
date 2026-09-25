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

    /** The FQNs a bare {@code name} may denote, in tier order, distinct
     *  ({@code name} is bare: a qualified spelling is its own declaration and
     *  never asks). */
    public static List<String> fqns(String name) {
        LinkedHashSet<String> out = new LinkedHashSet<>(EngineHandlers.fqnsOf(name));
        for (String pkg : NameResolver.CORE_IMPORTS) {
            out.add(pkg + "::" + name);
        }
        // the form's owned declarations SPELLED LIKE THE CALL (a form owning
        // select and newTDSRelationAccessor dispatches both; a call spelled
        // select denotes only the former)
        CoreFn form = CoreFn.parseNames().get(name);
        if (form != null) {
            for (String fqn : new java.util.TreeSet<>(form.ownedFqns())) {
                if (fqn.substring(fqn.lastIndexOf("::") + 2).equals(name)) {
                    out.add(fqn);
                }
            }
        }
        // the lite partition: a lite-package declaration is reachable from a
        // bare spelling only on the product surface (a form may own a
        // lite-internal FQN — joinSlot — that user text must never reach)
        var userResolvable = Pure.userResolvableFunctionFqns();
        out.removeIf(fqn -> fqn.startsWith(Pure.Lite.PKG) && !userResolvable.contains(fqn));
        return List.copyOf(out);
    }

    /** The catalog natives a bare {@code name} may denote — the overloads at
     *  every tier FQN. */
    public static List<NativeFunctionDefinition> catalog(String name) {
        List<NativeFunctionDefinition> out = new ArrayList<>();
        for (String fqn : fqns(name)) {
            out.addAll(Pure.nativeFunctionsAt(fqn));
        }
        return out;
    }
}
