// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.platform;
import com.legend.model.FunctionId;

import com.legend.builtin.NativeFn;
import com.legend.model.ClassMember;
import com.legend.model.NativeFunctionDefinition;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * EVERYTHING THE PLATFORM REGISTERS about what executes a function — the
 * {@link ImplementationTable}'s one input beside the declarations. A value, so
 * a table can be built over any registrations (a test's hand-written ones) and
 * every path of the builder is exercisable; the lowering, which owns the
 * registries, assembles the platform's own ({@code PlatformRegistrations}).
 *
 * @param catalog          the catalog natives — each lowering key IS one of
 *                         their {@code signatureKey()}s, so a key is matched to
 *                         its definition whole, never parsed
 * @param loweringKeys     per lowering position, the catalog signature keys registered there
 * @param featureOverrides per execution feature flag, the keys whose scalar rule the flag replaces
 * @param families         per implementer family (a {@link NativeFn.Member} enum), its members' overloads
 * @param forms            per language form, the exact FQNs whose overloads it owns
 * @param walledNatives    natives refused by decision, FQN → reason
 * @param walledBodies     bodies refused by decision, FQN → the wall
 * @param subsumed         programs whose value is never needed, by FQN
 * @param members          per implementer family, the CLASS MEMBERS (derived
 *                         properties) it implements — matched to lifted
 *                         declarations by their provenance
 */
public record Registrations(
        List<NativeFunctionDefinition> catalog,
        Map<Implementation.Position, Set<FunctionId>> loweringKeys,
        Map<Feature, Set<FunctionId>> featureOverrides,
        Map<Class<? extends NativeFn.Member>, List<NativeFunctionDefinition>> families,
        Map<CoreFn, Set<String>> forms,
        Map<String, String> walledNatives,
        Map<String, WalledBodies.Wall> walledBodies,
        Set<String> subsumed,
        Map<Class<? extends NativeFn.Member>, Set<ClassMember>> members) {

    public Registrations {
        catalog = List.copyOf(catalog);
        loweringKeys = Map.copyOf(loweringKeys);
        featureOverrides = Map.copyOf(featureOverrides);
        families = Map.copyOf(families);
        forms = Map.copyOf(forms);
        walledNatives = Map.copyOf(walledNatives);
        walledBodies = Map.copyOf(walledBodies);
        subsumed = Set.copyOf(subsumed);
        members = Map.copyOf(members);
    }
}
