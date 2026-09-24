// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.platform;

import com.legend.builtin.NativeFn;
import com.legend.builtin.Pure;
import com.legend.builtin.Subsumed;
import com.legend.compiler.spec.CoreFn;
import com.legend.compiler.spec.WalledBodies;
import com.legend.compiler.spec.typed.Feature;
import com.legend.lowering.RegistryKeys;
import com.legend.model.NativeFunctionDefinition;

import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * EVERYTHING THE PLATFORM REGISTERS about what executes a function — the
 * {@link ImplementationTable}'s one input beside the declarations. A value, so
 * a table can be built over any registrations (a test's hand-written ones) and
 * every path of the builder is exercisable; {@link #current()} reads the
 * platform's own registries.
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
 */
public record Registrations(
        List<NativeFunctionDefinition> catalog,
        Map<Implementation.Position, Set<String>> loweringKeys,
        Map<Feature, Set<String>> featureOverrides,
        Map<Class<? extends NativeFn.Member>, List<NativeFunctionDefinition>> families,
        Map<CoreFn, Set<String>> forms,
        Map<String, String> walledNatives,
        Map<String, WalledBodies.Wall> walledBodies,
        Set<String> subsumed) {

    public Registrations {
        catalog = List.copyOf(catalog);
        loweringKeys = Map.copyOf(loweringKeys);
        featureOverrides = Map.copyOf(featureOverrides);
        families = Map.copyOf(families);
        forms = Map.copyOf(forms);
        walledNatives = Map.copyOf(walledNatives);
        walledBodies = Map.copyOf(walledBodies);
        subsumed = Set.copyOf(subsumed);
    }

    /** The platform's own registrations, read from its registries. */
    public static Registrations current() {
        Map<Implementation.Position, Set<String>> keys = new EnumMap<>(Implementation.Position.class);
        keys.put(Implementation.Position.SCALAR, RegistryKeys.scalarRules());
        keys.put(Implementation.Position.AGGREGATE, RegistryKeys.reducers());
        keys.put(Implementation.Position.WINDOW, RegistryKeys.windowFunctions());
        keys.put(Implementation.Position.WINDOW_AGGREGATE, RegistryKeys.windowAggregates());
        Map<Class<? extends NativeFn.Member>, List<NativeFunctionDefinition>> families = new LinkedHashMap<>();
        for (List<? extends NativeFn.Member> members : NativeFn.families().values()) {
            for (NativeFn.Member m : members) {
                // a family is a closed enum: the member's declaring class names it
                Class<? extends NativeFn.Member> family = ((Enum<?>) m).getDeclaringClass()
                        .asSubclass(NativeFn.Member.class);
                families.computeIfAbsent(family, k -> new java.util.ArrayList<>()).addAll(m.overloads());
            }
        }
        Map<CoreFn, Set<String>> forms = new EnumMap<>(CoreFn.class);
        for (CoreFn form : CoreFn.values()) {
            if (!form.ownedFqns().isEmpty()) {
                forms.put(form, form.ownedFqns());
            }
        }
        Map<String, String> walledNatives = new LinkedHashMap<>();
        for (String fqn : Pure.walledNativeFqns()) {
            walledNatives.put(fqn, java.util.Objects.requireNonNull(Pure.walledNativeReason(fqn)));
        }
        Set<String> subsumed = new LinkedHashSet<>();
        for (Subsumed s : Subsumed.values()) {
            subsumed.add(s.fqn());
        }
        return new Registrations(Pure.all(), keys, RegistryKeys.featureOverrides(), families, forms,
                walledNatives, WalledBodies.reasons(), subsumed);
    }
}
