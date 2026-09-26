// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.builtin.NativeFn;
import com.legend.builtin.Pure;
import com.legend.builtin.Subsumed;
import com.legend.model.ClassMember;
import com.legend.model.NativeFunctionDefinition;
import com.legend.platform.CoreFn;
import com.legend.platform.Implementation;
import com.legend.platform.Registrations;
import com.legend.platform.WalledBodies;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * THE PLATFORM'S OWN REGISTRATIONS, assembled where the registries live: the
 * lowering's rule tables by position, the feature overrides, the implementer
 * families, the language forms' owned FQNs, the walls and the subsumed
 * programs — one {@link Registrations} value the tables are built from.
 * (Untangle step 4a: the tables sit below the compiler and the lowering; the
 * lowering registers INTO them, never the reverse.)
 */
public final class PlatformRegistrations {

    private PlatformRegistrations() {
    }

    private static final Registrations CURRENT = assemble();

    /** The platform's registrations, read once. */
    public static Registrations current() {
        return CURRENT;
    }

    private static final com.legend.platform.ImplementationTable CATALOG_TABLE =
            com.legend.platform.ImplementationTable.build(
                    com.legend.platform.DeclarationTable.of(Pure.all()), CURRENT);

    /** The implementation table over the catalog alone — for a lowering with no model behind it. */
    public static com.legend.platform.ImplementationTable catalogTable() {
        return CATALOG_TABLE;
    }

    private static Registrations assemble() {
        Map<Implementation.Position, Set<com.legend.model.FunctionId>> keys = new EnumMap<>(Implementation.Position.class);
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
                families.computeIfAbsent(family, k -> new ArrayList<>()).addAll(m.overloads());
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
            walledNatives.put(fqn, Objects.requireNonNull(Pure.walledNativeReason(fqn)));
        }
        Set<String> subsumed = new LinkedHashSet<>();
        for (Subsumed s : Subsumed.values()) {
            subsumed.add(s.fqn());
        }
        // the class members families implement: the row accessors, and the
        // routines standing in for a qualified property
        Map<Class<? extends NativeFn.Member>, Set<ClassMember>> members = new LinkedHashMap<>();
        for (NativeFn.RowGetter g : NativeFn.RowGetter.values()) {
            members.computeIfAbsent(NativeFn.RowGetter.class, k -> new LinkedHashSet<>()).add(g.member());
        }
        for (NativeFn.JavaRoutine r : NativeFn.JavaRoutine.values()) {
            r.implementedMember().ifPresent(m ->
                    members.computeIfAbsent(NativeFn.JavaRoutine.class, k -> new LinkedHashSet<>()).add(m));
        }
        return new Registrations(Pure.all(), keys, RegistryKeys.featureOverrides(), families, forms,
                walledNatives, WalledBodies.reasons(), subsumed, members);
    }
}
