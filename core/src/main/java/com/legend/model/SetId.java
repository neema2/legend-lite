// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.model;

/**
 * THE ONE RULE for a class mapping's effective set id (audit 2026-09-15
 * P2-1): the declared id, else the engine's default — the class FQN with
 * {@code ::} spelled {@code _} (engine {@code SetImplementation.id}
 * defaulting in the grammar walker). Sixteen sites across nine packages
 * used to spell this rule themselves, and one ({@code SetKeyFacts.setKey})
 * spelled a different one; every one of them now calls here, and a source
 * ratchet ({@code CodeShapeGuardrailTest.setIdSpellingHasOneOwner}) keeps
 * the substitution out of every other file. The same rule names an
 * enumeration mapping's default id ({@link #of(String, String)} with the
 * enumeration's FQN).
 */
public final class SetId {

    private SetId() {
    }

    /** The effective id: {@code declared}, else the default derived from
     * {@code fqn}. */
    public static String of(@com.legend.base.Nullable String declared, String fqn) {
        return declared != null && !declared.isEmpty() ? declared : defaultFor(fqn);
    }

    /** The engine's default id for {@code fqn}: {@code ::} spelled {@code _}. */
    public static String defaultFor(String fqn) {
        return fqn.replace("::", "_");
    }

    /** Whether {@code id} is exactly the default id of {@code fqn} (an
     * absent declaration, never a name — a short class name is NOT one:
     * two same-named classes in different packages must not match). */
    public static boolean isDefault(String id, String fqn) {
        return id.equals(defaultFor(fqn));
    }

    public static String of(ClassMapping cm) {
        return of(cm.setId(), cm.className());
    }

    public static String of(MappingDefinition.ClassBinding cb) {
        return of(cb.setId(), cb.classFqn());
    }
}
