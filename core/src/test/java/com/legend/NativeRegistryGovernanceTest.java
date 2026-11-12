// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testplan.BodyDriver;

import com.legend.compiler.element.type.PlatformTypes;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Registry == catalog (§10m, user push: "or just moving the ifs into a
 * table?"): every catalogued EFFECT/JAVA_ROUTINE row has a registered
 * implementation and every registered implementation has a row —
 * neither list can drift without failing here.
 */
class NativeRegistryGovernanceTest {

    private static Set<String> rows(java.util.List<? extends com.legend.builtin.NativeFn.Member> family) {
        return family.stream().map(com.legend.builtin.NativeFn.Member::fqn)
                .collect(java.util.stream.Collectors.toSet());
    }

    @Test
    @DisplayName("EFFECT rows == registered effect arms")
    void effectRegistryCoversCatalog() {
        assertEquals(rows(java.util.List.of(com.legend.builtin.NativeFn.Effect.values())),
                BodyDriver.registeredEffectKeys(),
                "EFFECT rows and registered arms drifted");
    }

    @Test
    @DisplayName("JAVA_ROUTINE rows == registered staged routines")
    void routineRegistryCoversCatalog() {
        assertEquals(rows(java.util.List.of(com.legend.builtin.NativeFn.JavaRoutine.values())),
                BodyDriver.registeredRoutineKeys(),
                "JAVA_ROUTINE rows and staged routines drifted");
    }

    /** RowGetter.of reads an APPLIED spelling, ofLifted a lifted FQN, and neither answers the other's (2026-11-12, the
     *  owner's ruling on slice 1b's report: of never resolving a lifted FQN is its contract, now pinned). */
    @Test
    @DisplayName("ROW_GETTER: of reads applied spellings, ofLifted lifted FQNs")
    void rowGetterLookupsReadTheirOwnSpellings() {
        for (com.legend.builtin.NativeFn.RowGetter g : com.legend.builtin.NativeFn.RowGetter.values()) {
            assertEquals(java.util.Optional.of(g), com.legend.builtin.NativeFn.RowGetter.of(g.property()), g.name());
            assertEquals(java.util.Optional.of(g),
                    com.legend.builtin.NativeFn.RowGetter.of(g.owner() + "::" + g.property()), g.name());
            assertEquals(java.util.Optional.of(g), com.legend.builtin.NativeFn.RowGetter.ofLifted(g.fqn()), g.name());
            assertEquals(java.util.Optional.empty(), com.legend.builtin.NativeFn.RowGetter.of(g.fqn()), g.name());
            assertEquals(java.util.Optional.empty(), com.legend.builtin.NativeFn.RowGetter.ofLifted(g.property()),
                    g.name());
        }
    }

    /** GroupAggregate has no by-id {@code of}: its call is read by name before typing and its overloads by
     *  {@code Pure.AT_RELATION_JOIN_STRINGS} -- the member is held to both (2026-11-12, the owner's ruling on slice 1b's
     *  report). */
    @Test
    @DisplayName("GROUP_AGGREGATE rows == the group desugar's name and the lowering's overloads")
    void groupAggregateIsTheDesugarsNameAndTheLoweringsOverloads() {
        assertEquals(Set.of(PlatformTypes.RELATION_JOIN_STRINGS),
                rows(java.util.List.of(com.legend.builtin.NativeFn.GroupAggregate.values())),
                "GROUP_AGGREGATE rows and GroupLambdaAggs' name drifted");
        assertEquals(Set.copyOf(com.legend.builtin.Pure.AT_RELATION_JOIN_STRINGS),
                com.legend.builtin.NativeFn.GroupAggregate.JOIN_STRINGS.overloads().stream()
                        .map(com.legend.model.FunctionId::of).collect(Collectors.toSet()),
                "GROUP_AGGREGATE overloads and the lowering's AT_RELATION_JOIN_STRINGS drifted");
    }
}
