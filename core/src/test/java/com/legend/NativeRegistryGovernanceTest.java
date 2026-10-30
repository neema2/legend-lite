// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.driver.BodyDriver;

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
}
