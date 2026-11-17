// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.testexec;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/**
 * THE COMPATIBILITY RULES' HOST EQUALITY, an edge test (census C1's K14, the orchestrator's ruling of 2026-11-17: a
 * test of upstream's behaviour is an edge test -- moved from {@code EqualityWorldsConformanceTest}, whose two worlds
 * are lite's own).
 */
class EqualityCompatEdgeTest {

    /** The compatibility rules with the edge's host forms, as the edge's run hears them. */
    private static final HostRules COMPAT = new HostRules(
            com.legend.testexecutionplan.TestPlan.JudgingRules.COMPILED_PURE_COMPAT,
            com.legend.edge.CompiledPureHostForms.INSTANCE);

    @Test
    void compatibilityModeEqualsAnIntegerAndADecimalByTheirTexts() {
        // register S127: compiled legend-pure's CompiledSupport.eq -- 1D == 1 by text, a Float equal to neither
        assertEquals(true, Equality.same(Equality.Typed.of(new java.math.BigDecimal("1")), Equality.Typed.of(1L), COMPAT));
        assertEquals(false, Equality.same(Equality.Typed.of(1.0d), Equality.Typed.of(1L), COMPAT));
        assertEquals(false, Equality.same(Equality.Typed.of(new java.math.BigDecimal("1")), Equality.Typed.of(1L),
                HostRules.LITE), "lite's runs: two number kinds never equal");
    }

    @Test
    void compatibilityModeHoldsTwoUlpsAndTheEngineSeamsScaleSensitiveEquals() {
        // register S20/S132: the 2-ULP tolerance is compatibility mode's alone
        assertEquals(true, Equality.same(Equality.Typed.of(0.1d + 0.2d), Equality.Typed.of(0.3d),
                COMPAT),
                "compatibility mode's 2-ULP tolerance holds 0.1 + 0.2 as 0.3");
        // register S139: compatibility mode keeps the engine assert seam's scale-sensitive equals
        assertEquals(false, Equality.same(Equality.Typed.of(new java.math.BigDecimal("3.0")),
                Equality.Typed.of(new java.math.BigDecimal("3.00")),
                COMPAT),
                "compatibility mode: the engine assert seam's scale-sensitive equals");
        // moved from PureAssertsTest.numericPolicies (X2, VERDICT_RULE_AUDIT: engine Decimal equality is
        // getValue().equals)
        assertEquals(false, Equality.same(Equality.Typed.of(new java.math.BigDecimal("1.50")),
                Equality.Typed.of(new java.math.BigDecimal("1.5")),
                COMPAT));
    }
}
