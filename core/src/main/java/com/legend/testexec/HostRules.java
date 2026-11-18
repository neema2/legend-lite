// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

/**
 * WHAT THE HOST JUDGE JUDGES A VERDICT BY (census C1's step 2c, 2026-11-17): the plan's judging rules for the verdict
 * (data, stamped on its step) and the run hook's behaviours ({@link HostForms}, the hearing's), carried together
 * through the host judge's comparisons.
 */
public record HostRules(com.legend.testexecutionplan.TestPlan.JudgingRules rules, HostForms forms) {

    /** Lite's rules and lite's behaviours. */
    public static final HostRules LITE = new HostRules(com.legend.testexecutionplan.TestPlan.JudgingRules.LITE,
            LiteHostForms.STRICT);

    public HostRules {
        java.util.Objects.requireNonNull(rules, "an equality names the rules it judges by");
        java.util.Objects.requireNonNull(forms, "an equality names the run's forms");
    }

    /** The plan's rule: a Float compared within {@link #floatUlps} ULP at all. */
    public boolean floatTolerance() {
        return rules.floatTolerance();
    }

    /** The plan's rule: the units in the last place two Floats may differ by and be equal (0: exact). */
    public int floatUlps() {
        return rules.floatUlps();
    }

    /** The plan's rule: a Decimal compared with its scale. */
    public boolean decimalScale() {
        return rules.decimalScale();
    }
}
