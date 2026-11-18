// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

/**
 * WHAT THE HOST JUDGE JUDGES A VERDICT BY (census C1's step 2c, 2026-11-17): the run hook's behaviours
 * ({@link HostForms}, the hearing's), carried through the host judge's comparisons. The plan's judging rules went
 * 2026-11-18 (train 17, the orchestrator's ruling): a Float's tolerance and a Decimal's scale are the forms' now.
 */
public record HostRules(HostForms forms) {

    /** Lite's behaviours. */
    public static final HostRules LITE = new HostRules(LiteHostForms.STRICT);

    public HostRules {
        java.util.Objects.requireNonNull(forms, "an equality names the run's forms");
    }
}
