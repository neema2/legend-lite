// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

/**
 * H10 SPIKE (claude/compat-judges-spike, never for merge): what the host judge's comparisons carry -- since CP-14 (iii)
 * no rules (lite's are inline in {@link Equality}), only the run's PRESENTER of a failure record, threaded here so the
 * sites without the run record word through it (a build passes the run record instead and drops this class).
 */
public record HostRules(java.util.function.Function<VerdictRecord, String> presenter) {

    /** Lite's presenter. */
    public static final HostRules LITE = new HostRules(LiteHostWords::present);

    public HostRules {
        java.util.Objects.requireNonNull(presenter, "the run's presenter");
    }
}
