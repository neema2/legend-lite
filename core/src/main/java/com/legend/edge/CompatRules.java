// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.testexecutionplan.TestPlan;

/**
 * THE COMPATIBILITY RULES (census C1, the orchestrator's ruling of 2026-11-17: the judging rules are data, and the
 * compatibility preset is the edge's): backwards compatibility with legend-pure's compiled runtime and legend-engine's
 * goldens (PCT channel B, the corpus), each leniency a register row -- S132/S15 (a Float within 2 ULP), S139 (a
 * Decimal's scale part of its value). Named by the edge's hook ({@link CompatEdge#rules}).
 */
public final class CompatRules {

    /** Compiled legend-pure's: a Float within 2 ULP, a Decimal's scale part of its value. */
    public static final TestPlan.JudgingRules COMPILED_PURE = new TestPlan.JudgingRules(2, true);

    private CompatRules() {
    }
}
