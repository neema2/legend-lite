// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexecutionplan;

import com.legend.error.NotImplementedException;
import com.legend.executionplan.Extension;

import java.util.List;

/**
 * THE PRODUCT'S EDGE HOOK (census C1, the orchestrator's ruling of 2026-11-17: a typed hook with the product's
 * refusal, where a null selected lite's rules until then): it records no ledger ({@link #records} false), so the test
 * door plans no record for it, and it refuses by name, pointing to the edge, each ask a ledger's recorder answers. A
 * ruled hook (HookRulingTest): every other {@link Edge} is the edge's ({@code com.legend.edge}).
 */
public enum NoEdge implements Edge {
    /** Lite's runs: no ledger is recorded. */
    NONE;

    private static final String WHY = " needs the compatibility edge's recorder (com.legend.edge): lite's own runs"
            + " record no ledger";

    @Override
    public boolean records() {
        return false;
    }

    @Override
    public List<Extension> effect(List<String> texts, List<Boolean> queries) {
        throw new NotImplementedException("an effect's ledger texts" + WHY);
    }

    @Override
    public List<Extension> seed(List<com.legend.executionplan.ExecutionPlan.LoadStep> steps,
            java.util.Map<String, String> content) {
        throw new NotImplementedException("a seed's referee's copy" + WHY);
    }

    @Override
    public void seeded(List<Extension> planned, List<Boolean> answeredRows) {
        throw new NotImplementedException("a seed's ledger" + WHY);
    }

    @Override
    public void effectRan(List<Extension> planned) {
        throw new NotImplementedException("an effect's ledger" + WHY);
    }
}
