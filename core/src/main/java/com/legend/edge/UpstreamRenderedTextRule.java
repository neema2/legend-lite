// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

/**
 * UPSTREAM'S RENDERED-TEXT RULE (registers S131, S132; census C1, the orchestrator's ruling of 2026-11-17): the edge's
 * form of the two-form {@link com.legend.testplan.RenderedTextRule} kind -- where a rendered text and its golden
 * differ, the ruled line and row comparison decides.
 */
public enum UpstreamRenderedTextRule implements com.legend.testplan.RenderedTextRule {
    INSTANCE;

    @Override
    public boolean ruledWhereTheyDiffer() {
        return true;
    }
}
