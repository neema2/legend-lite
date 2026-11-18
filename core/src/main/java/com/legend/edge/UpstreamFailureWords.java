// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

/**
 * LEGEND-PURE'S WORDS FOR A FAILURE (census C1, the orchestrator's ruling of 2026-11-17): the edge's form of the
 * two-form {@link com.legend.testplan.FailureWords} kind.
 */
public enum UpstreamFailureWords implements com.legend.testplan.FailureWords {
    INSTANCE;

    @Override
    public boolean upstreamWords() {
        return true;
    }
}
