// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

/**
 * The edge's engine-text assert arm (W9's judge half, 2026-11-16): the facet {@link com.legend.testplan.EngineTestText}
 * the edge's style answers. Until the arm's code moves here (the slices after S2b), it runs the test planner's.
 */
final class EdgeEngineTestText implements com.legend.testplan.EngineTestText {

    static final EdgeEngineTestText INSTANCE = new EdgeEngineTestText();

    private EdgeEngineTestText() {
    }

    @Override
    public com.legend.testplan.EngineTestText.@com.legend.base.Nullable Planned plan(
            com.legend.testplan.EngineTestText.Ask ask) {
        return com.legend.testplan.EngineTestText.arm(ask);
    }
}
