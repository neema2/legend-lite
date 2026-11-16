// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

/**
 * The edge's engine-text assert arm (W9's judge half, 2026-11-16): the facet {@link com.legend.testplan.EngineTestText}
 * the edge's style answers. Its planning entries arrive with the arm's move (S2b on).
 */
final class EdgeEngineTestText implements com.legend.testplan.EngineTestText {

    static final EdgeEngineTestText INSTANCE = new EdgeEngineTestText();

    private EdgeEngineTestText() {
    }
}
