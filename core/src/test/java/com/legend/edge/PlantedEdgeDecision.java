// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

/**
 * An edge dialect PLANTED to override a DECISION, for one test alone:
 * {@code EdgeSpellingHookTest.theSpellingOnlyRuleBitesAPlantedDecision} shows the rule that the edge overrides
 * spelling only fails on it. A test class: the rule's real check reads {@code PureText}'s classes, never this.
 */
public final class PlantedEdgeDecision extends com.legend.sql.dialect.DuckDb {

    /** The defect the rule refuses: compatibility deciding the plan's error read-back. */
    @Override
    public com.legend.sql.dialect.SqlDialect.@com.legend.base.Nullable RaiseReadBack raiseReadBack() {
        return null;
    }
}
