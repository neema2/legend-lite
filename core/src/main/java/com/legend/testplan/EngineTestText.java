// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

/**
 * THE ENGINE-TEXT ASSERT'S ARM, a test-planning facet of the plan style (W9's judge half, the orchestrator's ruling of
 * 2026-11-16; docs/records/e-java-w9-judge-half-2026-11-16.md): an assert that compares legend-engine's text (a
 * golden {@code toSQLString} or plan text) with ours is upstream's form, so its planning is the compatibility edge's.
 * The test planner recognizes the shape and asks the style for this facet; the product's style refuses it by name,
 * and the edge answers it ({@code edge.EdgeEngineTestText}). Its planning entries move behind it in small steps
 * (S2b on): the arm itself, {@code SqlTextVerdicts}, goes to the edge over a typed facade of the test planner.
 */
public interface EngineTestText {
}
