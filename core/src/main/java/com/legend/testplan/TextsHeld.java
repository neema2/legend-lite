// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

/**
 * How the plan holds an edge-judged comparison's two texts (census C3, 2026-11-17: asked of the {@link EngineTestText}
 * hook, the comparison's facts the edge's own record): the golden a side the step reads; our text a side the step
 * reads; both constants of the plan, equal or not. The test planner gates the assert on it; it knows no text.
 */
public enum TextsHeld { GOLDEN_READ, OURS_READ, CONSTANTS_EQUAL, CONSTANTS_DIFFER }
