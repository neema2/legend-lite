// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.base.Nullable;
import com.legend.executionplan.Extension;

/**
 * AN ENGINE-TEXT ASSERT'S COMPARISON, THE EDGE'S RECORD IN THE PLAN (census C3, 2026-11-17, the orchestrator's ruling:
 * moved out of the product's {@code HostJudgment}, held by its {@code EdgeJudged} judgment and written by
 * {@link EdgeCodec} under its own tag): the comparison the arm planned -- its name, the golden and our text where the
 * plan holds them as constants (null: the text is a side the step reads), their equality, and the decline decided
 * before them. Data alone.
 */
public record EngineTextJudgment(String name, @Nullable String golden, @Nullable String ours, boolean textEqual,
        @Nullable String decline) implements Extension {
}
