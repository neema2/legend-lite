// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.executionplan;

/**
 * THE EDGE'S DATA IN A PLAN (docs/PLAN_AS_DATA_2026_11_07.md 2.7, the user's ruling of 2026-11-08: the edge plans,
 * under the compatibility rules, into a typed extension point): a record of the compatibility edge's, held by a raise
 * site ({@link ExecutionPlan.Site#extensions}: an upstream mode's words of the site, K1 of the presentation program,
 * S1, 2026-11-19), in a product plan or a test plan's statements, or by a test
 * plan by step ({@code TestPlan#extensionsByStep}: an effect's or a seed's ledger texts) or by verdict
 * ({@code TestPlan#extensionsByVerdict}). Moved down from the test plan's package so both plan types share one seam
 * (S1, the design's H4). The plans declare it and the edge implements it -- the edge depends on the product, never the
 * reverse (G5); its JSON is the edge's codec's ({@link ExtensionCodec}). Under lite's own rules nothing is planned for
 * the edge, and the product never reads one.
 */
public interface Extension {
}
