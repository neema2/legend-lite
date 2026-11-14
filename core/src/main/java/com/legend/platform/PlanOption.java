// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.platform;

/**
 * LITE'S OWN PLANNING OPTIONS -- beside the engine's execution feature flags ({@link Feature}, which mirror
 * legend-engine's member for member and take no member of lite's). An option is a COMPILE-TIME choice of one query's
 * lowering ({@code Lowerer.withOptions}), off unless asked for: carried by a query ({@code TypedQuery.withOptions}), a
 * run's options ({@code ExecuteOptions.withOptions}) or a planning environment ({@code PlanEnv.withOptions}).
 */
public enum PlanOption {
    /**
     * AS-OF JOINS ANSWER THE SAME ROW ON EVERY DATABASE (the user, 2026-11-07: "Can we make as of deterministic?" -- "I
     * would have option to not take this cost penalty by default but then our tests should use the option"): where
     * right rows tie on an as-of join's match, the right side keeps one row per match before the join, the greatest
     * by its columns in order (Lowerer.oneRowPerMatch) -- a window over the right side, its cost. Off, an as-of join is
     * written as each database writes one: DuckDB's and Snowflake's own answer any one of tied rows, the emulation of
     * a database without one answers each (docs/SEMANTICS_REGISTER.md). The tests turn it on.
     */
    DETERMINISTIC_AS_OF
}
