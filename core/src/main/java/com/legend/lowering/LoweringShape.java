// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

/**
 * THE LOWERING'S SHAPE OPTIONS (Phase 6's B5, 2026-11-17, the orchestrator's ruling (A)): the choices that change the
 * SHAPE of a lowered statement without changing its answer. The product lowers {@link #LEAN}; another shape is chosen by
 * whoever asks the lowering for it (the compatibility edge, which writes another engine's text). Each option:
 *
 * @param shareRelations       a relation read more than once is written once, a common table expression its reads name
 *                             (smaller SQL, one evaluation)
 * @param foldKeyedCounts      a count of rows a declared key makes one is the constant 1 ({@link KeyedCounts}: work the
 *                             model's knowledge saves the database)
 * @param pruneComputed        an unread computed projection whose value raises nothing is dropped, beside the unread
 *                             columns always dropped ({@link SubselectPrune}: smaller SQL)
 * @param viewsAsDerivedTables a view at relation position is an aliased derived table of its own, not its body lowered
 *                             flat (a nesting that changes no answer)
 * @param elideWireCasts       a mapping's wire coercion is not written as a SQL cast: the reader converts the value on
 *                             the wire ({@link CastPolicy})
 * @param existsAsJoinDistinct a filter's correlated {@code EXISTS} over a to-many join is a join to the distinct keys
 *                             ({@link ExistsJoinForm}; until 2026-11-17 {@code withEngineExistsJoinForm}): each planner
 *                             chooses it as before, the product's planners too
 */
public record LoweringShape(boolean shareRelations, boolean foldKeyedCounts, boolean pruneComputed,
        boolean viewsAsDerivedTables, boolean elideWireCasts, boolean existsAsJoinDistinct) {

    /** The product's shape: lean SQL. */
    public static final LoweringShape LEAN = new LoweringShape(true, true, true, false, false, false);

    /** This shape, an {@code EXISTS} over a to-many join written as a join to the distinct keys. */
    public LoweringShape withExistsAsJoinDistinct() {
        return new LoweringShape(shareRelations, foldKeyedCounts, pruneComputed, viewsAsDerivedTables, elideWireCasts,
                true);
    }
}
