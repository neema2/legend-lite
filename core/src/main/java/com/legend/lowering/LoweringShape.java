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
 * @param forms                a view at relation position and a mapping's wire coercion, the shape's two-form choices
 *                             ({@link LoweringForms#view}, {@link LoweringForms#wireCast}; the audit's K5, 2026-11-17:
 *                             until then two flags the product branched on, set by the edge alone): the product's
 *                             {@link ProductLoweringForms} lowers a view flat and writes a wire cast; legend-engine's
 *                             (the edge's) nests a view as a derived table and leaves a wire coercion to its reader
 * @param existsAsJoinDistinct a filter's correlated {@code EXISTS} over a to-many join is a join to the distinct keys
 *                             ({@link ExistsJoinForm}; until 2026-11-17 {@code withEngineExistsJoinForm}): each planner
 *                             chooses it as before, the product's planners too
 */
public record LoweringShape(boolean shareRelations, boolean foldKeyedCounts, boolean pruneComputed,
        LoweringForms forms, boolean existsAsJoinDistinct) {

    /** The product's shape: lean SQL. */
    public static final LoweringShape LEAN = new LoweringShape(true, true, true, ProductLoweringForms.INSTANCE, false);

    /** This shape, an {@code EXISTS} over a to-many join written as a join to the distinct keys. */
    public LoweringShape withExistsAsJoinDistinct() {
        return new LoweringShape(shareRelations, foldKeyedCounts, pruneComputed, forms, true);
    }
}
