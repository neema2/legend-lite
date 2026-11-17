// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

/**
 * THE LOWERING'S SHAPE OPTIONS (Phase 6's B5, 2026-11-17, the orchestrator's ruling (A)): the choices that change the
 * SHAPE of a lowered statement without changing its answer. The product lowers {@link #LEAN}; another shape is chosen by
 * whoever asks the lowering for it (the compatibility edge, which writes another engine's text). Each option:
 *
 * (existsAsJoinDistinct: the LoweringForms member existsJoin since 2026-11-17, the edge audit's J19 -- the product's
 * EXISTS the leanest SQL, legend-engine's join to the distinct keys the edge's shape alone.)
 * (shareRelations, foldKeyedCounts, pruneComputed: members of {@link LoweringForms} since 2026-11-17, the edge audit's J23
 * -- the product's form writes lean SQL, legend-engine's shape none of it.)
 *
 * @param forms                a view at relation position and a mapping's wire coercion, the shape's two-form choices
 *                             ({@link LoweringForms#view}, {@link LoweringForms#wireCast}; the audit's K5, 2026-11-17:
 *                             until then two flags the product branched on, set by the edge alone): the product's
 *                             {@link ProductLoweringForms} lowers a view flat and writes a wire cast; legend-engine's
 *                             (the edge's) nests a view as a derived table and leaves a wire coercion to its reader
 */
public record LoweringShape(LoweringForms forms) {

    /** The product's shape: lean SQL. */
    public static final LoweringShape LEAN = new LoweringShape(ProductLoweringForms.INSTANCE);
}
