// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.model.FunctionId;
import com.legend.platform.Feature;

import java.util.Set;

/**
 * THE EDITION'S TABLE (the edition program, docs/records/edition-plan-to-finish-stage-b-2026-11-25.md section 2.2): what
 * a compatibility door hands the lowering so that a function becomes the SQL its audience means. A row names ANOTHER
 * PRODUCT NATIVE whose rule lowers the call ({@code substring} as {@code substringAsGiven}): data, never code, and never
 * SQL -- the rule it names is the product's, written where every rule is ({@link Scalars}), named by what it is and not
 * by who wants it. The product's own table ({@link LiteEditionTable}) has no row: lite means what its own rules say.
 *
 * <p>The lowering asks at the one place it picks a scalar native's rule ({@link Scalars#lower}); everything that
 * becomes SQL passes there, a function's body and a mapping's code included, because by then they are one tree.
 *
 * <p>THE STAMP picks the column. A call the planner stamped as outside any store query
 * ({@code TypedNativeCall.outsideStore}) is asked for with {@code outsideStore} true: at legend-engine's door that is
 * code its own Java runs (Pure's meaning), at the corpus's the test's own code around {@code execute()}. A door whose
 * two columns are the same never asks for stamps ({@link #stampsOutsideStore}).
 */
public interface EditionTable {

    /** The native whose rule lowers a call to {@code key}, or null for {@code key}'s own rule.
     *
     *  @param key          the call's resolved native
     *  @param outsideStore the call's stamp: outside any query sent to a store
     *  @param features     legend-engine's feature flags the statement runs under (a flag may choose the row) */
    @com.legend.base.Nullable FunctionId lowersAs(FunctionId key, boolean outsideStore, Set<Feature> features);

    /** Whether this table's two columns differ, so the planner stamps the calls outside a store query. */
    boolean stampsOutsideStore();
}
