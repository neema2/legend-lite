// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.model.FunctionId;
import com.legend.platform.Feature;

import java.util.Set;

/**
 * THE EDITION'S TABLE (the edition program, docs/records/edition-plan-to-finish-stage-b-2026-11-25.md section 2.2): what
 * a compatibility door hands the lowering so that a function becomes the SQL its audience means. A row names ANOTHER
 * PRODUCT NATIVE whose rule lowers the call ({@code substring} as {@code substringAsGiven}): data, never code, and never
 * SQL -- the rule it names is the product's, written where every rule is ({@link Scalars}), named by what it is and not
 * by who wants it. The product's own table ({@link LiteEditionTable}) has no row: lite means what its own rules say.
 * Beside the rows, the table says which of the product's checks its audience writes ({@link #checks}: the product
 * writes every one).
 *
 * <p>The lowering asks at the one place it picks a scalar native's rule ({@link Scalars#lower}); everything that
 * becomes SQL passes there, a function's body and a mapping's code included, because by then they are one tree.
 *
 * <p>THE STAMP picks the column. A call the planner stamped as outside any store query
 * ({@code TypedNativeCall.outsideStore}) is code the audience's own Java runs: at legend-engine's door a host-side
 * {@code let} (Pure's meaning), at the corpus's the test's own code around {@code execute()}.
 *
 * <p>A row is chosen by what the program fixes, never by a value: the call's resolved native, its stamp, the feature
 * flags its statement runs under and a fact its typed operands state (an operand that is always empty). A table is
 * made by the planner for the database its statement runs on ({@code planner.Edition.table}); the lowering names no
 * database and asks for none.
 */
public interface EditionTable {

    /** The native whose rule lowers {@code call}, or null for the call's own rule.
     *
     *  @param call     the call, typed: its resolved native, its stamp, its operands' types
     *  @param features legend-engine's feature flags the statement runs under (a flag may choose the row) */
    @com.legend.base.Nullable FunctionId lowersAs(TypedNativeCall call, Set<Feature> features);

    /** Whether a check the product writes is written for this audience: true, the guarded operation, which raises
     *  lite's error of {@code kind}; false, the operation alone, the database's own answer or error passing through
     *  (legend-engine's SQL writes no check for some kinds: its door's table says which).
     *
     *  @param kind the raise the check would make
     *  @param call the call the check belongs to, whose stamp may decide it (a call outside any store query is the
     *              audience's own Java's, which checks); null where the check is no call's own (a cast, a slice) */
    boolean checks(com.legend.sql.RaiseKind kind, @com.legend.base.Nullable TypedNativeCall call);
}
