// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.model.FunctionId;
import com.legend.platform.Feature;

import java.util.Set;
import java.util.function.Supplier;

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
 * ({@code TypedNativeCall.outsideStore}) is code the audience's own Java runs: at legend-engine's door a host-side
 * {@code let} (Pure's meaning), at the corpus's the test's own code around {@code execute()}. A door whose two columns
 * are the same never asks for stamps ({@link #stampsOutsideStore}).
 *
 * <p>A row is chosen by what the program fixes, never by a value: the call's resolved native, its stamp, the feature
 * flags its statement runs under, a fact its typed operands state (an operand that is always empty), and the database
 * the statement runs on ({@link #on}). A row whose answer is not checked on that database is refused by name there.
 */
public interface EditionTable {

    /** The native whose rule lowers {@code call}, or null for the call's own rule.
     *
     *  @param call     the call, typed: its resolved native, its stamp, its operands' types
     *  @param features legend-engine's feature flags the statement runs under (a flag may choose the row) */
    @com.legend.base.Nullable FunctionId lowersAs(TypedNativeCall call, Set<Feature> features);

    /** Whether this table's two columns differ, so the planner stamps the calls outside a store query. */
    boolean stampsOutsideStore();

    /** Whether, for this audience, a model-to-model mapping's transforms run outside any store query in a statement
     *  that fetches a graph or not ({@code fetchesAGraph}) and whose mappings are chained over a store or not
     *  ({@code chained}): where they do, the resolver stamps them as it composes them. Asked only of a table that
     *  stamps. */
    boolean transformsOutsideStore(boolean fetchesAGraph, boolean chained);

    /** This table for the statements of one planning, which run on {@code database} (asked only when a row is
     *  chosen, after the planner has decided the runtime; null where the planning names no single database). The
     *  lowering imports no dialect and names no database: the planner binds the table before handing it over. */
    EditionTable on(Supplier<@com.legend.base.Nullable DatabaseType> database);
}
