// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.base.Nullable;
import com.legend.lowering.EditionTable;
import com.legend.model.ConnectionDefinition.DatabaseType;

/**
 * A DOOR'S EDITION, AS THE PLANNER READS IT (the edition program's step 2, 2026-11-26;
 * docs/records/edition-plan-to-finish-stage-b-2026-11-25.md sections 2.2 and 2.3): what a compatibility door's style
 * hands the planner so that a program means what the door's audience means. Two things:
 * <ul>
 *   <li><b>the table</b> the lowering asks for each call ({@link EditionTable}), made for the database the statement
 *       runs on. The planner knows the database; the lowering never does;</li>
 *   <li><b>where the audience's own code runs</b>, which the planner marks on the typed tree (the stamp,
 *       {@code TypedNativeCall.outsideStore}) for the table to read: a store-free {@code let}, and a model-to-model
 *       mapping's transforms.</li>
 * </ul>
 * The product's edition ({@link LiteEdition}) has no row, stamps nothing and reads no flag: lite means what its own
 * rules say. Every question here is about the program's shape or the door, never about a value.
 */
public interface Edition {

    /** Whether this edition's table answers differently for code outside any store query, so the planner stamps the
     *  calls of a query-level {@code let}'s value, and of a last statement, that read no store. */
    boolean stampsOutsideStore();

    /** Whether a TEST BODY's own code is stamped as outside any store query: everything in the body but the queries
     *  its {@code execute()}s ran. legend-engine's own tests' door asks (the relational corpus: the test harness is
     *  Pure's, the queries are legend-engine's); a door whose test bodies are one audience's throughout does not. */
    boolean stampsTestBodies();

    /** Whether, for this audience, a class's constraints run outside any store query where a checked graph fetch
     *  evaluates them. Asked only of an edition that stamps. */
    boolean constraintsOutsideStore();

    /** Whether, for this audience, a model-to-model mapping's transforms run outside any store query in a statement
     *  that fetches a graph or not ({@code fetchesAGraph}) and whose mappings are chained over a store or not
     *  ({@code chained}): where they do, the resolver stamps them as it composes them. Asked only of an edition that
     *  stamps. */
    boolean transformsOutsideStore(boolean fetchesAGraph, boolean chained);

    /** The legend-engine feature flags a statement runs under when it carries them itself ({@code withFeatureFlags}
     *  calls written in the query), at this door: the ones this edition's table reads. Lite's own doors read none
     *  (register S130: the meaning is chosen by the door, never by a setting in the query). */
    java.util.Set<com.legend.platform.Feature> writtenFlagsRead();

    /** Whether this edition's rows depend on the database a statement runs on. Where they do not, the planner makes
     *  the table without deciding the runtime's database. */
    boolean rowsByDatabase();

    /** The table for statements that run on {@code database}. The argument is null where the edition's rows do not
     *  depend on the database ({@link #rowsByDatabase}), where the planning names no runtime, and where the runtime
     *  binds model data alone; the answer is never null. */
    EditionTable table(@Nullable DatabaseType database);
}
