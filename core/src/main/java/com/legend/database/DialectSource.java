// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.database;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.sql.dialect.SqlDialect;

/**
 * THE SPELLING HOOK (phase D's D1.0, docs/data-delivery-homework/D_BRIEF_2026_11_10.md §0; the design approved by the
 * orchestrator 2026-11-12): where a plan's statements take their dialect from, handed to the planner by its door. The
 * product's doors write {@link Databases#DIALECTS} -- the product's own dialects, lite's own spellings -- with no null,
 * no mode and no default branch anywhere below them. The compatibility edge ({@code com.legend.edge}) is the one other
 * supplier: its dialects subclass the product's and override only the arms that spell legend-pure's or legend-engine's
 * text, so the same typed plan is spelled upstream's way for PCT, the corpus and pure/v1 (G5: the edge depends on the
 * product, never the reverse). A memo of anything a dialect wrote keys on the dialect it was written in (the plan
 * caches' rule, the orchestrator's addition 1).
 */
@FunctionalInterface
public interface DialectSource {

    /** The dialect a statement on {@code type} is written in; a database with none refused by name, as
     *  {@link Databases#dialect} refuses it. */
    SqlDialect dialect(DatabaseType type);
}
