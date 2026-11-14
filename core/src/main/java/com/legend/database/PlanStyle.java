// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.database;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.sql.dialect.SqlDialect;

/**
 * THE PLAN STYLE (renamed from SqlSpelling 2026-11-12, the orchestrator's ruling: it will carry the lowering strategies
 * too, D's lowering hook). The spelling hook (phase D's D1.0, docs/data-delivery-homework/D_BRIEF_2026_11_10.md §0;
 * the design approved by the orchestrator 2026-11-12): where a plan's statements take their dialect from, handed to the planner by its door. The
 * product's doors write {@link #PRODUCT} -- the product's own dialects, lite's own spellings -- with no null,
 * no mode and no default branch anywhere below them. The compatibility edge ({@code com.legend.edge}) is the one other
 * supplier: its dialects subclass the product's and override only the arms that spell legend-pure's or legend-engine's
 * text, so the same typed plan is spelled upstream's way for PCT, the corpus and pure/v1 (G5: the edge depends on the
 * product, never the reverse). A memo of anything a dialect wrote keys on the dialect it was written in (the plan
 * caches' rule, the orchestrator's addition 1).
 */
public interface PlanStyle {

    /** The dialect a statement on {@code type} is written in; a database with none refused by name, as
     *  {@link Databases#dialect} refuses it. */
    SqlDialect dialect(DatabaseType type);

    /** legend-engine's SQL TEXT for {@code type}, as its {@code toSQLString} and its DDL string generators print it: the
     *  value of a native whose upstream answer is the engine's text. The edge's alone (D1.7's printers, pulled forward
     *  2026-11-12): the product's spelling refuses it by name. */
    com.legend.sql.dialect.EngineTextDialect engineText(DatabaseType type);

    /** legend-engine's PLAN text for {@code type} ({@code planToString}, a plan's allocations): the edge's alone, as {@link #engineText}. */
    com.legend.sql.dialect.EngineTextDialect enginePlanText(DatabaseType type, boolean quoteIdentifiers,
            @com.legend.base.Nullable String timeZone);

    /** THE SQL AN EXECUTE'S ACTIVITY RECORD REPORTS ({@code RelationalActivity.sql}; S310, RULED by the user
     *  2026-11-14, DE2 (a)): the product's is lite's own SQL for the chain, what actually runs; the compatibility edge's
     *  is legend-engine's text, for engine clients. */
    String activitySql(Activity activity);

    /** An execute's chain, written either way: the planner supplies both, the style picks one (no mode, no flag). */
    interface Activity {
        /** lite's own SQL for the chain: the product's lowering, written in the frame database's product dialect. */
        String lites();

        /** The database legend-engine's text is written for. */
        com.legend.model.ConnectionDefinition.DatabaseType engineDatabase();

        /** legend-engine's text for the chain, written by {@code printer}. */
        String engines(com.legend.sql.dialect.EngineTextDialect printer);
    }

    /** THE PRODUCT'S STYLE (phase D's D1.0): each declared type's statements written in the product's own dialect
     *  ({@link Databases#dialect}), and none of legend-engine's text; the compatibility edge hands its own (G5). */
    PlanStyle PRODUCT = new PlanStyle() {
        @Override
        public SqlDialect dialect(DatabaseType type) {
            return Databases.dialect(type);
        }

        @Override
        public com.legend.sql.dialect.EngineTextDialect engineText(DatabaseType type) {
            throw engineTextRefused("toSQLString and the DDL string generators", type);
        }

        @Override
        public com.legend.sql.dialect.EngineTextDialect enginePlanText(DatabaseType type, boolean quoteIdentifiers,
                @com.legend.base.Nullable String timeZone) {
            throw engineTextRefused("planToString and a plan's allocations", type);
        }

        @Override
        public String activitySql(Activity activity) {
            return activity.lites();
        }
    };

    /** The product writes none of legend-engine's text: the natives whose upstream value IS that text are the
     *  compatibility edge's (phase D, 2026-11-12; the product's own answer for them is the orchestrator's ruling). */
    private static com.legend.error.NotImplementedException engineTextRefused(String what, DatabaseType type) {
        return new com.legend.error.NotImplementedException(what + " on " + type + ": legend-engine's text is the"
                + " compatibility edge's (com.legend.edge), never the product's");
    }
}
