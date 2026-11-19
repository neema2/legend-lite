// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import java.util.Objects;

/**
 * AN UPSTREAM MODE'S WORDS OF ONE RAISE SITE, PLANNED (K10b of the edge audit, the orchestrator's ruling (A) of
 * 2026-11-17; K1 of the presentation program, S1, 2026-11-19): the compatibility edge's record a raise site carries
 * ({@code ExecutionPlan.Site#extensions}), worded at planning from the literal operands its render wrote
 * ({@link PureMessages}) by the edge's form of the planner's two-form {@code SiteFacts} ({@link #planned}). The edge's
 * data, never a product field of
 * the site: a run's raise of the site carries the site whole, and the edge presents it in these words
 * ({@link RaisedWords}, {@code EdgeHostWords#caught}). Until S1 a recorder minted per planning call kept the words
 * beside the plan, keyed by the statement's text (and, in a test plan, by its step and index) and the site's number.
 */
public record SiteWords(String words) implements com.legend.executionplan.Extension {
    public SiteWords {
        Objects.requireNonNull(words, "a site's words");
    }

    /** THE COMPATIBILITY EDGE'S FORM OF THE PLANNER'S TWO-FORM {@code SiteFacts} (K1, S1, 2026-11-19): legend-pure's
     *  words of the raise site a render wrote, worded at planning from its literal operands, the site's record; none
     *  for a site whose operands are not literals the edge words at planning (an edge dialect writes legend-pure's
     *  sentence of such a raise in the SQL itself, and writes no site: {@link PureMessages#wordable}). Stateless: until
     *  then the edge's recorder, one per planning call, kept the words beside the plan. */
    static java.util.List<com.legend.executionplan.Extension> planned(com.legend.sql.dialect.RaiseSite site) {
        String words = PureMessages.wordsOrNull(site.kind(), site.operands());
        return words == null ? java.util.List.of() : java.util.List.of(new SiteWords(words));
    }

    /** The words {@code site} carries, or null where it carries none (the product's site, or one whose operands are
     *  not literals the edge words at planning: legend-pure's sentence is then written in the SQL itself, and the site
     *  is never raised). */
    static @com.legend.base.Nullable String of(com.legend.executionplan.ExecutionPlan.Site site) {
        String found = null;
        for (com.legend.executionplan.Extension e : site.extensions()) {
            if (e instanceof SiteWords w) {
                if (found != null) {
                    throw new IllegalStateException("raise site " + site.n() + " carries two records of its words");
                }
                found = w.words();
            }
        }
        return found;
    }
}
