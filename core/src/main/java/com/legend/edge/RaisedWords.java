// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

/**
 * LEGEND-PURE'S WORDS OF A RAISE, WHERE THE EDGE RUNS THE PRODUCT'S DOOR (K10 of the edge audit, the orchestrator's
 * ruling (A) of 2026-11-17; K1 of the presentation program, S1, 2026-11-19): the product's plan says a raise site's own
 * sentence, and exec says it; the edge's routes that answer as legend-engine does (pure/v1's execute, the PCT's plan
 * route) plan in their style, which plans legend-pure's words of each site as the site's own record
 * ({@link SiteWords}, {@link SiteWords#planned}), and answer a raise of a planned site in those words -- its kind and place
 * unchanged. One stateless call: the raised site carries its words ({@code exec.RaisedErrors.Raised#site}), so nothing
 * is kept beside the plan (until S1 a recorder minted per planning call kept them, and was read once after it). A site
 * planned with no words (its operands not literals the edge words at planning: legend-pure's sentence is then written
 * in the SQL itself, and no site is raised) is refused by name, never answered in the product's sentence.
 */
public final class RaisedWords {
    private RaisedWords() {
    }

    /** {@code run} given {@code style}; a raise of a planned site it throws answered in the words the site carries. */
    public static <T, X extends Exception> T run(com.legend.database.PlanStyle style, Run<T, X> run) throws X {
        try {
            return run.apply(style);
        } catch (com.legend.error.DataError e) {
            throw worded(e);
        }
    }

    /** A run of the product's door in a style, as its caller makes it. */
    @FunctionalInterface
    public interface Run<T, X extends Exception> {
        T apply(com.legend.database.PlanStyle style) throws X;
    }

    /** {@code e} in legend-pure's words where it is a raise of a planned site; else as it is. */
    static com.legend.error.DataError worded(com.legend.error.DataError e) {
        if (!(e.getCause() instanceof com.legend.exec.RaisedErrors.Raised r) || r.site() == null) {
            return e;
        }
        return new com.legend.error.DataError(com.legend.error.ErrorKind.valueOf(r.site().kind().name()),
                r.site().where(), wordsOf(r), r);
    }

    /** legend-pure's words of the raised site {@code r}, worded HERE, after the run, from the site's own typed record:
     *  its kind and its operands ({@link PureMessages#worded}; H13 spike, slice 1.2). Nothing was worded at planning
     *  and nothing is looked up: the plan carries no upstream words. An operand the plan did not know (computed) is
     *  withheld (A3). */
    static String wordsOf(com.legend.exec.RaisedErrors.Raised r) {
        com.legend.executionplan.ExecutionPlan.Site site = java.util.Objects.requireNonNull(r.site(),
                "a raise of a planned site");
        PureMessages.Worded worded = PureMessages.worded(site.kind(), site.operands());
        H13Log.raised(site, worded);
        return worded.sentence();
    }
}
