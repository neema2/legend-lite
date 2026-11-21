// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

/**
 * LEGEND-PURE'S WORDS OF A RAISE, WHERE THE EDGE RUNS THE PRODUCT'S DOOR (K10 of the edge audit, the orchestrator's
 * ruling (A) of 2026-11-17): the edge's routes that answer as legend-engine does (pure/v1's execute, the PCT's plan
 * route) run the product's door in their style, and answer a raise of a planned site in legend-pure's words, worded
 * after the run from the site's record ({@link PureMessages#worded}; stage B's B1, 2026-11-20: until then a recorder of
 * the planning call kept them) -- its kind and place unchanged.
 */
public final class RaisedWords {

    private RaisedWords() {
    }

    /** {@code run} in {@code style}; a raise of a planned site it throws answered in legend-pure's words. */
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
                r.site().where(), PureMessages.worded(r.site()), r);
    }
}
