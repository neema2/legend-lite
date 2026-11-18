// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

/**
 * LEGEND-PURE'S WORDS OF A RAISE, WHERE THE EDGE RUNS THE PRODUCT'S DOOR (K10 of the edge audit, the orchestrator's
 * ruling (A) of 2026-11-17): the product's plan says a raise site's own sentence, and exec says it; the edge's routes
 * that answer as legend-engine does (pure/v1's execute, the PCT's plan route) plan in a fresh recorder of their style
 * ({@link PureText#recording}), which keeps legend-pure's words of each statement's sites, and answer a raise of a
 * planned site in those words -- its kind and place unchanged. A site the recorder holds no words for (its operands
 * not literals the edge words at planning: legend-pure's sentence is then written in the SQL itself, and no site is
 * raised) is refused by name, never answered in the product's sentence.
 */
public final class RaisedWords {

    private RaisedWords() {
    }

    /** {@code run} given a fresh recorder of {@code template}; a raise of a planned site it throws answered in the words
     *  the recorder kept. */
    public static <T, X extends Exception> T run(com.legend.database.PlanStyle template, Run<T, X> run) throws X {
        SiteWordsRecorder recording = PureText.recording(template);
        try {
            return run.apply(recording);
        } catch (com.legend.error.DataError e) {
            throw worded(e, recording.read());
        }
    }

    /** A run of the product's door in a style, as its caller makes it. */
    @FunctionalInterface
    public interface Run<T, X extends Exception> {
        T apply(com.legend.database.PlanStyle style) throws X;
    }

    /** {@code e} in legend-pure's words where it is a raise of a planned site, as {@code words} holds them; else as it
     *  is. */
    static com.legend.error.DataError worded(com.legend.error.DataError e,
            java.util.Map<String, java.util.List<SiteWords>> words) {
        if (!(e.getCause() instanceof com.legend.exec.RaisedErrors.Raised r) || r.site() == null) {
            return e;
        }
        String statement = java.util.Objects.requireNonNull(r.statement(),
                "a raise of a planned site carries its statement's text");
        for (SiteWords w : words.getOrDefault(statement, java.util.List.of())) {
            if (w.n() == r.site().n()) {
                return new com.legend.error.DataError(com.legend.error.ErrorKind.valueOf(r.site().kind().name()),
                        r.site().where(), w.words(), r);
            }
        }
        throw new IllegalStateException("legend-pure's words of raise site " + r.site().n() + " of a statement the"
                + " plan's recorder words no site of that number of: " + statement, e);
    }
}
