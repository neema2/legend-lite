// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import java.util.Objects;

/**
 * LEGEND-PURE'S WORDS OF ONE RAISE SITE, PLANNED (K10b of the edge audit, the orchestrator's ruling (A) of 2026-11-17):
 * site {@code n} of the statement the step that holds it sends at index {@code at} of its sent texts
 * ({@code TestPlan.sentTexts}), worded at planning from the operands its render wrote ({@link PureMessages#words}) by
 * the edge's recorder ({@link SiteWordsRecorder}). The compatibility edge's data, never the product plan's: a test plan
 * holds it at the step that sends the statement, where the edge's hearing looks it up ({@code Appeals#raisedWords}).
 * Keyed by {@code (step, at, n)}: the statement's text is the plan's once, at its step, never copied here (the K10 trim,
 * 2026-11-18). pure/v1's host answers keep their own table, by text, of the plan they answer.
 */
public record SiteWords(int at, int n, String words) implements com.legend.testexecutionplan.Extension {

    public SiteWords {
        Objects.requireNonNull(words, "a site's words");
        if (at < 0) {
            throw new IllegalArgumentException("a statement's index among its step's sent texts is from 0: " + at);
        }
        if (n < 1) {
            throw new IllegalArgumentException("a raise site is numbered from 1: " + n);
        }
    }
}
