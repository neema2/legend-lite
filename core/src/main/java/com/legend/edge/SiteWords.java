// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import java.util.Objects;

/**
 * LEGEND-PURE'S WORDS OF ONE RAISE SITE, PLANNED (K10b of the edge audit, the orchestrator's ruling (A) of 2026-11-17):
 * site {@code n} of the statement whose text is {@code text}, worded at planning from the operands its render wrote
 * ({@link PureMessages#words}) by the edge's recorder ({@link SiteWordsRecorder}). The compatibility edge's data, never
 * the product plan's: a test plan holds it at the step that sends {@code text}, where the edge's hearing looks it up
 * ({@code Appeals#raisedWords}); pure/v1's host answers keep their own table of it. Keyed by {@code (text, n)}.
 */
public record SiteWords(String text, int n, String words) implements com.legend.testexecutionplan.Extension.Heard {

    public SiteWords {
        Objects.requireNonNull(text, "a site's statement text");
        Objects.requireNonNull(words, "a site's words");
        if (n < 1) {
            throw new IllegalArgumentException("a raise site is numbered from 1: " + n);
        }
    }
}
