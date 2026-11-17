// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexecutionplan;

import com.legend.base.Nullable;

/**
 * HOW A JUDGE READS A JSON TEXT ({@code assertJsonStringsEqual}'s two texts, register S144; census C1, the orchestrator's
 * ruling of 2026-11-17): a two-form kind ({@code PlanStyle.twoForm}, {@code testplan.PlanStyleFacets.TWO_FORMS}) -- the
 * product's form is strict JSON ({@link JudgeJson#STRICT}), the edge's upstream's json-simple 1.1.1 grammar. Asked at
 * planning through the plan style, at run time through the run's hearing ({@code testexec.EdgeHearing#json}).
 * Signatures only.
 */
public interface JsonGrammar {

    /** {@code text} read as a JSON value (a Long, a BigDecimal, a String, a Boolean, a List, a Map, or null); a text
     *  the grammar refuses raises {@link IllegalStateException}. */
    @Nullable Object read(String text);
}
