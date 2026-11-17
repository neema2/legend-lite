// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

import com.legend.testexecutionplan.TestPlan.JudgingRules;

import java.util.function.Predicate;

/**
 * A PLANTED VIOLATOR for {@code JudgingRulesReachTest} (the GUARD stream, C.3, 2026-11-17): a class in a product package
 * that selects legend-pure's behaviour by the judging rules WITHOUT naming {@code COMPILED_PURE_COMPAT} -- a behaviour
 * asked, a behaviour by method reference, the flag read as {@code != LITE} and as an ordinal. Never called; read as
 * bytecode only. It lives in the edge's test library beside its test, so no product target carries it.
 */
public final class PlantedRulesReach {

    private PlantedRulesReach() {
    }

    static boolean byText(JudgingRules rules) {
        return rules.integerDecimalByText();
    }

    static Predicate<JudgingRules> words() {
        return JudgingRules::legendPureWords;
    }

    static boolean notLite(JudgingRules rules) {
        return rules != JudgingRules.LITE;
    }

    static boolean byOrdinal(JudgingRules rules) {
        return rules.ordinal() == 1;
    }

    static boolean data(JudgingRules rules) {
        return rules.floatTolerance();
    }
}
