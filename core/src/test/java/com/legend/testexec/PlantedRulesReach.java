// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

import com.legend.testexecutionplan.TestPlan.JudgingRules;

/**
 * A PLANTED PRODUCT CLASS for {@code JudgingRulesReachTest.theGuardBites} (the GUARD stream's C.3; re-cut 2026-11-18 for
 * C1 item 5, the rules a record of data): it builds the compatibility rules by their values (a constructor reach,
 * pinned by count -- the one way left to pick legend-pure's rules without naming the edge's preset), compares a rules
 * value with lite's (equals, ==), hashes one, and reads a data parameter by call and by method reference. Test sources
 * only; never in a product jar.
 */
public final class PlantedRulesReach {

    private PlantedRulesReach() {
    }

    static Object built() {
        return new JudgingRules(2, true);
    }

    static boolean lite(JudgingRules rules) {
        return rules.equals(JudgingRules.LITE) || rules == JudgingRules.LITE;
    }

    static int hashed(JudgingRules rules) {
        return rules.hashCode();
    }

    static Object data(JudgingRules rules) {
        java.util.function.Predicate<JudgingRules> tolerant = JudgingRules::floatTolerance;
        return java.util.List.of(rules.floatUlps(), tolerant.test(rules));
    }
}
