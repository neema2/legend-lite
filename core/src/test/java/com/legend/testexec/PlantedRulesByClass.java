// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

/**
 * A PLANTED PRODUCT CLASS for {@code JudgingRulesReachTest.theGuardBites}: it names the rules' type by class literal
 * alone (reflection over its record components) and holds the compatibility constant's old name as a string -- neither
 * a member reach. Test sources only.
 */
public final class PlantedRulesByClass {

    private PlantedRulesByClass() {
    }

    static Object components() {
        return java.util.List.of(com.legend.testexecutionplan.TestPlan.JudgingRules.class.getRecordComponents(),
                "COMPILED_PURE_COMPAT");
    }
}
