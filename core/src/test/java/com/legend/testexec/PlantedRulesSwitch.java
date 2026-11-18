// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

import com.legend.testexecutionplan.TestPlan.JudgingRules;

/**
 * A PLANTED PRODUCT CLASS for {@code JudgingRulesReachTest.theGuardBites}: a pattern switch on the rules' type alone,
 * whose label javac hands {@code SwitchBootstraps.typeSwitch} as a class constant -- no member reach shows it, its
 * constant pool does. Test sources only.
 */
public final class PlantedRulesSwitch {

    private PlantedRulesSwitch() {
    }

    static int of(Object o) {
        return switch (o) {
            case JudgingRules r -> 1;
            default -> 0;
        };
    }
}
