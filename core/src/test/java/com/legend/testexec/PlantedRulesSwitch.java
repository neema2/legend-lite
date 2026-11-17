// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

import com.legend.testexecutionplan.TestPlan.JudgingRules;

/**
 * A PLANTED VIOLATOR for {@code JudgingRulesReachTest} (the GUARD stream, C.3's review, 2026-11-17): a pattern switch
 * over the judging rules ({@code case null} makes javac compile it to {@code SwitchBootstraps.enumSwitch}, its labels
 * strings): no field access, no call, no reference -- no reach a dependency graph records. Its constant pool names the
 * type and holds the labels. Never called; read as bytecode only (the edge's test library).
 */
public final class PlantedRulesSwitch {

    private PlantedRulesSwitch() {
    }

    static int pattern(JudgingRules rules) {
        return switch (rules) {
            case null -> -1;
            case LITE -> 0;
            case COMPILED_PURE_COMPAT -> 1;
        };
    }
}
