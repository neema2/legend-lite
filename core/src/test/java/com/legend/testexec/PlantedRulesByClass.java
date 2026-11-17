// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

import com.legend.testexecutionplan.TestPlan.JudgingRules;

/**
 * A PLANTED VIOLATOR for {@code JudgingRulesReachTest} (the GUARD stream, C.3's review, 2026-11-17): the judging rules
 * named by class literal alone -- every constant by {@code EnumSet.allOf}, the compatibility one by its name as a
 * string -- so no member of the type is reached. Its constant pool names the type and holds the name. Never called;
 * read as bytecode only (the edge's test library).
 */
public final class PlantedRulesByClass {

    private PlantedRulesByClass() {
    }

    static Object all() {
        return java.util.EnumSet.allOf(JudgingRules.class);
    }

    static Object byName() {
        return Enum.valueOf(JudgingRules.class, "COMPILED_PURE_COMPAT");
    }
}
