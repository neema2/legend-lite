// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import com.legend.testexecutionplan.Extension;
import com.legend.testexecutionplan.TestPlan;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * THE APPEALS A TEST PLAN'S VERDICTS NAME (the plan-as-data design's step 8b, 2026-11-08): each engine-text judgment's
 * appeal and each database root's row's, by the verdict that names it -- the plan's {@code extensionsByVerdict}, built
 * from the appeals the arms planned; and, at run, the appeals the plan carries by their numbers.
 */
final class AppealsByVerdict {

    private AppealsByVerdict() {
    }

    /** The appeals {@code plan}'s verdicts name, from {@code planned}, by verdict id. */
    static Map<Integer, List<Extension>> of(TestPlan plan, EngineTextAppeals planned) {
        Map<Integer, List<Extension>> out = new LinkedHashMap<>();
        com.legend.testexecutionplan.PlannedAppeals.named(plan).forEach((verdict, numbers) -> {
            List<Extension> at = new ArrayList<>();
            for (int n : numbers) {
                at.add(planned.of(n));
            }
            out.put(verdict, at);
        });
        return out;
    }






}
