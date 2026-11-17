// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.testexecutionplan.Extension;
import com.legend.testexecutionplan.TestPlan;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * THE APPEALS A TEST PLAN CARRIES, BY NUMBER (census C2, 2026-11-17: moved from the plan's records with the appeal):
 * read at run by the edge's hearing; the numbers its verdicts name are the product's {@code NamedExtensions}.
 */
final class PlannedAppeals {

    private PlannedAppeals() {
    }

    /** Every appeal number {@code plan}'s verdicts name is carried: refused by name otherwise. */
    static void requireCarried(TestPlan plan) {
        com.legend.testexecutionplan.NamedExtensions.requireCarried(plan);
    }

    /** The appeals {@code plan} carries, by number, in their numbers' order -- the order their arms were planned. */
    static Map<Integer, Appeal> byNumber(TestPlan plan) {
        Map<Integer, Appeal> found = new java.util.HashMap<>();
        int last = 0;
        for (List<Extension> at : plan.extensionsByVerdict().values()) {
            for (Extension e : at) {
                if (e instanceof Appeal a) {
                    found.put(a.number(), a);
                    last = Math.max(last, a.number());
                }
            }
        }
        Map<Integer, Appeal> out = new LinkedHashMap<>();
        for (int n = 1; n <= last; n++) {
            Appeal a = found.get(n);
            if (a != null) {
                out.put(n, a);
            }
        }
        return out;
    }
    /** The appeal numbered {@code n} the plan carries: one heard that the plan does not carry is refused by name. */
    static Appeal of(Map<Integer, Appeal> planned, int n) {
        Appeal a = planned.get(n);
        if (a == null) {
            throw new IllegalStateException("an appeal heard (number " + n + ") that the test plan does not carry");
        }
        return a;
    }
}
