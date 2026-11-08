// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.driver;

import com.legend.testexecutionplan.Appeal;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A BODY'S APPEALS AS THEY ARE PLANNED, BY NUMBER (docs/PLAN_AS_DATA_2026_11_07.md 2.7; the design's step 8b,
 * 2026-11-08): each engine-text assert's appeal, the test plan's data ({@link Appeal}), numbered as its arm plans it --
 * the number its verdict's judgment, or its database root's row, names. One per body, shared by every copy of its
 * environment; the plan carries the appeals its verdicts name ({@code TestPlan.extensionsByVerdict}) and the run reads
 * them there. Until step 8b each was a closure with the environment it ran in, published for the body's run.
 */
final class EngineTextAppeals {

    private final Map<Integer, Appeal> planned = new LinkedHashMap<>();
    private int last;

    /** Holds {@code a}, answering its number (1 for a body's first). */
    int add(Appeal a) {
        last++;
        planned.put(last, a.numbered(last));
        return last;
    }

    /** The appeal numbered {@code n}. */
    Appeal of(int n) {
        Appeal a = planned.get(n);
        if (a == null) {
            throw new IllegalStateException("no appeal numbered " + n + " planned in the body");
        }
        return a;
    }
}
