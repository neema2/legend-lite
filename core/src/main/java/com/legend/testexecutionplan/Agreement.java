// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexecutionplan;

/**
 * THE AGREEMENT JUDGE'S PAIRING AS DATA (docs/PLAN_AS_DATA_2026_11_07.md, the design's step 6, 2026-11-08): one
 * statement-root assert judged by BOTH judges, carried on its last verdict group ({@link TestPlan.Verdicts#agreement}),
 * which closes it -- the agreement rule decides once that group's last verdict is in. The database half's root loop is
 * the same group's {@link TestPlan.Verdicts#roots} (null where the database judge declines the assert: the agreement
 * rule's failure). Until this step the planner's objects held the pairing, found by verdict at run.
 *
 * @param hostVerdict    the id of the verdict that carries the host's base: the group's first (the host's base on the
 *                       database's first member), or, for an assertError over an assert, the host's own step's, a group
 *                       before this one
 * @param raisingVerdict an assertError's raising verdict (S135: its row the database half's, comparing what the
 *                       function raised); {@link RootLoop#NO_VERDICT} for any other assert
 */
public record Agreement(int hostVerdict, int raisingVerdict) {

    /** Whether the assert is an assertError judged by its raising verdict. */
    public boolean raising() {
        return raisingVerdict != RootLoop.NO_VERDICT;
    }
}
