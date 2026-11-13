// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexecutionplan;

/**
 * THE AGREEMENT JUDGE'S PAIRING AS DATA (docs/PLAN_AS_DATA_2026_11_07.md, the design's step 6, 2026-11-08): one
 * statement-root assert judged by BOTH judges, carried on the last verdict group of its database segment
 * ({@link TestPlan.Verdicts#agreements}, in root order), which closes it -- the agreement rule decides once that group's
 * last verdict is in. The database half's root loop is the same group's {@link TestPlan.Verdicts#roots} (null where the
 * database judge declines the assert: the agreement rule's failure), the assert its root {@code root}: one segment's
 * group may carry several asserts (E-5c, the user's ruling O5 of 2026-11-13). Until step 6 the planner's objects held
 * the pairing, found by verdict at run.
 *
 * @param root           the assert's root in its segment's loop, by index (0 where the group carries one assert: a
 *                       loop of one root, a raising assert's or a declined one's group, which has no loop)
 * @param hostVerdict    the id of the verdict that carries the host's base: the group's first (the host's base on the
 *                       database's first member), or, for an assertError over an assert, the host's own step's, a group
 *                       before this one
 * @param raisingVerdict an assertError's raising verdict (S135: its row the database half's, comparing what the
 *                       function raised); {@link RootLoop#NO_VERDICT} for any other assert
 */
public record Agreement(int root, int hostVerdict, int raisingVerdict) {

    public Agreement {
        if (root < 0) {
            throw new IllegalArgumentException("an agreement's root is an index of its segment's loop: " + root);
        }
    }

    /** Whether the assert is an assertError judged by its raising verdict. */
    public boolean raising() {
        return raisingVerdict != RootLoop.NO_VERDICT;
    }
}
