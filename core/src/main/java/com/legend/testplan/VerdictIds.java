// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

/**
 * A BODY'S VERDICT IDS (the plan-as-data design's step 0, docs/PLAN_AS_DATA_2026_11_07.md, 2026-11-08): one counter per
 * test body, shared by every planner that makes a verdict for it (the host judge's, the database judge's batch, the
 * agreement judge's), so each verdict of the body's plan has an id no other has. Every reference to a verdict -- the
 * runner's row for it, its rules, its batch member, its agreement pair -- is by this id, never by the Java object, so a
 * plan read back from its JSON names the same verdicts. Planning state: it never reaches a run. A verdict rebuilt from
 * another (a raising verdict's statement, a caller's message carried, the agreement judge's combined verdict) keeps the
 * id of the one it replaces.
 */
public final class VerdictIds {

    private int last;

    /** The next id: 1 for a body's first verdict. */
    public int next() {
        return ++last;
    }
}
