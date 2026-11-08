// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.driver;

import com.legend.base.Nullable;

import java.util.HashMap;
import java.util.Map;

/**
 * A BODY'S ENGINE-TEXT APPEALS, BY NUMBER (the plan-as-data design's step 4, 2026-11-08; docs/PLAN_AS_DATA_2026_11_07.md
 * 2.2 and 2.7): an engine-text assert's comparison is the test plan's data ({@code HostJudgment.EngineText}) but for
 * its APPEAL -- the rows leg, harness Java by the user's ruling (R3, T13, R6) -- and the environment it runs under,
 * which the plan names by number and this holds beside it. One per body, shared by every copy of its environment;
 * published for the body's run ({@link #during}) and read where a text fails ({@link #held}). An out-of-plan channel,
 * named: the design's step 8 makes the appeal the edge's extension and deletes this.
 */
final class EngineTextAppeals {

    /** An appeal and the environment its arm planned it under. */
    record Held(SqlTextVerdicts.TextAppeal appeal, BodyDriver.ExecEnv env) {
    }

    private final Map<Integer, Held> held = new HashMap<>();
    /** The database judge's row appeals (a failed engine-text row heard under the compatibility rules; step 5,
     *  2026-11-08), numbered from the same count. */
    private final Map<Integer, com.legend.judge.VerdictBatch.Appeal> rows = new HashMap<>();
    private int last;

    /** Holds the database judge's row appeal {@code a}, answering its number. */
    int addRow(com.legend.judge.VerdictBatch.Appeal a) {
        rows.put(++last, a);
        return last;
    }

    /** The row appeal numbered {@code n} of the body whose run is in progress. */
    static com.legend.judge.VerdictBatch.Appeal rowAppeal(int n) {
        EngineTextAppeals current = CURRENT.get();
        if (current == null) {
            throw new IllegalStateException("a row appeal asked for outside a body's run: " + n);
        }
        com.legend.judge.VerdictBatch.Appeal a = current.rows.get(n);
        if (a == null) {
            throw new IllegalStateException("no row appeal numbered " + n + " in the body's run");
        }
        return a;
    }

    /** Holds {@code h}, answering its number (1 for a body's first). */
    int add(Held h) {
        held.put(++last, h);
        return last;
    }

    private static final ThreadLocal<@Nullable EngineTextAppeals> CURRENT = new ThreadLocal<>();

    /** The body's appeals published for its run: a nested body's run (a fixture program inside an appeal) publishes
     *  its own and restores these when it ends. */
    static RunReader.Scope during(EngineTextAppeals appeals) {
        EngineTextAppeals prior = CURRENT.get();
        CURRENT.set(appeals);
        return () -> {
            if (prior == null) {
                CURRENT.remove();
            } else {
                CURRENT.set(prior);
            }
        };
    }

    /** The appeal numbered {@code n} of the body whose run is in progress. */
    static Held held(int n) {
        EngineTextAppeals current = CURRENT.get();
        if (current == null) {
            throw new IllegalStateException("an engine-text appeal asked for outside a body's run: " + n);
        }
        Held h = current.held.get(n);
        if (h == null) {
            throw new IllegalStateException("no engine-text appeal numbered " + n + " in the body's run");
        }
        return h;
    }
}
