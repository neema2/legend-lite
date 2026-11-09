// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.driver;

import com.legend.base.Nullable;
import com.legend.error.AssertFailed;
import com.legend.exec.PlanRunner;
import com.legend.testexecutionplan.AssertListener;
import com.legend.judge.RootLoops;
import com.legend.testexecutionplan.TypedSide;
import com.legend.testexecutionplan.Agreement;
import com.legend.testexecutionplan.RootLoop;
import com.legend.testexecutionplan.TestPlan;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * THE AGREEMENT JUDGE, RUN FROM THE PLAN (docs/PLAN_AS_DATA_2026_11_07.md, the design's step 6, 2026-11-08): each
 * assert's pairing ({@link Agreement}) and its database half's root loop read off the verdict group that closes it --
 * the host half's verdict judged over the sides the runner read and HELD, the database half's rows to its loop, and
 * the agreement rule ({@link AgreementArm#agree}) deciding once the group's last verdict is in. The run state is one
 * {@link Run} per assert; nothing here is planned (the pairing was the planner's objects, found by verdict, until this
 * step: {@code AgreementAsserts.One}).
 */
final class AgreementRuns {

    private final Map<Integer, Run> byVerdict = new HashMap<>();
    /** A database root's failed row's appeal, by its number in the plan (step 8b). */
    private final java.util.function.IntFunction<com.legend.judge.RootLoops.Appeal> rowAppeals;

    /** The runs of {@code steps}' asserts (a branch's sides included), each keyed by every verdict it reads. */
    AgreementRuns(List<? extends TestPlan.TestStep> steps,
            java.util.function.IntFunction<com.legend.judge.RootLoops.Appeal> rowAppeals) {
        this.rowAppeals = rowAppeals;
        add(steps);
    }

    private void add(List<? extends TestPlan.TestStep> steps) {
        for (TestPlan.TestStep step : steps) {
            switch (step) {
                case TestPlan.Verdicts group -> {
                    Agreement pairing = group.agreement();
                    if (pairing == null) {
                        continue;
                    }
                    Run run = new Run(group, pairing, rowAppeals);
                    for (TestPlan.Verdict v : group.verdicts()) {
                        byVerdict.put(v.id(), run);
                    }
                    // an assertError over an assert: the host's own step is a group before this one
                    byVerdict.put(pairing.hostVerdict(), run);
                }
                case TestPlan.Branch b -> {
                    add(b.then());
                    add(b.otherwise());
                }
                case TestPlan.Run r -> {
                }
                case TestPlan.FrameRun f -> {
                }
            }
        }
    }

    /** The assert the runner handed {@code verdict} for. */
    Run of(TestPlan.Verdict verdict) {
        Run run = byVerdict.get(verdict.id());
        if (run == null) {
            throw new IllegalStateException("a verdict step no assert of the plan pairs: " + verdict.name());
        }
        return run;
    }

    /** One assert's run: its pairing, its database half's loop as it runs (null: the database judge declines it),
     *  the host half's held verdict, an assertError's raising row. */
    static final class Run {
        private final Agreement pairing;
        private final RootLoops.@Nullable Run database;
        private final int closing;
        private AssertVerdicts.@Nullable HostHeld held;
        /** The host's facts the held verdict was judged by: the plan's, from the verdict the runner handed. */
        private TestPlan.@Nullable Host heldHost;
        private @Nullable List<Object> raisingRow;
        private final java.util.function.IntFunction<com.legend.judge.RootLoops.Appeal> rowAppeals;

        Run(TestPlan.Verdicts group, Agreement pairing,
                java.util.function.IntFunction<com.legend.judge.RootLoops.Appeal> rowAppeals) {
            this.pairing = pairing;
            this.rowAppeals = rowAppeals;
            RootLoop loop = group.roots();
            this.database = loop == null ? null : new RootLoops.Run(loop);
            this.closing = group.verdicts().get(group.verdicts().size() - 1).id();
        }

        /** The host half's sides for {@code verdict} as the runner read them ({@code rows}: their rows, or the error
         *  reading them raised, the judgment deciding it), judged and HELD ({@link AssertVerdicts#heldAtItsStep}): the
         *  verdict of record is decided once the database half's rows are in ({@link #received}); a database member's
         *  (no base, so nothing read) is nothing to the host, but an error reading it is raised, never dropped. */
        void hostSides(TestPlan.Verdict verdict, java.util.function.Supplier<List<PlanRunner.Rows>> rows,
                java.util.function.@Nullable Supplier<PlanRunner.Rows> message, @Nullable AssertListener listener,
                TestPlan.JudgingRules rules) {
            if (verdict.id() != pairing.hostVerdict()) {
                if (verdict.id() != pairing.raisingVerdict()) {
                    rows.get();
                }
                // the database's raising step is read for the database half: what it raised is its statement's, bound
                return;
            }
            // the host's facts as the plan carries them on the verdict the runner handed (step 4, 2026-11-08)
            TestPlan.Host h = java.util.Objects.requireNonNull(verdict.host(),
                    "a host verdict the plan carries no host for");
            held = AssertVerdicts.heldAtItsStep(h, listener,
                    () -> HostSides.shapedBase(h.assertion().name(), h.shapes(), rows.get()), rules,
                    message == null ? null : HostSides.message(h.shapes(), message));
            heldHost = h;
        }

        /** The database half's row for {@code verdict}; null where {@code verdict} is not the assert's last. At the
         *  last, both judges have read: the agreement rule decides, the listener is told the verdict of record, and a
         *  failure raises (the walk's tryAdjudicate over its agreement arm). */
        @Nullable TypedSide received(TestPlan.Verdict verdict, @Nullable List<Object> row,
                @Nullable AssertListener listener, com.legend.judge.@Nullable SideReader reader) {
            if (pairing.raising()) {
                if (verdict.id() != pairing.raisingVerdict()) {
                    return null;   // the host's own step: its rows are the host's, read at hostSides
                }
                raisingRow = row;
                return decided(verdict, listener, reader);
            }
            if (database != null) {
                // a member's row to the database half's loop (the host's verdict on the first member is one); a verdict
                // of no member answers none (the host's own step where every root was decided at planning)
                database.received(verdict, row);
            }
            if (verdict.id() != closing) {
                return null;
            }
            return decided(verdict, listener, reader);
        }

        /** The runner's word on the group's fused statement, to the database half's loop. */
        void fused(com.legend.error.@Nullable DataError rejected) {
            java.util.Objects.requireNonNull(database, "a fused statement for an assert the database declined")
                    .fused(rejected);
        }

        /** The assert decided at its last row, inside the program's run: a row its database half planned ahead read
         *  through the program's {@code reader} (THE WALL, 2026-11-06: no run of its own). */
        private TypedSide decided(TestPlan.Verdict last, @Nullable AssertListener listener,
                com.legend.judge.@Nullable SideReader reader) {
            AssertVerdicts.HostHeld h = held;
            TestPlan.Host host = heldHost;
            if (h == null || host == null) {
                throw new IllegalStateException(last.name() + ": the database half's rows arrived before the host half"
                        + " judged its sides");
            }
            com.legend.testexecutionplan.Assertion a = host.assertion();
            RootLoops.Run db = database;
            List<Object> raised = raisingRow;
            boolean raising = pairing.raising();
            java.util.function.IntFunction<com.legend.judge.RootLoops.Appeal> appeals = rowAppeals;
            return AssertVerdicts.agreedAtItsStep(a, listener, () -> AgreementArm.agree(a.name(), listener,
                    () -> {
                        AssertFailed failed = h.failed();
                        if (failed != null) {
                            throw failed;
                        }
                        return h.verdict();
                    },
                    () -> {
                        if (raising) {
                            // an assertError (S135): the database's statement compared what the function raised
                            return DatabaseJudge.verdictOf(a.name(), java.util.Objects.requireNonNull(raised,
                                    "an assertError's verdict statement answered no row"));
                        }
                        if (db == null) {
                            return null;   // the database judge declines: not this shape -- strict, a failure
                        }
                        // the verdict alone: a failure both judges agree on is told in the host's words, its message
                        // the host's side on the assert's verdict (AgreementArm)
                        db.judge(null, reader, false, DatabaseJudge::verdictOf, appeals);
                        return AssertVerdicts.ok();
                    }));
        }
    }
}
