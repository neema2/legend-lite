// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;


import com.legend.base.Nullable;
import com.legend.error.AssertFailed;
import com.legend.exec.PlanRunner;
import com.legend.testexecutionplan.AssertListener;
import com.legend.testexecutionplan.TypedSide;
import com.legend.testexecutionplan.Agreement;
import com.legend.testexecutionplan.RootLoop;
import com.legend.testexecutionplan.TestPlan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * THE AGREEMENT JUDGE, RUN FROM THE PLAN (docs/PLAN_AS_DATA_2026_11_07.md, the design's step 6, 2026-11-08): each
 * assert's pairing ({@link Agreement}) and its database half's root loop read off the verdict group that closes it --
 * the host half's verdict judged over the sides the runner read and HELD, the database half's rows to its loop, and
 * the agreement rule ({@link AgreementArm#agree}) deciding once the group's last verdict is in. The run state is one
 * {@link Run} per assert; nothing here is planned (the pairing was the planner's objects, found by verdict, until this
 * step: {@code AgreementAsserts.One}).
 *
 * <p>PER ROOT (E-5c, step 5b, the user's ruling O5 of 2026-11-13): a database segment's closing group may carry
 * several asserts, one {@link Agreement} per root of its loop. The segment's {@link Segment} holds the database half's
 * ONE loop run, shared by its roots, and one {@link Run} per assert; at the segment's closing verdict each root is
 * decided in root order -- the host's held verdict for that root against the database half's verdict for THAT root
 * alone ({@link RootLoops.Run#judgeRoot}) -- the first failure raising after the roots before it were decided and told,
 * as the database judge's loop does.
 */
final class AgreementRuns {

    private final Map<Integer, Segment> byVerdict = new HashMap<>();
    /** A database root's failed row's appeal, by its number in the plan (step 8b). */
    private final java.util.function.IntFunction<com.legend.testexec.RootLoops.Appeal> rowAppeals;
    /** The run record (hole 6): the held appeals and the caller's-message depth the two halves share. */
    private final BodyRun run;

    /** The runs of {@code steps}' asserts (a branch's sides included), each segment keyed by every verdict it reads:
     *  its closing group's, its loop's members in the groups before, and each assert's host verdict. */
    AgreementRuns(List<? extends TestPlan.TestStep> steps,
            java.util.function.IntFunction<com.legend.testexec.RootLoops.Appeal> rowAppeals, BodyRun run) {
        this.rowAppeals = rowAppeals;
        this.run = run;
        add(steps);
    }

    private void add(List<? extends TestPlan.TestStep> steps) {
        for (TestPlan.TestStep step : steps) {
            switch (step) {
                case TestPlan.Verdicts group -> {
                    if (group.agreements().isEmpty()) {
                        continue;
                    }
                    Segment segment = new Segment(group, rowAppeals, this.run);
                    for (TestPlan.Verdict v : group.verdicts()) {
                        key(v.id(), segment);
                    }
                    // a row of the segment's loop in a group before its closing one
                    for (int member : segment.members()) {
                        key(member, segment);
                    }
                    // an assertError over an assert: the host's own step is a group before this one
                    for (Run r : segment.runs) {
                        key(r.pairing.hostVerdict(), segment);
                    }
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

    private void key(int verdict, Segment segment) {
        Segment before = byVerdict.putIfAbsent(verdict, segment);
        if (before != null && before != segment) {
            throw new IllegalStateException("verdict " + verdict + " belongs to two agreement segments of the plan");
        }
    }

    /** The segment the runner handed {@code verdict} for. */
    Segment of(TestPlan.Verdict verdict) {
        Segment segment = byVerdict.get(verdict.id());
        if (segment == null) {
            throw new IllegalStateException("a verdict step no assert of the plan pairs: " + verdict.name());
        }
        return segment;
    }

    /** The segment {@code group}'s verdicts belong to: one, every verdict of the group its. */
    Segment of(TestPlan.Verdicts group) {
        Segment segment = of(group.verdicts().get(0));
        for (TestPlan.Verdict v : group.verdicts()) {
            if (of(v) != segment) {
                throw new IllegalStateException("a verdict group whose verdicts belong to two agreement segments: "
                        + v.name());
            }
        }
        return segment;
    }

    /** One database segment's asserts under the agreement judge: the database half's loop as it runs (null: the
     *  database judge declines the group's one assert), shared by its roots, and one {@link Run} per root, in root
     *  order -- or the group's one raising assert (an assertError, S135: no loop). */
    static final class Segment {
        private final RootLoops.@Nullable Run database;
        private final int closing;
        private final List<Run> runs;
        private final Map<Integer, Run> byHost = new HashMap<>();
        /** The group's one raising assert, or null (the plan's invariant: a raising assert's group closes it alone). */
        private final @Nullable Run raising;

        Segment(TestPlan.Verdicts group,
                java.util.function.IntFunction<com.legend.testexec.RootLoops.Appeal> rowAppeals, BodyRun bodyRun) {
            RootLoop loop = group.roots();
            this.database = loop == null ? null : new RootLoops.Run(loop);
            this.closing = group.verdicts().get(group.verdicts().size() - 1).id();
            List<Run> made = new ArrayList<>(group.agreements().size());
            for (Agreement a : group.agreements()) {
                Run r = new Run(a, database, rowAppeals, bodyRun);
                made.add(r);
                if (byHost.put(a.hostVerdict(), r) != null) {
                    throw new IllegalStateException("two asserts of one segment paired with the host verdict "
                            + a.hostVerdict());
                }
            }
            this.runs = List.copyOf(made);
            Run only = runs.get(0);
            this.raising = only.pairing.raising() ? only : null;
        }

        private Set<Integer> members() {
            RootLoops.Run db = database;
            return db == null ? Set.of() : db.members();
        }

        /** The host half's sides for {@code verdict} as the runner read them ({@code rows}: their rows, or the error
         *  reading them raised, the judgment deciding it): an assert's host verdict to its run, judged and held; a
         *  database member's (no base, so nothing read) is nothing to the host, but an error reading it is raised,
         *  never dropped; the database's raising step is read for the database half. */
        void hostSides(TestPlan.Verdict verdict, java.util.function.Supplier<List<PlanRunner.Rows>> rows,
                java.util.function.@Nullable Supplier<PlanRunner.Rows> message, @Nullable AssertListener listener,
                TestPlan.JudgingRules rules) {
            Run r = byHost.get(verdict.id());
            if (r != null) {
                r.hostSides(verdict, rows, message, listener, rules);
                return;
            }
            Run raised = raising;
            if (raised != null && verdict.id() == raised.pairing.raisingVerdict()) {
                // what the database's raising step raised is its statement's, bound
                return;
            }
            rows.get();
        }

        /** The database half's row for {@code verdict}; null where {@code verdict} is not the segment's last. At the
         *  last, both judges have read: each root's agreement decided in root order, the listener told each verdict
         *  of record, the first failure raising (the walk's tryAdjudicate over its agreement arm); the last root's
         *  verdict returned. */
        @Nullable TypedSide received(TestPlan.Verdict verdict, @Nullable List<Object> row,
                @Nullable AssertListener listener, com.legend.testexec.@Nullable SideReader reader) {
            Run raised = raising;
            if (raised != null) {
                if (verdict.id() != raised.pairing.raisingVerdict()) {
                    return null;   // the host's own step: its rows are the host's, read at hostSides
                }
                raised.raisingRow = row;
                return raised.decided(verdict, listener, reader);
            }
            RootLoops.Run db = database;
            if (db != null) {
                // a member's row to the database half's loop (an assert's host verdict on its first member is one); a
                // verdict of no member answers none (the carrier where every root was decided at planning)
                db.received(verdict, row);
            }
            if (verdict.id() != closing) {
                return null;
            }
            TypedSide last = null;
            for (Run r : runs) {
                last = r.decided(verdict, listener, reader);
            }
            return last;
        }

        /** The runner's word on one of the segment's fused statements, to the database half's loop. */
        void fused(com.legend.error.@Nullable DataError rejected) {
            java.util.Objects.requireNonNull(database, "a fused statement for an assert the database declined")
                    .fused(rejected);
        }
    }

    /** One assert's run: its pairing, its segment's database loop (shared; null: the database judge declines it), the
     *  host half's held verdict, an assertError's raising row. */
    static final class Run {
        private final Agreement pairing;
        private final RootLoops.@Nullable Run database;
        private com.legend.testexec.AssertRuns.@Nullable HostHeld held;
        /** The host's facts the held verdict was judged by: the plan's, from the verdict the runner handed. */
        private TestPlan.@Nullable Host heldHost;
        private @Nullable List<Object> raisingRow;
        private final java.util.function.IntFunction<com.legend.testexec.RootLoops.Appeal> rowAppeals;
        private final BodyRun bodyRun;

        Run(Agreement pairing, RootLoops.@Nullable Run database,
                java.util.function.IntFunction<com.legend.testexec.RootLoops.Appeal> rowAppeals, BodyRun bodyRun) {
            this.pairing = pairing;
            this.database = database;
            this.rowAppeals = rowAppeals;
            this.bodyRun = bodyRun;
        }

        /** The host half's sides for its host verdict, judged and HELD ({@link AssertVerdicts#heldAtItsStep}): the
         *  verdict of record is decided once the database half's rows are in ({@link #decided}). */
        private void hostSides(TestPlan.Verdict verdict, java.util.function.Supplier<List<PlanRunner.Rows>> rows,
                java.util.function.@Nullable Supplier<PlanRunner.Rows> message, @Nullable AssertListener listener,
                TestPlan.JudgingRules rules) {
            // the host's facts as the plan carries them on the verdict the runner handed (step 4, 2026-11-08)
            TestPlan.Host h = java.util.Objects.requireNonNull(verdict.host(),
                    "a host verdict the plan carries no host for");
            held = com.legend.testexec.AssertRuns.heldAtItsStep(h, listener,
                    () -> com.legend.testexec.HostSideShapes.shapedBase(h.assertion().name(), h.shapes(), rows.get()), rules,
                    message == null ? null : com.legend.testexec.HostSideShapes.message(h.shapes(), message), bodyRun);
            heldHost = h;
        }

        /** The assert decided at its segment's last row, inside the program's run: a row its database half planned
         *  ahead read through the program's {@code reader} (THE WALL, 2026-11-06: no run of its own); its database
         *  half the verdict of its own root alone. */
        private TypedSide decided(TestPlan.Verdict last, @Nullable AssertListener listener,
                com.legend.testexec.@Nullable SideReader reader) {
            com.legend.testexec.AssertRuns.HostHeld h = held;
            TestPlan.Host host = heldHost;
            if (h == null || host == null) {
                throw new IllegalStateException(last.name() + ": the database half's rows arrived before the host half"
                        + " judged its sides");
            }
            com.legend.testexecutionplan.Assertion a = host.assertion();
            RootLoops.Run db = database;
            List<Object> raised = raisingRow;
            boolean raising = pairing.raising();
            int root = pairing.root();
            java.util.function.IntFunction<com.legend.testexec.RootLoops.Appeal> appeals = rowAppeals;
            BodyRun br = bodyRun;
            return com.legend.testexec.AssertRuns.agreedAtItsStep(a, listener, () -> AgreementArm.agree(a.name(), listener,
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
                            return com.legend.testexec.DatabaseVerdicts.verdictOf(a.name(), java.util.Objects.requireNonNull(raised,
                                    "an assertError's verdict statement answered no row"));
                        }
                        if (db == null) {
                            return null;   // the database judge declines: not this shape -- strict, a failure
                        }
                        // this assert's root alone: a failure both judges agree on is told in the host's words, its
                        // message the host's side on the assert's verdict (AgreementArm)
                        db.judgeRoot(root, null, reader, false, com.legend.testexec.DatabaseVerdicts::verdictOf, appeals,
                                br);
                        return com.legend.testexecutionplan.Verdicts.ok();
                    }));
        }
    }
}
