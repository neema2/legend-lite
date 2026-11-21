// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;


import com.legend.testexecutionplan.Failures;

import com.legend.testexecutionplan.AssertListener;


import com.legend.diagnostics.Census;
import com.legend.exec.PlanRunner;
import com.legend.testexecutionplan.Failure;
import com.legend.testexecutionplan.RootLoop;
import com.legend.testexecutionplan.TestPlan;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * THE DATABASE JUDGE'S ROOT LOOP, RUN FROM THE PLAN (docs/PLAN_AS_DATA_2026_11_07.md, the design's step 5,
 * 2026-11-08): a segment's {@link RootLoop} judged over the rows the runner read for its verdicts -- every root in body
 * order, each row by the verdict's judgment ({@link Judge}, handed by the caller: a plain function), a
 * decided outcome raised as planned; each root's verdict to the listener; the first failure raising. The census and
 * listener protocol are the batch's of before this step ({@code VerdictBatch.Planned}), which held the same facts in
 * maps keyed by object identity and judged them itself; nothing here is planned.
 */
public final class RootLoops {

    /** The verdict arm's reading of one row ({@code verdict, expected,
     * actual, unjudged, lenient, holds, message}): returns where the row
     * holds, raises the row's own message otherwise (the test-is-the-product
     * plan's M8b: the polarity and the message are the statement's). A plain
     * function the run hands {@link RootLoops} (the plan-as-data design's step
     * 5, 2026-11-08: the batch no longer holds it; here since slice 5, 2026-11-09). */
    @FunctionalInterface
    public interface Judge {
        void judge(String name, List<Object> row);
    }

    // (RowAppeal, a verdict row's APPEAL -- block-compiler rung 2a, consulted when the row failed: deleted with the
    // database judge's appeal path, A4 of the compatibility program, stage A ratified by the user 2026-11-19: the host
    // judge is the only judge in compatibility, so no database row is appealed)


    private RootLoops() {
    }

    /** One segment's run: the rows its verdicts answered, by verdict id, and whether its flush is counted. */
    public static final class Run {

        private final RootLoop loop;
        private final Set<Integer> members = new HashSet<>();
        private final Map<Integer, List<Object>> rows = new HashMap<>();
        /** Whether no root reads a verdict's row (every root decided at planning, or each member a canon the dialect
         *  cannot spell): the segment's one verdict is then its carrier, a member of no row, with no statement. */
        private final boolean decided;
        /** The flush counted once, as the segment's groups start to run (its first row or fused word, or its judging). */
        private boolean counted;

        public Run(RootLoop loop) {
            this.loop = loop;
            for (RootLoop.Root r : loop.roots()) {
                for (RootLoop.Step s : r.steps()) {
                    if (s instanceof RootLoop.Row row) {
                        members.add(row.verdict());
                    }
                }
            }
            this.decided = members.isEmpty();
        }

        public RootLoop loop() {
            return loop;
        }

        /** The verdicts whose rows the loop reads, across the segment's groups. */
        public Set<Integer> members() {
            return java.util.Collections.unmodifiableSet(members);
        }

        private void counted() {
            if (!counted) {
                counted = true;
                Census.inc(Census.Key.VERDICT_FLUSHES);
            }
        }

        /** The runner's word on one of the segment's fused statements: sent, or rejected -- counted here; nothing
         *  retried another way (P1.2, G7, 2026-11-07). A rejection is not held: the caller hands it to
         *  {@link #judgeRejected} at once (the user, 2026-11-20 ~13:10, "Yes remove mutable next"). */
        public void fused(com.legend.error.@com.legend.base.Nullable DataError rejected) {
            counted();
            if (rejected == null) {
                Census.inc(Census.Key.VERDICT_FUSED);
            } else {
                Census.verdictFallback(rejected, loop.fragments());
            }
        }

        /** The segment judged at once on the database's rejection of one of its fused statements: its later groups are
         *  never sent (a fused group starts at each verdict that may raise, VerdictBatch: the compatibility owner's
         *  (b1), 2026-11-19), so the roots with rows are judged in body order and the first that reads none raises
         *  {@code rejected}, as the host's side batch raises it at its first verdict's (2026-11-19). Never returns:
         *  where every root held, {@code rejected} is raised whole, the body failing with the database's error (PR
         *  #127's review, 2026-11-19, the owner's ruling) -- not reached by any plan, since a rejected group's verdicts
         *  are members and a member read with no row raises it first. The rejection is this call's argument, never
         *  the run's state (until 2026-11-20 a field set by {@link #fused} and read here). */
        public void judgeRejected(com.legend.error.DataError rejected, @com.legend.base.Nullable AssertListener l,
                @com.legend.base.Nullable SideReader reader, boolean messages, Judge judge) {
            counted();
            for (RootLoop.Root r : loop.roots()) {
                root(r, l, reader, messages, judge, rejected);
            }
            throw rejected;
        }

        /** The row the runner read for {@code verdict} (null where none: a verdict with no statement); the root loop
         *  reaches an absent row at its place. A verdict of no row of the loop answers none: the segment's carrier where
         *  every root was decided at planning. */
        public void received(TestPlan.Verdict verdict, @com.legend.base.Nullable List<Object> row) {
            counted();
            if (!members.contains(verdict.id())) {
                // only the decided segment's carrier answers no row of the loop (the slice 2 audit, 2026-11-08: any
                // other non-member is a plan that names the wrong segment, refused as before step 5)
                if (row != null || !decided || verdict.wrapper() != null) {
                    throw new IllegalStateException("a verdict row for no member of this segment: " + verdict.name());
                }
                return;
            }
            if (row != null) {
                rows.put(verdict.id(), row);
            }
        }

        /** Every root judged in body order (above); {@code messages} false where the caller's message is another
         *  judge's to read -- the agreement judge's: a failure both judges agree on is told in the HOST's words
         *  (AgreementArm), so this half judges the verdict alone and reads no message. A failing root's message, a
         *  side the plan carries on the root's first verdict, is read through the program's {@code reader}, never a
         *  run of its own. */
        public void judge(@com.legend.base.Nullable AssertListener l, @com.legend.base.Nullable SideReader reader,
                boolean messages, Judge judge) {
            counted();
            for (RootLoop.Root r : loop.roots()) {
                root(r, l, reader, messages, judge, null);
            }
        }

        /** The loop's root {@code root} alone judged, as {@link #judge} judges each root at its turn: the agreement
         *  judge's database half, one assert of the segment at a time (E-5c, step 5b, 2026-11-13: a segment's group
         *  carries several asserts, each agreed in root order). Returns where it holds; raises its failure. */
        public void judgeRoot(int root, @com.legend.base.Nullable AssertListener l,
                @com.legend.base.Nullable SideReader reader, boolean messages, Judge judge) {
            if (root < 0 || root >= loop.roots().size()) {
                throw new IllegalStateException("root " + root + " of a segment's loop of " + loop.roots().size()
                        + " roots");
            }
            counted();
            root(loop.roots().get(root), l, reader, messages, judge, null);
        }

        /** One root judged at its turn: its verdict to the listener, its failure raised; {@code rejected} the database's
         *  rejection of one of the segment's fused statements where the segment is judged on it, else null. */
        private void root(RootLoop.Root r, @com.legend.base.Nullable AssertListener l,
                @com.legend.base.Nullable SideReader reader, boolean messages, Judge judge,
                com.legend.error.@com.legend.base.Nullable DataError rejected) {
            RuntimeException failure = null;
            boolean reported = true;
            // whether a row of this root was judged (or its refusal counted): the verdict census's own fact for
            // this root, at its turn (the slice 3 invariant audit, 2026-11-08: the census read state planning wrote)
            boolean spoke = false;
            if (r.steps().stream().noneMatch(s -> s instanceof RootLoop.Row)) {
                Census.inc(Census.Key.VERDICT_HOST_DECIDED);
            }
            for (RootLoop.Step s : r.steps()) {
                try {
                    switch (s) {
                        case RootLoop.Row p -> {
                            // (the decline the engine-text arm decided before its texts were compared, reported here at
                            // the row's turn since hole 5 of 2026-11-08: deleted with the row's field, A4 of the
                            // compatibility program, stage A ratified by the user 2026-11-19)
                            spoke = true;
                            row(p, judge, rejected);
                        }
                        case RootLoop.Judged j -> {
                            // a verdict decided at planning, counted at its turn as a row is at its judgment
                            // (DatabaseVerdicts): never at planning (J39, 2026-11-18)
                            com.legend.diagnostics.VerdictCensus.sqlJudgedInDatabase(r.listenerName());
                            spoke = true;
                        }
                        case RootLoop.Decided d -> {
                            reported = d.reported();
                            RuntimeException raised = Failures.raised(d.failure());
                            // the verdict census at the root's turn, from the plan's data, before the decided
                            // outcome raises (a wall raises past this loop's catch): an UNJUDGED failure as such,
                            // a WALL no row of the root judged as a raise on the root's family -- a failure
                            // decided at planning was judged there, never "not attempted". Counted by the judge of
                            // record alone: under the agreement judge this is its database half, whose outcome the
                            // agreement counts (the slice 3 follow-up audit, 2026-11-08: counted twice)
                            if (messages && raised instanceof com.legend.error.AssertFailed af
                                    && af.unjudgedReason() != null) {
                                com.legend.diagnostics.VerdictCensus.sqlUnjudged(family(r.listenerName()), af.unjudgedReason());
                            } else if (messages && !spoke && !d.reported()) {
                                com.legend.diagnostics.VerdictCensus.sqlRaisedAt(family(r.listenerName()));
                            }
                            throw raised;
                        }
                    }
                } catch (com.legend.error.AssertFailed | com.legend.error.DataError e) {
                    failure = e;
                    break;
                }
            }
            if (failure == null) {
                if (l != null) {
                    l.verdict(r.listenerName(), true, null);
                }
                return;
            }
            RootLoop.Message message = r.message();
            if (r.unplanned() != null && failure instanceof com.legend.error.AssertFailed af
                    && af.unjudgedReason() == null) {
                // computing the caller's message raises: that error is the outcome, as upstream's (item 3, the user's
                // lazy rule; the host judge's HostJudgment.Messaged#unplanned)
                failure = Failures.raised(r.unplanned());
            } else if (messages && message != null && failure instanceof com.legend.error.AssertFailed af
                    && af.unjudgedReason() == null) {
                failure = messaged(r.listenerName(), message, reader);
            }
            if (l != null && reported) {
                if (failure instanceof com.legend.error.AssertFailed af && af.unjudgedReason() != null) {
                    l.unjudged(r.listenerName(), af.unjudgedReason());
                }
                l.verdict(r.listenerName(), false, failure.getMessage());
            }
            throw failure;   // first-failure sequencing, as before
        }

        /** {@code p} judged: its row (a member the dialect cannot spell refuses the body at planning, D).
         *  (A row's ruled comparison decided at planning raised here where the generic row failed until A4 of the
         *  compatibility program, stage A ratified by the user 2026-11-19.) */
        private void row(RootLoop.Row p, Judge judge, com.legend.error.@com.legend.base.Nullable DataError rejected) {
            List<Object> row = rows.get(p.verdict());
            if (row == null && rejected != null) {
                // the segment's fused statement was rejected: its database error is this root's failure, raised at its
                // turn as the host's side batch raises it at its first verdict's
                throw rejected;
            }
            if (row == null) {
                // an absent row is the runner's defect (a member the dialect cannot spell refuses the body at planning)
                throw new IllegalStateException(p.name() + ": the runner answered no row for a verdict of the plan");
            }
            judge.judge(p.name(), row);
        }
    }

    /** A root's assert family, as the verdict census names it: the bare name of the function it calls. */
    private static String family(String listenerName) {
        int at = listenerName.lastIndexOf("::");
        return at < 0 ? listenerName : listenerName.substring(at + 2);
    }

    /** The failure of a root that did not hold, with its caller's message: the message's row read now, at the root's
     *  place, its value the failure's text -- or the error reading it raised. */
    private static RuntimeException messaged(String listenerName, RootLoop.Message message,
            @com.legend.base.Nullable SideReader reader) {
        com.legend.diagnostics.Diagnostics.judgeSend("message", listenerName);
        TestPlan.Side side = message.side();
        if (!message.carried()) {
            // a root with no verdict in the plan (every member refused, or decided at planning): its message rides no
            // statement of the plan, so it is not read -- refused by name (the milestone audit's 3)
            return new com.legend.error.NotImplementedException(listenerName + ": the assert's caller's message reads"
                    + " the database, and the assert has no statement in the plan to carry it (decided or refused at"
                    + " planning)");
        }
        if (reader == null) {
            return new IllegalStateException(listenerName + ": a caller's message is read inside the body's run, by"
                    + " the run's reader");
        }
        try {
            PlanRunner.Rows rows = reader.read(side);
            if (rows.rows().size() != 1) {
                return new IllegalStateException(listenerName + ": the message's statement answered "
                        + rows.rows().size() + " rows");
            }
            return new com.legend.error.AssertFailed(String.valueOf(rows.rows().get(0).get(6)));
        } catch (com.legend.error.AssertFailed | com.legend.error.DataError e) {
            return e;
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    /**
     * The database judge's runs of one plan, keyed by the verdicts of their segments: built from the plan's verdict
     * groups (each segment's last group carries its loop), so the run finds a verdict's segment in the plan, never in
     * the planner's maps. The segment is judged after its closing verdict -- its last group's last.
     */
    public static final class Runs {

        private final Map<Integer, Run> byVerdict = new HashMap<>();
        private final Set<Integer> closing = new HashSet<>();

        public Runs(List<? extends TestPlan.TestStep> steps) {
            add(steps);
        }

        private void add(List<? extends TestPlan.TestStep> steps) {
            for (TestPlan.TestStep step : steps) {
                switch (step) {
                    case TestPlan.Verdicts group -> {
                        RootLoop loop = group.roots();
                        if (loop == null) {
                            continue;
                        }
                        Run run = new Run(loop);
                        for (int id : run.members) {
                            byVerdict.put(id, run);
                        }
                        for (TestPlan.Verdict v : group.verdicts()) {
                            byVerdict.put(v.id(), run);
                        }
                        closing.add(group.verdicts().get(group.verdicts().size() - 1).id());
                    }
                    case TestPlan.Branch b -> {
                        add(b.then());
                        add(b.otherwise());
                    }
                    case TestPlan.Run r -> {
                    }
                    case TestPlan.FrameRun f -> {
                    }
                    case TestPlan.Isolated p -> throw new IllegalStateException("an isolated pair is run at the top, as two plans");
                }
            }
        }

        /** The segment {@code verdict} belongs to. */
        public Run of(TestPlan.Verdict verdict) {
            Run run = byVerdict.get(verdict.id());
            if (run == null) {
                throw new IllegalStateException("a verdict of no database segment of the plan: " + verdict.name());
            }
            return run;
        }

        /** Whether the segment is judged after {@code verdict}: its last group's last verdict. */
        public boolean closes(TestPlan.Verdict verdict) {
            return closing.contains(verdict.id());
        }
    }
}
