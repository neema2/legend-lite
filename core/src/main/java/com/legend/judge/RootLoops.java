// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.judge;

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
 * order, each row by the verdict's judgment ({@link VerdictBatch.Judge}, handed by the caller: a plain function), a
 * decided outcome raised as planned; each root's verdict to the listener; the first failure raising. The census and
 * listener protocol are the batch's of before this step ({@code VerdictBatch.Planned}), which held the same facts in
 * maps keyed by object identity and judged them itself; nothing here is planned.
 */
public final class RootLoops {

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
                    if (s instanceof RootLoop.Row row && row.verdict() != RootLoop.NO_VERDICT) {
                        members.add(row.verdict());
                    }
                }
            }
            this.decided = members.isEmpty();
        }

        public RootLoop loop() {
            return loop;
        }

        private void counted() {
            if (!counted) {
                counted = true;
                Census.inc(Census.Key.VERDICT_FLUSHES);
            }
        }

        /** The runner's word on one of the segment's fused statements: sent, or rejected -- counted here; the runner
         *  then fails the body with the database's error, nothing retried another way (P1.2, G7, 2026-11-07). */
        public void fused(com.legend.error.@com.legend.base.Nullable DataError rejected) {
            counted();
            if (rejected == null) {
                Census.inc(Census.Key.VERDICT_FUSED);
            } else {
                fallback(rejected, loop.fragments());
            }
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
         *  run of its own; a failed row's appeal is {@code appeals}' by its number. */
        public void judge(@com.legend.base.Nullable AssertListener l, @com.legend.base.Nullable SideReader reader,
                boolean messages, VerdictBatch.Judge judge,
                java.util.function.IntFunction<VerdictBatch.Appeal> appeals) {
            counted();
            for (RootLoop.Root r : loop.roots()) {
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
                                // the decline the arm decided before its texts were compared, the row's data: reported
                                // at the row's turn, before it is judged (hole 5, 2026-11-08)
                                if (l != null && p.decline() != null) {
                                    l.declined(p.name(), p.decline());
                                }
                                spoke = true;
                                row(p, r, judge, appeals);
                            }
                            case RootLoop.Decided d -> {
                                reported = d.reported();
                                throw Failures.raised(d.failure());
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
                    continue;
                }
                RootLoop.Message message = r.message();
                if (messages && message != null && failure instanceof com.legend.error.AssertFailed af
                        && af.unjudgedReason() == null) {
                    failure = messaged(r.listenerName(), message, reader);
                }
                // the verdict census at the root's turn, from the plan's data: an UNJUDGED failure counted as such (its
                // planning counted nothing), a raise no row of the root judged counted on the root's own family
                if (failure instanceof com.legend.error.AssertFailed af && af.unjudgedReason() != null) {
                    CanonicalDivergence.sqlUnjudged(family(r.listenerName()), af.unjudgedReason());
                } else if (!spoke) {
                    CanonicalDivergence.sqlRaisedAt(family(r.listenerName()));
                }
                if (l != null && reported) {
                    if (failure instanceof com.legend.error.AssertFailed af && af.unjudgedReason() != null) {
                        l.unjudged(r.listenerName(), af.unjudgedReason());
                    }
                    l.verdict(r.listenerName(), false, failure.getMessage());
                }
                throw failure;   // first-failure sequencing, as before
            }
        }

        /** {@code p} judged: its row (a member the dialect refused at planning raises its refusal, nothing sent; a
         *  generic row whose ruled comparison was decided at planning raises that decision where it fails or was never
         *  written), a failed row heard by its appeal where it has one. */
        private void row(RootLoop.Row p, RootLoop.Root r, VerdictBatch.Judge judge,
                java.util.function.IntFunction<VerdictBatch.Appeal> appeals) {
            List<Object> row = p.verdict() == RootLoop.NO_VERDICT ? null : rows.get(p.verdict());
            Failure orElse = p.orElse();
            if (row == null && orElse != null && p.verdict() == RootLoop.NO_VERDICT) {
                // a generic row the dialect cannot write, whose ruled row was decided at planning: that decision
                // raises, nothing sent
                throw orElseDecides(p.name(), orElse);
            }
            if (row == null) {
                // a member the dialect cannot spell, refused at planning: raised at its place, nothing sent; any other
                // absent row is the runner's defect (a rejected fused statement fails the body before the root loop)
                Failure refusal = p.refusal();
                if (refusal == null) {
                    throw new IllegalStateException(p.name() + ": the runner answered no row for a verdict of the plan");
                }
                throw refused(p.name(), refusal);
            }
            try {
                judge.judge(p.name(), row);
            } catch (com.legend.error.AssertFailed e) {
                if (orElse != null) {
                    // the generic row failed, its ruled comparison decided at planning (the dialect's refusal of it, or a
                    // failure its planning raised): that decision raises
                    throw orElseDecides(p.name(), orElse);
                }
                if (p.appeal() == RootLoop.NO_APPEAL) {
                    throw e;
                }
                // rung 2a: the row failed -- the appeal is heard: held for after the body's run (the driver's Appeals,
                // P1.2, 2026-11-07), the root holding until it is decided; a messaged root marked, its message unread
                // after the run
                com.legend.diagnostics.Diagnostics.judgeSend("appeal", p.name());
                VerdictBatch.Appeal appeal = appeals.apply(p.appeal());
                if (r.message() != null) {
                    try (var messaged = CallerMessage.judging()) {
                        appeal.appeal(row);
                    }
                } else {
                    appeal.appeal(row);
                }
            }
        }
    }

    /** A root's assert family, as the verdict census names it: the bare name of the function it calls. */
    private static String family(String listenerName) {
        int at = listenerName.lastIndexOf("::");
        return at < 0 ? listenerName : listenerName.substring(at + 2);
    }

    /** A refusal decided at planning raised at its place: counted UNJUDGED, raised as itself. */
    private static RuntimeException refused(String name, Failure refused) {
        CanonicalDivergence.sqlUnjudged(name, "dialect-capability: " + String.valueOf(refused.message()).split("\n")[0]);
        return Failures.raised(refused);
    }

    /** A row's ruled comparison as decided at planning, raised where the generic row fails: the dialect's refusal of
     *  it (counted UNJUDGED, raised as the dialect's own refusal on both roads), or a failure its planning raised. */
    private static RuntimeException orElseDecides(String name, Failure decided) {
        if (decided.kind() == Failure.Kind.DIALECT_CAPABILITY) {
            CanonicalDivergence.sqlUnjudged(name, "dialect-capability: "
                    + String.valueOf(decided.message()).split("\n")[0]);
        }
        return Failures.raised(decided);
    }

    /** The failure of a root that did not hold, with its caller's message: the message's row read now, at the root's
     *  place, its value the failure's text -- or the error reading it raised. */
    private static RuntimeException messaged(String listenerName, RootLoop.Message message,
            @com.legend.base.Nullable SideReader reader) {
        com.legend.diagnostics.Diagnostics.judgeSend("message", listenerName);
        Failure refusal = message.refusal();
        if (refusal != null) {
            return refused(listenerName, refusal);
        }
        TestPlan.Side side = java.util.Objects.requireNonNull(message.side());
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

    /** A rejected fused statement's accounting (the database's rejection, which then fails the body; the planning
     *  rung's own at {@link VerdictBatch#plan}): counted, with the reason's head and the frames the message names. */
    static void fallback(RuntimeException e, Map<String, String> fragments) {
        Census.inc(Census.Key.VERDICT_FALLBACKS);
        String m = String.valueOf(e.getMessage());
        Census.FALLBACK_REASONS.add(e.getClass().getSimpleName() + ": "
                + m.substring(0, Math.min(m.length(), 160)).replace('\n', ' ')
                + attribute(m, fragments));
    }

    /** The fragments a database message names (a frame alias appears in DuckDB's binder errors and H2's column
     *  errors), or an empty string. */
    static String attribute(String message, Map<String, String> fragments) {
        StringBuilder sb = new StringBuilder();
        for (var e : fragments.entrySet()) {
            if (e.getKey().startsWith("frame_") && message.contains(e.getKey())) {
                sb.append(sb.length() == 0 ? " — near " : ", ").append(e.getValue());
            }
        }
        return sb.toString();
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
