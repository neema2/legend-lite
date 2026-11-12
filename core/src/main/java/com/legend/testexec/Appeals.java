// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;


import com.legend.base.Nullable;
import com.legend.exec.PlanRunner;
import com.legend.exec.PlanSessions;
import com.legend.exec.Sessions;
import com.legend.executionplan.ExecutionPlan;
import com.legend.testexecutionplan.TestPlan;
import com.legend.testexecutionplan.AssertListener;
import com.legend.testexecutionplan.TypedSide;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * A BODY'S HELD APPEALS (P1.2, docs/GOALS_OF_RECORD_2026_11_07.md, G4 and G5): a compatibility appeal is the edge's, so
 * it plans and runs its own statements AFTER the body's run, never through the body's reader -- the body's run sends
 * its plan's statements and nothing else.
 *
 * <p>While the body runs, an engine-text comparison that fails under the compatibility rules is HELD here and the
 * assert holds provisionally, as it holds upstream, whose text is the golden; from the first held appeal on, the
 * listener's reports are kept back. When the run ends, each held appeal is decided in body order: its rows -- planned
 * with its assert -- each read as a plan of its own on the body's sessions, and the referee replaying the golden beside
 * them. The first appeal that fails is the body's failure (first-failure sequencing: its assert's report is told as a
 * failure and the reports after it are dropped); where every appeal holds, the kept reports are told and the run's own
 * outcome stands. A body measured on 2026-11-07 (both DuckDB corpus judges): 1,010 bodies appeal, 48 of them after a
 * write of their own and none before a later write, so the rows read after the run are the rows at the assert.
 */
final class Appeals {

    /** An appeal heard at its assert, as the run records it (the plan-as-data design's step 8b, 2026-11-08: data, the
     *  run's output): the appeal's number in the plan, the two texts as read and their equality; {@code at} the kept
     *  reports before it; {@code messaged} whether its assert carries a caller's message. */
    private record Held(String name, int number, String golden, String ours, boolean textEqual, int at,
            boolean messaged) {
    }

    /** A report kept back while an appeal is held: told in order once the appeals before it hold. */
    private record Report(Kind kind, String name, boolean pass, @Nullable String detail) {
        enum Kind { VERDICT, DECLINED, UNJUDGED, REFEREED }

        void tell(AssertListener to) {
            switch (kind) {
                case VERDICT -> to.verdict(name, pass, detail);
                case DECLINED -> to.declined(name, String.valueOf(detail));
                case UNJUDGED -> to.unjudged(name, String.valueOf(detail));
                case REFEREED -> to.refereed(name, String.valueOf(detail));
            }
        }
    }

    private final @Nullable AssertListener listener;
    private final @Nullable AssertListener keeping;
    private final List<Held> held = new ArrayList<>();
    private final List<Report> kept = new ArrayList<>();
    /** Deciding: the appeals' own reports go straight to the caller's listener. */
    private boolean deciding;
    /** A write of the body's ran after the first appeal was held: the rows after the run are not the rows at it. */
    private boolean wroteAfterHold;
    /** A write of the body's ran at all: a failed run's rollback undoes it before the appeals are decided. */
    private boolean wrote;

    Appeals(@Nullable AssertListener listener) {
        this.listener = listener;
        this.keeping = listener == null ? null : new Keeping(listener);
    }

    /** The listener the body's judges tell: the caller's, its reports kept back from the first held appeal on. */
    @Nullable AssertListener listener() {
        return keeping;
    }

    /** The appeal numbered {@code number} heard over the failed text of {@code name}: recorded for after the run; the
     *  assert holds until then; {@code messaged} whether its assert carries a caller's message (the run record's). */
    TypedSide hold(String name, int number, String golden, String ours, boolean textEqual, boolean messaged) {
        held.add(new Held(name, number, golden, ours, textEqual, kept.size(), messaged));
        return com.legend.testexecutionplan.Verdicts.ok();
    }

    /** A second hearing of appeal {@code number}, held earlier in this run by the assert's other half (the agreement
     *  judge's database half, A1): joined to it -- decided once, told once. The appeal must be held, on the same
     *  texts; anything else is refused by name. */
    TypedSide join(String name, int number, String golden, String ours) {
        for (Held h : held) {
            if (h.number() == number) {
                if (!h.golden().equals(golden) || !h.ours().equals(ours)) {
                    throw new IllegalStateException(name + " (sql-text): the agreement judge's two halves heard one"
                            + " appeal on different texts");
                }
                return com.legend.testexecutionplan.Verdicts.ok();
            }
        }
        throw new IllegalStateException(name + " (sql-text): a second hearing of appeal " + number + ", which no half"
                + " holds");
    }

    /** A step of the body that writes is about to run ({@code PlanRunner.ProgramSink#writing}), told to the body's open
     *  appeals (each body's run holds its own record of them, slice 5). */
    void writing() {
        wrote = true;
        if (!held.isEmpty()) {
            wroteAfterHold = true;
        }
    }

    /** The held appeals of a run that FAILED with {@code runFailed}, decided as {@link #decide} decides them: the error
     *  the body raises -- the first held appeal's failure, which precedes the run's (raised here, the run's failure
     *  suppressed in it), else the run's own, returned for the caller to raise. Where the failed run wrote, its rollback has undone what the appeals would read: they
     *  are refused by name (the milestone audit's 2, 2026-11-07). */
    RuntimeException afterFailure(RuntimeException runFailed, Sessions.Session session,
            com.legend.exec.SystemDatabase.Leases systemSessions,
            Function<ExecutionPlan.Target, Sessions.Session> sessions, Decider decider) {
        if (held.isEmpty()) {
            deciding = true;
            return runFailed;
        }
        if (wrote) {
            deciding = true;
            RuntimeException refused = new IllegalStateException(held.get(0).name() + " (sql-text): an appeal held in"
                    + " a body whose run failed after writing: the rollback undid what its rows read, so it is not"
                    + " decided -- the run's failure: " + runFailed.getMessage(), runFailed);
            failed(held.get(0), told(held.get(0)), refused);
            return refused;
        }
        try {
            decide(session, systemSessions, sessions, decider);
        } catch (com.legend.error.AssertFailed | com.legend.error.DataError appealFailed) {
            // the first held appeal's failure precedes the run's: raised here, the run's suppressed in it
            appealFailed.addSuppressed(runFailed);
            throw appealFailed;
        }
        return runFailed;
    }

    /** The kept reports before {@code h}'s place, told. */
    private int told(Held h) {
        for (int i = 0; i < h.at(); i++) {
            tell(kept.get(i));
        }
        return h.at();
    }

    /** The held appeals decided after the body's run (completed, or failed after them), each reading its rows as plans
     *  of their own on the body's {@code session} and the graph's {@code systemSessions}, the referee the body's
     *  {@code sessions} by target, inside a judging scope that plans nothing ({@code RunScope.judging}): the first that
     *  fails raises its failure; else the kept reports are told and the run's own outcome stands. An appeal held
     *  before a later write of the body's is refused by name: the rows after the run are not the rows at its assert
     *  (the milestone audit's 2, 2026-11-07; the corpus measured none). */
    void decide(Sessions.Session session,
            com.legend.exec.SystemDatabase.Leases systemSessions,
            Function<ExecutionPlan.Target, Sessions.Session> sessions, Decider decider) {
        deciding = true;
        if (held.isEmpty()) {
            return;
        }
        if (wroteAfterHold) {
            RuntimeException refused = new IllegalStateException(held.get(0).name() + " (sql-text): an appeal held"
                    + " before a later write of the body's: the rows after the run are not the rows at its assert, so"
                    + " it is not decided");
            failed(held.get(0), told(held.get(0)), refused);
            throw refused;
        }
        // our rows read for the referee: counted under REFEREE_OURS (the reader's own mark, on its sessions)
        PlanSessions.Source source = PlanSessions.marked(PlanSessions.of(session, systemSessions),
                com.legend.exec.StatementOrigin.REFEREE_OURS);
        // the edge's reader: each planned side a plan of its own, run on the body's sessions after its run
        com.legend.testexec.SideReader own = side -> switch (side) {
            case TestPlan.Side.Rows r -> PlanRunner.rows(ExecutionPlan.reading(List.of(), r.result()), Map.of(),
                    source);
            case TestPlan.Side.KeyedRows k -> PlanRunner.rows(ExecutionPlan.reading(List.of(), k.keyed()), Map.of(),
                    source);
            case TestPlan.Side.Constant c -> throw new IllegalStateException("an appeal's side is a constant: its"
                    + " answer is the planned side's own, never read");
        };
        int told = 0;
        for (Held h : held) {
            for (; told < h.at(); told++) {
                tell(kept.get(told));
            }
            try (var noPlanning = com.legend.base.RunScope.judging()) {
                decider.decide(h.number(), h.golden(), h.ours(), h.textEqual(), keeping, own, sessions);
            } catch (com.legend.error.AssertFailed | com.legend.error.DataError e) {
                failed(h, told, e);
                throw e;
            }
        }
        for (; told < kept.size(); told++) {
            tell(kept.get(told));
        }
    }

    /** {@code h}'s appeal failed with {@code e}: the reports kept before its assert's verdict told, its assert's verdict
     *  told as the failure, the reports after it dropped; counted as raised, as a failure at its step is. */
    private void failed(Held h, int told, RuntimeException e) {
        int verdictAt = -1;
        for (int i = h.at(); i < kept.size(); i++) {
            if (kept.get(i).kind() == Report.Kind.VERDICT) {
                verdictAt = i;
                break;
            }
        }
        if (h.messaged()) {
            // the caller's message is a side of the body's run, unread after it: refused by name, never told without
            // its message (measured 2026-11-07: no DuckDB corpus body, under either judge, holds one)
            throw new IllegalStateException(h.name() + " (sql-text): the appeal failed on an assert with a caller's"
                    + " message, which is read inside the body's run and not after it: " + e.getMessage(), e);
        }
        // (2026-11-08, the slice 3 invariant audit: no "not-attempted ... raised" count here -- the text row was judged,
        // so the assert was attempted, and the count read the family the last planned assert left)
        if (listener == null) {
            return;
        }
        if (verdictAt < 0) {
            throw new IllegalStateException(h.name() + " (sql-text): an appeal held with no verdict told after it");
        }
        for (int i = told; i < verdictAt; i++) {
            tell(kept.get(i));
        }
        listener.verdict(kept.get(verdictAt).name(), false, e.getMessage());
    }

    private void tell(Report r) {
        r.tell(java.util.Objects.requireNonNull(listener));
    }

    /** How a held appeal is decided after the run: from the plan's data by its number, the texts the run recorded and
     *  the run's listener (the edge's decision; step 8b, 2026-11-08). */
    @FunctionalInterface
    interface Decider {
        TypedSide decide(int number, String golden, String ours, boolean textEqual,
                @Nullable AssertListener listener, SideReader reader,
                Function<ExecutionPlan.Target, Sessions.Session> sessions);
    }

    /** The caller's listener, its reports kept back while an appeal is held and the run goes on. */
    private final class Keeping implements AssertListener {
        private final AssertListener to;

        Keeping(AssertListener to) {
            this.to = to;
        }

        private boolean keep() {
            return !deciding && !held.isEmpty();
        }

        @Override
        public void verdict(String assertName, boolean pass, @Nullable String detail) {
            if (keep()) {
                kept.add(new Report(Report.Kind.VERDICT, assertName, pass, detail));
            } else {
                to.verdict(assertName, pass, detail);
            }
        }

        @Override
        public void declined(String assertName, String reason) {
            if (keep()) {
                kept.add(new Report(Report.Kind.DECLINED, assertName, false, reason));
            } else {
                to.declined(assertName, reason);
            }
        }

        @Override
        public void unjudged(String assertName, String reason) {
            if (keep()) {
                kept.add(new Report(Report.Kind.UNJUDGED, assertName, false, reason));
            } else {
                to.unjudged(assertName, reason);
            }
        }

        @Override
        public void refereed(String assertName, String outcome) {
            if (keep()) {
                kept.add(new Report(Report.Kind.REFEREED, assertName, false, outcome));
            } else {
                to.refereed(assertName, outcome);
            }
        }
    }
}
