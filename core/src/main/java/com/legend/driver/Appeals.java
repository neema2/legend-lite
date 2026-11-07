// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.driver;

import com.legend.base.Nullable;
import com.legend.exec.PlanRunner;
import com.legend.exec.PlanSessions;
import com.legend.exec.Sessions;
import com.legend.executionplan.ExecutionPlan;
import com.legend.judge.AssertListener;
import com.legend.judge.TypedSide;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

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

    private static final ThreadLocal<@Nullable Appeals> OPEN = new ThreadLocal<>();

    /** An appeal held at its assert: decided after the run; {@code at} the kept reports before it; {@code messaged}
     *  whether its assert carries a caller's message. */
    private record Held(String name, Supplier<TypedSide> decide, int at, boolean messaged) {
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

    Appeals(@Nullable AssertListener listener) {
        this.listener = listener;
        this.keeping = listener == null ? null : new Keeping(listener);
    }

    /** The listener the body's judges tell: the caller's, its reports kept back from the first held appeal on. */
    @Nullable AssertListener listener() {
        return keeping;
    }

    /** This body's appeals open for its run: an appeal heard inside it is held. */
    Scope open() {
        if (OPEN.get() != null) {
            throw new IllegalStateException("a body's appeals opened inside another body's run");
        }
        OPEN.set(this);
        return OPEN::remove;
    }

    /** The appeal {@code decide} of the failed text of {@code name} held for after the run; the assert holds until
     *  then. Refused outside a body's run: an appeal is heard at its assert's verdict step alone. */
    static TypedSide hold(String name, Supplier<TypedSide> decide) {
        Appeals open = OPEN.get();
        if (open == null) {
            throw new IllegalStateException(name + " (sql-text): an appeal heard outside a body's run");
        }
        open.held.add(new Held(name, decide, open.kept.size(), com.legend.judge.CallerMessage.present()));
        return AssertVerdicts.ok();
    }

    /** The held appeals decided after the body's run (completed, or failed after them), each reading its rows as plans
     *  of their own on the body's {@code session} and the graph's {@code systemSessions}, the referee the body's
     *  {@code sessions} by target: the first that fails raises its failure; else the kept reports are told and the
     *  run's own outcome stands. */
    void decide(Sessions.Session session,
            Function<ExecutionPlan.Database.System, Sessions.Session> systemSessions,
            Function<ExecutionPlan.Target, Sessions.Session> sessions) {
        deciding = true;
        if (held.isEmpty()) {
            return;
        }
        PlanSessions.Source source = PlanSessions.of(session, null, systemSessions);
        // the edge's reader: each planned side a plan of its own, run on the body's sessions after its run
        PlanRunner.SideReader own = side -> switch (side) {
            case ExecutionPlan.Side.Rows r -> PlanRunner.rows(new ExecutionPlan(List.of(), r.result()), Map.of(),
                    source);
            case ExecutionPlan.Side.KeyedRows k -> PlanRunner.rows(new ExecutionPlan(List.of(), k.keyed()), Map.of(),
                    source);
            case ExecutionPlan.Side.Constant c -> throw new IllegalStateException("an appeal's side is a constant: its"
                    + " answer is the planned side's own, never read");
        };
        int told = 0;
        for (Held h : held) {
            for (; told < h.at(); told++) {
                tell(kept.get(told));
            }
            try (var judging = RunReader.judging(own, sessions)) {
                h.decide().get();
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
        com.legend.judge.CanonicalDivergence.sqlRaised();
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

    /** A judgment's scope, closed when it ends. */
    interface Scope extends AutoCloseable {
        @Override
        void close();
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

        @Override
        public java.util.function.@Nullable BooleanSupplier fixtureAhead(String storeFqn) {
            return to.fixtureAhead(storeFqn);
        }

        @Override
        public void beforeRun() {
            to.beforeRun();
        }
    }
}
