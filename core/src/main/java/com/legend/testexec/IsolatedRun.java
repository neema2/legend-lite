// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

import com.legend.base.Nullable;
import com.legend.exec.SessionSource;
import com.legend.exec.Sessions;
import com.legend.result.Output;
import com.legend.testexecutionplan.AssertListener;
import com.legend.testexecutionplan.TestPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * BOTH JUDGES, ISOLATED (the user's judge model, 2026-11-17; the orchestrator's (b') and its ruling on given sessions,
 * the same day): a pair's two plans each RUN ALONE, the host judge's first, each on its own session where the caller's
 * source opens one per plan, both in turn on a caller's GIVEN session where neither writes. Neither run sees the other's
 * rows or verdicts. Each run's verdicts and failure are recorded, then COMPARED: as many asserts, in the same order,
 * each with the same verdict and the same message, and the same failure text (an assert's NAME is each planner's
 * report of it -- a helper's, or the assert inside it -- and is not compared). Where they agree the caller hears the
 * host's verdicts and the run ends as the host's did (the host is the product's default judge until the database's gap
 * list is empty); where they differ the body fails by name, both texts quoted ({@link JudgesDisagree}).
 *
 * <p>A plan that writes is refused on a given session before anything is opened or sent, by name: the second judge's
 * run would read the first's writes.
 */
final class IsolatedRun {

    /** The refusal's text: a pair whose plans write, on a caller's given session. */
    static final String NEEDS_A_SESSION_PER_JUDGE =
            "isolated BOTH needs a session per judge: run it on a Fresh or Declared source";

    private IsolatedRun() {
    }

    /** What a judge reported of one assert: a verdict, or that it declined the assert or left it unjudged. */
    enum Report { VERDICT, DECLINED, UNJUDGED, REFEREED }

    /** One assert's report, as the listener heard it, in order ({@code detail} a verdict's message, or a decline's or
     *  an unjudged assert's reason). */
    record Heard(Report report, String name, boolean pass, @Nullable String detail) {
        Heard(String name, boolean pass, @Nullable String detail) {
            this(Report.VERDICT, name, pass, detail);
        }
    }

    /** One judge's run, recorded: its reports in order (each assert's verdict, or its decline or unjudged reason) and
     *  how it ended. */
    static final class Recording implements AssertListener {
        final List<Heard> verdicts = new ArrayList<>();
        @Nullable RuntimeException failure;

        @Override
        public void verdict(String assertName, boolean pass, @Nullable String detail) {
            verdicts.add(new Heard(assertName, pass, detail));
        }

        @Override
        public void declined(String assertName, String reason) {
            verdicts.add(new Heard(Report.DECLINED, assertName, false, reason));
        }

        @Override
        public void unjudged(String assertName, String reason) {
            verdicts.add(new Heard(Report.UNJUDGED, assertName, false, reason));
        }

        @Override
        public void refereed(String assertName, String outcome) {
            verdicts.add(new Heard(Report.REFEREED, assertName, true, outcome));
        }

        /** The reports two judges' runs are compared by: every one but the referee's (a compatibility appeal's
         *  decision, the host's report to its caller; its verdict is the assert's). */
        List<Heard> compared() {
            return verdicts.stream().filter(h -> h.report() != Report.REFEREED).toList();
        }

        /** The run's end, as text: its failure's class and message, or none. */
        String ending() {
            return failure == null ? "passed" : failure.getClass().getSimpleName() + ": " + failure.getMessage();
        }
    }

    /** One judge's plan run on its own session, {@code answer} its answer's sink (null: answering nobody): the runner's
     *  own run, handed in so this class names nothing of the run's but the plan. */
    @FunctionalInterface
    interface Runner {
        void run(TestPlan plan, Sessions.Session session, AssertListener listener, @Nullable Output answer);
    }

    /** {@code pair} run on {@code sessions} by {@code runner}: each plan alone, then compared, their messages too
     *  (both judges write lite's sentences: the run hook's {@code wordsAlike} decided it until A4 of the compatibility program, stage A ratified by the user 2026-11-19,
     *  when the host judge became the only judge in compatibility). */
    static void run(TestPlan.Isolated pair, SessionSource sessions, Runner runner, @Nullable AssertListener callers,
            @Nullable Output answer) {
        if (sessions instanceof SessionSource.Given && (pair.host().writes() || pair.database().writes())) {
            throw new IllegalArgumentException(NEEDS_A_SESSION_PER_JUDGE);
        }
        Recording host = runAlone(pair.host(), sessions, runner, answer);
        // the database judge's run answers nobody: the host's answer is the body's
        Recording database = runAlone(pair.database(), sessions, runner, null);
        conclude(host, database, callers);
    }

    /** The two recorded runs concluded, their verdicts and messages compared: the caller hears the host's reports; then
     *  {@link JudgesDisagree} where the runs differ, else the host's failure rethrown, else the body passed. A recorded
     *  failure is never swallowed. */
    static void conclude(Recording host, Recording database, @Nullable AssertListener callers) {
        @Nullable String difference = difference(host, database);
        if (callers != null) {
            for (Heard h : host.verdicts) {
                switch (h.report()) {
                    case VERDICT -> callers.verdict(h.name(), h.pass(), h.detail());
                    case DECLINED -> callers.declined(h.name(), Objects.requireNonNull(h.detail()));
                    case UNJUDGED -> callers.unjudged(h.name(), Objects.requireNonNull(h.detail()));
                    case REFEREED -> callers.refereed(h.name(), Objects.requireNonNull(h.detail()));
                }
            }
        }
        if (difference != null) {
            throw new JudgesDisagree(difference);
        }
        if (host.failure != null) {
            throw host.failure;
        }
    }

    private static Recording runAlone(TestPlan plan, SessionSource sessions, Runner runner, @Nullable Output answer) {
        Recording recording = new Recording();
        try (var callbacks = com.legend.base.RunScope.callingBack();
                Sessions.Session session = sessions.open(plan.session())) {
            runner.run(plan, session, recording, answer);
        } catch (RuntimeException failed) {
            recording.failure = failed;
        }
        return recording;
    }

    /** The first difference between the two runs, both texts quoted, or null where they agree: on every verdict, its
     *  message, and the run's ending's text; both judge every assert. (The compatibility rules' comparison -- the
     *  verdicts alone, an assert one judge declined not compared -- went with A4 of the compatibility program, stage A ratified by the user 2026-11-19.) */
    static @Nullable String difference(Recording host, Recording database) {
        List<Heard> hs = host.compared();
        List<Heard> ds = database.compared();
        int n = Math.max(hs.size(), ds.size());
        for (int i = 0; i < n; i++) {
            Heard h = i < hs.size() ? hs.get(i) : null;
            Heard d = i < ds.size() ? ds.get(i) : null;
            // the verdict (and its message): the name each planner gives an assert (a helper's, or the assert inside
            // it) is how it is reported, not what it decides
            if (h == null || d == null || h.report() != d.report() || h.pass() != d.pass()
                    || !Objects.equals(h.detail(), d.detail())) {
                return "the judges disagree at assert " + i + ": host " + describe(h) + "; database " + describe(d)
                        + (h == null ? " (host's run " + quote(host.ending()) + ")" : "")
                        + (d == null ? " (database's run " + quote(database.ending()) + ")" : "");
            }
        }
        if (!host.ending().equals(database.ending())) {
            return "the judges' runs ended differently: host " + quote(host.ending()) + "; database "
                    + quote(database.ending());
        }
        return null;
    }

    private static String describe(@Nullable Heard h) {
        if (h == null) {
            return "(none)";
        }
        String what = switch (h.report()) {
            case VERDICT -> h.pass() ? "held" : "failed";
            case DECLINED -> "declined";
            case UNJUDGED -> "unjudged";
            case REFEREED -> "refereed";
        };
        return h.name() + " " + what + (h.detail() == null ? "" : " " + quote(h.detail()));
    }


    private static String quote(String text) {
        return "\"" + text + "\"";
    }

    /** The two isolated judges reached different verdicts or messages on one body. */
    static final class JudgesDisagree extends RuntimeException {
        JudgesDisagree(String message) {
            super(message);
        }
    }
}
