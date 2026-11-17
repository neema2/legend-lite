// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

import com.legend.base.Nullable;
import com.legend.exec.SessionSource;
import com.legend.exec.Sessions;
import com.legend.exec.StatementOrigin;
import com.legend.result.Output;
import com.legend.testexecutionplan.AssertListener;
import com.legend.testexecutionplan.Edge;
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

    /** One assert's report, as the listener heard it. */
    record Heard(String name, boolean pass, @Nullable String detail) {
    }

    /** One judge's run, recorded: its verdicts in order, the asserts it declined or left unjudged, and how it ended. */
    static final class Recording implements AssertListener {
        final List<Heard> verdicts = new ArrayList<>();
        final List<String> notes = new ArrayList<>();
        @Nullable RuntimeException failure;

        @Override
        public void verdict(String assertName, boolean pass, @Nullable String detail) {
            verdicts.add(new Heard(assertName, pass, detail));
        }

        @Override
        public void declined(String assertName, String reason) {
            notes.add("declined " + assertName + ": " + reason);
        }

        @Override
        public void unjudged(String assertName, String reason) {
            notes.add("unjudged " + assertName + ": " + reason);
        }

        /** The run's end, as text: its failure's class and message, or none. */
        String ending() {
            return failure == null ? "passed" : failure.getClass().getSimpleName() + ": " + failure.getMessage();
        }
    }

    /** {@code pair} run on {@code sessions}: each plan alone, then compared. */
    static void run(TestPlan.Isolated pair, SessionSource sessions, com.legend.executionplan.DataStores stores,
            @Nullable Edge edge, @Nullable AssertListener callers, @Nullable SqlReplayReferee referee,
            @Nullable Output answer, java.util.UUID execution) {
        if (sessions instanceof SessionSource.Given && (pair.host().writes() || pair.database().writes())) {
            throw new IllegalArgumentException(NEEDS_A_SESSION_PER_JUDGE);
        }
        Recording host = runAlone(pair.host(), sessions, stores, edge, referee, answer, execution);
        // the database judge's run answers nobody: the host's answer is the body's
        Recording database = runAlone(pair.database(), sessions, stores, edge, referee, null, execution);
        conclude(host, database, callers);
    }

    /** The two recorded runs concluded: the caller hears the host's verdicts; then {@link JudgesDisagree} where the runs
     *  differ, else the host's failure rethrown, else the body passed. A recorded failure is never swallowed. */
    static void conclude(Recording host, Recording database, @Nullable AssertListener callers) {
        @Nullable String difference = difference(host, database);
        if (callers != null) {
            for (Heard h : host.verdicts) {
                callers.verdict(h.name(), h.pass(), h.detail());
            }
        }
        if (difference != null) {
            throw new JudgesDisagree(difference);
        }
        if (host.failure != null) {
            throw host.failure;
        }
    }

    private static Recording runAlone(TestPlan plan, SessionSource sessions,
            com.legend.executionplan.DataStores stores, @Nullable Edge edge, @Nullable SqlReplayReferee referee,
            @Nullable Output answer, java.util.UUID execution) {
        Recording recording = new Recording();
        try (var callbacks = com.legend.base.RunScope.callingBack();
                Sessions.Session session = sessions.open(plan.session())) {
            TestRunner.run(plan, session, stores, edge, recording, referee, answer, StatementOrigin.OTHER, execution);
        } catch (RuntimeException failed) {
            recording.failure = failed;
        }
        return recording;
    }

    /** The first difference between the two runs, both texts quoted, or null where they agree on every verdict, every
     *  message and the run's end. */
    static @Nullable String difference(Recording host, Recording database) {
        int n = Math.max(host.verdicts.size(), database.verdicts.size());
        for (int i = 0; i < n; i++) {
            Heard h = i < host.verdicts.size() ? host.verdicts.get(i) : null;
            Heard d = i < database.verdicts.size() ? database.verdicts.get(i) : null;
            // the verdict and its message: the name each planner gives an assert (a helper's, or the assert inside it)
            // is how it is reported, not what it decides
            if (h == null || d == null || h.pass() != d.pass() || !Objects.equals(h.detail(), d.detail())) {
                return "the judges disagree at assert " + i + ": host " + describe(h) + "; database " + describe(d)
                        + (h == null ? " (host's run " + quote(host.ending()) + ")" : "")
                        + (d == null ? " (database's run " + quote(database.ending()) + ")" : "") + notes(database);
            }
        }
        if (!host.ending().equals(database.ending())) {
            return "the judges' runs ended differently: host " + quote(host.ending()) + "; database "
                    + quote(database.ending()) + notes(database);
        }
        return null;
    }

    private static String describe(@Nullable Heard h) {
        if (h == null) {
            return "(none)";
        }
        return h.name() + " " + (h.pass() ? "held" : "failed") + (h.detail() == null ? "" : " " + quote(h.detail()));
    }

    private static String notes(Recording database) {
        return database.notes.isEmpty() ? "" : " (database: " + String.join("; ", database.notes) + ")";
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
