// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.ExecutionPlan;
import com.legend.result.Output;
import com.legend.result.ResultSink;

import java.io.IOException;
import java.util.Map;

/**
 * A PRODUCT QUERY'S PLAN RUN (the plan-as-data design's slice 5, 2026-11-09: the door's {@code runPlan} until then,
 * moved here so the door holds no logic): what the plan says alone decides where it runs. A plan that writes runs on
 * its statements' own sessions (decision A, docs/WRITES_DESIGN_2026_10_18.md §8: a write runs on a database of its own,
 * a database the user names or a URL reaches keeps it); one that reads runs on the session {@code sessions} opens for
 * its answer's target, established as {@link Establish} says; a constant answer sends nothing and opens no session (the
 * test-is-the-product plan's M2, B11: its value is the plan's). The rows stream in batches to the writer the result
 * library makes for the caller's {@link Output} from the plan, before any statement runs.
 *
 * <p>What the caller hands the run is data and receivers, never a decision (2026-11-09, the user's question "why do we
 * have any callbacks?"): the sources are closed sets exec interprets ({@link SessionSource}, {@link PlanSessions.Source}),
 * the output a format and a stream; the one caller code the run calls is a receiver's ({@link Output.Rows}), told the
 * answer and deciding nothing, and the host's connection ({@link Sessions.Opener}), a resource. Every one of them is
 * called inside {@code RunScope.callingBack}, which refuses planning.
 */
public final class ProductRun {

    private ProductRun() {
    }

    /** How a plan that reads takes its session: established as legend-engine establishes a connection on every
     *  acquisition, the target's declared setup run on it first ({@code pure/v1} execute), or as the caller set it up
     *  (its own tables, its own connection). The door's {@code Execution.Establish} until slice 5. */
    public enum Establish { DECLARED_SETUP, AS_GIVEN }

    /** {@code plan} run, its answer written in {@code output} -- the writer made from the plan before any statement
     *  runs, so a format that has no form for the answer refuses it untouched: a program's writes never land behind a
     *  refused answer. Returns the plan that ran. */
    public static ExecutionPlan run(ExecutionPlan plan, SessionSource sessions, PlanSessions.Source planSessions,
            Establish establish, Output output, int batchRows) throws IOException {
        // a receiver of the caller's is told the answer here, before the plan's run opens: it plans nothing
        try (var callbacks = com.legend.base.RunScope.callingBack()) {
            ResultSink sink = output.writerFor(plan);
            runCalledBack(plan, sessions, planSessions, establish, sink, batchRows);
        }
        return plan;
    }

    /** {@code plan} run as the server runs it (legend-engine's {@code pure/v1/execution/execute}): a read on its
     *  answer's target's project database, established first as legend-engine establishes a connection on every
     *  acquisition (its declared test data run on it); a program that writes on the plan's own shared sessions. Its
     *  answer written in {@code output}, in the stream's default batches. */
    public static ExecutionPlan onProjectDatabases(ExecutionPlan plan, Output output) throws IOException {
        return run(plan, PlanSessions.PROJECT_DATABASES, PlanSessions.shared(), Establish.DECLARED_SETUP, output,
                com.legend.result.Batcher.DEFAULT_CAPACITY);
    }

    /** {@link #onProjectDatabases(ExecutionPlan, Output)}, its live sources read under the fence the host's
     *  configuration names ({@link SourceAdmission#configured}: null, any file). */
    public static ExecutionPlan onProjectDatabases(ExecutionPlan plan, @com.legend.base.Nullable String admittedSources,
            Output output) throws IOException {
        return run(plan, PlanSessions.PROJECT_DATABASES, PlanSessions.admitting(PlanSessions.shared(),
                SourceAdmission.configured(admittedSources)), Establish.DECLARED_SETUP, output,
                com.legend.result.Batcher.DEFAULT_CAPACITY);
    }

    /** {@link #run} inside the scope that refuses planning while the caller's sessions are opened. */
    private static void runCalledBack(ExecutionPlan plan, SessionSource sessions, PlanSessions.Source planSessions,
            Establish establish, ResultSink sink, int batchRows) throws IOException {
        // a write on the caller's session, its declared setup established, is established as a read is: the program's
        // target's acquisition loads run on the session first, and released when the request lets it go (A5,
        // 2026-11-10: until then the caller's session refused them, so a write never read its declared test data)
        boolean establishedOnTheCallers = establish == Establish.DECLARED_SETUP
                && sessions instanceof SessionSource.Given;
        if (!plan.writes() && !plan.sendsTheAnswer()) {
            PlanRunner.stream(plan, Map.of(), NOTHING_SENT, sink, batchRows);
            return;
        }
        ExecutionPlan.Target target = plan.writes() ? home(plan) : plan.answerSql().target();
        if (plan.writes() && !establishedOnTheCallers
                && !(target.database() instanceof ExecutionPlan.Database.System)) {
            PlanRunner.stream(plan, Map.of(), planSessions, sink, batchRows);
            return;
        }
        if (target.database() instanceof ExecutionPlan.Database.System) {
            // a query reading the metamodel alone, answered on the process's catalog (L4's product half, 2026-11-10):
            // the run's turn on it, the model's registration inside it, rolled back when the run ends -- never the
            // caller's session, which holds no catalog
            try (SystemDatabase.Leases leases = SystemDatabase.leases(Map.of())) {
                PlanRunner.stream(plan, Map.of(), PlanSessions.admitting(PlanSessions.system(leases),
                        planSessions.admitted()), sink, batchRows);
            }
            return;
        }
        try (Sessions.Session session = sessions.open(target)) {
            PlanSessions.Source on = switch (establish) {
                // a project's database was made with its ONCE loads (PlanSessions.projectDatabase); every other session
                // runs only what each acquisition establishes, and refuses the rest by name
                case DECLARED_SETUP -> sessions instanceof SessionSource.ProjectDatabases
                        ? PlanSessions.acquiredOnMade(session, target) : PlanSessions.acquired(session, target);
                case AS_GIVEN -> {
                    // the caller set its session up: a target that loads data on acquisition would run without it,
                    // refused by name, never skipped (the M1 tenets audit's finding 7, 2026-11-09)
                    if (!target.loads().isEmpty()) {
                        throw new IllegalStateException("the plan's target loads data when a session is acquired for"
                                + " it (" + target.loads().size() + " load(s)), and its session is as the caller set it"
                                + " up: establish its loads (DECLARED_SETUP) or plan a target with none");
                    }
                    yield PlanSessions.of(session);
                }
            };
            // the caller's admission of live sources holds on the answer's session too (L6, 2026-11-10); what the
            // request's acquisition made is released whatever its outcome (§3 rules 2 and 4, A4), before its session
            // -- and a kept session's lock -- goes: a release that fails after the run failed is suppressed into it
            try (PlanSessions.Releasing released = PlanSessions.releasing(on)) {
                PlanRunner.stream(plan, Map.of(), PlanSessions.admitting(on, planSessions.admitted()), sink, batchRows);
            }
        }
    }

    /** The one target a writing plan's steps run on (the runner holds them to one: {@code PlanRunner.sequence}). */
    private static ExecutionPlan.Target home(ExecutionPlan plan) {
        if (!(plan.root() instanceof ExecutionPlan.Sequence s)) {
            throw new IllegalStateException("a plan that writes is a sequence of steps");
        }
        for (ExecutionPlan.Node step : s.steps()) {
            ExecutionPlan.@com.legend.base.Nullable Target t = switch (step) {
                case ExecutionPlan.Update u -> u.sql().target();
                case ExecutionPlan.Check k -> k.sql().target();
                case ExecutionPlan.Effect e -> e.sql().target();
                case ExecutionPlan.TdsResult r -> r.sql().target();
                case ExecutionPlan.Keyed k -> k.body().target();
                case ExecutionPlan.Load l -> l.target();
                case ExecutionPlan.Constant c -> null;
                case ExecutionPlan.Sequence q -> throw new IllegalStateException("a sequence inside a sequence");
            };
            if (t != null) {
                return t;
            }
        }
        throw new IllegalStateException("a plan that writes sends a statement");
    }

    /** A constant answer's sessions: it sends no statement, so none is opened (until slice 5 the door opened the
     *  runtime's session for it and sent nothing on it). Asked for one, it refuses by name. */
    private static final PlanSessions.Source NOTHING_SENT =
            PlanSessions.refusing("a constant answer sends no statement: no session is opened for it");
}
