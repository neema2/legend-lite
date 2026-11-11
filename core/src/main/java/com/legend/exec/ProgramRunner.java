// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.base.Nullable;
import com.legend.error.NotImplementedException;
import com.legend.executionplan.ExecutionPlan;
import com.legend.result.ResultSink;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * THE PROGRAM RUNNER (docs/PLAN_AS_DATA_2026_11_07.md 2.8, the design's step 3, 2026-11-08): exec is the one walker. A
 * {@link Program}'s steps run in order on one session -- the product's nodes as {@link PlanRunner} runs them, in the
 * transaction or snapshot the program's shape asks -- and a step exec does not know (a test's verdict group or branch)
 * is a {@link Callback}: at its place the runner calls back, handing the caller the {@link OpenProgram}, through which
 * it reads only the statements the callback declared, inside the program. The program's half of the runner, its own
 * target ({@code //core:exec_program}, the package's): a product caller that runs plans reaches none of it.
 */
public final class ProgramRunner {

    private ProgramRunner() {
    }

    /** A program: its declared parameters and its steps, in order. */
    public record Program(List<ExecutionPlan.Parameter> parameters, List<Step> steps) {
        public Program {
            parameters = List.copyOf(parameters);
            steps = List.copyOf(steps);
            if (steps.isEmpty()) {
                throw new IllegalArgumentException("a program has at least one step");
            }
        }
    }

    /** A step of a program. */
    public sealed interface Step permits Node, Callback {
    }

    /** A product node, sent under {@code origin} where no origin is marked (null: as the node's kind sends it). */
    public record Node(ExecutionPlan.Node node, @Nullable StatementOrigin origin) implements Step {
        public Node {
            Objects.requireNonNull(node, "node");
            if (node instanceof ExecutionPlan.Sequence) {
                throw new IllegalArgumentException("a program's step is one node, never a sequence");
            }
        }
    }

    /**
     * A step the runner does not know: the caller's {@link Callbacks#step} at its place, under {@code origin} where no
     * origin is marked. {@code declared}: every statement the step may read -- typed or keyed results -- which the
     * program's shape counts (its target, its snapshot) and which alone its reads may send.
     */
    public record Callback(int id, List<ExecutionPlan.Node> declared, @Nullable StatementOrigin origin)
            implements Step {
        public Callback {
            declared = List.copyOf(declared);
            for (ExecutionPlan.Node n : declared) {
                if (!(n instanceof ExecutionPlan.TdsResult) && !(n instanceof ExecutionPlan.Keyed)) {
                    throw new IllegalArgumentException("a callback declares typed or keyed results, not "
                            + n.getClass().getSimpleName());
                }
            }
        }
    }

    /** The caller's half of a program: each {@link Callback} at its place. */
    @FunctionalInterface
    public interface Callbacks {
        /** The callback {@code id}, at its place: {@code open} is the program's one handle, the same at every
         *  callback, valid until the program ends. */
        void step(int id, OpenProgram open) throws IOException;
    }

    /** A savepoint's three statements, as the dialect spells them: set before a statement whose failure the program
     *  survives, rolled back to where it fails, released where it does not. */
    public record Savepoint(String set, String rollbackTo, String release) {
        public Savepoint {
            Objects.requireNonNull(set, "set");
            Objects.requireNonNull(rollbackTo, "rollbackTo");
            Objects.requireNonNull(release, "release");
        }
    }

    /** Work run under a savepoint. */
    @FunctionalInterface
    public interface Work<T> {
        T run() throws IOException;
    }

    /** What the caller makes of the guarded work's failure ({@code e}: the work's own exception, a runtime one or an
     *  I/O one), once its savepoint is rolled back to: its answer, or a raise. */
    @FunctionalInterface
    public interface Failed<T> {
        @Nullable T failed(Exception e) throws IOException;
    }

    /** The open program, handed to every callback: one per program, valid until it ends. */
    public interface OpenProgram {
        /** {@code read}'s typed rows -- a statement some callback of the program declared -- on the session its
         *  statement names, inside the program, the program's live values with {@code bindings} over them; under
         *  {@code origin} where no origin is marked. A statement no callback declared is refused by name. */
        PlanRunner.Rows read(ExecutionPlan.Node read, @Nullable StatementOrigin origin,
                Map<String, PlanParameters.Checked> bindings) throws IOException;

        /** Whether the program's session is inside a transaction now (its own, or a caller's it joined). */
        boolean insideATransaction();

        /** {@code work} under {@code savepoint} (none: run as it is): set before, released after; where the WORK fails,
         *  rolled back to and its failure handed to {@code failed}. A savepoint statement that fails is the program's
         *  failure, never the work's (the slice 1 audit, 2026-11-08: as the runner sent them before the split). */
        <T> @Nullable T guarded(@Nullable Savepoint savepoint, Work<T> work, Failed<T> failed) throws IOException;
    }

    /** Where a program's steps deliver: its answer, as any typed result's, and what the steps did. */
    public interface ProgramSink extends ResultSink {

        /** The program's seed step {@code step} ran (a step the session held established runs nothing and is not
         *  told): each of its setup steps that ran, in order, whether it answered rows -- where one failed, those
         *  before it. */
        default void seeded(int step, List<Boolean> answeredRows) {
        }

        /** The program's effect step {@code step} ran: its script applied. */
        default void effectRan(int step) {
        }

        /** A step of the program that writes (an effect or an update) is about to run. */
        default void writing(int step) {
        }
    }

    /**
     * Runs {@code program}: its steps in order on one session, in one transaction where one changes the database (a
     * keyed step's or a declared keyed read's snapshot the same), each callback at its place, and the program's answer
     * -- its one result step's typed rows, if it has one -- streamed to the sink once the steps have run. A load's
     * data references resolve through {@code stores} (data: docs/DATA_IN_THE_PLAN_2026_11_09.md 2.7).
     */
    public static void run(Program program, Map<String, ?> values, PlanSessions.Source sessions,
            com.legend.executionplan.DataStores stores, Callbacks callbacks, ProgramSink sink) throws IOException {
        // the runtime wall (the deep audit, 2026-11-05): while the program runs, nothing on this thread plans
        try (var run = com.legend.base.RunScope.enter()) {
            programRun(program, values, sessions, stores, callbacks, sink);
        }
    }

    private static void programRun(Program program, Map<String, ?> values, PlanSessions.Source sessions,
            com.legend.executionplan.DataStores stores, Callbacks callbacks, ProgramSink sink) throws IOException {
        // every live source the program's statements read, admitted once at its entry (A5): each step's path finds it
        List<ExecutionPlan.Node> nodes = new ArrayList<>();
        for (Step s : program.steps()) {
            switch (s) {
                case Node n -> nodes.add(n.node());
                case Callback c -> nodes.addAll(c.declared());
            }
        }
        Map<String, PlanParameters.Checked> checked = PlanRunner.admitSources(
                PlanParameters.check(program.parameters(), values), nodes, sessions.admitted());
        Shape shape = shapeOf(program, checked);
        PlanRunner.Held held = new PlanRunner.Held();
        ExecutionPlan.Target on = shape.target();
        // the statements the callbacks declared: the one thing the open program reads (P1.2, 2026-11-07, G4: exec runs
        // only the plan's statements -- a statement no callback declared is refused, never read)
        Set<ExecutionPlan.Node> declared = new HashSet<>();
        for (Step s : program.steps()) {
            if (s instanceof Callback c) {
                declared.addAll(c.declared());
            }
        }
        // the values a step produces (a write's count, a run's stamp), bound for the steps after it
        Map<String, PlanParameters.Checked> live = new HashMap<>(checked);
        if (on == null) {
            // a program that sends nothing (the test-is-the-product plan's B2, M2): its constant answer, held, no session
            // opened; a callback reads nothing here -- a statement it declared would have given the program a target
            Open open = new Open(declared, live, null);
            try {
                for (Step step : program.steps()) {
                    switch (step) {
                        case Node n when n.node() instanceof ExecutionPlan.Constant c -> PlanRunner.constant(c, held);
                        case Node n -> throw new IllegalStateException("a program that sends nothing holds a "
                                + n.node().getClass().getSimpleName() + " step: only a constant or a callback runs"
                                + " with no session");
                        case Callback c -> callbacks.step(c.id(), open);
                    }
                }
            } finally {
                open.close();
            }
        } else {
            try (Sessions.Session session = shape.changes() ? sessions.openToChange(on) : sessions.open(on)) {
                // the caller's mark, on the sessions it handed (a fixture's seeding: SEED), every step's mark under it
                Statements st = PlanRunner.opened(session, on, sessions.mark());
                ExecutionPlan.Target system = shape.system();
                if (system == null) {
                    boolean homeIsSystem = on.database() instanceof ExecutionPlan.Database.System;
                    // a program on the graph's system database reads the user's database for a statement that names it
                    // (a compatibility appeal's rows: the walk deletion's step 3, 2026-11-06), on its own session,
                    // opened at the first such statement and closed with the program
                    UserBeside user = homeIsSystem ? new UserBeside(sessions) : null;
                    try {
                        Routes rt = new Routes(st, homeIsSystem, null, null, user);
                        Open open = new Open(declared, live, rt);
                        try {
                            bracketed(shape, st, () -> runSteps(program, live, callbacks, session, rt, stores, held, sink,
                                    open));
                        } finally {
                            open.close();
                        }
                    } finally {
                        if (user != null) {
                            user.close();
                        }
                    }
                } else {
                    // a statement reading the graph's system metamodel store beside the body's: its own session, read
                    // only, outside the program's transaction (the walker-deletion program's per-statement routing,
                    // 2026-11-03: the first step to a program over two stores)
                    try (Sessions.Session systemSession = sessions.open(system)) {
                        Routes rt = new Routes(st, false, PlanRunner.opened(systemSession, system, sessions.mark()),
                                systemSession,
                                null);
                        Open open = new Open(declared, live, rt);
                        try {
                            bracketed(shape, st, () -> runSteps(program, live, callbacks, session, rt, stores, held, sink,
                                    open));
                        } finally {
                            open.close();
                        }
                    }
                }
            }
        }
        if (shape.answers()) {
            // every step ran, the result step among them: its rows, held, streamed now its changes have committed
            PlanRunner.streamHeld(held, sink);
        }
    }

    /** The open program: the declared statements, the live values, the program's routes (none where it sends
     *  nothing). */
    private static final class Open implements OpenProgram {
        private final Set<ExecutionPlan.Node> declared;
        private final Map<String, PlanParameters.Checked> live;
        private final @Nullable Routes routes;
        private boolean closed;
        /** The mark of the step whose callback is reading: the program's own outside one (explicit since slice 5,
         *  2026-11-09: the reads ran under a thread-scoped mark). */
        private StatementOrigin stepMark = StatementOrigin.OTHER;

        Open(Set<ExecutionPlan.Node> declared, Map<String, PlanParameters.Checked> live, @Nullable Routes routes) {
            this.declared = declared;
            this.live = live;
            this.routes = routes;
        }

        void close() {
            closed = true;
        }

        @Override
        public PlanRunner.Rows read(ExecutionPlan.Node read, @Nullable StatementOrigin origin,
                Map<String, PlanParameters.Checked> bindings) throws IOException {
            if (closed) {
                throw new IllegalStateException("the program's reader: a read once the program has ended");
            }
            if (!declared.contains(read)) {
                throw new IllegalStateException("the run's reader: a statement no step of the program declared (exec"
                        + " runs only the plan's statements): " + read.getClass().getSimpleName());
            }
            Routes rt = routes;
            if (rt == null) {
                throw new IllegalStateException("a read in a program that sends nothing");
            }
            Map<String, PlanParameters.Checked> values = live;
            if (!bindings.isEmpty()) {
                values = new HashMap<>(live);
                values.putAll(bindings);
            }
            StatementOrigin mark = origin == null ? stepMark : StatementOrigin.under(stepMark, origin);
            PlanRunner.Held h = new PlanRunner.Held();
            switch (read) {
                case ExecutionPlan.TdsResult t -> PlanRunner.result(t, values,
                        Statements.marked(rt.on(t.sql()), mark), h);
                case ExecutionPlan.Keyed k -> PlanRunner.keyedOn(k, values, Statements.marked(rt.on(k.keys()), mark),
                        PlanRunner.heldSink(h));
                default -> throw new IllegalStateException("a program's read is a typed or keyed result");
            }
            return PlanRunner.collected(h);
        }

        @Override
        public boolean insideATransaction() {
            Routes rt = routes;
            return rt != null && !rt.home().ownsTransaction();
        }

        @Override
        public <T> @Nullable T guarded(@Nullable Savepoint savepoint, Work<T> work, Failed<T> failed)
                throws IOException {
            Routes rt = routes;
            if (savepoint == null || rt == null) {
                try {
                    return work.run();
                } catch (RuntimeException | IOException e) {
                    return failed.failed(e);
                }
            }
            Statements home = Statements.marked(rt.home(), stepMark);
            PlanRunner.sendStatement(savepoint.set(), home);
            T out;
            try {
                out = work.run();
            } catch (RuntimeException | IOException e) {
                PlanRunner.sendStatement(savepoint.rollbackTo(), home);
                return failed.failed(e);
            }
            PlanRunner.sendStatement(savepoint.release(), home);
            return out;
        }
    }

    /**
     * A program's SHAPE, read from its steps before any of them runs: the one target its steps run on (null when it
     * sends nothing), whether it changes the database (a store write: the program's transaction), the strictest
     * snapshot its keyed steps and declared keyed reads ask, whether it answers, and the graph's system database beside
     * its own; a program that names two targets, a write's count named as a declared parameter, or more than one
     * result step is refused before anything runs. An effect commits itself, so it opens no transaction (the
     * transaction rule, the test-is-the-product plan's R17), and a seed step takes part, its target the program's.
     */
    private record Shape(ExecutionPlan.@Nullable Target target, boolean changes,
            ExecutionPlan.Keyed.@Nullable Snapshot snapshot, boolean answers,
            ExecutionPlan.@Nullable Target system) {
    }

    private static Shape shapeOf(Program program, Map<String, PlanParameters.Checked> checked) {
        ExecutionPlan.Target target = null;
        ExecutionPlan.Target system = null;
        int results = 0;
        boolean changes = false;
        ExecutionPlan.Keyed.Snapshot snapshot = null;
        for (Step each : program.steps()) {
            List<ExecutionPlan.Sql> sqls = new ArrayList<>();
            switch (each) {
                case Callback c -> {
                    for (ExecutionPlan.Node n : c.declared()) {
                        switch (n) {
                            case ExecutionPlan.TdsResult t -> sqls.add(t.sql());
                            case ExecutionPlan.Keyed k -> {
                                snapshot = PlanRunner.stricter(snapshot, k.snapshot());
                                sqls.add(k.keys());
                                sqls.add(k.body());
                            }
                            default -> throw new IllegalStateException("a callback declares typed or keyed results");
                        }
                    }
                }
                case Node node -> {
                    switch (node.node()) {
                        case ExecutionPlan.Update u -> {
                            changes = true;
                            if (u.count() != null && checked.containsKey(u.count())) {
                                throw new IllegalStateException("a write's count is named '" + u.count() + "', as a"
                                        + " declared parameter is");
                            }
                            writesHome(u.sql().target(), "a write");
                            sqls.add(u.sql());
                        }
                        case ExecutionPlan.Check k -> sqls.add(k.sql());
                        case ExecutionPlan.Effect e -> {
                            writesHome(e.sql().target(), "an effect");
                            sqls.add(e.sql());
                        }
                        case ExecutionPlan.Load u -> {
                            refuseTargetCadence(u);
                            if (u.target().database() instanceof ExecutionPlan.Database.System) {
                                // the graph's system store's own seeds: the one write its database takes, once per
                                // session
                                PlanRunner.systemStore(u);
                                system = sameTarget(system, u.target());
                            } else {
                                target = sameTarget(target, u.target());
                            }
                        }
                        case ExecutionPlan.TdsResult t -> {
                            results++;
                            sqls.add(t.sql());
                        }
                        case ExecutionPlan.Constant c -> results++;
                        case ExecutionPlan.Keyed k -> {
                            results++;
                            snapshot = PlanRunner.stricter(snapshot, k.snapshot());
                            sqls.add(k.keys());
                            sqls.add(k.body());
                        }
                        case ExecutionPlan.Sequence s -> throw new IllegalStateException("a sequence inside a"
                                + " program: a program's steps are one list");
                    }
                }
            }
            for (ExecutionPlan.Sql sql : sqls) {
                PlanRunner.nullTypes(sql);
                if (sql.target().database() instanceof ExecutionPlan.Database.System) {
                    system = sameTarget(system, sql.target());
                } else {
                    target = sameTarget(target, sql.target());
                }
            }
        }
        if (results > 1) {
            throw new IllegalStateException("a program of " + results + " result steps: a program's answer is at"
                    + " most one");
        }
        // a program reading the system store alone runs there, as one target; beside the program's own, the system
        // database is a second, read-only session (per-statement routing, 2026-11-03)
        return target == null ? new Shape(system, changes, snapshot, results > 0, null)
                : new Shape(target, changes, snapshot, results > 0, system);
    }

    /** A write or an effect names {@code target}: refused on the graph's system database, which is read only beside a
     *  program's own target (its rows are the graph's, written once by the store's seed step). */
    private static void writesHome(ExecutionPlan.Target target, String what) {
        if (target.database() instanceof ExecutionPlan.Database.System s) {
            throw new IllegalStateException(what + " on the graph's system database (" + s.type() + "): it is read"
                    + " only, written once by the system store's own seed step");
        }
    }

    /** {@code next}, the program's one target so far being {@code sofar} (none yet when null). */
    private static ExecutionPlan.Target sameTarget(ExecutionPlan.@Nullable Target sofar, ExecutionPlan.Target next) {
        if (sofar != null && !sofar.equals(next)) {
            throw new NotImplementedException("a plan whose steps name different targets: its steps run on one"
                    + " session, in one transaction");
        }
        return next;
    }

    /** {@code work} run in the bracket {@code shape} asks: the strictest snapshot its keyed reads ask, where the
     *  session owns its transaction; else one transaction where the program changes the database; else no bracket. */
    private static void bracketed(Shape shape, Statements st, Statements.Work work) throws IOException {
        ExecutionPlan.Keyed.Snapshot snapshot = shape.snapshot();
        if (snapshot != null && snapshot != ExecutionPlan.Keyed.Snapshot.NONE && st.ownsTransaction()) {
            st.inSnapshot(snapshot, work);
        } else if (shape.changes()) {
            try {
                st.inTransaction(() -> {
                    try {
                        work.run();
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                    return null;
                });
            } catch (UncheckedIOException e) {
                throw e.getCause();
            }
        } else {
            // reads alone, no snapshot asked: each statement as the session sends it, no bracket -- a verdict group's
            // statements ran so before they were a plan's steps (S2e, 2026-10-30), and a bracket would carry an abort
            // the program survives (an assertError's side) into the steps that follow it on a database that aborts the
            // transaction on any error (Postgres)
            work.run();
        }
    }

    /** The program's steps run in order, each under its origin where none is marked; its answer held in
     *  {@code held}. */
    private static void runSteps(Program program, Map<String, PlanParameters.Checked> live, Callbacks callbacks,
            Sessions.Session session, Routes rt, com.legend.executionplan.DataStores stores, PlanRunner.Held held,
            ProgramSink sink, Open open)
            throws IOException {
        List<Step> steps = program.steps();
        for (int i = 0; i < steps.size(); i++) {
            Step step = steps.get(i);
            StatementOrigin named = switch (step) {
                case Node n -> n.origin();
                case Callback c -> c.origin();
            };
            // the step's mark: its own under the program's, where it names one
            StatementOrigin mark = named == null ? rt.home().mark() : StatementOrigin.under(rt.home().mark(), named);
            switch (step) {
                case Node n -> runStep(n.node(), i, live, session, rt, stores, held, sink, mark);
                case Callback c -> {
                    open.stepMark = mark;
                    try {
                        callbacks.step(c.id(), open);
                    } finally {
                        open.stepMark = rt.home().mark();
                    }
                }
            }
        }
    }

    private static void runStep(ExecutionPlan.Node step, int i, Map<String, PlanParameters.Checked> live,
            Sessions.Session session, Routes rt, com.legend.executionplan.DataStores stores, PlanRunner.Held held,
            ProgramSink sink, StatementOrigin mark)
            throws IOException {
        Statements st = Statements.marked(rt.home(), mark);
        switch (step) {
            case ExecutionPlan.Update u -> {
                // a store write may change a seeded table: every seed establishes again after it, as after an effect
                // (the design audit's H1, 2026-11-02: an update left the seeds marked in place)
                sink.writing(i);
                session.state().markWriting();
                PlanRunner.update(u, live, st);
            }
            case ExecutionPlan.Check k -> {
                // a statement of the body run for what it raises, its rows dropped: a statement on its own, as the
                // walk marks a value statement it runs (an unread let's, the user's ruling of 2026-11-02); a mark in
                // force (a frame's eager run, LET) stands
                PlanRunner.check(k, live, Statements.marked(rt.on(k.sql()),
                        StatementOrigin.under(mark, StatementOrigin.STATEMENT)));
            }
            case ExecutionPlan.Effect e -> {
                // marked written BEFORE it runs, as the walk marks it: a script that fails part-applied leaves the
                // session's seeds to establish again (the M3 audit's B5)
                sink.writing(i);
                session.state().markWriting();
                // a failure is the database's own error, raised as it is (upstream's executeInDb), its script's abort
                // sent first; nothing reads which statement failed (P1, 2026-11-07)
                PlanRunner.script(e, live, st, !st.ownsTransaction());
                sink.effectRan(i);
            }
            case ExecutionPlan.Load u -> {
                refuseTargetCadence(u);
                // the system store's load on the system database's session where it is a second one; every other load
                // on the program's
                boolean beside = rt.systemSession() != null
                        && u.target().database() instanceof ExecutionPlan.Database.System;
                if (u.cadence() == ExecutionPlan.Load.Cadence.AT_STEP) {
                    // a program's own load writes, as an effect does
                    sink.writing(i);
                }
                PlanRunner.seed(u, beside ? Objects.requireNonNull(rt.systemSession()) : session,
                        beside ? Statements.marked(Objects.requireNonNull(rt.system()), mark) : st, stores,
                        answered -> sink.seeded(i, answered));
            }
            case ExecutionPlan.TdsResult t -> {
                // the program's answering statement is a statement of the body on its own, as the walk marks a value
                // statement (BodyDriver.answerValue); a mark in force (a judge's, a fixture's) stands
                PlanRunner.result(t, live, Statements.marked(rt.on(t.sql()),
                        StatementOrigin.under(mark, StatementOrigin.STATEMENT)), held);
            }
            case ExecutionPlan.Constant c -> PlanRunner.constant(c, held);
            case ExecutionPlan.Keyed k -> {
                PlanRunner.keyedOn(k, live, Statements.marked(rt.on(k.keys()),
                        StatementOrigin.under(mark, StatementOrigin.STATEMENT)), PlanRunner.heldSink(held));
            }
            case ExecutionPlan.Sequence s -> throw new IllegalStateException("a sequence inside a program");
        }
    }

    /** Where a program's statements run: the program's own session, and the graph's system database's where a
     *  statement names that target beside the program's (read only: no write, effect or seed is planned there). */
    private record Routes(Statements home, boolean homeIsSystem, @Nullable Statements system,
            Sessions.@Nullable Session systemSession, @Nullable UserBeside user) {
        /** The session {@code sql} runs on: the system database's where it names that target -- the program's own
         *  where the program runs there, else its second session -- and the program's otherwise. A statement naming
         *  the system database in a program that opened none for it is refused by name, never run on the user's
         *  session (the final audit, 2026-11-06: a row read on demand through the program's reader is routed here
         *  with no target check of its own). */
        Statements on(ExecutionPlan.Sql sql) {
            if (sql.target().database() instanceof ExecutionPlan.Database.System s) {
                if (homeIsSystem) {
                    return home;
                }
                if (system == null) {
                    throw new IllegalStateException("a statement on the graph's system database (" + s.type()
                            + ") in a program that planned none there: never run on the program's own session");
                }
                return system;
            }
            if (homeIsSystem) {
                // a statement naming the user's database in a program on the system database: its own session, never
                // the system one (until 2026-11-06 it ran on the home session, the system database, silently)
                if (user == null) {
                    throw new IllegalStateException("a statement on " + sql.target().database() + " in a program on"
                            + " the graph's system database that opened no route to it");
                }
                return user.on(sql.target());
            }
            return home;
        }
    }

    /** A database a program reaches beside its own: the user's beside a program on the graph's system database, or
     *  a side's beside a program of constants -- its session opened from the program's sessions source at the first
     *  statement that names it, kept for the program, closed with it. One such target a program. */
    private static final class UserBeside {
        private final PlanSessions.Source sessions;
        /** The session and its statements once opened. */
        private final java.util.concurrent.atomic.AtomicReference<@Nullable Opened> opened =
                new java.util.concurrent.atomic.AtomicReference<>();

        private record Opened(Sessions.Session session, Statements statements, ExecutionPlan.Target target) {
        }

        UserBeside(PlanSessions.Source sessions) {
            this.sessions = sessions;
        }

        Statements on(ExecutionPlan.Target t) {
            Opened o = opened.get();
            if (o == null) {
                Sessions.Session s = sessions.open(t);
                o = new Opened(s, PlanRunner.opened(s, t, sessions.mark()), t);
                opened.set(o);
            } else if (!t.equals(o.target())) {
                throw new IllegalStateException("a program names two databases beside its own (" + o.target() + ", "
                        + t + "): one beside it");
            }
            return o.statements();
        }

        void close() {
            Opened o = opened.getAndSet(null);
            if (o != null) {
                o.session().close();
            }
        }
    }

    /** A program's own load node is a seed ({@code BEFORE_STEP}) or a load at its place ({@code AT_STEP}); a target's
     *  load ({@code PER_ACQUISITION}, {@code ONCE}) runs where its target is acquired or made, never as a node: refused by
     *  name (the cadence a contract exec honours or refuses: the M1 tenets audit's finding 7, 2026-11-09). */
    static void refuseTargetCadence(ExecutionPlan.Load load) {
        if (load.cadence() == ExecutionPlan.Load.Cadence.PER_ACQUISITION
                || load.cadence() == ExecutionPlan.Load.Cadence.ONCE) {
            throw new IllegalStateException("a " + load.cadence() + " load (" + load.identity() + ") is a target's,"
                    + " established where its target is acquired or made: never a program's node");
        }
    }
}
