// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.cache.Hash;
import com.legend.cache.HandleStore;
import com.legend.executionplan.ExecutionPlan;

/**
 * THE SESSIONS A PLAN'S TARGET NAMES (execution plan step 3, 2026-10-09, docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md
 * §9): where the plan runner gets a session for a target -- the caller's own session ({@link #of}), or one opened
 * for the target and kept by it ({@link #shared}: a database the user names or a URL reaches is opened fresh per use,
 * an in-memory one is kept by the target's content so its tables persist across plans), its setup run once
 * ({@link #setUp}). Since D70 (2026-10-30, the seam's step A) this class holds sessions, never a connection: a
 * session is made by {@link Sessions} alone, and the handle stores here keep owned sessions and hand out views.
 */
public final class PlanSessions {
    private PlanSessions() {
    }

    /** Where the runner gets the session for a plan's target: the caller's own, or one opened and kept for the
     *  target. {@link #openToChange}: a session for a plan that WRITES (docs/WRITES_DESIGN_2026_10_18.md, decision A:
     *  a write runs on a database of its own, a database the user names or a URL reaches keeps it). */
    @FunctionalInterface
    public interface Source {
        Sessions.Session open(ExecutionPlan.Target target);

        default Sessions.Session openToChange(ExecutionPlan.Target target) {
            return open(target);
        }

        /** The statement origin the statements opened from these sessions are counted under (explicit since slice 5,
         *  2026-11-09: a thread-scoped mark until then). */
        default StatementOrigin mark() {
            return StatementOrigin.OTHER;
        }
    }

    /** {@code sessions} whose statements are counted under {@code mark}. */
    public static Source marked(Source sessions, StatementOrigin mark) {
        Source inner = sessions instanceof MarkedSource m ? m.inner() : sessions;
        return new MarkedSource(inner, mark);
    }

    private record MarkedSource(Source inner, StatementOrigin mark) implements Source {
        @Override
        public Sessions.Session open(ExecutionPlan.Target target) {
            return inner.open(target);
        }

        @Override
        public Sessions.Session openToChange(ExecutionPlan.Target target) {
            return inner.openToChange(target);
        }
    }

    /** The sessions kept by a target's content: a platform database or a held in-memory one, set up once. */
    private static final HandleStore<Sessions.Session> SHARED = new HandleStore<>();

    /** The named in-memory databases kept alive by a keeper session (the session that opened them). */
    private static final HandleStore<Named> NAMED = new HandleStore<>();

    private record Named(Sessions.Session keeper, String name) {
    }

    private static final java.util.concurrent.atomic.AtomicLong ATTEMPTS = new java.util.concurrent.atomic.AtomicLong();

    /** The sessions a target opens for itself and keeps: the plan path's own (the server's, the readers'). */
    public static Source shared() {
        return new Source() {
            @Override
            public Sessions.Session open(ExecutionPlan.Target target) {
                return openShared(target);
            }

            @Override
            public Sessions.Session openToChange(ExecutionPlan.Target target) {
                return openOwn(target);
            }
        };
    }

    /** The caller's own session for every target: a non-closing view of it ({@link Sessions#view}). The one way a
     *  held session reaches the runner (D70): a connection enters through {@link Sessions#owned} or
     *  {@link Sessions#borrowed} first. */
    public static Source of(Sessions.Session session) {
        return of(session, null);
    }

    /** {@link #of(Sessions.Session)} whose statements publish each query's execution-trace stamp to {@code trace}
     *  (the body driver's, track B's B1b). */
    public static Source of(Sessions.Session session, @com.legend.base.Nullable ExecutionTrace trace) {
        return of(session, trace, null);
    }

    /** {@link #of(Sessions.Session, ExecutionTrace)} where a target naming the graph's SYSTEM database
     *  ({@link ExecutionPlan.Database.System}) opens {@code system}'s session for it instead -- a non-closing view, as
     *  the caller's own (the test-is-the-product plan's M10a: the body driver holds the graph's system database,
     *  opened once per graph -- its store written by the plan's own seed step -- and hands it here; the runner knows
     *  no database). Null {@code system}: every target the caller's session. */
    public static Source of(Sessions.Session session, @com.legend.base.Nullable ExecutionTrace trace,
            java.util.function.@com.legend.base.Nullable Function<ExecutionPlan.Database.System, Sessions.Session> system) {
        return target -> target.database() instanceof ExecutionPlan.Database.System s && system != null
                ? Sessions.view(system.apply(s), trace)
                : Sessions.view(session, trace);
    }

    /** {@link #of(Sessions.Session)} with {@code target}'s setup run on the session first, once (the plan's seeds, the
     *  session statements, under the SEED mark): run INSIDE the plan's run, when the runner first opens a session
     *  of this source (THE WALL's rule 3, 2026-11-06: until then it ran here, before the run, a statement sent
     *  outside every run); a plan whose setup has rows for a bulk loader is refused by name there, before any
     *  statement, on a database that has none. */
    public static Source setUp(Sessions.Session session, ExecutionPlan.Target target) {
        Source source = of(session);
        // [0] established, [1] a setup that failed: never run again on the same source (some of its steps ran)
        boolean[] state = {false, false};
        return new Source() {
            @Override
            public Sessions.Session open(ExecutionPlan.Target t) {
                synchronized (state) {
                    if (state[1]) {
                        throw new IllegalStateException("the session's setup failed on its first open, and is not run"
                                + " again (some of its steps ran)");
                    }
                    if (!state[0]) {
                        state[1] = true;
                        establish(session, target);
                        state[1] = false;
                        state[0] = true;
                    }
                }
                return source.open(t);
            }
        };
    }

    /** {@code target}'s setup run on {@code session} now, inside the run that opened it (the runner's own setup steps:
     *  {@link PlanRunner#setUpIn}); refused by name before any statement where its rows need a bulk loader the
     *  session's database has none of. */
    private static void establish(Sessions.Session session, ExecutionPlan.Target target) {
        if (target.setup().stream().anyMatch(ExecutionPlan.SetupStep.Rows.class::isInstance)
                && !Sessions.loadsRows(session)) {
            throw new IllegalStateException("the plan's setup has rows for a bulk loader, and the session's"
                    + " database has none (" + Sessions.product(session) + ")");
        }
        PlanRunner.setUpIn(target.setup(), target, marked(of(session), StatementOrigin.SEED), null);
    }

    /** A session on the target's database kept by the target's content (its setup run once when opened): a view of a
     *  stored owned session, or an owned session of the target's own where the database keeps itself (the user's
     *  named in-memory database, a URL). */
    private static Sessions.Session openShared(ExecutionPlan.Target target) {
        Hash key = Hash.ofUtf8(com.legend.executionplan.PlanJson.writeTarget(target));
        return switch (target.database()) {
            // the graph's system database is the body driver's (opened once per graph, its store the plan's seed step):
            // it reaches the runner through a caller's source with a system opener (M10a), never the shared one
            case ExecutionPlan.Database.System s -> throw new IllegalStateException("a target naming the graph's system"
                    + " database on " + s.type() + " opens through its holder's sessions source, not the shared one");
            case ExecutionPlan.Database.Platform p -> Sessions.view(SHARED.getOrOpenSlowly(key, Sessions::closed,
                    () -> setUpOpened(Sessions.openPrivate(p.type()), target)));
            case ExecutionPlan.Database.Declared d -> {
                DatabaseConnector connector = Connectors.of(d.connection().databaseType());
                if (connector.ownSessions()) {
                    refuseSetup(target, d, "a database its connector opens");
                    yield connector.open(d.connection());
                }
                yield switch (Sessions.openingFor(d.connection())) {
                    case Sessions.Opening.Held h -> Sessions.view(SHARED.getOrOpenSlowly(key, Sessions::closed,
                            () -> setUpOpened(Sessions.openHeld(h), target)));
                    case Sessions.Opening.Named n -> {
                        String usersName = n.name();
                        if (usersName != null) {
                            refuseSetup(target, d, "an in-memory database the user named ('" + usersName + "')");
                            yield Sessions.openNamed(n, usersName);
                        }
                        Named named = NAMED.getOrOpenSlowly(key, held -> Sessions.closed(held.keeper()), () -> {
                            String name = "plan_" + key.hex().substring(0, 16) + "_" + ATTEMPTS.incrementAndGet();
                            return new Named(setUpOpened(Sessions.openKept(n, name), target), name);
                        });
                        yield Sessions.joinKept(n, named.name());
                    }
                    case Sessions.Opening.Url u -> {
                        refuseSetup(target, d, "a database reached by a URL");
                        yield Sessions.open(u);
                    }
                };
            }
        };
    }

    /** A session of the target's own for a plan that writes: a private database set up for it, or the database the
     *  user names or the URL reaches (shared: those keep themselves). */
    private static Sessions.Session openOwn(ExecutionPlan.Target target) {
        return switch (target.database()) {
            case ExecutionPlan.Database.System s -> throw new IllegalStateException("a target naming the graph's system"
                    + " database on " + s.type() + " is read-only: no plan writes it");
            case ExecutionPlan.Database.Platform p -> setUpOpened(Sessions.openPrivate(p.type()), target);
            case ExecutionPlan.Database.Declared d -> switch (Sessions.openingFor(d.connection())) {
                case Sessions.Opening.Held h -> setUpOpened(Sessions.openHeld(h), target);
                case Sessions.Opening.Named n -> {
                    if (n.name() != null) {
                        yield openShared(target);
                    }
                    String name = "plan_own_" + ATTEMPTS.incrementAndGet();
                    yield setUpOpened(Sessions.openKept(n, name), target);
                }
                case Sessions.Opening.Url u -> openShared(target);
            };
        };
    }

    private static void refuseSetup(ExecutionPlan.Target target, ExecutionPlan.Database.Declared d, String what) {
        if (!target.setup().isEmpty()) {
            throw new IllegalStateException("connection '" + d.connection().qualifiedName() + "' is " + what
                    + ", and the plan has setup for it: never run on it");
        }
    }

    /** {@code session} with {@code target}'s setup run on it; a setup that fails closes the session it was opened
     *  for, the setup's failure the one told. */
    /** The projects' own databases (C5), kept by their identity: made once, then reused. */
    private static final HandleStore<Sessions.Session> PROJECTS = new HandleStore<>();

    /**
     * A PROJECT'S OWN DATABASE for {@code target} (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C5; the plan-as-data design's
     * hole 7, the user's go of 2026-11-08): found or made from the plan's target alone, never the model -- kept by the
     * database's identity (its connection and its project: named by what it holds, so an edited row is a fresh database
     * and two models whose stores differ never share one, type audit D100), made ONCE with the project's
     * {@code made} steps where this makes the database itself (a held in-memory one), kept for the process. An
     * unnamed in-memory database and the platform's are keyed apart by the project and filled with nothing (as the
     * server kept them); a URL's or a user-named database is the user's own and opens as it is. The per-query setup
     * is not run here: the door runs the target's setup on each query ({@code Execution.Establish}), as legend-engine
     * establishes a connection on every acquisition.
     */
    public static Sessions.Session projectDatabase(ExecutionPlan.Target target) {
        ExecutionPlan.Target identity = new ExecutionPlan.Target(target.database(), java.util.List.of(),
                java.util.List.of());
        Hash key = Hash.ofUtf8(com.legend.executionplan.PlanJson.writeTarget(identity));
        return switch (target.database()) {
            case ExecutionPlan.Database.System s -> throw new IllegalStateException("a target naming the graph's system"
                    + " database on " + s.type() + " opens through its holder's sessions source, not a project's");
            case ExecutionPlan.Database.Platform p -> Sessions.view(PROJECTS.getOrOpenSlowly(key, Sessions::closed,
                    () -> Sessions.openPrivate(p.type())));
            case ExecutionPlan.Database.Declared d -> {
                DatabaseConnector connector = Connectors.of(d.connection().databaseType());
                if (connector.ownSessions()) {
                    yield connector.open(d.connection());
                }
                yield switch (Sessions.openingFor(d.connection())) {
                    case Sessions.Opening.Url u -> Sessions.open(u);
                    // a named in-memory database is kept alive by its name: the user's, else one per project and
                    // connection (D5); each use opens a connection the caller closes
                    case Sessions.Opening.Named n -> Sessions.openNamed(n, n.name() != null ? n.name()
                            : "c_" + key.hex().substring(0, 16));
                    case Sessions.Opening.Held h -> Sessions.view(PROJECTS.getOrOpenSlowly(key, Sessions::closed,
                            () -> made(Sessions.openHeld(h), target, d.project())));
                };
            }
        };
    }

    /** {@code session} made into the project's database: the project's {@code made} steps run on it once (the
     *  statements that make and fill its tables from the Data rows) as a run of their own (THE WALL: a statement is
     *  sent only inside a plan's run), under the SEED mark; a failure closes it. */
    private static Sessions.Session made(Sessions.Session session, ExecutionPlan.Target target,
            ExecutionPlan.@com.legend.base.Nullable Project project) {
        if (project == null || project.made().isEmpty()) {
            return session;
        }
        return closedOnFailure(session, () -> {
            ExecutionPlan.Target filling = new ExecutionPlan.Target(target.database(), target.session(),
                    project.made());
            PlanRunner.setUp(filling.setup(), filling, marked(of(session), StatementOrigin.SEED), null);
        });
    }

    private static Sessions.Session setUpOpened(Sessions.Session session, ExecutionPlan.Target target) {
        return closedOnFailure(session, () -> establish(session, target));
    }

    /** {@code session} after {@code work} ran on it; closed, and the failure raised, where {@code work} fails. */
    private static Sessions.Session closedOnFailure(Sessions.Session session, Runnable work) {
        try {
            work.run();
            return session;
        } catch (RuntimeException | Error e) {
            try {
                session.close();
            } catch (com.legend.error.DataError closing) {
                e.addSuppressed(closing);
            }
            throw e;
        }
    }
}
