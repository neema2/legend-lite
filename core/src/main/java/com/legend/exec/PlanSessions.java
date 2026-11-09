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
     *  a write runs on a database of its own, a database the user names or a URL reaches keeps it). A closed set of
     *  exec's own (2026-11-09, the user's question "why do we have any callbacks?"): a caller names one, exec decides by
     *  its rule; until then a caller's function decided, inside the run, which session a statement went to. */
    public sealed interface Source permits Marked, Shared, Given, Acquired, Refusing, Lane {
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
        Source inner = sessions instanceof Marked m ? m.inner() : sessions;
        return new Marked(inner, mark);
    }

    private record Marked(Source inner, StatementOrigin mark) implements Source {
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
        return new Shared();
    }

    private record Shared() implements Source {
        @Override
        public Sessions.Session open(ExecutionPlan.Target target) {
            return openShared(target);
        }

        @Override
        public Sessions.Session openToChange(ExecutionPlan.Target target) {
            return openOwn(target);
        }
    }

    /** A source that opens nothing, refusing by name ({@code why}): a plan that sends no statement (a constant answer)
     *  runs on it. */
    public static Source refusing(String why) {
        return new Refusing(why);
    }

    private record Refusing(String why) implements Source {
        @Override
        public Sessions.Session open(ExecutionPlan.Target target) {
            throw new IllegalStateException(why);
        }
    }

    /**
     * A SERVER LANE'S SESSIONS (the PCT's, moved here from its harness as data, 2026-11-09): a target with setup on the
     * plan's own sessions ({@link #shared}: a server's database is the user's, so its own source refuses it by name); a
     * target whose session statements are {@code optionsOnly} -- the dialect's own options, run on any session again as
     * on a new one -- on {@code held}, the lane's held session, where it holds one; any other on a new owned session from
     * {@code fresh}, closed with the plan's use of it (a table of the session's own, the system metamodel store's, lives
     * only on a session of its own: round 14, 2026-11-03).
     */
    public static Source lane(Sessions.@com.legend.base.Nullable Session held, java.util.List<String> optionsOnly,
            Sessions.Opener fresh) {
        return new Lane(held, java.util.List.copyOf(optionsOnly), java.util.Objects.requireNonNull(fresh, "fresh"));
    }

    private record Lane(Sessions.@com.legend.base.Nullable Session held, java.util.List<String> optionsOnly,
            Sessions.Opener fresh) implements Source {
        @Override
        public Sessions.Session open(ExecutionPlan.Target target) {
            if (!target.loads().isEmpty()) {
                return openShared(target);
            }
            Sessions.Session h = held;
            return h != null && target.session().equals(optionsOnly) ? Sessions.view(h) : Sessions.open(fresh);
        }
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
            SystemDatabase.@com.legend.base.Nullable Leases system) {
        return new Given(session, trace, system);
    }

    private record Given(Sessions.Session session, @com.legend.base.Nullable ExecutionTrace trace,
            SystemDatabase.@com.legend.base.Nullable Leases system) implements Source {
        @Override
        public Sessions.Session open(ExecutionPlan.Target target) {
            var sys = system;
            return target.database() instanceof ExecutionPlan.Database.System s && sys != null
                    ? Sessions.view(sys.sessionFor(s), trace)
                    : Sessions.view(session, trace);
        }
    }

    /** {@link #of(Sessions.Session)} with {@code target}'s loads established on the session first, once
     *  ({@link ExecutionPlan.Load.Cadence#PER_ACQUISITION}: the session is acquired for the target): run INSIDE the
     *  plan's run, when the runner first opens a session of this source (THE WALL's rule 3, 2026-11-06), each through
     *  the one deliverer ({@link PlanRunner#establishOn}); a load whose rows need a bulk loader is refused by name there,
     *  before any statement, on a database that has none. Until L2 (2026-11-09) the target's {@code setup}. */
    public static Source acquired(Sessions.Session session, ExecutionPlan.Target target) {
        return new Acquired(session, target);
    }

    private static final class Acquired implements Source {
        private final Sessions.Session session;
        private final ExecutionPlan.Target target;
        private final Source source;
        // [0] established, [1] an establishing that failed: never run again on the same source (some of its steps ran)
        private final boolean[] state = {false, false};

        Acquired(Sessions.Session session, ExecutionPlan.Target target) {
            this.session = session;
            this.target = target;
            this.source = of(session);
        }

        @Override
        public Sessions.Session open(ExecutionPlan.Target t) {
            synchronized (state) {
                if (state[1]) {
                    throw new IllegalStateException("the session's loads failed on its first open, and are not run"
                            + " again (some of their steps ran)");
                }
                if (!state[0]) {
                    state[1] = true;
                    loads(session, target);
                    state[1] = false;
                    state[0] = true;
                }
            }
            return source.open(t);
        }
    }

    /** {@code target}'s loads established on {@code session} now, inside the run that acquired it; refused by name
     *  before any statement where a load's rows need a bulk loader the session's database has none of. */
    private static void loads(Sessions.Session session, ExecutionPlan.Target target) {
        loads(session, target, ExecutionPlan.Load.Cadence.PER_ACQUISITION);
    }

    /** {@code target}'s loads of {@code cadence} established on {@code session}: its acquisition's, or the project's
     *  Data where the platform makes its database ({@link #made}). */
    private static void loads(Sessions.Session session, ExecutionPlan.Target target,
            ExecutionPlan.Load.Cadence cadence) {
        java.util.List<ExecutionPlan.Load> loads = target.loads().stream().filter(l -> l.cadence() == cadence)
                .toList();
        for (ExecutionPlan.Load load : loads) {
            if (load.steps().stream().anyMatch(ExecutionPlan.LoadStep.Rows.class::isInstance)
                    && !Sessions.loadsRows(session)) {
                throw new IllegalStateException("the plan's load has rows for a bulk loader, and the session's"
                        + " database has none (" + Sessions.product(session) + ")");
            }
        }
        for (ExecutionPlan.Load load : loads) {
            PlanRunner.establishOn(load, session);
        }
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
                    refuseLoads(target, d, "a database its connector opens");
                    yield connector.open(d.connection());
                }
                yield switch (Sessions.openingFor(d.connection())) {
                    case Sessions.Opening.Held h -> Sessions.view(SHARED.getOrOpenSlowly(key, Sessions::closed,
                            () -> setUpOpened(Sessions.openHeld(h), target)));
                    case Sessions.Opening.Named n -> {
                        String usersName = n.name();
                        if (usersName != null) {
                            refuseLoads(target, d, "an in-memory database the user named ('" + usersName + "')");
                            yield Sessions.openNamed(n, usersName);
                        }
                        Named named = NAMED.getOrOpenSlowly(key, held -> Sessions.closed(held.keeper()), () -> {
                            String name = "plan_" + key.hex().substring(0, 16) + "_" + ATTEMPTS.incrementAndGet();
                            return new Named(setUpOpened(Sessions.openKept(n, name), target), name);
                        });
                        yield Sessions.joinKept(n, named.name());
                    }
                    case Sessions.Opening.Url u -> {
                        refuseLoads(target, d, "a database reached by a URL");
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

    private static void refuseLoads(ExecutionPlan.Target target, ExecutionPlan.Database.Declared d, String what) {
        if (!target.loads().isEmpty()) {
            throw new IllegalStateException("connection '" + d.connection().qualifiedName() + "' is " + what
                    + ", and the plan loads data into it: never run on it");
        }
    }

    /** The projects' own databases (C5), kept by their identity: made once, then reused. */
    private static final HandleStore<Sessions.Session> PROJECTS = new HandleStore<>();

    /** The server's session source: each plan's target opened as a project's own database ({@link #projectDatabase})
     *  -- a declared connection by {@code Sessions}' opening, the platform's engine for a runtime binding only model
     *  data. {@code ConnectionResolver.SOURCE} until slice 5 (2026-11-09): the source is the runtime's, named here, so
     *  the server names no runtime class. */
    public static final SessionSource PROJECT_DATABASES = new SessionSource.ProjectDatabases();

    /**
     * A PROJECT'S OWN DATABASE for {@code target} (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C5; the plan-as-data design's
     * hole 7, the user's go of 2026-11-08): found or made from the plan's target alone, never the model -- kept by the
     * database's identity (its connection and its project: named by what it holds, so an edited row is a fresh database
     * and two models whose stores differ never share one, type audit D100), made ONCE with the project's
     * {@code made} steps where this makes the database itself (a held in-memory one), kept for the process. An
     * unnamed in-memory database and the platform's are keyed apart by the project and filled with nothing (as the
     * server kept them); a URL's or a user-named database is the user's own and opens as it is. The per-query setup
     * is not run here: the door runs the target's setup on each query ({@code com.legend.exec.ProductRun.Establish}), as legend-engine
     * establishes a connection on every acquisition.
     */
    public static Sessions.Session projectDatabase(ExecutionPlan.Target target) {
        // a plan the server did not plan names no project: refused by name, never an empty database shared by every
        // model (the follow-up audit, 2026-11-09: since the project is the server's road's alone, TypedQuery.projectPlan)
        ExecutionPlan.Project project = switch (target.database()) {
            case ExecutionPlan.Database.Platform p -> p.project();
            case ExecutionPlan.Database.Declared d -> d.project();
            case ExecutionPlan.Database.System s -> null;
        };
        if (project == null && !(target.database() instanceof ExecutionPlan.Database.System)) {
            throw new IllegalArgumentException("the server's project databases run a plan the server planned "
                    + "(TypedQuery.projectPlan); this plan's target names no project: " + target.database());
        }
        // the database's identity: its connection and its project's key -- never the statements that make it, which
        // the key already names (the full audit, 2026-11-09: the whole fill was serialized and hashed on every open)
        ExecutionPlan.Target identity = new ExecutionPlan.Target(target.database(), java.util.List.of(),
                java.util.List.of());
        Hash key = Hash.ofUtf8(com.legend.executionplan.PlanJson.writeTarget(identity));
        return switch (target.database()) {
            case ExecutionPlan.Database.System s -> throw new IllegalStateException("a target naming the graph's system"
                    + " database on " + s.type() + " opens through its holder's sessions source, not a project's");
            case ExecutionPlan.Database.Platform p -> Sessions.view(PROJECTS.getOrOpenSlowly(key, Sessions::closed,
                    () -> made(Sessions.openPrivate(p.type()), target)));
            case ExecutionPlan.Database.Declared d -> {
                DatabaseConnector connector = Connectors.of(d.connection().databaseType());
                if (connector.ownSessions()) {
                    refuseFill(target, d, "a database its connector opens");
                    yield connector.open(d.connection());
                }
                yield switch (Sessions.openingFor(d.connection())) {
                    case Sessions.Opening.Url u -> {
                        refuseFill(target, d, "a database reached by a URL");
                        yield Sessions.open(u);
                    }
                    // a named in-memory database is kept alive by its name: the user's, else one per project and
                    // connection (D5), made once with the project's Data (its maker kept as the marker); each use
                    // opens a connection the caller closes
                    case Sessions.Opening.Named n -> {
                        if (n.name() != null) {
                            refuseFill(target, d, "an in-memory database the user named ('" + n.name() + "')");
                            yield Sessions.openNamed(n, n.name());
                        }
                        String name = "c_" + key.hex().substring(0, 16);
                        PROJECTS.getOrOpenSlowly(key, Sessions::closed, () -> made(Sessions.openNamed(n, name), target));
                        yield Sessions.openNamed(n, name);
                    }
                    case Sessions.Opening.Held h -> Sessions.view(PROJECTS.getOrOpenSlowly(key, Sessions::closed,
                            () -> made(Sessions.openHeld(h), target)));
                };
            }
        };
    }

    /** {@code session} made into the project's database: the project's refusal raised, then the target's ONCE loads
     *  (the project's Data rows, S145) run on it once, as a run of their own (THE WALL: a statement is sent only inside a
     *  plan's run), under the SEED mark; a failure closes it. Kept by the caller for the process; each query's
     *  PER_ACQUISITION loads run on the session it acquires ({@link #acquired}). */
    private static Sessions.Session made(Sessions.Session session, ExecutionPlan.Target target) {
        return closedOnFailure(session, () -> {
            refuseRefused(target);
            try (var run = com.legend.base.RunScope.enter()) {
                loads(session, target, ExecutionPlan.Load.Cadence.ONCE);
            }
        });
    }

    /** The project's refusal, raised where its database would be made (the model's Data rows cannot make it). */
    private static void refuseRefused(ExecutionPlan.Target target) {
        ExecutionPlan.Project project = switch (target.database()) {
            case ExecutionPlan.Database.Declared d -> d.project();
            case ExecutionPlan.Database.Platform p -> p.project();
            case ExecutionPlan.Database.System s -> null;
        };
        if (project != null && project.refused() != null) {
            throw new IllegalArgumentException(project.refused());
        }
    }

    /** A project's Data (a ONCE load), or its refusal, for a database the platform does not make: refused by name, never
     *  loaded into the user's own database nor skipped (the planner plans neither there: ConnectionSpecification's
     *  platformMakes, the one rule). */
    private static void refuseFill(ExecutionPlan.Target target, ExecutionPlan.Database.Declared d, String what) {
        boolean fills = target.loads().stream().anyMatch(l -> l.cadence() == ExecutionPlan.Load.Cadence.ONCE);
        if (fills || (d.project() != null && d.project().refused() != null)) {
            throw new IllegalStateException("connection '" + d.connection().qualifiedName() + "' is " + what
                    + ", and the plan fills it with its project's Data: never run on it");
        }
    }

    /** {@code session} with {@code target}'s loads established on it, as a run of their own; a load that fails closes
     *  the session it was opened for, the load's failure the one told. */
    private static Sessions.Session setUpOpened(Sessions.Session session, ExecutionPlan.Target target) {
        // a database the platform made for the target: the project's refusal, its Data (ONCE), then what each
        // acquisition establishes (the M1 tenets audit's finding 1: until then the ONCE loads were skipped here)
        return closedOnFailure(session, () -> {
            refuseRefused(target);
            loads(session, target, ExecutionPlan.Load.Cadence.ONCE);
            loads(session, target);
        });
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
