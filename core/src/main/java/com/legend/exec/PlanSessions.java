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
        return target -> Sessions.view(session, trace);
    }

    /** {@link #of(Sessions.Session)} with {@code target}'s setup run on the session first, once (the plan's seeds, the
     *  session statements: {@link PlanRunner#setUp}, under the SEED mark); a plan whose setup has rows for a bulk
     *  loader is refused by name on a database that has none. */
    public static Source setUp(Sessions.Session session, ExecutionPlan.Target target) {
        Source source = of(session);
        if (target.setup().stream().anyMatch(ExecutionPlan.SetupStep.Rows.class::isInstance)
                && !Sessions.loadsRows(session)) {
            throw new IllegalStateException("the plan's setup has rows for a bulk loader, and the session's"
                    + " database has none (" + Sessions.product(session) + ")");
        }
        try (var origin = StatementOrigin.enter(StatementOrigin.SEED)) {
            PlanRunner.setUp(target.setup(), target, source, null);
        }
        return source;
    }

    /** A session on the target's database kept by the target's content (its setup run once when opened): a view of a
     *  stored owned session, or an owned session of the target's own where the database keeps itself (the user's
     *  named in-memory database, a URL). */
    private static Sessions.Session openShared(ExecutionPlan.Target target) {
        Hash key = Hash.ofUtf8(com.legend.executionplan.PlanJson.writeTarget(target));
        return switch (target.database()) {
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
    private static Sessions.Session setUpOpened(Sessions.Session session, ExecutionPlan.Target target) {
        try {
            setUp(session, target);
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
