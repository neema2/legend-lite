// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.ExecutionPlan;

import java.util.Objects;

/**
 * Where a reading plan's session comes from, for the target its answer names. The door's {@code Execution.Source} until
 * slice 5 (2026-11-09): since hole 7 it takes the plan's target and nothing of the model, so it is the runtime's
 * contract, beside {@link PlanSessions.Source} (a plan's sessions for each statement's target).
 *
 * <p><strong>A closed set of data, which exec interprets</strong> (2026-11-09, the user's question "why do we have any
 * callbacks, esp dangerous ones that can break our tenet?"): until then a caller handed the run a function it called
 * for each target -- the caller's code deciding, inside the run, which session a statement goes to. Now a caller names
 * one of these and exec decides by its rule: a session the caller holds ({@link Given}), the server's project databases
 * ({@link ProjectDatabases}), a new session per use from the host's connection ({@link Fresh}), the plan's declared
 * connection by its connector ({@link Declared}), or none ({@link None}). (A held session for a target whose session
 * statements were a dialect's options alone, else a new one -- the system metamodel store's session tables, round 14
 * -- went with B4, 2026-11-11: a held session is {@link Given}.) The host's connection itself ({@link Sessions.Opener}) is a resource the
 * host owns, as a data source is: it opens a connection and decides nothing of the run.
 */
public sealed interface SessionSource
        permits SessionSource.Given, SessionSource.None, SessionSource.ProjectDatabases, SessionSource.Fresh,
        SessionSource.Declared {

    /** The session for {@code target}: a view where the source holds it, an owned one where it opens one for the use. */
    default Sessions.Session open(ExecutionPlan.Target target) {
        return switch (this) {
            case Given g -> Sessions.view(g.session());
            case ProjectDatabases p -> PlanSessions.projectDatabase(target);
            case Fresh f -> Sessions.open(f.connection());
            case Declared d -> {
                if (!(target.database() instanceof ExecutionPlan.Database.Declared declared)) {
                    throw new IllegalStateException("a source of declared connections, and the plan's answer names "
                            + "none: " + target.database());
                }
                yield Connectors.of(declared.connection().databaseType()).open(declared.connection());
            }
            case None n -> throw new IllegalStateException(n.why());
        };
    }

    /** {@code session} as the caller holds it: a non-closing view of it for every target (D68, 2026-10-30: the test
     *  runners hold sessions, not connections). It checks nothing at open: the runner checks each statement's target
     *  against the session it is sent on ({@code JdbcStatements.check}). */
    static SessionSource given(Sessions.Session session) {
        return new Given(session);
    }

    /** A session the caller holds, a view of it for every target. */
    record Given(Sessions.Session session) implements SessionSource {
        public Given {
            Objects.requireNonNull(session, "session");
        }
    }

    /** A source that opens nothing, refusing by name: for a run that must send no statement of its own. */
    record None(String why) implements SessionSource {
        public None {
            Objects.requireNonNull(why, "why");
        }
    }

    /** The server's project databases ({@link PlanSessions#projectDatabase}): legend-engine's execute's. */
    record ProjectDatabases() implements SessionSource {
    }

    /** A new session per use from the host's connection, owned, closed with the plan's use of it. */
    record Fresh(Sessions.Opener connection) implements SessionSource {
        public Fresh {
            Objects.requireNonNull(connection, "connection");
        }
    }

    /** The plan's declared connection, opened by its database's connector (a database whose sessions are its
     *  connector's: MongoDB's). */
    record Declared() implements SessionSource {
    }
}
