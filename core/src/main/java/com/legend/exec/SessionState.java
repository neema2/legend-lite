// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * What a body driver ESTABLISHED on a session, living exactly as long as the session (the owning session's own since
 * D70, 2026-10-30, shared by its views; until then a process-wide weak map keyed by the driver's connection, because a
 * connection was wrapped as a session anew at every call): the seeds it ran, by their SOURCES (a value key the driver builds: the from()'s
 * inline setups and the runtimes it names, never the rendered text, 2026-09-16); the database's reported column types
 * per statement text (leg 3.3, {@link WireTypes}); and whether a writing statement ran since the last seed (then the
 * next establishment re-seeds). Until track B's B1b (2026-10-29) this was the driver's own {@code ESTABLISHED} map,
 * keyed by the {@code java.sql.Connection} it held; the driver now holds a {@link Sessions.Session} and asks it.
 */
public final class SessionState {

    private final Set<Record> seeded = new HashSet<>();

    /** the database's reported column types per prepared statement text */
    private final Map<String, List<WireTypes.ReportedColumn>> wireTypes = new HashMap<>();

    /** The targets' session statement lists that have run on this session, each once: the dialect's session setup is
     *  idempotent by contract ({@code SqlDialect.sessionSetup}), so a list runs before the connection's first
     *  statement for its target and not again (D66, 2026-10-30; until then the plan runner re-sent it before EVERY
     *  statement -- a round trip per statement on a server, seven on MariaDB). Cleared by a rollback
     *  ({@link #rolledBack}): Postgres's {@code SET} is transactional, so a list run inside a transaction that rolled
     *  back is undone with it, and the next statement runs it again. */
    private final Set<List<String>> sessionStatementsRun = new HashSet<>();

    private boolean dirty;

    /** A session no connection keys (a connector's own): its state is its own. */
    public SessionState() {
    }

    /** Whether the seeds {@code key} names are in place: run on this session, and no write run since. */
    public synchronized boolean established(Record key) {
        return !dirty && seeded.contains(key);
    }

    /** The seeds {@code key} names were just run on this session. */
    public synchronized void establish(Record key) {
        seeded.add(key);
        dirty = false;
    }

    /** A statement that WRITES is about to run on the session: the next establishment re-seeds. */
    public synchronized void markWriting() {
        dirty = true;
    }

    /** The session's memo of the database's reported column types per prepared statement text. */
    public Map<String, List<WireTypes.ReportedColumn>> wireTypes() {
        return wireTypes;
    }

    /** Whether {@code statements}, a target's session statements, have still to run on this session: not run since
     *  the connection was opened or last rolled back. */
    public synchronized boolean needsSessionStatements(List<String> statements) {
        return !sessionStatementsRun.contains(statements);
    }

    /** {@code statements}, a target's session statements, all ran on this session just now. */
    public synchronized void sessionStatementsRan(List<String> statements) {
        sessionStatementsRun.add(List.copyOf(statements));
    }

    /** The session's transaction was rolled back: what a session statement set inside it may be undone (Postgres), so
     *  every target's session statements run again before its next statement. */
    public synchronized void rolledBack() {
        sessionStatementsRun.clear();
    }
}
