// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import java.sql.Connection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * What a body driver ESTABLISHED on a session, living exactly as long as the session's connection (a weak map keyed by
 * the connection, so a connection wrapped as a session any number of times -- the corpus harness's shared lane opens
 * one per suite -- keeps one state): the seeds it ran, by their SOURCES (a value key the driver builds: the from()'s
 * inline setups and the runtimes it names, never the rendered text, 2026-09-16); the database's reported column types
 * per statement text (leg 3.3, {@link WireTypes}); and whether a writing statement ran since the last seed (then the
 * next establishment re-seeds). Until track B's B1b (2026-10-29) this was the driver's own {@code ESTABLISHED} map,
 * keyed by the {@code java.sql.Connection} it held; the driver now holds a {@link Sessions.Session} and asks it.
 */
public final class SessionState {

    private static final Map<Connection, SessionState> OF =
            java.util.Collections.synchronizedMap(new java.util.WeakHashMap<>());

    /** The state of the session on {@code connection}, made on first ask. */
    static SessionState of(Connection connection) {
        return OF.computeIfAbsent(connection, c -> new SessionState());
    }

    /** the seed SOURCES established on the session (value keys) */
    private final Set<Record> seeded = new HashSet<>();

    /** the database's reported column types per prepared statement text */
    private final Map<String, List<WireTypes.ReportedColumn>> wireTypes = new HashMap<>();

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
}
