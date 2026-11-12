// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * What a body driver ESTABLISHED on a session, living exactly as long as the session (the owning session's own since
 * D70, 2026-10-30, shared by its views; until then a process-wide weak map keyed by the driver's connection, because a
 * connection was wrapped as a session anew at every call): the seeds it ran, by their SOURCES (a value key the driver builds: the from()'s
 * inline setups and the runtimes it names, never the rendered text, 2026-09-16), or a seed step's key; the session
 * statements each target ran on it; the run's registrations on the catalog. A writing statement, or a rollback, forgets
 * every key's seeds: the next establishment of each re-seeds; a rollback alone forgets a registration. Until track B's B1b (2026-10-29) this was the driver's own {@code ESTABLISHED} map,
 * keyed by the {@code java.sql.Connection} it held; the driver now holds a {@link Sessions.Session} and asks it.
 */
public final class SessionState {

    private final Set<Record> seeded = new HashSet<>();

    /** The run's REGISTRATIONS on the process's catalog in place on this session ({@code PER_TRANSACTION} loads: the
     *  model's rows inserted into the catalog's keyed tables, inside the run's turn), held apart from the seeds: no write
     *  undoes them -- a write is the user's, or a staged table's, never the catalog's own rows -- so they stand until the
     *  turn's transaction rolls back ({@link #rolledBack}). Until the B checkpoint's correctness audit (C1, 2026-11-11)
     *  they were seeds, which a write forgot: the next catalog step registered the model again, its rows inserted twice
     *  into keyed tables (a duplicate key). */
    private final Set<Record> registered = new HashSet<>();

    /** The targets' session statement lists that have run on this session, each once: the dialect's session setup is
     *  idempotent by contract ({@code SqlDialect.sessionSetup}), so a list runs before the connection's first
     *  statement for its target and not again (D66, 2026-10-30; until then the plan runner re-sent it before EVERY
     *  statement -- a round trip per statement on a server, seven on MariaDB). Cleared by a rollback
     *  ({@link #rolledBack}): Postgres's {@code SET} is transactional, so a list run inside a transaction that rolled
     *  back is undone with it, and the next statement runs it again. */
    private final Set<List<String>> sessionStatementsRun = new HashSet<>();

    /** Whether the session is in a run's TURN on the process's catalog (L4b): a transaction its holder rolls back when
     *  the run ends, in which a failed statement is rolled back at once ({@link #inTurn}). */
    private final java.util.concurrent.atomic.AtomicBoolean inTurn = new java.util.concurrent.atomic.AtomicBoolean();

    /** The error read-back the plan's target declared when it last checked this session (C2, 2026-11-12: MariaDB's and
     *  MySQL's, the charter's A3(c)): what the seam sends after a statement, or a bracket's commit, fails on it; null
     *  for a target whose database's errors carry a raise, or before any check. Plan data, never the connector's. */
    private volatile com.legend.executionplan.ExecutionPlan.@com.legend.base.Nullable RaiseReadBack raiseReadBack;

    /** A session no connection keys (a connector's own): its state is its own. */
    public SessionState() {
    }

    /** The read-back the target that checked this session declares ({@code JdbcStatements.check}). */
    void declareRaiseReadBack(com.legend.executionplan.ExecutionPlan.@com.legend.base.Nullable RaiseReadBack declared) {
        raiseReadBack = declared;
    }

    /** The read-back the session's last checked target declared; null for none. */
    com.legend.executionplan.ExecutionPlan.@com.legend.base.Nullable RaiseReadBack raiseReadBack() {
        return raiseReadBack;
    }

    /** Whether the seeds {@code key} names are in place: run on this session, and no write run since. */
    public synchronized boolean established(Record key) {
        return seeded.contains(key);
    }

    /** The seeds {@code key} names were just run on this session -- and no other key's are known to be in place: a
     *  seed drops and creates the tables it fills, and this record does not know which tables another key filled, so
     *  seeding A, then B over the same table, then A again must seed A again (the design audit's H1, 2026-11-02: the
     *  third seed was skipped and read B's rows). Two keys over disjoint tables re-seed in turn: a cost, never a wrong
     *  row. */
    public synchronized void establish(Record key) {
        seeded.clear();
        seeded.add(key);
    }

    /** A statement that WRITES is about to run on the session: every key's seeds establish again (a write may change
     *  any seeded table; until MRe one flag stood for them all, and establishing one key cleared it for every other --
     *  the MR inventory's latent defect). A registration stands ({@link #registered}). */
    public synchronized void markWriting() {
        seeded.clear();
    }

    /** Whether the registration {@code key} names is in place: run on this session in its transaction, and no rollback
     *  since. */
    public synchronized boolean registered(Record key) {
        return registered.contains(key);
    }

    /** The registration {@code key} names was just run on this session, inside its transaction. */
    public synchronized void register(Record key) {
        registered.add(key);
    }

    /** The database settings this session is known to have (the C checkpoint's SHOULD-FIX 3, 2026-11-12): written where
     *  this platform opened its database, or read back from it once ({@code JdbcStatements.check}). A database's
     *  settings are no transaction's, so a rollback keeps them. */
    private final Set<com.legend.executionplan.ExecutionPlan.DatabaseSettings> settingsHeld = new HashSet<>();

    /** {@code settings}, written where this session's database was opened or read back from it: held (none for null). */
    public synchronized void settingsHeld(com.legend.executionplan.ExecutionPlan.@com.legend.base.Nullable DatabaseSettings settings) {
        if (settings != null) {
            settingsHeld.add(settings);
        }
    }

    /** Whether this session is known to have {@code settings}: written where it was opened, or read back. */
    public synchronized boolean holdsSettings(com.legend.executionplan.ExecutionPlan.DatabaseSettings settings) {
        return settingsHeld.contains(settings);
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
     *  every target's session statements run again before its next statement; and seeds run inside it may be undone
     *  (a program's seed step inside its transaction, the test-is-the-product plan's M3 audit, B1), so the next
     *  establishment seeds again, as after a write. */
    public synchronized void rolledBack() {
        sessionStatementsRun.clear();
        seeded.clear();
        registered.clear();
    }

    /** A run's turn on the catalog began on this session ({@code Sessions.beginTurn}). */
    void turnBegan() {
        inTurn.set(true);
    }

    /** The run's turn ended ({@code Sessions.endTurn}). */
    void turnEnded() {
        inTurn.set(false);
    }

    /** Whether the session is in a run's turn on the catalog: a statement that fails there rolls the turn's
     *  transaction back at once, and the run's registrations establish again at its next step
     *  (docs/DATA_DELIVERY_DESIGN_2026_11_10.md §3 rule 3, A4: DuckDB aborts its whole transaction on an error, and
     *  refuses every statement after it until a rollback). */
    boolean inTurn() {
        return inTurn.get();
    }
}
