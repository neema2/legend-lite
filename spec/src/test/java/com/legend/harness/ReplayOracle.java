// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.harness;

import com.legend.exec.ExecutionResult;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * THE REPLAY ORACLE (SQLTEXT_ROW_VERDICT_CHARTER §8 slice 1): the ONE
 * named harness service owning the oracle SESSION machinery — the
 * family-mirror lifecycle, the seed-ledger application (incremental
 * cursor + poison), the fresh-replay fallback — and the verify
 * entry points that orchestrate golden-SQL row verification over it.
 * Before this class the machinery was statics spread across
 * Runner/H2Verify with the mirror-or-fresh session acquisition
 * duplicated four times. {@link H2Verify} keeps COMPARISON POLICY
 * only (the §6 normalization inventory, the census instruments).
 *
 * <p>Also the harness's implementation of the platform SPI
 * {@link com.legend.exec.SqlReplayOracle} ({@code INSTANCE}),
 * registered per run beside the AssertListener; the platform's
 * verdict arms (charter §8 slice 3) read it off ExecEnv.
 */
public final class ReplayOracle implements com.legend.exec.SqlReplayOracle {
    /** Row-verdict outcomes by SPI entry — DISPLAY only (the harness prints it). */
    public static final java.util.concurrent.ConcurrentHashMap<String,
            java.util.concurrent.atomic.LongAdder> OUTCOMES = new java.util.concurrent.ConcurrentHashMap<>();


    /** The raw-SQL ledger of the execution this referee serves (Phase 2b):
     * the harness creates it per test, hands it to the executor through
     * {@code ExecuteOptions}, and to this referee — the seeds the goldens
     * replay over. One referee per test; the mirror stays run-wide. */
    private final com.legend.sql.dialect.RawSqlBoundary.Recorder recorder;

    public ReplayOracle(com.legend.sql.dialect.RawSqlBoundary.Recorder recorder) {
        this.recorder = recorder;
    }

    private static final java.util.concurrent.atomic.AtomicInteger COUNTER =
            new java.util.concurrent.atomic.AtomicInteger();

    // =================================================================
    // INCREMENTAL FAMILY MIRROR (task #112 follow-up): the corpus runner
    // keeps ONE live H2 per family session and verification applies
    // only the ledger statements not yet mirrored — the fresh-replay
    // path re-ran the family's WHOLE history per verification (O(n^2)).
    // A statement H2 rejects POISONS the mirror for the rest of the
    // family: under fresh replay every later test re-hit the same
    // statement in its ledger prefix, so the decline set is identical.
    // =================================================================
    private static final class MirrorState {
        final Connection conn;
        int applied;
        @com.legend.Nullable String poison;
        boolean suspended;

        MirrorState(Connection conn) {
            this.conn = conn;
        }
    }

    private static @com.legend.Nullable MirrorState MIRROR;

    /** Install the family session's live mirror (runner-owned). */
    public static void mirrorBegin(Connection h2) {
        // F6.7: the engine's H2 extension functions register on the
        // MIRROR too — only the fresh-replay branch installed them, and
        // Runner makes the incremental mirror the DEFAULT path for
        // DuckDB sweeps, so golden SQL calling legend_h2_extension_*
        // declined verification on the path that actually runs (the C1
        // fix H2Verify exists for was not in effect).
        try (Statement st = h2.createStatement()) {
            for (String alias : H2ExtensionFunctions.aliases()) {
                st.execute(alias);
            }
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "h2 mirror extension registration failed", e);
        }
        MIRROR = new MirrorState(h2);
    }

    /** Detach the mirror (the runner closes the connection). */
    public static void mirrorEnd() {
        MIRROR = null;
    }

    /** A PRIVATE-session test's recording is not the family ledger —
     * its verification must use the fresh-replay path. */
    public static void mirrorSuspend(boolean suspended) {
        if (MIRROR != null) {
            MIRROR.suspended = suspended;
        }
    }

    // =================================================================
    // THE ATOMIC-ATTEMPT PROTOCOL (effectful cutover): one owner for
    // "execute an effect-bearing body against the family-session WORLD
    // — session connection + seed ledger + mirror — atomically". The
    // txn is CALLER policy on a connection the harness owns (the
    // platform stays policy-free: it executes on the connection state
    // it is handed). Today's caller is WholeTestFlip; at cutover the
    // call site moves to the runner and these verbs move with it —
    // the protocol survives as failure hygiene for the shared family
    // session either way.
    // =================================================================

    /** Open the attempt: transaction on, world position marked. */
    public com.legend.sql.dialect.RawSqlBoundary.Recorder.Mark
            beginAttempt(Connection conn) throws SQLException {
        conn.setAutoCommit(false);
        ATTEMPT_GOLDENS.clear();
        ATTEMPT_SQL_GOLDENS.clear();
        return recorder.mark();
    }

    private static String flat(String sql) {
        return sql.replace("\n", "").replace("\t", "");
    }

    /** The attempt's generator-fetch goldens by hop index (the walk's
     * TDG_GOLDENS, attempt-scoped): a chained hop's ancestor temps
     * materialize from these. Cleared per attempt. */
    private static final java.util.Map<Integer, String> ATTEMPT_GOLDENS =
            new java.util.HashMap<>();

    @Override
    public com.legend.exec.SqlReplayOracle.RowVerdict verifyFetchChain(
            Connection session, int hopIndex, String goldenSql,
            String ourSql,
            java.util.function.Supplier<com.legend.exec.SqlReplayOracle
                    .FetchTranscript> transcript) {
        com.legend.exec.SqlReplayOracle.RowVerdict v = verifyFetchChain0(session, hopIndex, goldenSql, ourSql, transcript);
        OUTCOMES.computeIfAbsent("verifyFetchChain " + v.outcome(), k -> new java.util.concurrent.atomic.LongAdder()).increment();
        return v;
    }

    public com.legend.exec.SqlReplayOracle.RowVerdict verifyFetchChain0(
            Connection session, int hopIndex, String goldenSql,
            String ourSql,
            java.util.function.Supplier<com.legend.exec.SqlReplayOracle
                    .FetchTranscript> transcript) {
        if (hopIndex >= 0) {
            ATTEMPT_GOLDENS.put(hopIndex, goldenSql);
        }
        if (!ourSql.contains("tdg_")) {
            return verifyFetchTexts(session, goldenSql, ourSql);
        }
        try {
            com.legend.exec.SqlReplayOracle.FetchTranscript r =
                    transcript.get();
            List<com.legend.exec.SqlReplayOracle.FetchHop> fetches =
                    r.hops();
            if (hopIndex < 0 || hopIndex >= fetches.size()) {
                throw new H2Verify.Unverifiable(
                        "chained fetch — transcript index out of range", null);
            }
            // the receipt compares under the corpus's own flattening
            // (sqlRemoveFormatting strips newlines and tabs): the
            // H2Compatible spelling hands the arm the flattened hop text
            if (!flat(r.sqls().get(hopIndex)).equals(flat(ourSql))) {
                throw new H2Verify.Unverifiable("chained fetch — transcript"
                        + " text mismatch (determinism receipt failed)", null);
            }
            com.legend.exec.SqlReplayOracle.FetchHop f =
                    fetches.get(hopIndex);
            if (f.parentIndex() < 0) {
                throw new H2Verify.Unverifiable("chained fetch — view fetch"
                        + " over temps (no single-parent chain)", null);
            }
            List<Integer> chain = new java.util.ArrayList<>();
            for (int j = f.parentIndex(); j >= 0;
                    j = fetches.get(j).parentIndex()) {
                chain.add(0, j);
            }
            List<String[]> ancestors = new java.util.ArrayList<>();
            for (int j : chain) {
                String gj = ATTEMPT_GOLDENS.get(j);
                if (gj == null) {
                    throw new H2Verify.Unverifiable("chained fetch — ancestor"
                            + " golden (index " + j + ") not asserted before"
                            + " this hop", null);
                }
                ancestors.add(new String[]{
                        "testDataGen_Temp_" + fetches.get(j).table(), gj});
            }
            String parentTemp = "testDataGen_Temp_"
                    + fetches.get(f.parentIndex()).table();
            if (!goldenSql.toLowerCase().contains(parentTemp.toLowerCase())) {
                throw new H2Verify.Unverifiable("chained fetch — golden does"
                        + " not reference derived parent temp " + parentTemp,
                        null);
            }
            String d = tdgChainedReplay(
                    recorder.seeds(),
                    ancestors, goldenSql,
                    H2Verify.transcriptRows(f.columns(), f.rows()));
            return d == null
                    ? com.legend.exec.SqlReplayOracle.RowVerdict.match()
                    : com.legend.exec.SqlReplayOracle.RowVerdict
                            .diverged(d);
        } catch (H2Verify.Unverifiable u) {
            return outcome("verdict-arm-tdg: ", u);
        }
    }

    /** Commit the attempt: the world advances (ledger entries stand,
     * mirror cursor position stays honest — it applied real state). */
    public static void commitAttempt(Connection conn) throws SQLException {
        conn.commit();
        conn.setAutoCommit(true);
    }

    /** Roll the WORLD back to the mark — the one invariant of the
     * failure path: session transaction rolled back, ledger truncated
     * to the mark (unrecordLast, range edition), and a mirror whose
     * cursor ran ahead mid-body DETACHED (H2 cannot roll back; the
     * family's remaining verifies ride the fresh-replay path over the
     * truncated ledger — correct, slower, failure-path only; the
     * runner still owns and closes the connection). Returns true when
     * a detach happened (the caller censuses it). Throws when the
     * session rollback itself fails — world state unknown, the caller
     * must stay LOUD. */
    public boolean rollbackAttempt(Connection conn,
            com.legend.sql.dialect.RawSqlBoundary.Recorder.Mark mark)
            throws SQLException {
        conn.rollback();
        conn.setAutoCommit(true);
        recorder.truncateTo(mark);
        MirrorState mirror = MIRROR;
        // the mirror's cursor and the mark are both in SEED space (batch 132)
        if (mirror != null && mirror.applied > mark.seeds()) {
            MIRROR = null;
            return true;
        }
        return false;
    }

    /** Work to run on a seeded oracle {@link Statement}. */
    interface Work<T> {
        T run(Statement st) throws SQLException;
    }

    /** A session policy: the fresh in-memory database name and the
     * FAILURE SPELLINGS — decline reasons are census keys, preserved
     * verbatim from the four pre-extraction sites. A null
     * {@code seedFailPrefix} lets a fresh-path seed failure ride the
     * outer {@code freshFailPrefix} catch (the TDG spelling). */
    record Session(String freshDbName,
            @com.legend.Nullable String seedFailPrefix,
            String mirrorFailPrefix, String freshFailPrefix) {
    }

    static final Session VERIFY_SESSION = new Session(
            "advisory", "seed replay: ", "h2 connection: ",
            "h2 connection: ");

    static final Session TDG_SESSION = new Session(
            "tdgreplay", null, "golden replay: ", "golden fresh replay: ");

    /**
     * THE ONE oracle-session acquisition (previously duplicated across
     * verify/freshVerify/tdgSqlReplay/tdgChainedReplay): a live family
     * mirror (not suspended) gets the INCREMENTAL ledger replay and
     * poison discipline; otherwise a fresh in-memory H2 replays the
     * full recorded history (extensions + seeds). {@code work} runs on
     * the seeded statement either way.
     */
    static <T> T onOracle(java.util.@com.legend.Nullable List<String> seeds,
            Session session, Work<T> work) {
        MirrorState mirror = MIRROR;
        if (mirror != null && !mirror.suspended) {
            if (mirror.poison != null) {
                throw new H2Verify.Unverifiable(mirror.poison, null, true);
            }
            try (Statement st = mirror.conn.createStatement()) {
                applyPendingSeeds(mirror, st, seeds);
                return work.run(st);
            } catch (SQLException e) {
                throw new H2Verify.Unverifiable(session.mirrorFailPrefix()
                        + e.getMessage(), e);
            }
        }
        int id = COUNTER.getAndIncrement();
        try (Connection h2 = DriverManager.getConnection(
                "jdbc:h2:mem:" + session.freshDbName() + id
                        + H2Verify.SETTINGS, "sa", "");
                Statement st = h2.createStatement()) {
            for (String alias : H2ExtensionFunctions.aliases()) {
                st.execute(alias);
            }
            for (String seed : seeds == null ? List.<String>of() : seeds) {
                for (String one : seed.split(";\\s*\n|;\\s*$")) {
                    if (one.isBlank()) {
                        continue;
                    }
                    if (session.seedFailPrefix() == null) {
                        st.execute(one);
                    } else {
                        try {
                            st.execute(one);
                        } catch (SQLException e) {
                            throw new H2Verify.Unverifiable(
                                    session.seedFailPrefix()
                                            + e.getMessage(), e, true);
                        }
                    }
                }
            }
            return work.run(st);
        } catch (SQLException e) {
            throw new H2Verify.Unverifiable(session.freshFailPrefix()
                    + e.getMessage(), e);
        }
    }

    /** The mirror's incremental seed replay (verify + the TDG replay
     * share it — never a twin). */
    private static void applyPendingSeeds(MirrorState mirror, Statement st,
            java.util.@com.legend.Nullable List<String> seeds) {
        List<String> ledger = seeds == null ? List.of() : seeds;
        while (mirror.applied < ledger.size()) {
            String seed = ledger.get(mirror.applied);
            for (String one : seed.split(";\\s*\n|;\\s*$")) {
                if (one.isBlank()) {
                    continue;
                }
                try {
                    st.execute(one);
                } catch (SQLException e) {
                    mirror.poison = "seed replay: " + e.getMessage();
                    throw new H2Verify.Unverifiable(mirror.poison, e, true);
                }
            }
            mirror.applied++;
        }
    }

    // =================================================================
    // VERIFY ENTRY POINTS (moved from H2Verify; comparison policy —
    // compareFrame and the §6 inventory — stays there)
    // =================================================================

    public static @com.legend.Nullable String verify(
            java.util.@com.legend.Nullable List<String> seeds,
            java.util.@com.legend.Nullable List<String> extraSeeds,
            String goldenSql,
            ExecutionResult ours,
            java.util.Map<Integer, java.util.Map<String, String>> enumDecode,
            java.util.function.Function<String, java.util.Map<String, String>> graphEnumProp,
            com.legend.exec.SqlReplayOracle.ReplayFacts facts) {
        if (!H2Verify.ready()) {
            throw new H2Verify.Unverifiable("h2 driver not on classpath",
                    null);
        }
        H2Verify.enumPrecheck(ours, enumDecode);
        MirrorState mirror = MIRROR;
        if (mirror != null && !mirror.suspended) {
            // INCREMENTAL path: extras run OUTSIDE the cursor (§9a) —
            // failures decline THIS verify only, never poison the mirror
            return onOracle(seeds, VERIFY_SESSION, st -> {
                for (String x : extraSeeds == null ? List.<String>of()
                        : extraSeeds) {
                    try {
                        st.execute(x);
                    } catch (SQLException e) {
                        throw new H2Verify.Unverifiable("seed replay: "
                                + e.getMessage(), e);
                    }
                }
                return H2Verify.compareFrame(st, goldenSql, ours, enumDecode,
                        graphEnumProp, facts);
            });
        }
        // FRESH path: per-verify extras append to the replayed history
        java.util.List<String> all = seeds;
        if (extraSeeds != null && !extraSeeds.isEmpty()) {
            all = new java.util.ArrayList<>(seeds == null
                    ? List.of() : seeds);
            all.addAll(extraSeeds);
        }
        java.util.List<String> allSeeds = all;
        return onOracle(allSeeds, VERIFY_SESSION,
                st -> H2Verify.compareFrame(st, goldenSql, ours, enumDecode,
                        graphEnumProp, facts));
    }

    /** Route by the session backend: an H2 session verifies DIRECTLY
     * (the database already holds every table the test built —
     * model-driven DDL included, which the seed-replay oracle can miss
     * as "Table not found"); anything else replays the recorded seeds
     * into the oracle. {@code extraSeeds} (§9a cursor fix, 2026-08-30):
     * PER-VERIFY synthesized statements (tempTableForIn derivations)
     * that must NEVER advance the family mirror's incremental cursor. */
    public static @com.legend.Nullable String verifyAuto(Connection session,
            java.util.@com.legend.Nullable List<String> seeds,
            java.util.@com.legend.Nullable List<String> extraSeeds,
            String goldenSql, ExecutionResult ours,
            java.util.Map<Integer, java.util.Map<String, String>> enumDecode,
            java.util.function.Function<String, java.util.Map<String, String>> graphEnumProp,
            com.legend.exec.SqlReplayOracle.ReplayFacts facts)
            throws SQLException {
        return "H2".equals(session.getMetaData().getDatabaseProductName())
                ? verifyOnSession(session, goldenSql, ours, enumDecode,
                        graphEnumProp, facts)
                : verify(seeds, extraSeeds, goldenSql, ours, enumDecode,
                        graphEnumProp, facts);
    }

    /**
     * The SESSION-direct golden verify (parity workstream 1): the
     * golden SELECT runs read-only on the session connection and
     * compares against our rows exactly like the replay path.
     */
    public static @com.legend.Nullable String verifyOnSession(
            Connection session, String goldenSql, ExecutionResult ours,
            java.util.Map<Integer, java.util.Map<String, String>> enumDecode,
            java.util.function.Function<String, java.util.Map<String, String>> graphEnumProp,
            com.legend.exec.SqlReplayOracle.ReplayFacts facts) {
        H2Verify.enumPrecheck(ours, enumDecode);
        try (Statement st = session.createStatement()) {
            return H2Verify.compareFrame(st, goldenSql, ours, enumDecode,
                    graphEnumProp, facts);
        } catch (SQLException e) {
            throw new H2Verify.Unverifiable("session golden execution: "
                    + e.getMessage(), e);
        }
    }

    /** TDG 49er replay (docs/TDG_LANE_CHARTER.md residue): BOTH sides
     * are FETCH texts — the GOLDEN rides the ordinary family-mirror H2
     * route (pending seeds applied), OURS runs on the ambient DuckDB;
     * rows compare ORDER-INSENSITIVELY under the shared cell canon
     * (two-sided comparison policy: neither side's incidental row order
     * is contract — the fetches carry no ORDER BY). null = VERIFIED
     * match; text = REAL divergence; Unverifiable = the caller's
     * counted decline. */
    public static @com.legend.Nullable String tdgSqlReplay(
            java.util.@com.legend.Nullable List<String> seeds,
            String goldenSql, Connection duck, String ourSql) {
        if (!H2Verify.ready()) {
            throw new H2Verify.Unverifiable("h2 driver not on classpath",
                    null);
        }
        // ORDER-INSENSITIVE compare is GATED on a compile-time fact
        // (harness discipline C2.3): generator fetches carry no ORDER BY
        // on either side — an ordered text names its own decline
        if (goldenSql.toLowerCase().contains("order by")
                || ourSql.toLowerCase().contains("order by")) {
            throw new H2Verify.Unverifiable("ordered fetch — multiset"
                    + " compare not applicable", null);
        }
        // CHAINED fetches join the generator's tdg_N_* temp tables —
        // dropped in its finally, unreplayable standalone (sequence
        // replay = unmodeled residue, named)
        if (ourSql.contains("tdg_") || goldenSql.contains("tdg_")) {
            throw new H2Verify.Unverifiable("chained fetch — generator"
                    + " temp tables not replayable", null);
        }
        List<String> ourRows;
        // a DUPLICATE connection (the DuckWorkspaces idiom): the ambient
        // connection may hold an open streaming result mid-walk. The
        // duplicate starts OUTSIDE the test's workspace catalog — point
        // it back (getCatalog is metadata, safe mid-stream).
        // TXN-AWARE (transactional flip attempt): a duplicate is a
        // SEPARATE snapshot-isolated transaction context — the attempt's
        // uncommitted writes are invisible there. When the session
        // carries an open transaction (autoCommit off = a flip attempt;
        // the walk never does this) the read runs on the session
        // connection itself, inside its own transaction.
        try {
            if (!duck.getAutoCommit()) {
                try (Statement st = duck.createStatement()) {
                    ourRows = H2Verify.rawRows(st, ourSql);
                }
            } else {
                try (Connection dup = duck.unwrap(
                                org.duckdb.DuckDBConnection.class).duplicate();
                        Statement st = dup.createStatement()) {
                    String ws = duck.getCatalog();
                    if (ws != null && !ws.isBlank()) {
                        st.execute("USE " + ws);
                    }
                    ourRows = H2Verify.rawRows(st, ourSql);
                }
            }
        } catch (SQLException e) {
            throw new H2Verify.Unverifiable("our-side replay: "
                    + e.getMessage(), e);
        }
        List<String> golden = onOracle(seeds, TDG_SESSION,
                st -> H2Verify.rawRows(st, goldenSql));
        return H2Verify.multisetCompare(golden, ourRows);
    }

    /** The CHAINED-fetch golden replay (live-session refereeing, census
     * §10o leg 1): a chained golden references its parent's
     * {@code testDataGen_Temp_*} table — an ENGINE-session artifact the
     * mirror does not hold. The engine's own mechanics (testDataGeneration
     * .pure chained arm) fill that temp with the PARENT fetch's rows,
     * i.e. the parent GOLDEN's result — so the synthesis materializes
     * each ancestor temp on the mirror FROM ITS OWN GOLDEN, root-first,
     * then executes this hop's golden and referees against our
     * live-session transcript rows. Golden-side only — fully
     * independent of our side. {@code ancestors} = {tempName, goldenSql}
     * pairs root-first. */
    public static @com.legend.Nullable String tdgChainedReplay(
            java.util.@com.legend.Nullable List<String> seeds,
            List<String[]> ancestors, String goldenSql,
            List<String> oursRows) {
        if (!H2Verify.ready()) {
            throw new H2Verify.Unverifiable("h2 driver not on classpath",
                    null);
        }
        List<String> golden = onOracle(seeds, TDG_SESSION,
                st -> goldenWithTemps(st, ancestors, goldenSql));
        return H2Verify.multisetCompare(golden, oursRows);
    }

    /** Materialize the ancestor temps (root-first; a self-join chain
     * reuses the engine's per-root temp NAME, so a repeat materializes
     * through a stage swap — the stage reads the OLD temp before it
     * drops, exactly the engine's sequential create/insert/drop
     * discipline), execute the hop's golden, and ALWAYS drop what was
     * created — the family mirror is shared state. */
    private static List<String> goldenWithTemps(Statement st,
            List<String[]> ancestors, String goldenSql)
            throws SQLException {
        java.util.LinkedHashSet<String> created =
                new java.util.LinkedHashSet<>();
        try {
            for (String[] a : ancestors) {
                String name = a[0];
                String sql = a[1];
                if (created.contains(name)) {
                    st.execute("create table TDG_TEMP_STAGE as " + sql);
                    st.execute("drop table " + name);
                    st.execute("alter table TDG_TEMP_STAGE rename to "
                            + name);
                } else {
                    st.execute("drop table if exists " + name);
                    st.execute("create table " + name + " as " + sql);
                    created.add(name);
                }
            }
            return H2Verify.rawRows(st, goldenSql);
        } finally {
            for (String name : created) {
                try {
                    st.execute("drop table if exists " + name);
                } catch (SQLException ignored) {
                    // cleanup best-effort; the next use drops-if-exists
                }
            }
            try {
                st.execute("drop table if exists TDG_TEMP_STAGE");
            } catch (SQLException ignored) {
                // same
            }
        }
    }

    // =================================================================
    // THE PLATFORM SPI (SQLTEXT charter §2)
    // =================================================================

    /** The row verdict (charter §3.5d-6): golden replay + the §6/§7
     * comparison policy, outcomes as DATA. The enum decode derives
     * from the ARM-supplied mapping/class facts (the producer's own
     * children — never AST re-discovery here). Declines COUNT through
     * the one funnel ({@link H2Verify#decline}). */
    @Override
    public com.legend.exec.SqlReplayOracle.RowVerdict verify(
            java.sql.Connection session, String goldenSql,
            ExecutionResult ours,
            @com.legend.Nullable String mappingFqn,
            @com.legend.Nullable String rootClassFqn,
            com.legend.exec.SqlReplayOracle.ReplayFacts facts,
            com.legend.compiler.element.ModelContext ctx) {
        com.legend.exec.SqlReplayOracle.RowVerdict v = verify0(session, goldenSql, ours, mappingFqn, rootClassFqn, facts, ctx);
        OUTCOMES.computeIfAbsent("verify " + v.outcome(), k -> new java.util.concurrent.atomic.LongAdder()).increment();
        return v;
    }

    public com.legend.exec.SqlReplayOracle.RowVerdict verify0(
            java.sql.Connection session, String goldenSql,
            ExecutionResult ours,
            @com.legend.Nullable String mappingFqn,
            @com.legend.Nullable String rootClassFqn,
            com.legend.exec.SqlReplayOracle.ReplayFacts facts,
            com.legend.compiler.element.ModelContext ctx) {
        // the STATIC extent-subset fact of the verified chain (computed on
        // the platform's typed chain — a class extent through subset-
        // preserving ops) arms the graph compare's pk-collapse exactly as
        // the old runner's walk lane armed it (deleted in batch 115)
        return verify(session, goldenSql, ours, mappingFqn, rootClassFqn,
                facts, ctx, List.of());
    }

    /** The engine-session temp tables of a golden, materialized on the
     * oracle for this verify (batch 65 — the walk's literalTempSeeds
     * behind the SPI): PER-VERIFY statements, never the mirror's cursor
     * (verifyAuto's extraSeeds). */
    @Override
    public com.legend.exec.SqlReplayOracle.RowVerdict verify(
            java.sql.Connection session, String goldenSql,
            ExecutionResult ours,
            @com.legend.Nullable String mappingFqn,
            @com.legend.Nullable String rootClassFqn,
            com.legend.exec.SqlReplayOracle.ReplayFacts facts,
            com.legend.compiler.element.ModelContext ctx,
            List<com.legend.exec.SqlReplayOracle.TempTable> temps) {
        com.legend.exec.SqlReplayOracle.RowVerdict v = verify1(session, goldenSql, ours, mappingFqn, rootClassFqn, facts, ctx, temps);
        OUTCOMES.computeIfAbsent("verify8 " + v.outcome(), k -> new java.util.concurrent.atomic.LongAdder()).increment();
        return v;
    }

    public com.legend.exec.SqlReplayOracle.RowVerdict verify1(
            java.sql.Connection session, String goldenSql,
            ExecutionResult ours,
            @com.legend.Nullable String mappingFqn,
            @com.legend.Nullable String rootClassFqn,
            com.legend.exec.SqlReplayOracle.ReplayFacts facts,
            com.legend.compiler.element.ModelContext ctx,
            List<com.legend.exec.SqlReplayOracle.TempTable> temps) {
        try {
            List<String> seeds = tempSeeds(temps);
            // a POPULATION temp (batch 67): the engine's two-statement
            // in-list plan fills tempTableForIn_<let> with its population
            // statement's rows — the attempt's most recent `select
            // distinct` golden, remembered below
            for (var t : temps) {
                if (t.kind().startsWith("population")) {
                    String pop = null;
                    for (int i = ATTEMPT_SQL_GOLDENS.size() - 1; i >= 0; i--) {
                        if (ATTEMPT_SQL_GOLDENS.get(i).toLowerCase(java.util.Locale.ROOT)
                                .startsWith("select distinct")) {
                            pop = ATTEMPT_SQL_GOLDENS.get(i);
                            break;
                        }
                    }
                    if (pop == null) {
                        throw new H2Verify.Unverifiable("population temp "
                                + t.name() + ": no population golden asserted"
                                + " before this statement", null);
                    }
                    seeds = new java.util.ArrayList<>(seeds == null ? List.of() : seeds);
                    seeds.add("DROP TABLE IF EXISTS " + t.name());
                    seeds.add("CREATE LOCAL TEMPORARY TABLE " + t.name()
                            + " (ColumnForStoringInCollection "
                            + (t.kind().endsWith("integer") ? "BIGINT" : "VARCHAR(1024)")
                            + ")");
                    seeds.add("INSERT INTO " + t.name() + " " + pop);
                }
            }
            ATTEMPT_SQL_GOLDENS.add(goldenSql);
            return verifyArmed(session, goldenSql, ours, mappingFqn,
                    rootClassFqn, ctx, seeds, facts);
        } catch (H2Verify.Unverifiable u) {
            return outcome("verdict-arm: ", u);
        } finally {
        }
    }

    /** The ONE funnel from a referee refusal to an outcome (Phase 0.6): a
     * modeled GAP is a counted DECLINE (the arm's text policy applies); a
     * FAULT of the referee's own machinery is counted apart and never
     * lets the text stand in. No broad catch remains in the referee — a
     * bug in the compare propagates and fails the test. */
    private static com.legend.exec.SqlReplayOracle.RowVerdict outcome(String arm,
            H2Verify.Unverifiable u) {
        String m = arm + u.getMessage();
        if (u.fault()) {
            H2Verify.fault(m);
            return com.legend.exec.SqlReplayOracle.RowVerdict.fault(String.valueOf(u.getMessage()));
        }
        H2Verify.decline(m);
        return com.legend.exec.SqlReplayOracle.RowVerdict.declined(String.valueOf(u.getMessage()));
    }

    /** Every golden verified in the attempt, in order — a later
     * statement's population temp fills from the earlier one. Cleared
     * per attempt. */
    private static final List<String> ATTEMPT_SQL_GOLDENS =
            new java.util.ArrayList<>();

    /** The golden PLAN replay (batch 66): {@link PlanReplay} runs the
     * plan's nodes in order on this oracle (Allocation values fetched
     * here, template operations evaluated by the engine's published
     * bodies) and the final node's filled SQL replays for rows. */
    @Override
    public com.legend.exec.SqlReplayOracle.RowVerdict verifyPlan(
            Connection session, String goldenPlan,
            java.util.Map<String, List<String>> bindings,
            ExecutionResult ours,
            @com.legend.Nullable String mappingFqn,
            @com.legend.Nullable String rootClassFqn,
            com.legend.exec.SqlReplayOracle.ReplayFacts facts,
            com.legend.compiler.element.ModelContext ctx) {
        com.legend.exec.SqlReplayOracle.RowVerdict v = verifyPlan0(session, goldenPlan, bindings, ours, mappingFqn, rootClassFqn, facts, ctx);
        OUTCOMES.computeIfAbsent("verifyPlan " + v.outcome(), k -> new java.util.concurrent.atomic.LongAdder()).increment();
        return v;
    }

    public com.legend.exec.SqlReplayOracle.RowVerdict verifyPlan0(
            Connection session, String goldenPlan,
            java.util.Map<String, List<String>> bindings,
            ExecutionResult ours,
            @com.legend.Nullable String mappingFqn,
            @com.legend.Nullable String rootClassFqn,
            com.legend.exec.SqlReplayOracle.ReplayFacts facts,
            com.legend.compiler.element.ModelContext ctx) {
        // allocation tables materialized on the oracle for THIS replay —
        // dropped after the verdict (the family mirror is shared state)
        List<String> allocTables = new java.util.ArrayList<>();
        try {
            String sql = PlanReplay.finalSql(goldenPlan, bindings, s -> {
                try {
                    return rows(s);
                } catch (SQLException e) {
                    throw new H2Verify.Unverifiable("plan-text: allocation"
                            + " node replay: " + e.getMessage(), e);
                }
            }, (name, allocSql, labels) -> {
                // the placeholder's columns read BARE downstream
                // (`"tdsvar_0_1".eID`): the table is created with bare
                // names so the oracle folds them exactly as the engine's
                // realized relation does; a non-identifier label keeps
                // its quotes. Column METADATA only — the rows stay in the
                // database.
                StringBuilder cols = new StringBuilder();
                for (String l : labels) {
                    cols.append(cols.length() > 0 ? ", " : "")
                            .append(l.matches("[A-Za-z_][A-Za-z0-9_]*")
                                    ? l : '"' + l + '"');
                }
                try {
                    execute("drop table if exists " + name);
                    execute("create table " + name + "(" + cols + ") as "
                            + allocSql);
                } catch (SQLException e) {
                    throw new H2Verify.Unverifiable("plan-text: allocation"
                            + " table '" + name + "': " + e.getMessage(), e);
                }
                allocTables.add(name);
                return "select * from " + name;
            });
            return verifyArmed(session, sql, ours, mappingFqn, rootClassFqn,
                    ctx, null, facts);
        } catch (H2Verify.Unverifiable u) {
            return outcome("verdict-arm-plan: ", u);
        } finally {
            for (String name : allocTables) {
                try {
                    execute("drop table if exists " + name);
                } catch (SQLException ignored) {
                    // cleanup best-effort; the next replay drops-if-exists
                }
            }
        }
    }

    /** H2 statements creating a golden's temp tables from their Pure
     * literal values: drop-first (re-runnable on the live mirror), the
     * engine's own column name, the literal kind's H2 type. */
    static @com.legend.Nullable List<String> tempSeeds(
            List<com.legend.exec.SqlReplayOracle.TempTable> temps) {
        if (temps.isEmpty()) {
            return null;
        }
        List<String> out = new java.util.ArrayList<>();
        for (var t : temps) {
            String colType = switch (t.kind()) {
                case "date" -> "DATE";
                case "datetime" -> "TIMESTAMP";
                case "integer" -> "BIGINT";
                default -> "VARCHAR(1024)";
            };
            out.add("DROP TABLE IF EXISTS " + t.name());
            out.add("CREATE LOCAL TEMPORARY TABLE " + t.name()
                    + " (ColumnForStoringInCollection " + colType + ")");
            for (String v : t.values()) {
                String lit = switch (t.kind()) {
                    case "date" -> "DATE '" + v + "'";
                    // pure date literals are UTC; H2's TIMESTAMP parser
                    // takes the bare form
                    case "datetime" -> "TIMESTAMP '"
                            + v.replaceAll("(\\+0000|Z)$", "").replace('T', ' ')
                            + "'";
                    case "integer" -> v;
                    default -> "'" + v.replace("'", "''") + "'";
                };
                out.add("INSERT INTO " + t.name() + " VALUES (" + lit + ")");
            }
        }
        return out;
    }

    private com.legend.exec.SqlReplayOracle.RowVerdict verifyArmed(
            java.sql.Connection session, String goldenSql,
            ExecutionResult ours,
            @com.legend.Nullable String mappingFqn,
            @com.legend.Nullable String rootClassFqn,
            com.legend.compiler.element.ModelContext ctx,
            @com.legend.Nullable List<String> extraSeeds,
            com.legend.exec.SqlReplayOracle.ReplayFacts facts) {
        java.util.Map<Integer, java.util.Map<String, String>> enumDecode =
                new java.util.LinkedHashMap<>();
        if (mappingFqn != null
                && !(ours instanceof ExecutionResult.Graph)) {
            for (int i = 0; i < ours.columns().size(); i++) {
                if (ours.columns().get(i).pureType()
                        instanceof com.legend.compiler.element.type.Type
                                .EnumType et) {
                    var dec = H2Verify.decodeOf(ctx, mappingFqn, et.fqn());
                    if (dec != null) {
                        enumDecode.put(i, dec);
                    }
                }
            }
        }
        java.util.function.Function<String,
                java.util.Map<String, String>> enumProp = key -> {
            if (mappingFqn == null || rootClassFqn == null) {
                return null;
            }
            var prop = ctx.findProperty(rootClassFqn, key);
            if (prop.isEmpty() || !(prop.get().type()
                    instanceof com.legend.compiler.element.type.Type
                            .EnumType et)) {
                return null;
            }
            var dec = H2Verify.decodeOf(ctx, mappingFqn, et.fqn());
            return dec == null ? java.util.Map.of() : dec;
        };
        try {
            String r = verifyAuto(session,
                    recorder.seeds(),
                    extraSeeds, goldenSql, ours, enumDecode, enumProp, facts);
            return r == null
                    ? com.legend.exec.SqlReplayOracle.RowVerdict.match()
                    : com.legend.exec.SqlReplayOracle.RowVerdict
                            .diverged(r);
        } catch (H2Verify.Unverifiable u) {
            return outcome("verdict-arm: ", u);
        } catch (java.sql.SQLException e) {
            // the session itself failed us (metadata, statement): a FAULT
            String m = "verdict-arm: " + String.valueOf(e.getMessage()).replace('\n', ' ');
            H2Verify.fault(m);
            return com.legend.exec.SqlReplayOracle.RowVerdict.fault(m);
        }
    }

    /** The TDG fetch-text verdict (SPI): the walk's tdgSqlReplay
     * semantics behind the oracle interface — ours on the session's
     * DuckDB, golden on the mirror, multiset compare; the walk's
     * compile-time declines (ordered fetch, chained temp tables)
     * surface as DECLINED with their own names. Seeds = the recorded
     * raw-SQL history (testing-side policy, exactly the walk's). */
    @Override
    public com.legend.exec.SqlReplayOracle.RowVerdict verifyFetchTexts(
            java.sql.Connection session, String goldenSql, String ourSql) {
        com.legend.exec.SqlReplayOracle.RowVerdict v = verifyFetchTexts0(session, goldenSql, ourSql);
        OUTCOMES.computeIfAbsent("verifyFetchTexts " + v.outcome(), k -> new java.util.concurrent.atomic.LongAdder()).increment();
        return v;
    }

    public com.legend.exec.SqlReplayOracle.RowVerdict verifyFetchTexts0(
            java.sql.Connection session, String goldenSql, String ourSql) {
        try {
            String d = tdgSqlReplay(
                    recorder.seeds(),
                    goldenSql, session, ourSql);
            return d == null
                    ? com.legend.exec.SqlReplayOracle.RowVerdict.match()
                    : com.legend.exec.SqlReplayOracle.RowVerdict
                            .diverged(d);
        } catch (H2Verify.Unverifiable u) {
            return outcome("verdict-arm-tdg: ", u);
        }
    }

    /** Rows for a SQL text on the seeded oracle (the recorded ledger
     * applies exactly as the verify paths apply it). Raw
     * {@code getObject} cells — comparison policy normalizes, never
     * the oracle. Consumed by the platform's verdict arms (charter §8
     * slice 3); any failure surfaces as the oracle declining. */
    /** One statement on the seeded oracle (DDL for a replay's allocation
     * tables — same session and ledger discipline as {@link #rows}). */
    private void execute(String sql) throws SQLException {
        onOracle(recorder.seeds(),
                VERIFY_SESSION, st -> {
                    st.execute(sql);
                    return Boolean.TRUE;
                });
    }

    @Override
    public com.legend.exec.SqlReplayOracle.OracleRows rows(String sql)
            throws SQLException {
        return onOracle(recorder.seeds(),
                VERIFY_SESSION, st -> {
                    try (java.sql.ResultSet rs = st.executeQuery(sql)) {
                        var md = rs.getMetaData();
                        int n = md.getColumnCount();
                        java.util.List<String> labels =
                                new java.util.ArrayList<>(n);
                        java.util.List<Integer> types =
                                new java.util.ArrayList<>(n);
                        for (int i = 1; i <= n; i++) {
                            labels.add(md.getColumnLabel(i));
                            types.add(md.getColumnType(i));
                        }
                        java.util.List<java.util.List<Object>> rows =
                                new java.util.ArrayList<>();
                        while (rs.next()) {
                            java.util.List<Object> row =
                                    new java.util.ArrayList<>(n);
                            for (int i = 1; i <= n; i++) {
                                row.add(rs.getObject(i));
                            }
                            rows.add(row);
                        }
                        return new com.legend.exec.SqlReplayOracle
                                .OracleRows(labels, types, rows);
                    }
                });
    }
}
