// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

/**
 * THE REPLAY-ORACLE SPI (SQLTEXT_ROW_VERDICT_CHARTER §2, the
 * {@link AssertListener} precedent): rows for a SQL text, executed on
 * the referee's oracle database with the run's recorded seed ledger
 * applied. The platform defines only this seam and carries it on
 * {@code ExecEnv}; a test harness registers its implementation per run
 * (the H2 mirror). Production registers nothing — a SQL-text assert
 * that needs the oracle then WALLS loudly, which is correct: no
 * goldens exist outside tests. The platform never learns what a
 * golden is, where the ledger lives, or which database answers.
 *
 * <p>The plan-replay entry (charter §5) joins this interface with the
 * plan-replayer slice — its signature derives from that replayer's
 * real consumption, never guessed ahead of it.
 */
public interface SqlReplayOracle {

    /**
     * Execute {@code sql} on the seeded oracle session and return its
     * result whole: column labels, JDBC type codes, and raw cell
     * values ({@code getObject} arrivals, untransformed — comparison
     * policy normalizes, never the oracle). Any exception means the
     * oracle could not answer; the caller's decline policy applies.
     */
    OracleRows rows(String sql) throws java.sql.SQLException;

    /** THE TDG FETCH-TEXT VERDICT (charter burn map, TDG scoring
     * flip): BOTH sides are generator fetch TEXTS — ours executes on
     * the calling session's own database, the golden replays on the
     * oracle, rows compare under the referee's multiset policy (the
     * generator's fetches carry no ORDER BY by construction; an
     * ordered or chained text DECLINES with its named reason). The
     * platform arm consumes the outcome; seeds and session policy
     * live with the implementation (testing side). */
    RowVerdict verifyFetchTexts(java.sql.Connection session,
            String goldenSql, String ourSql);

    /** The CHAINED generator-fetch verdict (batch 64 — the walk's
     * chained replay behind the SPI): hop {@code hopIndex}'s golden is
     * remembered for the attempt; a hop whose fetch joins the generator's
     * own temp tables replays its golden on the oracle with every
     * ANCESTOR temp materialized from that ancestor's remembered golden
     * (root-first — the engine fills each temp with the parent fetch's
     * rows, i.e. the parent golden's result) and multiset-compares
     * against the hop's transcript rows: the generator re-run through
     * {@code transcript} (deterministic reads over static seeds; the
     * hop's text must match byte-exactly, a receipt). A hop without
     * temps is {@link #verifyFetchTexts}. */
    RowVerdict verifyFetchChain(java.sql.Connection session, int hopIndex,
            String goldenSql, String ourSql,
            java.util.function.Supplier<FetchTranscript> transcript);

    /** A generator run's fetch transcript in the SPI's own terms (exec
     * never depends on the generator package): the fetch texts and each
     * hop's parent index, table, columns and rows. */
    record FetchTranscript(java.util.List<String> sqls,
            java.util.List<FetchHop> hops) {
    }

    /** One generator fetch: its parent hop (-1 = a root), the table it
     * fetched, and the rows it fetched. */
    record FetchHop(int parentIndex, String table,
            java.util.List<String> columns,
            java.util.List<java.util.List<Object>> rows) {
    }

    /** One materialized oracle result: {@code labels}/{@code
     * jdbcTypes} aligned by column, {@code rows} of raw cells. */
    record OracleRows(java.util.List<String> labels,
            java.util.List<Integer> jdbcTypes,
            java.util.List<java.util.List<@com.legend.base.Nullable Object>> rows) {
    }

    /**
     * THE ROW VERDICT (charter §3.5d-6): replay {@code goldenSql} on
     * the seeded oracle and compare its rows against {@code ours}
     * under the oracle's OWN comparison policy (the §6 normalization
     * inventory + the §7 order rule live with the referee — charter
     * §2 puts comparison policy on the testing side; the platform arm
     * consumes the OUTCOME and owns the assert's judgment).
     * {@code session} is the executing connection (an H2 session
     * verifies directly); {@code mappingFqn}/{@code rootClassFqn}
     * drive the enum source-code→name decode where the query selects
     * raw codes. Never throws for a comparison problem — every
     * non-answer is a DECLINED outcome with its counted reason.
     */
    RowVerdict verify(java.sql.Connection session, String goldenSql,
            ExecutionResult ours,
            @com.legend.base.Nullable String mappingFqn,
            @com.legend.base.Nullable String rootClassFqn,
            ReplayFacts facts,
            com.legend.compiler.element.ModelContext ctx);

    /** The verdict arm's STATIC facts about the verified chain, the
     * referee's comparison gates (Phase 0.5 — they rode thread-locals
     * the old runner set per test until batch 115 deleted the writer and
     * the ordered compare silently became a multiset compare):
     * {@code extentSubset} — the chain is a class extent through
     * subset-preserving ops (the graph compare may collapse the golden's
     * join fan-out by full-row identity); {@code ordered} — the chain
     * ENDS IN A SORT ({@code AssertVerdicts.orderView == SORTED}): row
     * order is contract and the referee compares IN ORDER, ties grouped;
     * {@code sortKeys} — the tail-most sort's key column/property names
     * (the tie boundaries), null when underivable (a computed key): an
     * ordered chain without keys keeps the multiset compare, COUNTED. */
    record ReplayFacts(boolean extentSubset, boolean ordered,
            java.util.@com.legend.base.Nullable List<String> sortKeys,
            @com.legend.base.Nullable ExecutionResult population) {
        public static final ReplayFacts NONE = new ReplayFacts(false, false, null, null);

        /** The compile-time facts alone (no page). */
        public ReplayFacts(boolean extentSubset, boolean ordered,
                java.util.@com.legend.base.Nullable List<String> sortKeys) {
            this(extentSubset, ordered, sortKeys, null);
        }

        /** A PAGED chain (batch 0.5b): {@code population} = OUR rows for the
         * same chain WITHOUT its tail page (limit / drop / slice / take). A
         * page's contents over ties or over an unsorted chain are the
         * database's arrival order, not a contract; what IS defined is that
         * every golden page row is a member of the population and that the
         * page sizes agree — the PAGE-MEMBERSHIP verdict. */
        public ReplayFacts withPopulation(ExecutionResult rows) {
            return new ReplayFacts(extentSubset, ordered, sortKeys, rows);
        }
    }

    /** MATCH = rows agree (the verdict of record); DIVERGED = rows
     * differ ({@code detail} says how — a REAL failure whatever the
     * text said); DECLINED = the oracle could not answer
     * ({@code detail} = the counted reason; the caller's §4 policy
     * applies — e.g. text stays the contract). */
    /** A golden's ENGINE-SESSION temp table the referee materializes
     * before the replay (batch 65): the engine's {@code tempTableForIn_N}
     * holds the query's in-list literal — {@code values} in the
     * literal's Pure spelling (dates as the literal's engine string),
     * {@code kind} the literal's Pure type ({@code date}, {@code
     * datetime}, {@code string}, {@code integer}). The oracle spells the
     * table in its own dialect. */
    record TempTable(String name, String kind, java.util.List<String> values) {
    }

    /** {@link #verify} with the golden's temp tables materialized on the
     * oracle session first; the default ignores them (an oracle without
     * temp support declines through the missing table as before). */
    default RowVerdict verify(java.sql.Connection session, String goldenSql,
            ExecutionResult ours,
            @com.legend.base.Nullable String mappingFqn,
            @com.legend.base.Nullable String rootClassFqn,
            ReplayFacts facts,
            com.legend.compiler.element.ModelContext ctx,
            java.util.List<TempTable> temps) {
        return verify(session, goldenSql, ours, mappingFqn, rootClassFqn,
                facts, ctx);
    }

    /** A golden PLAN's replay (batch 66): the plan text's nodes run in
     * order on the oracle — an Allocation's value (a Constant's literal;
     * a Relational node's rows) binds the later holes, the template
     * operations (collectionSize, renderCollection,
     * optionalVarPlaceHolderOperationSelector, GMTtoTZ) evaluate over
     * the bindings, and the final Relational node's filled SQL replays;
     * its rows compare with ours. {@code bindings}: parameter name →
     * raw spellings (a scalar is one element). The default declines. */
    default RowVerdict verifyPlan(java.sql.Connection session, String goldenPlan,
            java.util.Map<String, java.util.List<String>> bindings,
            ExecutionResult ours,
            @com.legend.base.Nullable String mappingFqn,
            @com.legend.base.Nullable String rootClassFqn,
            ReplayFacts facts,
            com.legend.compiler.element.ModelContext ctx) {
        return RowVerdict.declined("plan replay not supported by this oracle");
    }

    record RowVerdict(Outcome outcome, @com.legend.base.Nullable String detail) {
        public enum Outcome { MATCH, DIVERGED, DECLINED, FAULT }

        public static RowVerdict match() {
            return new RowVerdict(Outcome.MATCH, null);
        }

        public static RowVerdict diverged(String detail) {
            return new RowVerdict(Outcome.DIVERGED, detail);
        }

        public static RowVerdict declined(String reason) {
            return new RowVerdict(Outcome.DECLINED, reason);
        }

        /** The referee's OWN machinery failed (Phase 0.6): a seed of our
         * ledger would not replay, an extension function we ship is
         * missing, the compare threw. Unlike DECLINED (a modeled gap in
         * what the golden can be judged on) a FAULT never lets the text
         * stand in for rows — the test fails until the referee is fixed. */
        public static RowVerdict fault(String detail) {
            return new RowVerdict(Outcome.FAULT, detail);
        }
    }
}
