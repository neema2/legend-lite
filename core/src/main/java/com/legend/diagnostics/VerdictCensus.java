// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.diagnostics;

/**
 * THE SQL-VERDICT CENSUS (moved from the judges' {@code CanonicalDivergence}, the plan-as-data design's slice 5,
 * 2026-11-09): per assert family, what the database judge decided, what it could not, and what no channel attempted.
 * Both halves count here -- planning as it plans an assert ({@link #sqlFamily}, {@link #sqlRoute}), the run at a row's
 * turn ({@link #sqlJudgedInDatabase}, {@link #sqlUnjudged}, {@link #sqlRaisedAt}) -- so it is the diagnostics', below
 * both. Attribution only: no verdict reads any of it.
 */
public final class VerdictCensus {

    private VerdictCensus() {
    }

    // (the full mute, MUTED and muteAll, deleted 2026-11-09: no caller set it -- a dead switch)

    // ── leg 3.0 (docs/DATABASE_MODE_HOMEWORK_2026_09_18.md §4): the
    // CLAIM / DECLINE census per assert family — what the SQL canon
    // already judges and what it declines, by reason. Attribution
    // only: no verdict reads any of this. CURRENT_FAMILY is the assert
    // family AssertVerdicts is adjudicating (set at its one entry), so
    // a decline recorded deeper (TdsCompare, the wrap) lands on it.
    private static final java.util.concurrent.atomic.AtomicReference<String>
            CURRENT_FAMILY = new java.util.concurrent.atomic.AtomicReference<>("?");
    private static final java.util.concurrent.ConcurrentHashMap<String, Long>
            SQL_CENSUS = new java.util.concurrent.ConcurrentHashMap<>();

    /** An assert of {@code family} was judged by a route that has NO
     * byte channel (the SQL-text rows arm, the rendered-text arm, the
     * grid-pair arm): counted so "never attempted" has a reason. */
    public static void sqlRoute(String family, String route) {
        SQL_CENSUS.merge("not-attempted " + family + " " + route, 1L, Long::sum);
        CHANNEL_SEEN.set(true);
    }

    /** The assert being adjudicated RAISED before any channel judged
     * (a side that errors, a failed verdict thrown as AssertFailed):
     * counted on the current family so "never attempted" has a reason. */
    public static void sqlRaised() {
        if (CHANNEL_SEEN.get()) {
            return;   // a judged assert that then failed is not "never attempted"
        }
        SQL_CENSUS.merge("not-attempted " + CURRENT_FAMILY.get() + " raised",
                1L, Long::sum);
        CHANNEL_SEEN.set(true);   // a nested entry's raise is one raise
    }

    /** A root of a database judge's plan RAISED at its turn with no row of it judged (a failure decided at planning,
     *  a wall its arm left through): counted on {@code family}, the root's own, read from the plan's data -- the run
     *  reads no attribution state planning wrote (the slice 3 invariant audit, 2026-11-08: {@link #sqlRaised} read the
     *  family the LAST planned assert left). */
    public static void sqlRaisedAt(String family) {
        SQL_CENSUS.merge("not-attempted " + family + " raised", 1L, Long::sum);
    }

    /** The current assert's switch arm yielded null (not this arm's
     * shape; the generic path continues): counted, so "adjudicated"
     * minus the channels is explained. */
    public static void sqlFellThrough() {
        SQL_CENSUS.merge("not-attempted " + CURRENT_FAMILY.get() + " fell-through",
                1L, Long::sum);
    }

    /** Whether any channel (claim, decline, route) spoke for the current
     * assert — set by them, reset at {@link #sqlFamily}. */
    private static final java.util.concurrent.atomic.AtomicBoolean CHANNEL_SEEN =
            new java.util.concurrent.atomic.AtomicBoolean();

    /** A statement-root assert enters the adjudicator, BEFORE its family
     * is known (the lineage, quantified, if-branch and SQL-text root
     * arms run first): a raise from there lands on its own row. */
    public static void sqlEnter() {
        CURRENT_FAMILY.set("(pre-arm)");
        CHANNEL_SEEN.set(false);
    }

    /** Leg 3.1: database mode decided the assert (the verdict row). */
    public static void sqlJudgedInDatabase(String family) {
        SQL_CENSUS.merge("judged-in-database " + family, 1L, Long::sum);
        CHANNEL_SEEN.set(true);
    }

    /** Leg 3.1: database mode could NOT decide the assert — the assert
     * failed with this reason; the per-mode ceiling the register pins. */
    public static void sqlUnjudged(String family, String reason) {
        Diagnostics.judgeDecline("unjudged", family, reason);
        int c = reason.indexOf(':');
        String head = c < 0 ? reason : reason.substring(0, c);
        SQL_CENSUS.merge("unjudged " + family + " " + head, 1L, Long::sum);
        CHANNEL_SEEN.set(true);
    }

    /** An assert of {@code family} enters adjudication. */
    public static void sqlFamily(String family) {
        CURRENT_FAMILY.set(family);
        CHANNEL_SEEN.set(false);
        SQL_CENSUS.merge("adjudicated " + family, 1L, Long::sum);
    }

    /** The census rows, sorted: {@code adjudicated <family>},
     * {@code claimed <family>} (a byte verdict was produced),
     * {@code declined <family> <reason-head>}. */
    public static java.util.SortedMap<String, Long> sqlCensus() {
        return new java.util.TreeMap<>(SQL_CENSUS);
    }

    /** The asserts the database judge decided (its verdict row read; {@link #sqlJudgedInDatabase}). The agreement
     *  gate's pins read it beside {@link #sqlUnjudgedCount} (the plan path program's step 6, 2026-10-18). */
    public static long sqlJudgedInDatabaseCount() {
        return sumOf("judged-in-database ");
    }

    /** The asserts the database judge named UNJUDGED ({@link #sqlUnjudged}). */
    public static long sqlUnjudgedCount() {
        return sumOf("unjudged ");
    }

    private static long sumOf(String prefix) {
        long n = 0;
        for (var e : SQL_CENSUS.entrySet()) {
            if (e.getKey().startsWith(prefix)) {
                n += e.getValue();
            }
        }
        return n;
    }
}
