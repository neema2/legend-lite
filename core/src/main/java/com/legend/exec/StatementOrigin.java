// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.exec;

/**
 * CENSUS: where every statement the platform or the test harness sends to a
 * database comes from — measurement only, printed by the corpus lanes, read by
 * no verdict. The north star (lean ladder, 2026-09-20) is ONE statement per test
 * body: every origin but {@link #BODY} is a statement outside it, and the census
 * names them by kind and by test so the next leg is chosen by count, not by
 * intuition. The origin is a thread-scoped mark ({@link #enter}) set by the site
 * that decides WHY a statement runs; the executor counts every statement under
 * the mark in force ({@link #count()}); harness senders that bypass the executor
 * count themselves.
 */
public enum StatementOrigin {
    /** The body's fused verdict statement (one per test body). */
    BODY,
    /** A body that fell back to per-assert statements. */
    FALLBACK,
    /** A let's frame RUN at the let (the host judge's path). */
    LET,
    /** A value-position execute: its result IS the value asked for. */
    VALUE,
    /** Seeding as RAW text: a fixture's executeInDb blobs, a runtime's declared setups. */
    SEED,
    /** Seeding GENERATED from data or the model: CSV loads, dropAndCreate DDL under a fixture. */
    SEED_GENERATED,
    /** Session setup: attach / use / settings / extension aliases. */
    SESSION,
    /** The read-only system metamodel database. */
    SYSTEM,
    /** The SQL-text referee running OUR plan or text for rows. */
    REFEREE_OURS,
    /** The SQL-text referee replaying the engine's golden SQL (and its seeds). */
    REFEREE_GOLDEN,
    /** A body's raw statement native ({@code executeInDb}, {@code dropAndCreate…InDb}) outside a fixture. */
    RAW,
    /** A verdict SIDE the judge evaluates outside the body's statement (the host judge's
     * sides; a database-mode shape the batch declined). */
    SIDE,
    /** A body statement that is neither a let frame nor an assert, executed on its own. */
    STATEMENT,
    /** The referee's H2 mirror replaying the seed ledger (the referee's cost, not the product's). */
    MIRROR_SEED,
    /** A metadata probe: reported columns, pivot keys. */
    PROBE,
    /** Test-data generation. */
    TDG,
    /** No site claimed the statement — the census's own residual. */
    OTHER;

    private static final ThreadLocal<StatementOrigin> CURRENT =
            ThreadLocal.withInitial(() -> OTHER);
    /** The mark in force on this thread. */
    public static StatementOrigin current() {
        return CURRENT.get();
    }

    /** {@link #enter} only when no site has marked the thread yet — a site that
     * serves marked callers (a fixture's seeding, the referee) and unmarked ones.
     * {@link #STATEMENT} is the weak outer mark of a body statement: a raw native
     * or a side evaluated inside one names itself over it. */
    public static Scope enterIfUnmarked(StatementOrigin origin) {
        StatementOrigin now = CURRENT.get();
        return enter(now == OTHER || now == STATEMENT && origin != STATEMENT ? origin : now);
    }

    /** A GENERATED seeding site: under a fixture it refines {@link #SEED} to
     * {@link #SEED_GENERATED}; elsewhere it is {@code otherwise}. */
    public static Scope enterGenerated(StatementOrigin otherwise) {
        StatementOrigin now = CURRENT.get();
        return enter(now == SEED ? SEED_GENERATED : now == OTHER || now == STATEMENT ? otherwise : now);
    }

    /** Sets the mark until the scope closes (restoring the previous one). */
    public static Scope enter(StatementOrigin origin) {
        StatementOrigin previous = CURRENT.get();
        CURRENT.set(origin);
        return new Scope(previous);
    }

    /** A scoped mark; closing restores what was in force before. */
    public record Scope(StatementOrigin previous) implements AutoCloseable {
        @Override
        public void close() {
            CURRENT.set(previous);
        }
    }

    /** {@code sql} sent to a database under the mark in force: one round trip, its characters and its origin counted
     *  (the Census), and with -Dlegend.diagnostics's SQL dump, printed with the origin it was sent under. */
    public static void sent(String sql) {
        Census.inc(Census.Key.SQL_ROUND_TRIPS);
        count();
        Census.add(Census.Key.SQL_CHARS, sql.length());
        if (com.legend.diagnostics.Diagnostics.dumpSql()) {
            System.err.println("[sql:" + current().name().toLowerCase(java.util.Locale.ROOT) + "] " + sql);
        }
    }

    /** One statement sent under the mark in force — counted in the Census's
     * {@code statements} family by origin name. */
    public static void count() {
        count(CURRENT.get());
    }

    public static void count(StatementOrigin origin) {
        Census.incKeyed("statements", origin.name());
    }

    /** The counts so far, by ordinal. */
    public static long[] snapshot() {
        StatementOrigin[] all = values();
        long[] out = new long[all.length];
        for (int i = 0; i < out.length; i++) {
            out[i] = Census.keyed("statements", all[i].name());
        }
        return out;
    }

    /** {@code name=count …} for a snapshot (or a delta of two). */
    public static String census(long[] counts) {
        StringBuilder sb = new StringBuilder();
        for (StatementOrigin o : values()) {
            if (sb.length() > 0) {
                sb.append(' ');
            }
            sb.append(o.name().toLowerCase(java.util.Locale.ROOT)).append('=')
                    .append(counts[o.ordinal()]);
        }
        return sb.toString();
    }
}
