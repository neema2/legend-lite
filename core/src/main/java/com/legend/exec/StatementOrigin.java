// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.exec;

import com.legend.diagnostics.Census;

/**
 * CENSUS: where every statement the platform or the test harness sends to a
 * database comes from — measurement only, printed by the corpus lanes, read by
 * no verdict. The north star (lean ladder, 2026-09-20) is ONE statement per test
 * body: every origin but {@link #BODY} is a statement outside it, and the census
 * names them by kind and by test so the next leg is chosen by count, not by
 * intuition. The origin is a mark the site that decides WHY a statement runs hands
 * down explicitly -- on the statements and the session source it passes
 * ({@code Statements.mark}, {@code PlanSessions.Source.mark}; a thread-scoped mark
 * until slice 5, 2026-11-09); the runner counts every statement under its mark
 * ({@link #sent}); harness senders that bypass the runner count themselves under
 * the origin they name ({@link #count}).
 */
public enum StatementOrigin {
    /** The body's fused verdict statement (one per test body). */
    BODY,
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
    // (TDG, the test-data generator's interleaved run, went 2026-11-11 with B2d.2: the generator is one planned
    // statement, sent as any statement of its body)
    /** No site claimed the statement — the census's own residual. */
    OTHER;

    /** The origin a site names for what it runs, under {@code inForce}, the mark the caller handed down: the site's
     *  own where no site has marked it yet; {@link #STATEMENT} is the weak outer mark of a body statement, a raw native
     *  or a side evaluated inside one naming itself over it (the rule a thread-scoped mark followed until slice 5,
     *  2026-11-09: the mark is passed explicitly now). */
    public static StatementOrigin under(StatementOrigin inForce, StatementOrigin origin) {
        return inForce == OTHER || inForce == STATEMENT && origin != STATEMENT ? origin : inForce;
    }

    // (the statement observer, OBSERVED and observing(), deleted 2026-11-09: process-wide mutable state a parity test
    // set; a test that compares the statements two roads send records them on the connection it hands in --
    // testbackend.RecordingConnection)

    // (planning(what) and isPlanning(), RunScope's planning scope under another name, deleted 2026-11-09: no caller --
    // planning holds its own scope, RunScope.plans)

    /** {@code sql} sent to a database under the mark in force: one round trip, its characters and its origin counted
     *  (the Census), and with -Dlegend.diagnostics's SQL dump, printed with the origin it was sent under. Refused while
     *  this thread plans ({@code RunScope.plans}). */
    public static void sent(StatementOrigin origin, String sql) {
        String planning = com.legend.base.RunScope.planningWhat();
        if (planning != null) {
            throw new IllegalStateException("a statement sent while planning " + planning + " (planning sends"
                    + " nothing): [" + origin.name().toLowerCase(java.util.Locale.ROOT) + "] "
                    + (sql.length() > 300 ? sql.substring(0, 300) + "..." : sql));
        }
        Census.inc(Census.Key.SQL_ROUND_TRIPS);
        count(origin);
        Census.add(Census.Key.SQL_CHARS, sql.length());
        if (com.legend.diagnostics.Diagnostics.dumpSql()) {
            System.err.println("[sql:" + origin.name().toLowerCase(java.util.Locale.ROOT) + "] " + sql);
        }
        if (com.legend.diagnostics.Diagnostics.on("sql-sizes")) {
            System.err.println("[sql-size] " + sql.length() + " " + System.getProperty("legend.test.current", "?") + " "
                    + origin.name().toLowerCase(java.util.Locale.ROOT));
        }
    }

    /** One statement sent under {@code origin} -- counted in the Census's {@code statements} family by origin name. */
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
