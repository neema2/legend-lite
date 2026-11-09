// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package org.finos.legend.lite.pct.channelb;

import org.finos.legend.lite.pct.extension.PctBackend;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * CHANNEL B ON EVERY LANE (2026-10-11, the user's ask): the same runner on the database channel A's lane runs on
 * ({@code LEGENDLITE_PCT_BACKEND}, {@link PctBackend}), diffed against THAT lane's channel A ledger
 * ({@code Test_LegendLite_<Lane>_<Suite>Functions_PCT}) and, where legend-engine publishes one, its PCT manifest for
 * the same database (Postgres, H2, Snowflake, Databricks; none for SQLite, MariaDB, MySQL or BigQuery, so there every
 * wire bug is a true one). DuckDB, the reference lane, keeps its own pins in each suite's test; every other lane is
 * judged here, by its own measured numbers.
 *
 * <p>The gate: the lane's own ledger, exact (ChannelBLedger, {@code ledger.tsv}; 2026-11-03, round 14 -- until then a
 * PASS floor and two disagreement ceilings): every failure a row with the text it says, every row's test failing. A
 * WIRE-BUG (channel B fails where channel A passes on the same database) and B-FIXES-A (the converse) are measured and
 * printed beside it. The never-happens invariants hold on every lane: no label lie escapes reconciliation, and the
 * database's verdict and the host referee never disagree.
 */
final class ChannelBLane {

    private ChannelBLane() {
    }

    /** Whether this JVM runs the reference lane, the platform's own engine, whose census pins are each suite's own. */
    static boolean isReference() {
        return PctBackend.isReference();
    }

    /** Judge {@code suite}'s outcomes on this lane against its channel A ledger and engine manifest. */
    static void judge(String suite, String tag, List<ChannelB.Outcome> out) throws IOException {
        String lane = PctBackend.name();
        String tagged = tag + "@" + lane;
        // the universe is the same on every lane: discovery is compilation, before any database
        assertEquals(PctRatchets.measured("channel_b." + suite.toLowerCase(java.util.Locale.ROOT) + ".discovered"),
                out.size(), suite + " discovery moved -- an upstream move or a discovery-rule change");
        Path ledger = com.legend.testing.Runfile.property("pct.suite." + suite);
        @com.legend.base.Nullable Path referee = System.getProperty("pct.oracle." + suite) == null
                ? null
                : com.legend.testing.Runfile.property("pct.oracle." + suite);
        System.out.println("[" + tagged + "] channel A ledger " + ledger.getFileName() + ", engine manifest "
                + (referee == null ? "none for this database" : referee.getParent().getFileName() + "/"
                        + referee.getFileName()));
        ChannelBDiff.Counts c = ChannelBDiff.report(tagged, out, ledger, referee);
        System.out.println("[" + tagged + "] measured: pass=" + c.pass() + " wireBug=" + c.wireBug()
                + " trueWireBug=" + c.trueWireBug() + " bFixesA=" + c.bFixesA());
        // every failure with its text, the lane's ledger built from these lines (2026-11-03, round 14)
        for (ChannelB.Outcome o : out) {
            if (o.status() != ChannelB.Status.PASS) {
                System.out.println("[" + tagged + "] OUTCOME " + o.status() + " " + o.testFqn() + " :: "
                        + o.detail().replace('\n', ' '));
            }
        }
        // THE EXACT GATE, as the reference lane's (2026-11-03, the walker-deletion program: a floor and two ceilings
        // let a failure change kind, or a body the closed walk refused, pass unseen): every failure a row of the lane's
        // ledger with its text, every row's test failing (ChannelBLedger, ledger.tsv)
        ChannelBLedger.judge(lane + " " + suite, out, ChannelBLedger.of(lane, suite));
        assertEquals(0, com.legend.testexec.SqlTypeCensus.mismatchCount(),
                "a label lie escaped reconciliation: " + com.legend.testexec.SqlTypeCensus.summary());
    }
}
