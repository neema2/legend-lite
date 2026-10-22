// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package org.finos.legend.lite.pct.channelb;

import org.finos.legend.lite.pct.extension.PctBackend;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

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
 * <p>The numbers: PASS may only grow and the two disagreement counts may only shrink, as DuckDB's. A WIRE-BUG is a
 * test channel B fails where channel A passes on the same database; B-FIXES-A, the converse, is measured and printed,
 * not pinned (channel B passing more is not a regression). The never-happens invariants hold on every lane: no label
 * lie escapes reconciliation, and the database's verdict and the host referee never disagree.
 */
final class ChannelBLane {

    private ChannelBLane() {
    }

    /** What a lane's suite is held to: PASS's floor, and the WIRE-BUG and TRUE-WIRE-BUG ceilings. */
    private record Pins(int passFloor, int wireBug, int trueWireBug) {
    }

    /** Each lane's measured numbers, per suite: first measured 2026-10-11 (docs/CLOUD_BACKENDS_PCT_2026_10_11.md §16,
     *  which names every disagreement's cause). A WIRE-BUG here is a test whose body channel B runs whole in the
     *  database where channel A's interpreter runs part of it in Java: list operations on a database without lists
     *  (H2), a regular expression's non-literal pattern (SQLite, MariaDB), a pivot's column order, a sort of a
     *  relation's rows. */
    private static final Map<String, Map<String, Pins>> PINS = Map.of(
            // the relation suite only, as channel A's H2 lane: 14 tests sort or join a group's values as a list in
            // their own check (12 not the engine's frontier). 429 -> 431, 14 -> 17 and 12 -> 17 (2026-10-20, the parity
            // phase): channel A now passes the five literal-instance projects and filters (ConstantLateralsAsRows,
            // CarrierStrategies.filteredLiteralLength), which channel B's body holds as a let's frame with a struct
            // column H2 has no value for (the judges on plans, the program's step 10, the home of that form)
            "h2", Map.of("Relation", new Pins(431, 17, 17)),
            // Postgres 16: 35 -> 1 (Standard) and 7 -> 3 (Relation) when a Float's text kept its '.0' (Postgres.java,
            // the same day); every Essential and Grammar wire bug is the engine's own frontier on Postgres
            // 285 -> 315, 128 -> 132, 186 -> 203 and 83 -> 87 (2026-10-12, Postgres's own pins burned down,
            // docs/PCT_PIN_WORKAROUNDS_2026_10_12.md): printf unrolled, the empty collection spelled, a Boolean list's
            // extremes, splitPart, hashCode, a struct decoded from its jsonb. 315 -> 326, 464 -> 465 (its wire bugs
            // 3 -> 2, the true one 1 -> 0) and 203 -> 204 (2026-10-12, the user's ask): maps as jsonb entries, a fold
            // or a reduction into a list, the LIST aggregate, and a raise no plan-time estimate evaluates. 326 -> 327
            // and 87 -> 91 (2026-10-12, the user's ask): a quoted literal typed for to_jsonb, Levenshtein and
            // Jaro-Winkler as recursive walks (no extension). 327 -> 329, its wire bugs 2 -> 1 (2026-10-12, the user's
            // ask): the ordered list aggregate typed as a list (array_agg here)
            // 2026-10-20, as measured at the branch head (object identity and the plan-route parity work: the
            // canon's key fields typed, an empty list arm of a coalesce typed by its siblings, the zip/put decodes):
            // Essential 329 -> 330, Relation 465 -> 467, Standard 204 -> 205, Unclassified 91 -> 92
            "postgres", Map.of(
                    "Essential", new Pins(330, 1, 0),
                    // wire bugs 3 -> 5 (2026-10-20, object identity, docs/OBJECT_IDENTITY_2026_10_20.md, measured at
                    // the branch head): channel A now passes eq's and equal's testEqVarIdentity and testEqNonPrimitive
                    // pairs, which channel B's legacy route still refuses on Postgres ("a list transform of unknown
                    // result type": its verdict lane's canon rider) -- they failed on both before, so the pass floor
                    // holds; the judges move to plans in the plan path program's step 10, the home of that route
                    "Grammar", new Pins(132, 5, 0),
                    "Relation", new Pins(467, 2, 0),
                    "Standard", new Pins(205, 1, 0),
                    "Unclassified", new Pins(92, 0, 0)),
            // no engine manifest for SQLite: every wire bug is a true one (10 of Relation's 12 a regular expression
            // with a non-literal pattern, 2 a pivot's column order)
            "sqlite", Map.of(
                    // 332 -> 333 (2026-10-11): the mixed sort's statement fits the 1 MB cap (FLOAT_CANON's text bound once)
                    "Essential", new Pins(333, 1, 1),
                    "Grammar", new Pins(133, 0, 0),
                    "Relation", new Pins(456, 12, 12),
                    // 203 -> 205 (2026-10-20, the parity phase): hashCode is the MD5 of the values' text written in
                    // SQLite's SQL (SqliteDigests), testHashCode and testHashCodeAggregate
                    "Standard", new Pins(205, 0, 0),
                    // 87 -> 92 (2026-10-20, the parity phase): the digests written in SQLite's SQL (SqliteDigests)
                    // and Jaro-Winkler's walk
                    "Unclassified", new Pins(92, 0, 0)),
            // no engine manifest for MariaDB. 325 -> 330 and 201 -> 204 (2026-10-11): a Float's text written once (the
            // mixed sort no longer takes the server past 12 GB), and a windowed mode counted per value
            "mariadb", Map.of(
                    "Essential", new Pins(330, 2, 2),
                    "Grammar", new Pins(135, 0, 0),
                    "Relation", new Pins(440, 17, 17),
                    "Standard", new Pins(204, 1, 1),
                    // 91 -> 93 (2026-10-20, the parity phase): Jaro-Winkler's walk (MariaDb.jaroWinkler)
                    "Unclassified", new Pins(93, 0, 0)),
            // no engine manifest for MySQL. Relation's 32: 21 window tests whose rows come back in another order (no
            // ORDER BY: SQL promises none; channel A's comparison is unordered), 9 regular expressions with a
            // non-literal pattern, 2 pivots' column order
            "mysql", Map.of(
                    "Essential", new Pins(331, 1, 1),
                    "Grammar", new Pins(136, 0, 0),
                    "Relation", new Pins(422, 32, 32),
                    "Standard", new Pins(204, 1, 1),
                    // 91 -> 93 (2026-10-20, the parity phase): Jaro-Winkler's walk (MariaDb.jaroWinkler)
                    "Unclassified", new Pins(93, 0, 0)),
            // the clouds, on the accounts (§4): Databricks' 10 Relation wire bugs fail with no detail from the
            // server; BigQuery's 13 are 10 regular expressions with a non-literal pattern, 2 pivots' column order and
            // an ORDER BY past a DISTINCT; Snowflake's 2 the pivots
            "databricks", Map.of(
                    "Essential", new Pins(334, 2, 1),
                    "Grammar", new Pins(135, 2, 0),
                    "Relation", new Pins(459, 10, 10),
                    "Standard", new Pins(205, 0, 0),
                    "Unclassified", new Pins(93, 0, 0)),
            "snowflake", Map.of(
                    "Essential", new Pins(334, 1, 0),
                    // wire bugs 2 -> 4 (2026-10-22, measured by the Snowflake lane at 81960bf7 against the merge-base
                    // 59b547a5e): channel A now passes eq's and equal's testEqNonPrimitive pair (the ledger REMOVEs),
                    // which channel B still refuses here as at the merge-base, "a field '__canon' of unknown type
                    // reached Snowflake", the same four tests with the same message; they failed on both before, so
                    // the pass floor holds -- Postgres's move of 2026-10-20, the same pairs; the judges move to plans
                    // in the plan path program's step 10
                    "Grammar", new Pins(133, 4, 0),
                    "Relation", new Pins(467, 2, 2),
                    "Standard", new Pins(204, 1, 0),
                    "Unclassified", new Pins(93, 0, 0)),
            "bigquery", Map.of(
                    "Essential", new Pins(330, 2, 2),
                    "Grammar", new Pins(135, 0, 0),
                    "Relation", new Pins(455, 13, 13),
                    "Standard", new Pins(205, 0, 0),
                    "Unclassified", new Pins(91, 0, 0)),
            // Trino 483 (2026-10-12, §18), judged against the engine's Trino manifest: Essential's 1 wire bug is the
            // engine's frontier; Relation's 5 are two sorts (Trino discards a subquery's ORDER BY: SQL promises no
            // order without one at the top), a pivot's column order, a struct field of no type, and Trino's own
            // planner refusing an arithmetic chain ("SignatureBinder.iterativeSolve does not converge")
            "trino", Map.of(
                    "Essential", new Pins(331, 1, 0),
                    // 131 -> 136 as measured, wire bugs 0 -> 1 (2026-10-20, the parity phase): channel A reads the
                    // metamodel's tables inline where Trino has no temporary table (testBasic), and channel B's legacy
                    // route still makes them the session's own (the judges on plans, the program's step 10, its home)
                    "Grammar", new Pins(136, 1, 0),
                    "Relation", new Pins(463, 6, 5),
                    "Standard", new Pins(205, 0, 0),
                    "Unclassified", new Pins(93, 0, 0)),
            // ClickHouse 25.8 (2026-10-12, §19), judged against the engine's ClickHouse manifest: Essential's 1 true
            // wire bug is a date past 2299, outside ClickHouse's dates (the dialect refuses it); Relation's 7, each the
            // engine's frontier too, are 5 JSON values whose text keeps its spaces (ClickHouse stores a JSON value's
            // text as written, DuckDB re-writes it) and a pivot's column order twice. 328 -> 333, 131 -> 136,
            // 445 -> 459 and 201 -> 202 (2026-10-12, ClickHouse's own pins burned down,
            // docs/PCT_PIN_WORKAROUNDS_2026_10_12.md): as-of and one-sided joins, correlated IN and LATERAL
            // decorrelated, Float RANGE offsets, a column's quantile level, the connection's query size, a session for
            // the system database, typed empty lists and struct fields. 136 -> 137 and 459 -> 461 (2026-10-12, the
            // user's ask): an absent instance as a struct field's Variant, a JSON null kept null through a list.
            // 461 -> 462 (2026-10-20, the parity phase): a LEFT join's condition both sides read is its matches first
            // (OuterJoinResidualsAsMatches), testNestedJoinArithmeticComparisonExpression
            "clickhouse", Map.of(
                    "Essential", new Pins(333, 3, 1),
                    "Grammar", new Pins(137, 0, 0),
                    "Relation", new Pins(462, 7, 0),
                    "Standard", new Pins(202, 0, 0),
                    "Unclassified", new Pins(93, 0, 0)));

    /** Whether this JVM runs the reference lane, the platform's own engine, whose pins are each suite's own. */
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
        Pins pins = PINS.getOrDefault(lane, Map.of()).get(suite);
        assertTrue(pins != null, "channel B on " + lane + " has no measured pins for " + suite
                + " (ChannelBLane.PINS): measure it, then pin the line above");
        assertTrue(c.pass() >= pins.passFloor(), lane + " " + suite + " PASS fell: " + c.pass() + " < "
                + pins.passFloor());
        assertTrue(c.wireBug() <= pins.wireBug(), lane + " " + suite + " WIRE-BUG census grew: " + c.wireBug()
                + " > " + pins.wireBug());
        assertTrue(c.trueWireBug() <= pins.trueWireBug(), lane + " " + suite + " TRUE-WIRE-BUG census grew: "
                + c.trueWireBug() + " > " + pins.trueWireBug());
        assertTrue(com.legend.exec.CanonicalDivergence.sqlDisagreeCount() == 0,
                "DUAL-VERDICT DISAGREEMENT: " + com.legend.exec.CanonicalDivergence.summary());
        assertEquals(0, com.legend.exec.SqlTypeCensus.mismatchCount(),
                "a label lie escaped reconciliation: " + com.legend.exec.SqlTypeCensus.summary());
    }
}
