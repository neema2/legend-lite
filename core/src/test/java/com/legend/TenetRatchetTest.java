// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.SourceFiles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * F1.10 — the DIRECT tenet metric (docs/TENET_CHARTER.md, enforcement
 * map): a shrink-only count of JDBC value-accessor CALL SITES in
 * production. The §0.2 metrics are proxies (funnel coverage, duplicate
 * counts); this is the number "Java orchestrates, the DATABASE
 * executes" says must go DOWN — every site is a place Java takes a
 * value off the wire, and Charter C1.2 licenses only CARRIAGE at such
 * a site, never computation. Phases 4-7 (DB-side rendering, the typed
 * bridge, compensation removal, ingress) each delete some of these;
 * this ratchet records the descent and refuses regrowth.
 *
 * <p>Counting rule: {@code .getXxx(} accessor spellings inside
 * src/main files that mention {@code java.sql} OR a driver-native
 * package (audit 2026-08-18 probe 6: an {@code org.sqlite.core} read
 * carries zero {@code java.sql} spellings — the old precondition let
 * it through GREEN; the accessor list likewise gained the audit's five
 * omitted spellings). Seeded 2026-08-16 at the F0.1 baseline:
 * TestDataGenerator 6, Executor 6, DynamicPivot 1, DbMetaData 1.
 * RE-SEEDED 14 -> 12 (documented-debts 2026-08-18, audit §7): the
 * LL_TMP_DEBUG COUNT(*) print (an extra round-trip just to log) is
 * DELETED and the assertTestData getLong double-read hoisted to a
 * local — TestDataGenerator 6 -> 3 raw sites minus one pureRepr read;
 * Executor 7 with the one-carrier timestamp re-fetch.
 * 12 -> 13 (2026-08-22 V11 single-query canon): harvestCanon's ONE
 * getString in the codec owner carries the DB-computed canonical
 * VARCHAR — pure text carriage (Charter C1.2's allowed class), and it
 * DELETED a whole per-side execution (runCanon) in exchange.
 * (V7 §8 leg 1: the grid canon's per-ROW text reads through the SAME
 * harvestCanon choke point as the scalar candidates — user ruling
 * 2026-08-28: consolidate, never sprawl; the count stays 13.)
 * (2026-08-31 chained-fetch live-session refereeing, TDG charter
 * §S5-L: TestDataGenerator.captureRows' ONE getObject carries each
 * fetch's live-session rows into Result.fetches — pure carriage
 * (C1.2's allowed class) of the ENGINE'S OWN return shape
 * (generateTestData pairs every SQL with its ResultSet). The count
 * STAYS 13: the createRowIdentifiers sample loop's own getObject
 * consolidated onto the same captureRows choke point — consolidate,
 * never sprawl.)
 * (2026-10-09, the execution plan's step 3: 13 -> 14. PlanRunner.text carries the
 * database's own text -- a wire statement's one cell, or one JSON object per row --
 * which the runner passes on unread (C1.2's carriage), one choke point for both. It
 * replaces Executor.wireText's and streamWireRows' two sites, which step 4 deletes
 * with the paths they serve: 12 then.)
 * (2026-10-11, the MariaDB dialect, docs/CLOUD_BACKENDS_PCT_2026_10_11.md §13: 14 -> 15.
 * Sessions.raisedMessage's ONE getString carries the message a MariaDB raise left in
 * its session variable, the database's own text passed on unread (C1.2's carriage): a
 * MariaDB SELECT can raise no error of its own text (SIGNAL runs only in a stored
 * program; every run-time error's text is fixed, measured), so the raise keeps it in
 * the session and fails on a fixed error. The site is MariaDbConnector.raisedMessage's since
 * 2026-10-16, the dialect program's Phase 5: the same one getString, moved with the read.)
 */
@Tag("guardrail")
class TenetRatchetTest {

    // 15 -> 20 (2026-10-18/19, the plan path program's step 6, typed rows: JdbcDecode reads a driver's typed
    // cells by the plan's slots, six sites of carriage -- a Map, an Array, a Struct, a Struct's attributes --
    // beside Executor's nine, which step 11 deletes with the legacy executor; the number then goes down)
    // 20 -> 21 (2026-10-22, main's merge): main's Executor holds ten, its DATE cell re-read as java.time (2026-10-17, the
    // corpus program's step 1) one more, under main's own pin of 15 (it counted 14); the two sides' sites add
    private static final int RESULT_SET_ACCESSOR_SITES = 21;

    private static final List<String> DRIVERS = List.of("org.duckdb", "org.h2.", "org.sqlite", "org.postgresql",
            "org.mariadb", "com.mysql", "com.clickhouse", "io.trino", "net.snowflake", "com.databricks", "com.simba",
            "com.google.cloud.bigquery",
            // MongoDB's driver (2026-10-17, docs/MONGODB_PLAN_2026_10_16.md D12): its connector's source set is walked
            "com.mongodb", "org.bson");

    private static final Pattern ACCESSOR = Pattern.compile(
            "\\.get(String|Object|Int|Long|Double|Boolean|BigDecimal"
            + "|Date|Timestamp|Time|Bytes|Array|Float|Short|Byte"
            + "|NString|URL|Clob|NClob|RowId|CharacterStream"
            + "|NCharacterStream|BinaryStream|AsciiStream|Blob|Ref"
            + "|SQLXML)\\(");

    @Test
    void resultSetConsumptionOnlyShrinks() throws IOException {
        List<String> sites = new ArrayList<>();
        int scanned = 0;
        // core's main sources: src/main/java and the DuckDB-only src/main/duckdb (Bazel workplan P3-27, CT-N10)
        String root = "core/src/main/java";
        try (Stream<Path> files = SourceFiles.under(root, "core/src/main/duckdb", "core/src/main/mongodb").stream()) {
            for (Path f : files.filter(p -> p.toString().endsWith(".java"))
                    .toList()) {
                scanned++;
                String src = Files.readString(f);
                // every driver's packages (2026-10-13, the dialect program's Phase 0: DuckDB, H2 and SQLite only before)
                if (!src.contains("java.sql") && DRIVERS.stream().noneMatch(src::contains)) {
                    continue;
                }
                String code = src.replaceAll("//.*", "")
                        .replaceAll("(?s)/\\*.*?\\*/", "");
                Matcher m = ACCESSOR.matcher(code);
                while (m.find()) {
                    sites.add(f.getFileName().toString());
                }
            }
        }
        GuardCoverage.assertFloor(/* 499->498: HostEval DELETED, Phase 1 batch 2 */ "TenetRatchetTest", scanned, 498);
        assertTrue(sites.size() <= RESULT_SET_ACCESSOR_SITES,
                "JDBC value-accessor sites grew to " + sites.size()
                + " (pinned at " + RESULT_SET_ACCESSOR_SITES + "): "
                + sites + " — the tenet's number goes DOWN (Charter"
                + " C1.2: carriage only; computation belongs to the"
                + " database)");
    }
}
