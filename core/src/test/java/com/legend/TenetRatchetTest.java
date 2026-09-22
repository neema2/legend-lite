// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.Repo;
import org.junit.jupiter.api.Test;

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
 */
class TenetRatchetTest {

    private static final int RESULT_SET_ACCESSOR_SITES = 13;

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
        Path root = Repo.module("src/main/java");
        try (Stream<Path> files = Files.walk(root)) {
            for (Path f : files.filter(p -> p.toString().endsWith(".java"))
                    .toList()) {
                scanned++;
                String src = Files.readString(f);
                if (!src.contains("java.sql")
                        && !src.contains("org.duckdb")
                        && !src.contains("org.h2.")
                        && !src.contains("org.sqlite")) {
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
