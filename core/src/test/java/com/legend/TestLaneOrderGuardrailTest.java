// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.Repo;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * THE ORDER RULING, PINNED (user, 2026-09-20): product SQL carries NO
 * ordering the query did not ask for — general queries are never slowed
 * down for tests that lean on H2's insertion order. The emulation of that
 * order (the {@code StableScanOrder} pass threading a scan ordinal through
 * frames, positional reads and order-sensitive string joins) is a
 * TEST-LANE FEATURE: it exists, it is never dropped, and it is switched on
 * ONLY by the corpus runner through {@code legend.exec.engineScanOrder}.
 *
 * <p>Three pins, each an exact set so a drift in either direction is loud:
 * <ol>
 *   <li>the scan-order key ({@code SqlExpr.RowOrder}) is minted in exactly
 *       the files that own it — the key owner, the test-lane pass, and the
 *       explode-to-union rewrite that keys its legs (plus the two rewrites
 *       that re-spell an existing key: pivot unqualifying, alias prefixing);</li>
 *   <li>the test-lane pass is installed in exactly one place, behind the
 *       switch, and the switch is READ once in product and SET nowhere in
 *       product;</li>
 *   <li>the always-on assert-boundary order ({@code ScanOrder.stabilize})
 *       is applied from exactly the test-lane pass and the assert canon
 *       wrap — never from a product render path.</li>
 * </ol>
 */
class TestLaneOrderGuardrailTest {

    private static final Pattern ROW_ORDER_MINT =
            Pattern.compile("new (?:com\\.legend\\.sql\\.)?SqlExpr\\.RowOrder\\(");
    private static final Pattern PASS_INSTALL = Pattern.compile("new StableScanOrder\\(\\)");
    private static final Pattern SWITCH_READ = Pattern.compile(
            "Boolean\\.getBoolean\\(\"legend\\.exec\\.engineScanOrder\"\\)");
    private static final Pattern SWITCH_LITERAL = Pattern.compile("legend\\.exec\\.engineScanOrder");
    private static final Pattern STABILIZE = Pattern.compile("ScanOrder\\.stabilize\\(");

    @Test
    void rowOrderIsMintedOnlyByItsOwners() throws IOException {
        assertEquals(new TreeSet<>(List.of(
                // AliasPrefix re-spells an EXISTING key under the frame's alias prefix
                // (never adds one); UnqualifyPivotArgs likewise re-spells
                "com/legend/sql/AliasPrefix.java",
                "com/legend/sql/ScanOrder.java",
                "com/legend/sql/dialect/LateralExplodeToUnion.java",
                "com/legend/sql/dialect/StableScanOrder.java",
                "com/legend/sql/dialect/UnqualifyPivotArgs.java")),
                filesMatching(ROW_ORDER_MINT),
                "SqlExpr.RowOrder is minted outside its owners: a product path is adding a"
                + " scan order the query did not ask for (user ruling 2026-09-20) — the"
                + " emulation belongs to the test-lane pass only");
    }

    @Test
    void emulationIsInstalledOnlyBehindTheSwitch() throws IOException {
        assertEquals(new TreeSet<>(List.of("com/legend/sql/dialect/DuckDb.java")),
                filesMatching(PASS_INSTALL), "StableScanOrder is installed outside DuckDb's pass list");
        assertEquals(new TreeSet<>(List.of("com/legend/sql/dialect/DuckDb.java")),
                filesMatching(SWITCH_LITERAL),
                "legend.exec.engineScanOrder is named outside DuckDb: only the corpus runner"
                + " (spec tests) sets it, only DuckDb's pass list reads it");
        String duck = Files.readString(Repo.module("src/main/java/com/legend/sql/dialect/DuckDb.java"));
        assertEquals(1, count(SWITCH_READ, duck), "the switch is read exactly once");
        assertEquals(1, count(PASS_INSTALL, duck), "the pass is installed exactly once");
        String shape = duck.replaceAll("\\s+", " ");
        assertEquals(true, shape.contains(
                "if (Boolean.getBoolean(\"legend.exec.engineScanOrder\")) { ps.add(new StableScanOrder()); }"),
                "the pass must be installed INSIDE the switch's if-block — never unconditionally");
        assertEquals(0, count(Pattern.compile("setProperty\\(\"legend\\.exec\\.engineScanOrder\""), duck),
                "product never sets the switch");
    }

    @Test
    void assertBoundaryOrderAppliedOnlyFromTheTestSurfaces() throws IOException {
        assertEquals(new TreeSet<>(List.of(
                "com/legend/lowering/CanonicalRenderSql.java",
                "com/legend/sql/dialect/StableScanOrder.java")),
                filesMatching(STABILIZE),
                "ScanOrder.stabilize is applied from a new site: the assert canon wrap and the"
                + " test-lane pass are the only two — a product render path must not order");
    }

    private static TreeSet<String> filesMatching(Pattern p) throws IOException {
        TreeSet<String> out = new TreeSet<>();
        Path root = Repo.module("src/main/java");
        for (Path f : mainSources()) {
            if (p.matcher(Files.readString(f)).find()) {
                out.add(root.relativize(f).toString().replace('\\', '/'));
            }
        }
        return out;
    }

    private static int count(Pattern p, String text) {
        int n = 0;
        Matcher m = p.matcher(text);
        while (m.find()) {
            n++;
        }
        return n;
    }

    private static List<Path> mainSources() throws IOException {
        try (Stream<Path> s = Files.walk(Repo.module("src/main/java"))) {
            List<Path> out = s.filter(f -> f.toString().endsWith(".java")).toList();
            GuardCoverage.assertFloor("TestLaneOrderGuardrailTest", out.size(), 498);
            return out;
        }
    }
}
