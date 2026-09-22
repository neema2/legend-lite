// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.Repo;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * THE PARKED-WORK LEDGER (2026-09-15). Work deliberately NOT done yet is
 * recorded in {@code docs/PARKED_WORK_LEDGER.md} — one row per item, each
 * with the date, who parked it, why, the cost of leaving it parked, and the
 * acceptance test that closes it — and every row is ANCHORED here.
 *
 * <p>An anchor is a mechanical fact about today's code that holds only while
 * the item is still parked: the wall a missing capability raises, the single
 * call site a missing pass would displace. It turns "we'll remember" into a
 * red test. When the situation changes — someone builds the capability, or
 * the shape drifts — this goes red and the row must be CLOSED or restated in
 * the same commit.
 *
 * <p>A green anchor is not approval: each row is a debt with a stated price.
 * Rows leave by being fixed, never by being loosened.
 */
class ParkedWorkLedgerTest {

    /** Ledger row id &rarr; (what the anchor matches, the product files that
     * may contain it). The file LIST is the pin: a new site, a removed site
     * or a moved site all fail. */
    private static final Map<String, Anchor> REGISTER = new TreeMap<>(Map.of(
            // PARK-1: cross-store associations require ONE shared predicate;
            // the engine's model is per-end. The wall is the anchor, and its
            // two sites also pin the duplicated implementation.
            "PARK-1 xstore per-end predicates",
            new Anchor("has direction-specific conditions",
                    List.of("MappingNormalizer.java", "XStorePureEnds.java")),
            // PARK-2: no common-subexpression pass in the normal lowering
            // path, so a union read twice is built twice. The CTE builder is
            // reachable ONLY from the opt-in parity post-processor.
            "PARK-2 union common-subexpression pass (call site)",
            new Anchor("extractSubqueriesAsCtes\\(", List.of("SqlPostProcessors.java")),
            "PARK-2 union common-subexpression pass (construction)",
            // leg 3.1 (2026-09-18): VerdictSql builds a WITH too — the
            // database-mode verdict statement (two side CTEs + one verdict
            // row), NOT a common-subexpression pass; PARK-2 stays parked
            // leg 3.4 step 2 (2026-09-20): SqlWith.prepend hoists a statement's
            // frame CTEs to its head — a construction helper, not a pass
            new Anchor("new SqlWith\\(", List.of("SqlRewriter.java", "SqlWith.java", "VerdictSql.java")),
            // PARK-3: the relational toString renders as the DATABASE's cast
            // in the engine; ours passes through to pure's ISO form. The
            // obvious arm collapses multiplicity (it LOST a corpus row), so
            // NOTHING dispatches on the member — that is the anchor.
            "PARK-3 toString emits pure's ISO form, not the database's cast",
            new Anchor("DynaFn\\.TO_STRING", List.of()),
            // PARK-4: the ~groupBy wrapper. The prune's refusal to touch a
            // grouped select is NOT the cause (the engine projects those
            // columns too — lifting it LOST a row); the wrapper needs a
            // select-merge pass. The refusal is the anchor.
            "PARK-4 the ~groupBy wrapper projects unread columns",
            new Anchor("projections\\(\\)\\.isEmpty\\(\\) \\|\\| sel\\.distinct\\(\\)\\s*\\n\\s*\\|\\| !sel\\.groupBy\\(\\)",
                    List.of("SubselectPrune.java"))));

    private record Anchor(String pattern, List<String> files) {
    }

    @Test
    @DisplayName("every parked row's anchor still holds (docs/PARKED_WORK_LEDGER.md)")
    void parkedRowsStillHold() throws IOException {
        List<Path> sources = mainSources();
        for (var row : REGISTER.entrySet()) {
            Anchor anchor = row.getValue();
            Pattern p = Pattern.compile(anchor.pattern());
            List<String> found = new java.util.ArrayList<>();
            for (Path f : sources) {
                if (p.matcher(Files.readString(f)).find()) {
                    found.add(f.getFileName().toString());
                }
            }
            java.util.Collections.sort(found);
            List<String> expected = anchor.files().stream().sorted().toList();
            assertEquals(expected, found, () -> "PARKED WORK CHANGED — " + row.getKey()
                    + ": the anchor /" + anchor.pattern() + "/ no longer sits in exactly "
                    + expected + ". If you CLOSED this item, delete its row here and in"
                    + " docs/PARKED_WORK_LEDGER.md in this commit. If you moved the code,"
                    + " re-point the row. A parked item is a debt with a stated price —"
                    + " it never leaves by being loosened.");
        }
    }

    private static List<Path> mainSources() throws IOException {
        try (Stream<Path> s = Files.walk(Repo.module("src/main/java"))) {
            List<Path> out = s.filter(p -> p.toString().endsWith(".java")).toList();
            GuardCoverage.assertFloor("ParkedWorkLedgerTest", out.size(), 490);
            return out;
        }
    }
}
