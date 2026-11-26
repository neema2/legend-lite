// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * THE REFEREE AND THE APPEALS, AND WHO REACHES THEM (the separation census's C2,
 * docs/records/f-separation-census-2026-11-17.md, 2026-11-17). G6: "the appeals and the referee are allowed only at the
 * edge". Today they live in the product's test packages and the test road's runner, planner and plan JSON read them;
 * their move to the edge is E-Java's (C2, L). Until it lands, each product file that names one is pinned here exactly
 * -- a NAMED ENTRY of the census line, dated, shrink-only: a new reacher fails (a compatibility path entering the
 * product's test road), a reacher gone is a re-pin with its date. The edge ({@code com.legend.edge}) reaches them
 * freely; it is where they go.
 */
@Tag("guardrail")
class RefereeAppealsCensusTest {

    private static final Path MAIN = Path.of("core/src/main/java/com/legend");

    /** The edge's test harness's sources (2026-11-26, the edition program's step 9, slice 1: the referee's SPI, the held
     *  appeals and the plan's appeals by number are test machinery, moved there from the edge's product sources). */
    private static final String HARNESS = "core/src/edge_harness/java/com/legend/";

    /** {@link #MAIN} as the prefix of a path from the repository root, '/'-separated on every platform. */
    private static final String MAIN_DIR = "core/src/main/java/com/legend/";

    /** The referee and the appeals, each with its own file by its path from the repository root (2026-11-26: until
     *  then a path under {@link #MAIN}; three of the files left it for {@link #HARNESS}, and the paths moved with
     *  them so each names a file that is there). */
    static final Map<String, String> CLASSES = Map.of(
            "SqlReplayReferee", HARNESS + "edge/SqlReplayReferee.java",
            "Appeals", HARNESS + "edge/Appeals.java",
            "Appeal", MAIN_DIR + "edge/Appeal.java",
            "PlannedAppeals", HARNESS + "edge/PlannedAppeals.java",
            "AppealsByVerdict", MAIN_DIR + "testplan/AppealsByVerdict.java",
            "EngineTextAppeals", MAIN_DIR + "testplan/EngineTextAppeals.java");

    /** Each class's product reachers outside the edge: NONE (2026-11-17, census C2 closed -- the referee and the
     *  appeals moved to com.legend.edge, the run reaching them through the run hook EdgeRun in the referee's slot, the
     *  planner holding the edge's records by number, opaque; AppealsByVerdict and EngineTextAppeals are gone, their
     *  generic bookkeeping the product's NumberedExtensions). Shrink-only: any reacher is a regression. */
    static final Map<String, Set<String>> REACHERS = Map.of(
            "SqlReplayReferee", Set.of(), "Appeals", Set.of(), "Appeal", Set.of(), "PlannedAppeals", Set.of(),
            "AppealsByVerdict", Set.of(), "EngineTextAppeals", Set.of());

    @Test
    void theRefereeAndTheAppealsAreReachedByTheirNamedEntriesAlone() throws IOException {
        List<Path> files;
        try (var walk = Files.walk(MAIN)) {
            files = walk.filter(p -> p.toString().endsWith(".java"))
                    .filter(p -> !MAIN.relativize(p).startsWith("edge")).sorted().toList();
        }
        GuardCoverage.assertFloor("RefereeAppealsCensusTest", files.size(), 900);
        Map<String, Set<String>> found = new TreeMap<>();
        for (Path f : files) {
            String rel = MAIN.relativize(f).toString();
            String code = EdgeOnlyNodesCensusTest.code(Files.readString(f));
            for (var c : CLASSES.entrySet()) {
                if (!(MAIN_DIR + rel.replace('\\', '/')).equals(c.getValue()) && Pattern.compile("\\b" + c.getKey() + "\\b").matcher(code).find()) {
                    found.computeIfAbsent(c.getKey(), k -> new TreeSet<>()).add(rel);
                }
            }
        }
        Map<String, Set<String>> pinned = new TreeMap<>();
        REACHERS.forEach((k, v) -> {
            if (!v.isEmpty()) {
                pinned.put(k, new TreeSet<>(v));
            }
        });
        assertEquals(pinned, found, "the referee's and the appeals' reachers moved: a new one is a compatibility path "
                + "entering the product's test road (G6: the edge's alone); one gone is a re-pin, dated, on census C2");
    }
}
