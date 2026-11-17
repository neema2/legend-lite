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

    /** The referee and the appeals (their own files under {@link #MAIN}). */
    static final Map<String, String> CLASSES = Map.of(
            "SqlReplayReferee", "testexec/SqlReplayReferee.java",
            "Appeals", "testexec/Appeals.java",
            "Appeal", "testexecutionplan/Appeal.java",
            "PlannedAppeals", "testexecutionplan/PlannedAppeals.java",
            "AppealsByVerdict", "testplan/AppealsByVerdict.java",
            "EngineTextAppeals", "testplan/EngineTextAppeals.java");

    /** Each class's product reachers outside the edge, measured on d-stage 885328d0 (2026-11-17): the census line C2's
     *  named entries, each burning down as the move lands. */
    static final Map<String, Set<String>> REACHERS = Map.of(
            "SqlReplayReferee", Set.of("TestExecution.java", "test/PureTestRunner.java", "test/TestObserver.java",
                    "testexec/SqlTextRuns.java", "testexec/TestRunner.java"),
            "Appeals", Set.of("testexec/BodyRun.java", "testexec/TestRunner.java"),
            "Appeal", Set.of("testexec/RootLoops.java",
                    "testexec/SqlReplayReferee.java", "testexec/SqlTextRuns.java", "testexec/TestRunner.java",
                    "testexecutionplan/HostJson.java",
                    "testexecutionplan/PlannedAppeals.java", "testexecutionplan/TestPlanJson.java",
                    "testplan/EngineTestText.java", "testplan/EngineTextAppeals.java",
                    "testplan/EngineTextPlanning.java", "testplan/JudgmentFacts.java"),
            "PlannedAppeals", Set.of("testexec/TestRunner.java", "testexecutionplan/TestPlanJson.java",
                    "testplan/AppealsByVerdict.java"),
            "AppealsByVerdict", Set.of("testplan/TestPlanner.java"),
            "EngineTextAppeals", Set.of("testplan/AppealsByVerdict.java",
                    "testplan/BodyDriver.java", "testplan/TestPlanner.java"));

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
                if (!rel.equals(c.getValue()) && Pattern.compile("\\b" + c.getKey() + "\\b").matcher(code).find()) {
                    found.computeIfAbsent(c.getKey(), k -> new TreeSet<>()).add(rel);
                }
            }
        }
        Map<String, Set<String>> pinned = new TreeMap<>();
        REACHERS.forEach((k, v) -> pinned.put(k, new TreeSet<>(v)));
        assertEquals(pinned, found, "the referee's and the appeals' reachers moved: a new one is a compatibility path "
                + "entering the product's test road (G6: the edge's alone); one gone is a re-pin, dated, on census C2");
    }
}
