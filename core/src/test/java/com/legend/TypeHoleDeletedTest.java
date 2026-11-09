// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.SourceFiles;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE TYPE HOLE STAYS DELETED (the total wall's M2, P4 of docs/GOALS_OF_RECORD_2026_11_07.md, D1 ruled 2026-11-07;
 * deleted 2026-11-09): a run-time parameter's placeholder is typed by its declared Pure type when the plan is made, and
 * the runner sends a plan's text as it is. Until then the dialect left a hole in H2's statement (and a date-time's on
 * every database) that the runner filled with the type of each value's literal -- SQL text written at run. Each symbol of
 * that mechanism reads zero in every main source tree; one that returns fails here.
 */
@Tag("census")
class TypeHoleDeletedTest {

    /** The hole's records, the writer's and the dialects' hooks, the runner's filling, and the value-typing they read. */
    private static final List<String> SYMBOLS = List.of("TypeHole", "typeHole(", "holeType(", "ValueTyping",
            "sql.ValueKind", "ExecutionPlan.ValueKind", "TypeSpelling", "one.hole()", "\"typeHole\"", "\"byValue\"");

    @Test
    void theTypeHolesSymbolsReadZero() throws IOException {
        List<Path> files = new ArrayList<>();
        for (String tree : List.of("core/src/main/java", "core/src/main/mongodb", "wasm")) {
            files.addAll(SourceFiles.under(tree));
        }
        assertTrue(files.size() > 500, "too few sources read to prove anything: " + files.size());
        List<String> found = new ArrayList<>();
        for (Path f : files) {
            if (!f.toString().endsWith(".java")) {
                continue;
            }
            String text = Files.readString(f, StandardCharsets.UTF_8);
            for (String symbol : SYMBOLS) {
                if (text.contains(symbol)) {
                    found.add("'" + symbol + "' in " + f);
                }
            }
        }
        assertEquals(List.of(), found, "the type hole is back");
    }
}
