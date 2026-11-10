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
 * that mechanism reads zero in every main source tree; one that returns fails here. That the runner sends the plan's own
 * text, character for character, is {@code planner.SentStatementsAreThePlansTest}'s (the re-audit's B-6).
 */
@Tag("census")
class TypeHoleDeletedTest {

    /** The hole's records, the writer's and the dialects' hooks, the runner's filling, and the value-typing they read. */
    private static final List<String> SYMBOLS = List.of("TypeHole", "typeHole(", "holeType(", "ValueTyping",
            "sql.ValueKind", "ExecutionPlan.ValueKind", "TypeSpelling", "one.hole()", "\"typeHole\"", "\"byValue\"");

    @Test
    void theTypeHolesSymbolsReadZero() throws IOException {
        List<Path> files = new ArrayList<>();
        // every main tree a session's statements are written in: the drivers' own (DuckDB's bulk loader, Postgres's
        // copy) included (the re-audit's B-6, 2026-11-09)
        for (String tree : List.of("core/src/main/java", "core/src/main/mongodb", "core/src/main/duckdb",
                "core/src/main/postgres", "wasm")) {
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

    /** The deleted records and methods are gone by type, not only by spelling (the M1 tenets audit's note: a census of
     *  spellings passes a re-spelling): each class does not load, each method is not declared. */
    @Test
    void theDeletedTypesAndMethodsAreGone() throws Exception {
        for (String gone : List.of("com.legend.executionplan.ExecutionPlan$TypeHole",
                "com.legend.executionplan.ExecutionPlan$TypeSpelling", "com.legend.executionplan.ExecutionPlan$ValueKind",
                "com.legend.sql.dialect.RenderedStatement$TypeHole", "com.legend.sql.ValueTyping", "com.legend.sql.ValueKind",
                "com.legend.executionplan.ExecutionPlan$LoadStep$Delivery$DatabaseReads",
                "com.legend.executionplan.ExecutionPlan$Setup",
                // the bulk delivery, deleted 2026-11-10 by the user's ruling of 2026-11-09 (DuckDB reads a file
                // natively); the streamed one was pinned gone here the same day and came back by the user's ruling of
                // 2026-11-10, "Okay I guess that can be used for databases that have no csv read then"
                "com.legend.executionplan.ExecutionPlan$LoadStep$Delivery$Bulk")) {
            org.junit.jupiter.api.Assertions.assertThrows(ClassNotFoundException.class, () -> Class.forName(gone), gone);
        }
        for (var m : Class.forName("com.legend.exec.PlanRunner").getDeclaredMethods()) {
            assertTrue(!List.of("setUp", "setUpIn", "typeOf", "kindOf").contains(m.getName()), "PlanRunner." + m.getName());
        }
        // Statements.readsFile was pinned gone here from L2 (the streamed delivery) until 2026-11-10: the user's ruling
        // of 2026-11-09 (the database reads a file, Delivery.Reads) brought it back; DataLoads' cells, pinned gone the
        // same day, came back by the user's ruling of 2026-11-10 (the streamed delivery, for a database with no reader
        // that reads the format as upstream does)
        for (var m : Class.forName("com.legend.executionplan.ExecutionPlan$Target").getDeclaredMethods()) {
            assertTrue(!m.getName().equals("setup"), "Target.setup");
        }
    }
}
