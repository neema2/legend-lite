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
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE DELETION CENSUS of docs/DATA_IN_THE_PLAN_2026_11_09.md section 6, P4 (the user, 2026-11-09: "provably deleting
 * all other 17 mechanisms ... that's the real guarantee that we fixed everything"): one row for each of the 17 ways
 * data reached a database on 2026-11-09 (docs/data-in-the-plan-homework/inventory.md), each DELETED (its symbols read
 * zero in every source tree; a symbol that returns fails here), PENDING (the slice that deletes it), or STAYING (not a
 * load at all: the guard that bounds it). A row moves from PENDING to DELETED with its slice, never back; a mechanism
 * with no row fails. The census is complete when no row is PENDING (L8).
 */
@Tag("census")
class DataMechanismCensusTest {

    /** A mechanism's state: deleted (with the symbols that must read zero), pending (its slice), or staying (its
     *  guard). */
    sealed interface State permits Deleted, Pending, Staying {
    }

    /** Deleted by {@code slice}: each of {@code symbols} reads zero in every source tree. */
    record Deleted(String slice, List<String> symbols) implements State {
    }

    /** Deleted by {@code slice}, not yet. */
    record Pending(String slice) implements State {
    }

    /** Not a load: it stays, bounded by {@code guard}. */
    record Staying(String guard) implements State {
    }

    record Row(String id, String mechanism, State state) {
    }

    /** The 17, in the inventory's order. */
    static final List<Row> ROWS = List.of(
            // L3, 2026-11-09: a project's Data rows are its target's ONCE load (S145), made once where the platform makes
            // the project's database and kept; the project names its key and refusal alone
            new Row("M1", "###Data into the server's project database (Project.made)", new Deleted("L3", List.of(
                    // the project's own fill steps, carried and read
                    "project.made()",
                    "o.put(\"made\"",
                    "getArr(\"made\")",
                    // the project stripped to its key on every open
                    "keyOnly("))),
            // L2, 2026-11-09: a target's declared test data is its PER_ACQUISITION loads, written once per plan and run
            // by the one deliverer every load takes (PlanRunner.seed); the target's setup and its own runner are gone
            new Row("M2", "a connection's declared test data on the product path (Target.setup)", new Deleted("L2",
                    List.of(
                            // the target's setup steps and the reader of them
                            "target.setup()",
                            "\"setup\", t.setup()",
                            // the setup's own runner and the sources that ran it
                            "PlanRunner.setUp(",
                            "setUpIn(",
                            "PlanSessions.setUp(",
                            "refuseSetup("))),
            // L2, 2026-11-09: the test road's seeds are loads made by the one load planner (LoadPlanner, one identity
            // scheme); the plan's connection description no longer carries the raw test data the loads carry. The
            // referee's copy of a seed's rows (the edge's SeedTexts) is M15's, replayed from the plan's loads at L8
            new Row("M3", "test data on the test road (the seed Load, SeedTexts, the raw text in a target)",
                    new Deleted("L2", List.of(
                            // the seeds' own identity scheme, folded into LoadPlanner.identity
                            "Seeds.key(",
                            // the plan's copy of a connection's raw test data, written and read
                            "x.testDataSetupCsv(), x.testDataSetupSqls()",
                            "putIfSet(o, \"testDataSetupCsv\"",
                            "o.optionalString(\"testDataSetupCsv\")",
                            // the node a seed was until L1
                            "ExecutionPlan.Setup"))),
            new Row("M4", "service-suite data through a synthetic connection", new Pending("L2")),
            new Row("M5", "the system metamodel store on the test road (SystemRoutes.seeds)", new Pending("L4")),
            new Row("M6", "the system metamodel store on the product road", new Pending("L4")),
            // L2, 2026-11-09: a seed form mapped over executeInDb is a LOAD at its place (AT_STEP, made by LoadPlanner,
            // its statements as the lambda's executeInDb sends them), never the program's effect script
            new Row("M7-seed", "the seed forms (setUpDataSQLs and its kin) planned through CsvSeed", new Deleted("L2",
                    List.of(
                            // a form's statements added to the open effect script
                            "script.addAll(EffectPlanner.statements("))),
            // L1, 2026-11-09: loadCsvToDbTable is a File load by reference; the path it put in a statement is gone
            new Row("M8", "loadCsvToDbTable: the planning machine's path in a statement (D3)", new Deleted("L1", List.of(
                    // the dialects' reader over a path written into the statement
                    "csvSource(",
                    "\"CSVREAD(\" + stringLit",
                    "read_csv(\" + stringLit",
                    // the MIR record's path
                    "lc.file()",
                    // the planner's resolver function and its reader
                    "resourceFile(",
                    "options.resources()",
                    "Function<String, java.nio.file.Path> resources"))),
            new Row("M9", "package setups and fixtures the harness provides between plan and run", new Pending("L5")),
            new Row("M10", "JSON model data: data: inlined, file: as a path in SQL", new Pending("L6")),
            new Row("M11", "a query's literal relations and instances, and their writes",
                    new Staying("P9: a statement's literal rows come from lowering alone")),
            new Row("M12", "model-derived constants in statements",
                    new Staying("P9: constants come from the compiled model alone")),
            new Row("M13-output", "the test-data generator's rows handed on as seed text", new Pending("L8")),
            new Row("M14", "the browser tab, Python and DataCube load data outside any plan", new Pending("L7")),
            new Row("M15", "harnesses seed by JDBC; the referee's mirror replays seed texts (the edge's SeedTexts)",
                    new Pending("L8")),
            new Row("M16", "parameters, keyed keys, counts, host rows",
                    new Staying("P9: parameters come from the caller alone")),
            new Row("M17", "data parsed and never loaded", new Pending("L8")),
            new Row("M7-authored", "executeInDb: SQL the user wrote",
                    new Staying("P9: an effect's statements are the author's text, folded")));

    /** Every source tree the census reads (the census target's declared files: core, spec, pct, parser-equivalence,
     *  native, testing, warehouse, wasm), each of the trees a mechanism lived in required to be read. */
    private static List<Path> sources() {
        for (String required : List.of("core/src/main/java", "core/src/test/java", "spec/src/test/java",
                "pct/src")) {
            assertTrue(!SourceFiles.under(required).isEmpty(), "the census read nothing under " + required);
        }
        List<Path> out = new ArrayList<>();
        for (String module : List.of("core", "spec", "pct", "parser-equivalence", "native", "testing", "warehouse",
                "wasm")) {
            out.addAll(SourceFiles.under(module));
        }
        // the census names the symbols it counts, and is no use of them
        out.removeIf(p -> p.getFileName().toString().equals("DataMechanismCensusTest.java"));
        return out;
    }

    @Test
    void everyMechanismHasItsRow() {
        Set<String> ids = ROWS.stream().map(Row::id).collect(Collectors.toCollection(TreeSet::new));
        assertEquals(ROWS.size(), ids.size(), "a mechanism has two rows: " + ROWS);
        // the inventory's 17: M7 counted once, in its two halves (the seed forms a load, the author's statements not)
        Set<String> want = new TreeSet<>(Arrays.asList("M1", "M2", "M3", "M4", "M5", "M6", "M7-seed", "M7-authored",
                "M8", "M9", "M10", "M11", "M12", "M13-output", "M14", "M15", "M16", "M17"));
        assertEquals(want, ids, "the census's rows are the inventory's mechanisms");
    }

    @Test
    void aDeletedMechanismsSymbolsReadZero() throws IOException {
        List<Path> files = sources();
        assertTrue(files.size() > 1000, "the census read too few sources to prove anything: " + files.size());
        List<String> found = new ArrayList<>();
        for (Path f : files) {
            if (!f.toString().endsWith(".java")) {
                continue;
            }
            String text = Files.readString(f, StandardCharsets.UTF_8);
            for (Row row : ROWS) {
                if (row.state() instanceof Deleted d) {
                    for (String symbol : d.symbols()) {
                        if (text.contains(symbol)) {
                            found.add(row.id() + " (" + d.slice() + ") '" + symbol + "' in " + f);
                        }
                    }
                }
            }
        }
        assertEquals(List.of(), found, "a deleted mechanism's symbol is back");
    }

    @Test
    void theDeletedRowsOnlyGrow() {
        // shrink-only in reverse: the rows deleted so far (2026-11-09, L1: M8; 2026-11-09, L2: M2, M3 and M7-seed;
        // 2026-11-09, L3: M1). A row leaves PENDING with its slice and this count rises with it, in the same change, with its date and slice
        long deleted = ROWS.stream().filter(r -> r.state() instanceof Deleted).count();
        assertEquals(5, deleted, "the deleted rows changed: move this count with the slice that deletes one");
    }
}
