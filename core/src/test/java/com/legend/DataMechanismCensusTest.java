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
 * zero in every source tree; a symbol that returns fails here), BOUNDED (kept by a ruling: the part that reached a plan
 * deleted, its symbols reading zero, the rest held by a named guard), PENDING (the slice that deletes it; what its
 * slices removed so far reads zero, {@link #REMOVED_SO_FAR}), or STAYING (not a load at all: the guard that bounds
 * it). The rule: a row's state moves only with its slice and a dated note -- toward DELETED, or to BOUNDED by a
 * decision recorded with whoever made it -- and a deleted symbol that returns fails here; a mechanism with no row
 * fails. The census is complete when no row is PENDING (L8).
 */
@Tag("census")
class DataMechanismCensusTest {

    /** A mechanism's state: deleted (with the symbols that must read zero), bounded (its deleted part and its guard),
     *  pending (its slice), or staying (its guard). */
    sealed interface State permits Deleted, Bounded, Pending, Staying {
    }

    /** Kept by a ruling, its part that reached a plan deleted ({@code symbols} read zero) and the rest bounded by
     *  {@code guard}: neither deleted nor pending (the M1 tenets audit's finding 2, 2026-11-09). */
    record Bounded(String slice, List<String> symbols, String guard) implements State {
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
            // L3, 2026-11-09: a project's Data rows are its target's ONCE load (S171), run on every database the platform
            // makes and refused on any other (the M1 audit's finding 1). DELETED at L6c, 2026-11-10 (the orchestrator's
            // ruling): the fill reaches a plan only as that ONCE load, its rows inline or, above 16 KB, in the load's
            // bundle -- no other way (since A2, 2026-11-10, a content blob of inserts whatever its size; where its
            // bytes travel is the plan writer's). What stays is the design, not residue: PlanSessions.made is exec establishing the
            // plan's load where it makes the database, and Project.refused a refusal carried as data, raised where the
            // database is made (slice 5's reason: at planning it failed every query of the model). Proof:
            // ProjectRowsTest.aProjectsDataIsItsTargetsOnceLoadWrittenOnce and aLargeFillTravelsInTheBundle
            new Row("M1", "###Data into the server's project database (Project.made)", new Deleted("L6c", List.of(
                    // L3: the project's own fill steps, carried and read; the project stripped to its key on every open
                    "project.made()", "o.put(\"made\"", "getArr(\"made\")", "keyOnly(",
                    // L6c: a plan's fill as statement text (the text form stays the host's own, ProjectData.of: M14,
                    // until L7)
                    "ProjectData.fill("))),
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
                            "refuseSetup(",
                            // the setup's own step record, replaced by LoadStep (the re-audit's small item, 2026-11-09)
                            "interface SetupStep",
                            "SetupStep.Statement(",
                            "SetupStep.Rows("))),
            // L2, 2026-11-09: the test road's seeds are loads made by the one load planner (LoadPlanner, one identity
            // scheme), and the plan's connection description no longer carries the raw test data. PENDING still (the
            // M1 tenets audit's finding 2): Seeds.planned remains the seeds' own path to their steps, and the referee's
            // copy of a seed's rows (the edge's SeedTexts) is replayed from the plan's loads only at L8. Its deleted
            // symbols so far read zero (theRemovedSymbolsReadZero)
            new Row("M3", "test data on the test road (the seed Load, SeedTexts, the raw text in a target)",
                    new Pending("L8 (SeedTexts; Seeds.planned's own path)")),
            // L2, 2026-11-09 (the orchestrator's decision: option 1, upstream's own harness shape): BOUNDED, not deleted (the M1 tenets audit's finding 2): the
            // harness keeps the upstream-faithful overlay (a synthetic connection carrying the suite's CSV, as
            // legend-engine's service test runner builds one, and RuntimeKey, a cache of overlay contexts by value); its
            // data reaches a plan only as a load, which the guard proves
            new Row("M4", "service-suite data through a synthetic connection (the overlay kept; its data a load)",
                    new Bounded("L2", List.of(), "ServiceTestProvisionKeyTest.aSuitesDataReachesThePlanOnlyAsALoad")),
            new Row("M5", "the system metamodel store on the test road (SystemRoutes.seeds)", new Pending("L4 (a server"
                    + " engine's whole store per graph: the catalog design's check 3)")),
            // L4's product half (2026-11-10): form 1 deleted -- every in-process product query reads the process's
            // catalog, the server's project road too (the server holds no model: each request is the one-request
            // case). Forms 2 and 3 (a server engine's TEMP tables, a server with none's inline rows) wait on a server
            // engine's catalog, M5's check 3, not designed
            new Row("M6", "the system metamodel store on the product road", new Pending("M5's server catalog (forms 2"
                    + " and 3: a server engine's session TEMP tables, a server with none's inline rows)")),
            // PENDING (the M1 correctness audit's C3, 2026-11-09): a seed form mapped over executeInDb was made a load
            // at L2 and put back the same day -- as a load it lost the effect script's transaction bracket and abort, its
            // ledger on failure and the B8 refusal. Its move is designed with the runner's transaction for loads
            new Row("M7-seed", "the seed forms (setUpDataSQLs and its kin) planned through CsvSeed", new Pending("L5b"
                    + " (2026-11-10, re-sliced: a load step inside the effect script, keeping its bracket, abort, ledger and"
                    + " B8 refusal; L5 itself was the fixtures, option C)")),
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
                    "Function<String, java.nio.file.Path> resources",
                    // 2026-11-10, the user's ruling of 2026-11-09 ("reject options we cannot support instead of
                    // building our own streamer"): DuckDB reads the file, and the bulk delivery of its cells is gone
                    // (the streamed delivery is back for every database with no reader that reads the format as
                    // upstream does: the user's ruling of 2026-11-10)
                    "Delivery.Bulk",
                    "csvStreamed",
                    // 2026-11-10, A3 (docs/DATA_DELIVERY_DESIGN_2026_11_10.md §1.3, option 1, the user's go on the
                    // design): a file is read by its database or refused by name -- the runner's streamed cells, the
                    // product's split of them and one row's insert are gone
                    "Delivery.Streamed",
                    "CsvLoad.Streamed",
                    "rowsInsert(",
                    "DataLoads.cells(",
                    "seeding.streams("))),
            // L5, 2026-11-10 (option C, upstream's shape: the orchestrator's word): (b) the on-demand fixtures the
            // harness provided between a body's plan and its run are DELETED -- measured first with the provision
            // skipped (a patch never committed): the corpus_duckdb and corpus_h2 rosters byte-identical and
            // stress_suites and stress_suites_h2 passing, so no verdict needed one. (a) a package's setups, run at the
            // package's start as the engine runs them, are BOUNDED: each a program planned whole and run by its plan
            // (TestExecution.runFixture, a door of one planning call and one run call)
            new Row("M9", "package setups and fixtures the harness provides between plan and run", new Bounded("L5",
                    List.of("fixturesByStore", "provideFixtures", "provideFixture(", "isolateFixture",
                            "fixtureIsolated", "fixtureProvided", "FixtureAside", "setFixtureAside", "replayAside",
                            "replayRestore", "fixture_aside_postgres_test"),
                    "ArchitectureTest.theDoorsOnlyCompose")),
            // L6, 2026-11-10 (the user's rulings: a file: source is a pointer, as a table is; data written in the
            // model travels with the plan, through one load at every size): the file: source's path pasted into the
            // statement, and the data: payload as a SQL literal
            // REOPENED 2026-11-10 (A5, the census kept honest, docs/DATA_DELIVERY_DESIGN_2026_11_10.md §5): a runtime
            // value's JsonModelConnection payload still reaches the statement as literal rows (resolver.JsonSourceFrame's
            // objectTexts and its VALUES), which L6 left standing and counted deleted. Its move onto the data: road was
            // measured at A5 and costs 20 DuckDB and 10 H2 corpus tests today: the test road's judges render statements
            // that take no data: rewrite, the referee replays on H2, and H2 holds no data: table. It moves with B2a/B2b.
            // L6's deletions stay guarded (REMOVED_SO_FAR)
            new Row("M10", "JSON model data: data: inlined, file: as a path in SQL", new Pending("B2a")),
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

    /** What a PENDING row's slices deleted so far, each symbol reading zero as a deleted row's do (the M1 tenets
     *  audit's finding 2, 2026-11-09: a row is DELETED only when its whole list is). */
    static final java.util.Map<String, List<String>> REMOVED_SO_FAR = java.util.Map.of(
            // L6: the file: source's path pasted into the statement, and DuckDB's data: payload as a SQL literal
            "M10", List.of("read_json_objects(\" + stringLit", "java.nio.file.Path.of(java.net.URI.create(url))",
                    "SELECT unnest(CAST(\" + stringLit(content)"),
            // L2: the seeds' own identity scheme (folded into LoadPlanner.identity), the plan's copy of a connection's
            // raw test data written and read, and the node a seed was until L1
            "M3", List.of("Seeds.key(", "x.testDataSetupCsv(), x.testDataSetupSqls()", "putIfSet(o, \"testDataSetupCsv\"",
                    "o.optionalString(\"testDataSetupCsv\")", "o.put(\"testDataSetupSqls\"",
                    "o.strings(\"testDataSetupSqls\")", "ExecutionPlan.Setup"),
            // L4a: the system store's load named by a constant, and the walk's own seeding door; L4b: the whole store
            // seeded per graph on an in-process engine (the catalog's platform rows made once, a model's registered)
            "M5", List.of("Load.SYSTEM_STORE", "seedSystemStore(", "seedSteps(", "route.graphKey()"),
            // L4's product half (2026-11-10): form 1, the metamodel's tables made in an in-process query's own database
            // as the target's setup -- every in-process product query reads the process's catalog
            "M6", List.of("setup.addAll(Ddl.metamodelSteps", "metamodel.setup()", "!projectDatabase && dialect.inProcess()"));

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
                List<String> symbols = switch (row.state()) {
                    case Deleted d -> d.symbols();
                    case Bounded b -> b.symbols();
                    case Pending p -> REMOVED_SO_FAR.getOrDefault(row.id(), List.of());
                    case Staying st -> List.of();
                };
                for (String symbol : symbols) {
                    if (text.contains(symbol)) {
                        found.add(row.id() + " '" + symbol + "' in " + f);
                    }
                }
            }
        }
        assertEquals(List.of(), found, "a deleted mechanism's symbol is back");
    }

    @Test
    void theDeletedRowsOnlyGrow() {
        // shrink-only in reverse: the rows deleted so far (2026-11-09, L1: M8; 2026-11-09, L2: M2 -- M1, M3, M4 and
        // M7-seed counted deleted until the M1 audits the same day: M1, M3 and M7-seed PENDING, M4 BOUNDED by its
        // ruling). A row leaves PENDING with its slice and this count rises with it, in the same change, with its date and slice
        long deleted = ROWS.stream().filter(r -> r.state() instanceof Deleted).count();
        // 2 -> 3 (2026-11-10, L6: M10 deleted, its file: path in SQL and its data: literal); 3 -> 4 (2026-11-10,
        // L6c: M1 deleted, the fill's statement text, the fill only a ONCE load); 4 -> 3 (2026-11-10, A5: M10 REOPENED
        // -- not a deletion undone, a count corrected: the JSON source frame's literal rows were never deleted, and the
        // design's §5 asks the census kept honest; its row says why and when it moves)
        assertEquals(3, deleted, "the deleted rows changed: move this count with the slice that deletes one");
    }
}
