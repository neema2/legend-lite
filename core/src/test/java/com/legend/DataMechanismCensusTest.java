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
                            "SetupStep.Rows(",
                            // A2, 2026-11-10 (docs/DATA_DELIVERY_DESIGN_2026_11_10.md §1.1; the A checkpoint's tenets
                            // audit's 4a): model data -- a connection's test data, a project's Data, a seed's rows -- is
                            // statements in a content blob; the rows step, the staged delivery, the bulk loaders and
                            // DuckDB's appender are gone (their names in code shapes: the history comments keep words)
                            "LoadStep.Rows",
                            "Delivery.Staged",
                            "class BulkLoad",
                            "interface BulkLoad",
                            "class DuckDbAppenderLoad",
                            "DuckDbAppenderLoad.",
                            "class ContentRows",
                            "loadsRowsInBulk(",
                            "RowLoad.staged",
                            ".loadsRows(",
                            "Statements.load("))),
            // L2, 2026-11-09: the test road's seeds are loads made by the one load planner (LoadPlanner, one identity
            // scheme), and the plan's connection description no longer carries the raw test data. B2a, 2026-11-11: the
            // seeds' steps are the load planner's (LoadPlanner.seeds, sharing the product's declared-data maker,
            // LoadPlanner.declared), Seeds.planned gone. DELETED by B2b, 2026-11-11: the referee's copy of a seed is a
            // load (the edge's RefereeLoad: the plan's steps in the referee's dialect, the rows a content blob), and
            // SeedTexts, emptied by 57e2c445, deleted by its creator (00e67e38)
            new Row("M3", "test data on the test road (the seed Load, SeedTexts, the raw text in a target)",
                    new Deleted("B2b", List.of("Seeds.key(", "Seeds.planned(",
                            "x.testDataSetupCsv(), x.testDataSetupSqls()", "putIfSet(o, \"testDataSetupCsv\"",
                            "o.optionalString(\"testDataSetupCsv\")", "o.put(\"testDataSetupSqls\"",
                            "o.strings(\"testDataSetupSqls\")", "ExecutionPlan.Setup", "new SeedTexts(",
                            "record SeedTexts(", "refereeCopy("))),
            // L2, 2026-11-09 (the orchestrator's decision: option 1, upstream's own harness shape): BOUNDED, not deleted (the M1 tenets audit's finding 2): the
            // harness keeps the upstream-faithful overlay (a synthetic connection carrying the suite's CSV, as
            // legend-engine's service test runner builds one, and RuntimeKey, a cache of overlay contexts by value); its
            // data reaches a plan only as a load, which the guard proves
            new Row("M4", "service-suite data through a synthetic connection (the overlay kept; its data a load)",
                    new Bounded("L2", List.of(), "ServiceTestProvisionKeyTest.aSuitesDataReachesThePlanOnlyAsALoad")),
            // L4a/L4b (2026-11-09): the system store's load named by its content, the walk's own seeding door gone, the
            // whole store seeded per graph on an in-process engine replaced by the process's catalog (the platform's rows
            // made once, a model's registered in the run's turn). DELETED by B4 (2026-11-11, the user's ruling "B4 sounds
            // good for now", reconfirmed that day: every statement reading the store runs on the in-process catalog): a
            // test body, a judge's side and a statement routed beside its body on a SERVER database read the catalog of
            // the platform's own engine, written in its dialect -- the server's form (the host's system connection, the
            // store as its session's temporary tables read through renames) deleted. Proof: CatalogOnEveryDatabaseTest,
            // DataDeliveryMatrixPostgresTest's TEST_BODY and SERVICE catalog cells
            new Row("M5", "the system metamodel store on the test road", new Deleted("B4", List.of(
                    "Load.SYSTEM_STORE", "seedSystemStore(", "seedSteps(", "route.graphKey()",
                    // B4: the server's system connection, its options and the routes' renames
                    "SystemRoutes.renames(", "SystemRoutes.routed(", "SystemRoutes.inProcess(", "openSystem(engine",
                    "systemConnections.get(engine)", "options.systemConnections()",
                    "public ExecuteOptions withSystemConnections",
                    "Map.of(backend.planType(), backend::openSystem)"))),
            // L4's product half (2026-11-10): form 1 deleted -- every in-process product query reads the process's
            // catalog. DELETED by B4 (2026-11-11, the same ruling): forms 2 and 3 -- a server engine's session TEMP
            // tables, a server with none's inline rows -- deleted; a query or a writing program reading the store on any
            // database runs on the catalog, written in its dialect, and one reading the store beside another source is
            // refused by name. Proof: CatalogOnEveryDatabaseTest, PlanMakerTest
            // .theMetamodelStoresTablesAreTheInProcessCatalogsOnEveryDatabase, DataDeliveryMatrixPostgresTest's READ and
            // WRITE catalog cells
            new Row("M6", "the system metamodel store on the product road", new Deleted("B4", List.of(
                    "setup.addAll(Ddl.metamodelSteps", "metamodel.setup()", "!projectDatabase && dialect.inProcess()",
                    // B4: the server's two forms -- the session's temporary tables and the query's inline rows
                    "metamodel.session()", "metamodel.read(", "Metamodel.NONE", "Ddl.metamodelTemporary(",
                    "metamodelTemporaryRows(", "Ddl.systemTable(", "temporaryTables()", "inlineRows(def",
                    "dialect.sessionTable(new"))),
            // STAYING (B1, 2026-11-11, docs/DATA_DELIVERY_DESIGN_2026_11_10.md §6's B1, the user's yes): a seed form
            // mapped over executeInDb is the author's statements beside executeInDb, folded into the effect's script --
            // its bracket, abort, ledger and the B8 refusal the script's -- and never converted to a load (made one at L2
            // and put back the same day: the M1 correctness audit's C3, 2026-11-09). Guard: the form's texts sit in an
            // effect's script and in no load (BodyRouterParityTest.seedFormEffectMapPrograms)
            new Row("M7-seed", "the seed forms (setUpDataSQLs and its kin) planned through CsvSeed",
                    new Staying("BodyRouterParityTest.seedFormEffectMapPrograms: the author's statements, folded into"
                            + " the effect's script, in no load")),
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
            // DELETED at B2a, 2026-11-11 (docs/DATA_DELIVERY_DESIGN_2026_11_10.md §5): the JSON connection's frame is its
            // payload's data: source relation, its records and their places -- at B2a the plan's load into a session
            // table each database spelled; since the JSON pass (the same day, M18) the statement's own bound parameter
            // or rows, a record's place the split's ordinal. The 20 DuckDB and 10 H2 corpus tests A5 measured pass.
            // Proof: JsonM2M*IntegrationTest on DuckDB, H2 and SQLite, EdgeLiveSourceTest, DataSourcesRecordsTest,
            // JsonPayloadFormsTest
            new Row("M10", "JSON model data: data: inlined, file: as a path in SQL", new Deleted("B2a", List.of(
                    // L6: the file: source's path pasted into the statement, and DuckDB's data: payload as a SQL literal
                    "read_json_objects(\" + stringLit", "java.nio.file.Path.of(java.net.URI.create(url))",
                    "SELECT unnest(CAST(\" + stringLit(content)",
                    // B2a: a runtime value's JSON connection, its payload cut into the query's VALUES rows
                    "objectTexts(", "sourceUrlFrame("))),
            // the text made true 2026-11-11 (the B checkpoint's tenets audit, LOW 7): lowering is not the only maker of
            // a statement's literal rows -- three others build them, each from a value the plan already holds, and the
            // passes and the plan's reader rebuild rows that exist (SqlRewriter, AliasPrefix, CarrierStrategies,
            // MirJson), adding none
            new Row("M11", "a query's literal relations and instances, and their writes",
                    new Staying("P9: a statement's literal rows come from lowering, and from three makers beside it,"
                            + " each over a value the plan holds: a data: payload's elements written as the statement's"
                            + " rows by its dialect (SqlDialect.inlineRows on H2, MongoDB and Databricks past its"
                            + " parameter limit, census M18; DataSources tags them with the payload's digest), an"
                            + " enum's code and name table (EnumValueTables: the compiled model's constants, M12), and"
                            + " a generated test-data text's expected rows compared in the database"
                            + " (TestDataPlanner.expectedCells: the assert's own literal, S195)")),
            new Row("M12", "model-derived constants in statements",
                    new Staying("P9: constants come from the compiled model alone")),
            // DELETED (B2d.2, 2026-11-11, the generator design the user approved 2026-11-10 and the read-only reload
            // ruled 2026-11-11): the generator is one planned read-only statement (TestDataPlanner) on the test road;
            // its text is compared as data in the database and its slice read by the query again (GeneratorSplice,
            // TypedTableSwap), nothing written; the tests' interleaved run that read rows into temporary tables, the
            // TDG appeal that replayed its fetches, and the generator's statement origin deleted
            new Row("M13-output", "the test-data generator's rows handed on as seed text", new Deleted("B2d.2",
                    List.of("class TestDataGeneratorRun", "Appeal.Kind.TDG", "StatementOrigin.TDG"))),
            // DELETED at B3, 2026-11-11 (docs/TOTAL_WALL_START_HERE_2026_11_11.md §5.2, the orchestrator's go and the
            // user's ruling of that day on the remote mount, "Yes do option 2"): every way a host put data into its
            // database is a planned load its runner runs (engine-client's runLoad, Python's projects.run_load) -- a
            // project's Data the ONCE load its database is made with (B3.4, LoadPlanner.projectData); a person's file,
            // an upload or Studio's put, a HOST_SESSION load naming the file by reference, bound to its reader's ? (B3.5,
            // LoadPlanner.hostFile); a remote file's mount a HOST_SESSION load whose secret and bearer URL the host binds
            // from its store by name, the view reading the URL through a session variable (B3's Q3,
            // LoadPlanner.remoteMount; docs/data-delivery-homework/B3_REMOTE_MOUNT_DESIGN_2026_11_11.md). Gone: the
            // fill's text, the seed text, a file and a remote file named in a host's operation, a secret's operation
            new Row("M14", "the browser tab, Python and DataCube load data outside any plan", new Deleted("B3", List.of(
                    // B3.4: a project's database filled from the fill's statement text
                    "out.put(\"fill\"", "p.fill()", "List<String> fill",
                    // B3.5: a model's rows handed over as seed text, a host's file named in an operation
                    "testDataSql(", "lite_test_data_sql", "record File(String at", "Source.File(",
                    // B3's Q3: a remote file's URL written into an operation's view, and a secret made by an operation
                    "record Remote(String url", "new HostOperation.Source.Remote(string(", "HostOperation.Secret(string(o"))),
            // M15 in its two halves, as M7 is (B2c, 2026-11-11, docs/DATA_DELIVERY_DESIGN_2026_11_10.md §6's B2c, the
            // user's yes): the Java test fixtures' own JDBC writes BOUNDED by their guard, not converted -- each file's
            // literal write sends pinned, a total ceiling that only falls; the referee's replay of seed texts moved
            // with B2b (2026-11-11): the mirror replays the referee's copy of the plan's loads, a file's by its own
            // CSVREAD over the verified bytes (ReplayRefereeFileReadTest), no Java splitting its cells; DELETED: the
            // splitter's file (base.CsvCells), emptied by 57e2c445, deleted by its creator (00e67e38)
            new Row("M15-fixtures", "harnesses seed by JDBC (the Java test fixtures' own writes)",
                    new Bounded("B2c", List.of(), "FixtureWriteCensusTest")),
            new Row("M15-referee", "the referee's mirror replays seed texts (the edge's SeedTexts)",
                    new Deleted("B2b", List.of("CsvCells.table(", "class CsvCells", "planned.recorded()"))),
            new Row("M16", "parameters, keyed keys, counts, host rows",
                    new Staying("P9: parameters come from the caller alone")),
            // BOUNDED (B2e, 2026-11-11, docs/DATA_DELIVERY_DESIGN_2026_11_10.md §6's B2e and §9's gaps, the user's
            // "not implemented yet"): every reader of a model's data switches over its kinds without a default; a
            // relational store's CSV rows are the one kind loaded, and a feature that would read another refuses it by
            // name (a service test's ModelStore, ExternalFormat, ServiceStore or Relation data), where it was dropped
            // unnamed (ProjectData's continues) or skipped (ServiceTestRunner). When built, such data takes the
            // model-data path, a load
            new Row("M17", "data parsed and never loaded", new Bounded("B2e", List.of(
                    "is not provisioned by this runner",
                    "if (!(data.body().value() instanceof Protocol.PRelationalCsvData csv))"),
                    "EmbeddedDataKindsTest")),
            new Row("M7-authored", "executeInDb: SQL the user wrote",
                    new Staying("P9: an effect's statements are the author's text, folded")),
            // BOUNDED (2026-11-11, the user's ruling: "we need to make json work everywhere"; the orchestrator's brief
            // of that day, docs/DATA_DELIVERY_DESIGN_2026_11_10.md §6): a data: payload is a session table the plan's
            // load makes wherever the database holds one, and the payload in the statement only where measured: its own
            // rows (VALUES, $documents) on Trino, ClickHouse and MongoDB, which hold none a plan can make and drop
            // safely; ONE bound parameter the database splits on Snowflake and BigQuery (round 2's probes, the user's
            // ruling of 2026-11-11: 0.48 s at 100,000 records on Snowflake, against 17 s as rows and 73 s as a table) --
            // each the dialect's answer (SqlDialect.deliver), never a default and never a fallback. THE JSON PASS
            // (2026-11-11, the user's rulings: "move everything off temp table", H2 inline "so we can get rid of all the
            // temp table code completely", "Yes on Databricks", the digest reference): ONE bound parameter on every
            // database that splits one (Postgres, MySQL, MariaDB, DuckDB, SQLite, Trino, ClickHouse, Databricks,
            // Snowflake, BigQuery; measured at 100,000 records), the statement's own rows on H2 and MongoDB and on
            // Databricks above its 1 MiB parameter limit; the payload's text once in the plan, its statement's content
            // by SHA-256. The session table, its load, its release and the edge's record of it DELETED: their symbols
            // read zero. Proof: DataDeliveryDeclaredTest, DataSourcesDeliveryTest, JsonPayloadFormsTest (in process,
            // Postgres, the servers), DataReleaseTest (no release exists)
            new Row("M18", "a data: payload in the statement (its own rows, or one bound parameter)",
                    new Bounded("the JSON pass", List.of("Delivered.Table", "sessionTable(", "dataTable(",
                            "dataSchema(", "LEGEND_SESSION", "LiveSourceLoad", "Cadence.AT_RELEASE", "Cadence.SOURCE",
                            "ExecutionPlan.Release", "placeMade(", "dataReleases(", "sharingTables(",
                            "DataSources.Tables", "PlanSessions.releasing(", "TEMP_SCHEMA", "liveSource()"),
                    "DataDeliveryDeclaredTest")));

    /** What a PENDING row's slices deleted so far, each symbol reading zero as a deleted row's do (the M1 tenets
     *  audit's finding 2, 2026-11-09: a row is DELETED only when its whole list is). None pending with a partial list
     *  since B4 (2026-11-11): M5's and M6's moved into their DELETED rows. */
    static final java.util.Map<String, List<String>> REMOVED_SO_FAR = java.util.Map.of();

    /**
     * A DELETED ROW'S HOST SPELLINGS (the B checkpoint's tenets audit, M4, 2026-11-11): where a mechanism lived in the
     * hosts -- the tab's and DataCube's TypeScript, Python's package -- its own spellings there, each present in the
     * hosts' history (git log -S) and reading zero in every host source now, as the Java symbols above do in the Java
     * trees. M14: the fill's statement text, the seed text, a host's file and remote file named in an operation, a
     * secret's operation (its view, mount, access and credential operations, their scope and binds), a table's seed
     * rows and the tables a project's Data loaded outside its plan.
     */
    static final java.util.Map<String, List<String>> HOST_SYMBOLS = java.util.Map.of("M14", List.of(
            // TypeScript (engine-client, datacube)
            "testDataSql(", "viewOperation(", "mountOperations(", "accessOperations(", "credentialOperations(",
            "credentialScope(", "secretBinds(", "op: 'secret'", "op: 'insert'", "TableSeed", "loadDataTables(",
            // Python (legend_lite)
            "lite_test_data_sql", "statements['fill']"));

    /** The hosts' sources the census reads (M4): DataCube's and engine-client's TypeScript, Python's package. */
    private static List<Path> hostSources() {
        List<Path> out = new ArrayList<>();
        for (String tree : List.of("datacube/src", "engine-client/src", "python/legend_lite")) {
            List<Path> files = SourceFiles.under(tree);
            assertTrue(!files.isEmpty(), "the census read nothing under " + tree);
            out.addAll(files);
        }
        return out;
    }

    @Test
    void aDeletedMechanismsHostSpellingsReadZero() throws IOException {
        List<Path> files = hostSources();
        assertTrue(files.size() > 100, "the census read too few host sources to prove anything: " + files.size());
        Set<String> deleted = ROWS.stream().filter(r -> r.state() instanceof Deleted).map(Row::id)
                .collect(Collectors.toSet());
        assertTrue(deleted.containsAll(HOST_SYMBOLS.keySet()), "host spellings are a DELETED row's: " + HOST_SYMBOLS.keySet());
        List<String> found = new ArrayList<>();
        for (Path f : files) {
            String name = f.getFileName().toString();
            if (!name.endsWith(".ts") && !name.endsWith(".py")) {
                continue;
            }
            String text = Files.readString(f, StandardCharsets.UTF_8);
            HOST_SYMBOLS.forEach((id, symbols) -> {
                for (String symbol : symbols) {
                    if (text.contains(symbol)) {
                        found.add(id + " '" + symbol + "' in " + f);
                    }
                }
            });
        }
        assertEquals(List.of(), found, "a deleted mechanism's host spelling is back");
    }

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
        // the inventory's 17: M7 counted once, in its two halves (the seed forms a load, the author's statements not);
        // M15 likewise since B2c, 2026-11-11 (the fixtures' own writes bounded, the referee's replay B2b's)
        Set<String> want = new TreeSet<>(Arrays.asList("M1", "M2", "M3", "M4", "M5", "M6", "M7-seed", "M7-authored",
                "M8", "M9", "M10", "M11", "M12", "M13-output", "M14", "M15-fixtures", "M15-referee", "M16", "M17",
                "M18"));
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
        // design's §5 asks the census kept honest; its row says why and when it moves); 3 -> 4 (2026-11-11, B2a: M10
        // deleted, the JSON connection's frame its payload's data: load on every road); 4 -> 6 (2026-11-11, B2b: M3 and
        // M15-referee deleted, the referee's copy of a seed a load and its files gone, 57e2c445 and 00e67e38); 6 -> 7
        // (2026-11-11, B2d.2: M13-output deleted, the generator one planned read-only statement, its interleaved run
        // and the TDG appeal gone); 7 -> 9 (2026-11-11, the merge of B4 into the generator's branch: M5 and M6 deleted,
        // every statement reading the system store on the in-process catalog, the server's forms gone); 9 -> 10
        // (2026-11-11, B3 merged: M14 deleted, every host's data a planned load -- a project's Data, a person's file, a
        // remote file's mount -- its symbols in its row)
        assertEquals(10, deleted, "the deleted rows changed: move this count with the slice that deletes one");
    }
}
