package com.legend.edge.wasm;

import org.teavm.jso.JSExport;
import planner.Boundary;
import planner.Folded;

/**
 * THE TAB'S ADAPTER to the {@link Boundary}: TeaVM's entry class (//wasm:planner), whose {@code @JSExport} functions
 * are the WebAssembly module's exports -- the tab's whole API, each a one-line delegation through {@link Folded}
 * (docs/PROTOCOL_PROGRAM_2026_10_05.md, invariant 5). legend-engine's operations the tab asks through
 * {@link #pureV1OrError}, as it would ask a server; no export here is its own version of a {@code pure/v1} endpoint
 * (and {@link #pureV1OrError} routes legend-lite's own {@code /api/lite/v1/compilation/compile} too, as the server
 * does).
 * The JVM differential ({@code JvmMain}) runs the same compositions over the boundary ({@code Folded.of} of
 * {@code Boundary.queryParse} then {@code queryPlan}), so it compares the tab's own answers.
 *
 * <p>AT THE EDGE (F's separation census line A1, 2026-11-17): four of these exports answer legend-engine's routes
 * through the edge ({@code EdgeBoundary}), so this class -- the module's one entry -- is the edge's
 * ({@code //wasm:edge_tab}, in the edge's package: until 2026-11-17 {@code planner.TabExports}), and the module is rooted at the edge as the server is (the orchestrator's extension of
 * ruling A4). Its exports and their names are unchanged.
 */
public final class TabExports {

    private TabExports() {
    }

    // ---- legend-engine's pure/v1

    /** One {@code pure/v1} call, as legend-lite's server answers it ({@link EdgeBoundary#pureV1}). */
    @JSExport
    public static String pureV1OrError(String path, String rawQuery, String body) {
        return Folded.http(() -> EdgeBoundary.pureV1(path, rawQuery, body));
    }

    /** {@link EdgeBoundary#executeJsonPlan}: execute's plan half -- the SQL the tab runs, and the answer but its rows (C1). */
    @JSExport
    public static String executeJsonPlanOrError(String body, String modelsJson) {
        return Folded.http(() -> EdgeBoundary.executeJsonPlan(body, modelsJson));
    }

    /** {@link EdgeBoundary#executeJsonAnswer}: the answer, the rows the tab's database answered written as the server's
     *  (C1). */
    @JSExport
    public static String executeJsonAnswerOrError(String plan, String rows) {
        return Folded.http(() -> EdgeBoundary.executeJsonAnswer(plan, rows));
    }

    /** {@link EdgeBoundary#executeArrowAnswer}: the answer in upstream's Arrow format, the rows the tab's database
     *  answered written by lite's Arrow writer, folded as base64 (track A, 2026-10-29). */
    @JSExport
    public static String executeArrowAnswerOrError(String plan, String rows) {
        return Folded.http(() -> EdgeBoundary.executeArrowAnswer(plan, rows));
    }

    // ---- models held: the binding's cache (lite API v1 §7a)

    /** {@link Boundary#holdModel}: the handle, or the refusal of a model that does not compile. */
    @JSExport
    public static String holdModelOrError(String model) {
        return Folded.of(() -> Integer.toString(Boundary.holdModel(model)));
    }

    /** {@link Boundary#holdModelOver}: the handle, or the refusal of a model that does not compile over the library. */
    @JSExport
    public static String holdModelOverOrError(String model, String library) {
        return Folded.of(() -> Integer.toString(Boundary.holdModelOver(model, library)));
    }

    /**
     * {@link Boundary#releaseModel}, folded: {@code OK\n}, or the failure as the planner protocol's other kinds answer
     * one -- as the native library's {@code lite_release_model} answers it (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C2).
     */
    @JSExport
    public static String releaseModelOrError(int handle) {
        return Folded.of(() -> {
            Boundary.releaseModel(handle);
            return "";
        });
    }

    /** {@code query/plan} over a model held ({@link Boundary#queryPlanHeld}). */
    @JSExport
    public static String queryPlanHeldOrError(int handle, String request) {
        return Folded.of(() -> Boundary.queryPlanHeld(handle, request));
    }

    /** {@code query/type} over a model held ({@link Boundary#queryTypeHeld}). */
    @JSExport
    public static String queryTypeHeldOrError(int handle, String request) {
        return Folded.of(() -> Boundary.queryTypeHeld(handle, request));
    }

    // ---- lite API v1's operations (docs/records/lite-api-v1-2026-11-17.md §2, the user's go of 2026-11-17): each the
    // ---- operation's request JSON in, its answer JSON out ({@link Boundary}'s member of the same name)

    /** {@code model/check} ({@link Boundary#modelCheck}). */
    @JSExport
    public static String modelCheckOrError(String request) {
        return Folded.of(() -> Boundary.modelCheck(request));
    }

    /** {@code model/elements} ({@link Boundary#modelElements}). */
    @JSExport
    public static String modelElementsOrError(String request) {
        return Folded.of(() -> Boundary.modelElements(request));
    }

    /** {@code query/parse} ({@link Boundary#queryParse}). */
    @JSExport
    public static String queryParseOrError(String request) {
        return Folded.of(() -> Boundary.queryParse(request));
    }

    /** {@code query/print} ({@link Boundary#queryPrint}). */
    @JSExport
    public static String queryPrintOrError(String request) {
        return Folded.of(() -> Boundary.queryPrint(request));
    }

    /** {@code query/type} ({@link Boundary#queryType}). */
    @JSExport
    public static String queryTypeOrError(String request) {
        return Folded.of(() -> Boundary.queryType(request));
    }

    /** {@code query/plan} ({@link Boundary#queryPlan}). */
    @JSExport
    public static String queryPlanOrError(String request) {
        return Folded.of(() -> Boundary.queryPlan(request));
    }

    /** {@code model/createClass} ({@link Boundary#modelCreateClass}). */
    @JSExport
    public static String modelCreateClassOrError(String request) {
        return Folded.of(() -> Boundary.modelCreateClass(request));
    }

    /** {@code model/createAssociation} ({@link Boundary#modelCreateAssociation}). */
    @JSExport
    public static String modelCreateAssociationOrError(String request) {
        return Folded.of(() -> Boundary.modelCreateAssociation(request));
    }

    /** {@code model/createDataSpace} ({@link Boundary#modelCreateDataSpace}). */
    @JSExport
    public static String modelCreateDataSpaceOrError(String request) {
        return Folded.of(() -> Boundary.modelCreateDataSpace(request));
    }

    /** {@code ts/item} ({@link Boundary#tsItem}). */
    @JSExport
    public static String tsItemOrError(String request) {
        return Folded.of(() -> Boundary.tsItem(request));
    }

    /** {@code ts/statement} ({@link Boundary#tsStatement}). */
    @JSExport
    public static String tsStatementOrError(String request) {
        return Folded.of(() -> Boundary.tsStatement(request));
    }

    // ---- a model's data and its tables

    /** {@link Boundary#fileLoad}: a host's own file read into its database, as a plan (B3). */
    @JSExport
    public static String fileLoadOrError(String request) {
        return Folded.of(() -> Boundary.fileLoad(request));
    }

    /** {@link Boundary#remoteMount}: a remote file mounted, as a plan (B3's Q3). */
    @JSExport
    public static String remoteMountOrError(String request) {
        return Folded.of(() -> Boundary.remoteMount(request));
    }

    /** {@link Boundary#snapStream}: a snapshot from a remote plane, as a plan (the user's ruling of 2026-11-11) -- its
     *  request the pull, planned in this call, never a statement (the user's ruling of 2026-11-12). */
    @JSExport
    public static String snapStreamOrError(String request) {
        return Folded.of(() -> Boundary.snapStream(request));
    }

    /** {@link Boundary#snapCopy}: a snapshot's copy into the tab's own database, as one plan (the final audit's 1(b),
     *  2026-11-12) -- its request the pull, planned in this call, never a statement. */
    @JSExport
    public static String snapCopyOrError(String request) {
        return Folded.of(() -> Boundary.snapCopy(request));
    }

    /** {@link Boundary#outputPlan}: a DataCube table Output as one plan (the final audit's item 2, 2026-11-12) -- the
     *  table made before the write where the Output makes it, its write, one transaction. */
    @JSExport
    public static String outputPlanOrError(String request) {
        return Folded.of(() -> Boundary.outputPlan(request));
    }

    /** {@link Boundary#authored}: a statement a person wrote, as a plan (Studio's SQL playground, 2026-11-12). */
    @JSExport
    public static String authoredOrError(String request) {
        return Folded.of(() -> Boundary.authored(request));
    }

    /** {@link Boundary#synthesize}: rows the database generates into a declared table, as a plan (SYNTHESIZE). */
    @JSExport
    public static String synthesizeOrError(String request) {
        return Folded.of(() -> Boundary.synthesize(request));
    }

    /** {@link Boundary#databaseFromCatalog}. */
    @JSExport
    public static String databaseFromCatalogOrError(String catalogJson) {
        return Folded.of(() -> Boundary.databaseFromCatalog(catalogJson));
    }

    /** {@link Boundary#tableModel}. */
    @JSExport
    public static String tableModelOrError(String tableJson) {
        return Folded.of(() -> Boundary.tableModel(tableJson));
    }

    /** {@link Boundary#catalogColumnsSql}. */
    @JSExport
    public static String catalogColumnsSqlOrError(String schema, String table) {
        return Folded.of(() -> Boundary.catalogColumnsSql(schema, table));
    }

    /** {@link Boundary#sessionSetup}. */
    @JSExport
    public static String sessionSetupOrError(String databaseType) {
        return Folded.of(() -> Boundary.sessionSetup(databaseType));
    }

    /** {@link Boundary#hostOperations}. */
    @JSExport
    public static String hostOperationsOrError(String databaseType, String operationsJson) {
        return Folded.of(() -> Boundary.hostOperations(databaseType, operationsJson));
    }

    /** {@link Boundary#projectData}. */
    @JSExport
    public static String projectDataOrError(String model, String databaseType) {
        return Folded.of(() -> Boundary.projectData(model, databaseType));
    }

    // ---- warming and probes

    /** {@link Boundary#warmModel}. */
    @JSExport
    public static int warmModel(String model) {
        return Boundary.warmModel(model);
    }

    /** {@link Boundary#warmModel}, folded, as the native library's {@code lite_warm_model} answers it (C2). */
    @JSExport
    public static String warmModelOrError(String model) {
        return Folded.of(() -> {
            Boundary.warmModel(model);
            return "";
        });
    }

    /** {@link Boundary#touchPrelude}. */
    @JSExport
    public static int touchPrelude() {
        return Boundary.touchPrelude();
    }

    /** {@link Boundary#touchSystemMetamodel}. */
    @JSExport
    public static int touchSystemMetamodel() {
        return Boundary.touchSystemMetamodel();
    }

    /** {@link Boundary#hashBootSource}. */
    @JSExport
    public static int hashBootSource() {
        return Boundary.hashBootSource();
    }

    /** {@link Boundary#resolveBootLayer}. */
    @JSExport
    public static int resolveBootLayer() {
        return Boundary.resolveBootLayer();
    }

    /** {@link Boundary#zoneProbe}. */
    @JSExport
    public static String zoneProbe(String utcIso, String zone) {
        return Boundary.zoneProbe(utcIso, zone);
    }

    // ---- the entry class's main: one plan of the demo model (datacube/demo/trades.pure, verbatim)

    private static final String MODEL = """
            ###Relational
            Database trades::DB
            (
                Table TRADES
                (
                    region VARCHAR(32), desk VARCHAR(32), book VARCHAR(32),
                    year INTEGER, qtr VARCHAR(8),
                    notional DOUBLE, pnl DOUBLE, qty INTEGER
                )
            )

            ###Connection
            RelationalDatabaseConnection trades::Conn
            {
                type: DuckDB;
                specification: DuckDB { };
                auth: Test;
            }

            ###Runtime
            Runtime trades::RT
            {
                mappings: [];
                connections:
                [
                    trades::DB: [ c1: trades::Conn ]
                ];
            }
            """;

    private static final String RUNTIME = "trades::RT";

    private static final String QUERY = "#>{trades::DB.TRADES}#"
            + "->filter(x|$x.region == 'EMEA')"
            + "->select(~[region, desk, notional])"
            + "->groupBy(~[region, desk], ~[total:x|$x.notional:y|$y->sum()])"
            + "->sort([~region->ascending()])->limit(10)";

    public static void main(String[] args) {
        // a text query is query/parse, then query/plan (lite API v1 §2)
        String parsed = Boundary.queryParse("{\"text\":" + com.legend.json.Json.toCompact(QUERY) + "}");
        String query = com.legend.json.Json.toCompact(com.legend.json.Json.parseObject(parsed).get("query"));
        System.out.println(Boundary.queryPlan("{\"model\":" + com.legend.json.Json.toCompact(MODEL) + ",\"query\":" + query
                + ",\"runtime\":" + com.legend.json.Json.toCompact(RUNTIME) + "}"));
    }
}
