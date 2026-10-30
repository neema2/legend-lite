package planner;

import org.teavm.jso.JSExport;

/**
 * THE TAB'S ADAPTER to the {@link Boundary}: TeaVM's entry class (//wasm:planner), whose {@code @JSExport} functions
 * are the WebAssembly module's exports -- the tab's whole API, each a one-line delegation through {@link Folded}
 * (docs/PROTOCOL_PROGRAM_2026_10_05.md, invariant 5). legend-engine's operations the tab asks through
 * {@link #pureV1OrError}, as it would ask a server; no export here is its own version of a {@code pure/v1} endpoint
 * (and {@link #pureV1OrError} routes legend-lite's own {@code /api/lite/v1/compilation/compile} too, as the server
 * does).
 * The JVM differential ({@code JvmMain}) calls these same functions, so it compares the tab's own answers.
 */
public final class TabExports {

    private TabExports() {
    }

    // ---- legend-engine's pure/v1

    /** One {@code pure/v1} call, as legend-lite's server answers it ({@link Boundary#pureV1}). */
    @JSExport
    public static String pureV1OrError(String path, String rawQuery, String body) {
        return Folded.http(() -> Boundary.pureV1(path, rawQuery, body));
    }

    /** {@link Boundary#executeJsonPlan}: execute's plan half -- the SQL the tab runs, and the answer but its rows (C1). */
    @JSExport
    public static String executeJsonPlanOrError(String body, String modelsJson) {
        return Folded.http(() -> Boundary.executeJsonPlan(body, modelsJson));
    }

    /** {@link Boundary#executeJsonAnswer}: the answer, the rows the tab's database answered written as the server's
     *  (C1). */
    @JSExport
    public static String executeJsonAnswerOrError(String plan, String rows) {
        return Folded.http(() -> Boundary.executeJsonAnswer(plan, rows));
    }

    /** {@link Boundary#executeArrowAnswer}: the answer in upstream's Arrow format, the rows the tab's database
     *  answered written by lite's Arrow writer, folded as base64 (track A, 2026-10-29). */
    @JSExport
    public static String executeArrowAnswerOrError(String plan, String rows) {
        return Folded.http(() -> Boundary.executeArrowAnswer(plan, rows));
    }

    // ---- planning and typing

    /** {@link Boundary#planSql}: the SQL alone, unfolded (a refusal is thrown). */
    @JSExport
    public static String plan(String model, String query, String runtime) {
        return Boundary.planSql(model, query, runtime);
    }

    /** {@link Boundary#plan}. */
    @JSExport
    public static String planOrError(String model, String query, String runtime) {
        return Folded.of(() -> Boundary.plan(model, query, runtime));
    }

    /** {@link Boundary#relationType}. */
    @JSExport
    public static String relationTypeOrError(String model, String query) {
        return Folded.of(() -> Boundary.relationType(model, query));
    }

    /** {@link Boundary#planJson}. */
    @JSExport
    public static String planJsonOrError(String model, String lambdaJson, String runtime) {
        return Folded.of(() -> Boundary.planJson(model, lambdaJson, runtime));
    }

    /** {@link Boundary#holdModel}: the handle, or the refusal of a model that does not compile. */
    @JSExport
    public static String holdModelOrError(String model) {
        return Folded.of(() -> Integer.toString(Boundary.holdModel(model)));
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

    /** {@link Boundary#planHeld}. */
    @JSExport
    public static String planHeldOrError(int handle, String query, String runtime) {
        return Folded.of(() -> Boundary.planHeld(handle, query, runtime));
    }

    /** {@link Boundary#planJsonHeld}. */
    @JSExport
    public static String planJsonHeldOrError(int handle, String lambdaJson, String runtime) {
        return Folded.of(() -> Boundary.planJsonHeld(handle, lambdaJson, runtime));
    }

    /** {@link Boundary#executionPlanJsonHeld}. */
    @JSExport
    public static String executionPlanJsonHeldOrError(int handle, String lambdaJson, String runtime) {
        return Folded.of(() -> Boundary.executionPlanJsonHeld(handle, lambdaJson, runtime));
    }

    /** {@link Boundary#relationTypeHeld}. */
    @JSExport
    public static String relationTypeHeldOrError(int handle, String query) {
        return Folded.of(() -> Boundary.relationTypeHeld(handle, query));
    }

    // ---- a model's data and its tables

    /** {@link Boundary#testDataSql}. */
    @JSExport
    public static String testDataSqlOrError(String model, String database, String tablesJson) {
        return Folded.of(() -> Boundary.testDataSql(model, database, tablesJson));
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
        System.out.println(Boundary.planSql(MODEL, QUERY, RUNTIME));
    }
}
