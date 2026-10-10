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

    // ---- warming and probes

    /** {@link Boundary#warmModel}. */
    @JSExport
    public static int warmModel(String model) {
        return Boundary.warmModel(model);
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
