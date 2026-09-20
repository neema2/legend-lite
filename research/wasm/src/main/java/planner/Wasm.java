package planner;

/**
 * legend-lite's PLANNER, compiled to WebAssembly.
 *
 * <p>Pure text in, SQL text out. No executor, no server, no JDBC — the
 * planner packages depend on java.base alone (jdeps), and the whole
 * pipeline was already proved to run under {@code --limit-modules
 * java.base}. This asks the next question: does it survive an AOT
 * compile to WASM, and does it then answer IDENTICALLY.
 *
 * <p>This one class is the entry point for BOTH targets — TeaVM
 * compiles it to WASM, and {@code JvmMain} calls it on the JVM — so the
 * differential test compares two builds of the same source rather than
 * two hand-kept copies that could drift apart silently.
 *
 * <h2>Why this calls {@code Compiler.plan} and nothing else</h2>
 *
 * <p>The first version of this class hand-assembled the pipeline —
 * parse, type, inline, resolve, lower, {@code new DuckDb().render} —
 * which was fine for asking "does a planner survive the compile" but
 * is exactly wrong for shipping. {@code POST /engine/plan} calls
 * {@link com.legend.Compiler#plan(String, String, String)}, so
 * anything else here would be a SECOND planner that has to agree with
 * the first about dialect selection, null ordering and aggregate
 * semantics — the divergence class this project exists to avoid.
 *
 * <p>Going through {@code Compiler} also pulls the runtime in, which
 * the hand-rolled version silently skipped: the dialect comes from the
 * Pure {@code Connection} ELEMENT that the named {@code Runtime}
 * resolves to, not from a hardcoded {@code new DuckDb()}.
 *
 * <p>That makes this class a live test of a subtle property.
 * {@code Compiler} keeps {@code java.sql.Connection} in six method
 * DESCRIPTORS (its execute overloads) while carrying no
 * {@code java.sql} catch clause — the distinction
 * {@code PlannerNeedsOnlyJavaBaseTest} pins, because the JVM verifier
 * resolves handler types at link time and descriptors lazily. An
 * ahead-of-time compiler does its own whole-program analysis and need
 * not honour that distinction at all. If this module builds, it does.
 */
public final class Wasm {

    private Wasm() {
    }

    /** The demo model, verbatim from {@code datacube/demo/trades.pure}. */
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

    /**
     * The planner, end to end — the SAME call {@code POST /engine/plan}
     * makes, so the browser plane and the server plane cannot drift.
     */
    @org.teavm.jso.JSExport
    public static String plan(String model, String query, String runtime) {
        return com.legend.Compiler.plan(model, query, runtime).sql();
    }

    /**
     * {@link #plan} with the failure path folded into the RETURN VALUE.
     *
     * <p>A refusal is an answer the planner is expected to give, so the
     * differential has to compare refusals too — and comparing them
     * through the return value rather than through a thrown exception
     * keeps the two targets honest without depending on how TeaVM
     * bridges Java throwables into JS.
     */
    @org.teavm.jso.JSExport
    public static String planOrError(String model, String query, String runtime) {
        try {
            return "OK\n" + plan(model, query, runtime);
        } catch (RuntimeException | StackOverflowError e) {
            String name = e.getClass().getName();
            return "ERR\n" + name + "\n" + (e.getMessage() == null ? "" : e.getMessage());
        }
    }

    /**
     * Force {@code Prelude}'s static initialiser and nothing else.
     *
     * <p>Cold start is ~550ms against ~10ms warm, and "the first plan
     * is slow" is not an actionable statement. Parsing
     * {@code prelude.pure} — 300 KB, 7,009 lines — happens in a
     * static initialiser, so timing a call that touches ONLY that
     * class separates it from the rest of the first plan. Whether
     * pre-baking the boot layer is worth building depends entirely on
     * which side of that split the milliseconds are on.
     *
     * @return the element count, so the call cannot be optimised away
     */
    @org.teavm.jso.JSExport
    public static int touchPrelude() {
        return com.legend.builtin.Prelude.elementFqns().size();
    }

    /** {@link #touchPrelude}'s twin for the system metamodel. */
    @org.teavm.jso.JSExport
    public static int touchSystemMetamodel() {
        return com.legend.builtin.SystemMetamodel.elements().size();
    }

    /**
     * Build everything a first plan would build, and throw it away.
     *
     * <p>Not a probe — the browser calls this. Loading the module is
     * NOT warming it: instantiate costs ~44ms, while the work that
     * actually makes a first plan slow happens in static initialisers
     * and a content-addressed cache that a plan touches on its way
     * past. Measured from the page, that is parsing the 300 KB Pure
     * prelude, the system metamodel, then resolving and normalizing
     * both into the boot layer — together ~1.1s of the ~1.3s.
     *
     * <p>`WasmPlanner.warmUp` calls this while DuckDB is still
     * starting, so the cost lands inside a wait the page was making
     * anyway instead of after it. {@code compileModel} is the public
     * door to all of it, and takes the real model so the graph it
     * builds is the one the first plan wants.
     *
     * @return 1, so the call cannot be optimised away
     */
    @org.teavm.jso.JSExport
    public static int warmModel(String model) {
        return com.legend.Compiler.compileModel(model) == null ? 0 : 1;
    }

    /**
     * The one {@code ZoneId.of} on the planner's path
     * ({@code LiteralSpelling.inZone}, the engine's dbTimeZone literal
     * rule). A timezone database is a RESOURCE, not code, so whether it
     * survives the WASM build is a question the census cannot answer by
     * reading — only by asking the built module.
     */
    @org.teavm.jso.JSExport
    public static String zoneProbe(String utcIso, String zone) {
        try {
            return com.legend.lowering.LiteralSpelling.inZone(utcIso, zone);
        } catch (RuntimeException e) {
            return "ERR " + e.getClass().getName() + ": " + e.getMessage();
        }
    }

    public static void main(String[] args) {
        System.out.println(plan(MODEL, QUERY, RUNTIME));
    }
}
