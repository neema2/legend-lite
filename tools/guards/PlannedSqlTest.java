package com.legend.tools.guards;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.testing.Runfile;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * THE PLANNED-SQL GUARD (the total wall's C3, 2026-11-12, the user's: "the hosts' separation proven structurally"). A
 * host sends its database only what a plan carries or the planner wrote. In TypeScript the engines and the host runner
 * take a {@code PlannedSql} (engine-client/src/planned-sql.ts), a branded string a hand-written {@code string} is not,
 * so a host's own SQL does not compile; in Python every statement goes through {@code _planned.send}, which refuses any
 * text that is not a {@code PlannedSql} by name (python/legend_lite/_planned.py). Both hold only while the type is made
 * where a plan is READ, and nowhere else: this guard pins where.
 *
 * <p>Read over every .ts of the repository (the inventory's files; the product's folders are host_sql.txt's, one list
 * with the host-SQL guard). Python's host is the other half's, read by Python's own parser
 * (python/tests/test_planned_guard.py, //python:planned_guard_test; 2026-11-12, the C checkpoint's code audit). Each
 * list below moves only with a dated justification naming the task (AGENTS.md, guard files), and shrinks: a site that
 * is gone is taken out (the stale checks).
 */
class PlannedSqlTest {

    /**
     * The product TypeScript files that MAKE a PlannedSql from a plan's text (import fromPlan or allFromPlan): the plan
     * readers and the planner clients, where a plan or the planner's answer is read. 2026-11-12 (C3): the hosts' plan
     * runner (plan-runner.ts: a plan's steps), the planner clients of the tab (legend/wasm-grammar.ts: Query's and
     * Studio's; DataCube's lite-planner.ts) and the reader of a server's generatePlan (DataCube's planner.ts serverPlan).
     */
    static final Set<String> MINTS = Set.of(
            "engine-client/src/plan-runner.ts",
            "engine-client/src/legend/wasm-grammar.ts",
            "datacube/src/lite-planner.ts",
            "datacube/src/planner.ts");

    // (NOT_PLANNED -- what a host still sent that no plan carried, named by planned-sql.ts's notPlanned: Studio's SQL
    // playground and, until the C3 follow-up, Query's demo warehouse fixture -- DELETED 2026-11-12, the user's ruling
    // "Yes do 5 and 6": the playground's statement is a plan's (Boundary.authored, plan-runner.ts runAuthored), and
    // notPlanned is gone with its last caller. No host sends a statement outside a plan.)

    /**
     * THE PLAYGROUND'S ONE PATH (the orchestrator's ruling of 2026-11-12; the user's of the same day, "Yes do 5 and 6"):
     * the person's text is a plan's one statement, carried by the planner as written (Boundary.authored), run by the
     * hosts' plan runner (runAuthored) -- the run backend's Playground object's send, apart from the Runner every run is
     * handed; the app makes it (app.ts, the factory's one importer) and hands it to the editor's context alone (editor.ts,
     * the type's one importer), whose SQL playground is its one reader.
     */
    static final String PLAYGROUND = "studio/src/backend/run.ts";
    static final Map<String, Set<String>> PLAYGROUND_IMPORTS = Map.of(
            "studio/src/app/app.ts", Set.of("playground"),
            "studio/src/ui/editor.ts", Set.of("Playground"));

    /**
     * EVERY USE OF A MINT, by identifier (2026-11-12, the C checkpoint's second audit: a mint file's own
     * {@code fromPlan(a.sql + ' LIMIT 1')} was unseen): each file's references to fromPlan, allFromPlan, notPlanned and
     * plannedProjectData in its code -- comments and string text blanked -- exactly. A new call in a mint file, or one
     * gone, moves its count, with a dated note here.
     */
    static final Map<String, Integer> MINT_REFERENCES = Map.of(
            // 2026-11-12 (the second audit): the hosts' plan runner, each step's statement typed as it is read; 17 -> 18
            // (the C final audit's H9): an authored plan's target's session statements, typed as they are read; 18 ->
            // 21 (the re-audit's S8, 2026-11-12): a target's transaction statements (begin, commit, rollback), typed as
            // they are read -- until then a host's session asked the planner for them at run (hostSession, deleted)
            "engine-client/src/plan-runner.ts", 21,
            // the planner clients: Query's and Studio's, DataCube's, and the reader of a server's generatePlan
            // 2026-11-12, 8 -> 14 (the re-audit's M1): its five plan answers (fileLoad, remoteMount, snapStream,
            // synthesize, authored) typed as read, and the mint's import; 14 -> 15 (the final audit's item 1(b),
            // 2026-11-12): a snapshot's copy's plan (snapCopy) typed as read
            "engine-client/src/legend/wasm-grammar.ts", 15,
            // 2026-11-12, 8 -> 9 (the C final audit's H2): DataCube's table model's copy select list, typed as its answer
            // is read, since a host operation's selectList takes a PlannedSql alone
            // 2026-11-12, 9 -> 15 (M1): its five plan answers (executionPlan, fileLoad, remoteMount, snapStream, synthesize)
            // typed as read, and the mint's import; 15 -> 16 (the final audit's items 1 and 2, 2026-11-12): the copy
            // select list's typing gone with it (a host operation takes no SQL: convert), a snapshot's copy's plan
            // (snapCopy) and a table Output's (outputPlan) typed as read
            "datacube/src/lite-planner.ts", 16,
            "datacube/src/planner.ts", 2,
            // the definitions themselves, and the mints' own test
            // 2026-11-12, 7 -> 9 (M1): planFromAnswer defined, and the project database's load typed by it
            "engine-client/src/planned-sql.ts", 9,
            // 2026-11-12, 5 -> 8 (the final audit's item 1): the type test's host operation carrying a planner's own
            // statement (a query, a select list, a replacement), each refused by the type
            "engine-client/test/planned-sql.test.ts", 8);
            // (studio/src/backend/run.ts, the SQL playground's notPlanned and its import: gone 2026-11-12 with
            // notPlanned; the plan runner's runAuthored types the playground's statement, its count one more)

    /**
     * THE TYPE SYSTEM'S ESCAPE HATCHES in the product's TypeScript (host_sql.txt's [typescript]; 2026-11-12, the C
     * checkpoint's second audit: {@code as never}, {@code as any} and a suppressed check satisfy the PlannedSql brand
     * as a cast does), counted per file in its code: shrink-only -- a new one fails, one gone lowers its file's count
     * here, dated.
     */
    static final Map<String, Integer> ESCAPES = Map.of(
            // 2026-11-12, measured (the second audit): each a cast of a JSON, protocol or library value, none a statement
            "datacube/src/app.ts", 1,
            "datacube/src/binning.ts", 1,
            "datacube/src/generate-rows.ts", 2,
            "datacube/src/headless/timeseries.ts", 1,
            "datacube/src/query.ts", 1,
            "query/src/app/persist.ts", 1,
            "query/src/ui/columns.ts", 9,
            "studio/src/backend/query-builder.ts", 4);

    /**
     * {@code as unknown as} in the product's TypeScript, by file (the C final audit's H5, 2026-11-12: it satisfies any
     * type, a record holding the brand among them, and ESCAPE did not see it): shrink-only -- a new one fails, one gone
     * lowers its file's count here, dated. A cast whose type names the brand or a record holding it is refused apart
     * (CAST, HOLDER_CASTS); these are each to a type that holds no statement.
     */
    static final Map<String, Integer> UNKNOWN_CASTS = Map.ofEntries(
            // 2026-11-12, measured (H5): 101 in 40 files, each a value read from JSON, a protocol or a library
            // as a type of its own (none a record holding the brand: HOLDER_CASTS is empty)
            Map.entry("datacube/src/app.ts", 6),
            Map.entry("datacube/src/app/datacube-app.ts", 3),
            Map.entry("datacube/src/chart-spec.ts", 1),
            Map.entry("datacube/src/cube-document.ts", 7),
            Map.entry("datacube/src/flow-boxes.ts", 1),
            Map.entry("datacube/src/grid-commands.ts", 1),
            Map.entry("datacube/src/grid/grid.ts", 4),
            Map.entry("datacube/src/headless/calls.ts", 1),
            Map.entry("datacube/src/headless/grid.ts", 8),
            Map.entry("datacube/src/headless/host.ts", 2),
            Map.entry("datacube/src/headless/join.ts", 2),
            Map.entry("datacube/src/headless/page.ts", 1),
            Map.entry("datacube/src/headless/timeseries.ts", 5),
            Map.entry("datacube/src/headless/url.ts", 1),
            Map.entry("datacube/src/headless/web.ts", 1),
            Map.entry("datacube/src/lite-planner.ts", 1),
            Map.entry("datacube/src/model-measure-commands.ts", 3),
            Map.entry("datacube/src/page-commands.ts", 5),
            Map.entry("datacube/src/page-gestures.ts", 1),
            Map.entry("datacube/src/page-parameters.ts", 1),
            Map.entry("datacube/src/timeseries-edit.ts", 2),
            Map.entry("datacube/src/timeseries-option.ts", 2),
            Map.entry("datacube/src/timeseries-render.ts", 3),
            Map.entry("datacube/src/ui/sheet-tabs.ts", 1),
            Map.entry("datacube/src/upload.ts", 2),
            Map.entry("engine-client/src/arrow-ipc.ts", 2),
            Map.entry("engine-client/src/duckdb-tab.ts", 1),
            Map.entry("engine-client/src/duckdb.ts", 1),
            Map.entry("engine-client/src/engine-remote.ts", 1),
            Map.entry("engine-client/src/legend/planner-worker.ts", 1),
            Map.entry("engine-client/src/plan-runner.ts", 1),
            Map.entry("engine-client/src/snap.ts", 1),
            Map.entry("engine-client/src/warehouse.ts", 2),
            Map.entry("pure-protocol/src/validate.ts", 1),
            Map.entry("query-store/src/share.ts", 2),
            Map.entry("query/src/app/run.ts", 1),
            Map.entry("query/src/builder/build.ts", 1),
            Map.entry("query/src/builder/load.ts", 18),
            Map.entry("query/src/ui/dom.ts", 2),
            Map.entry("studio/src/app/app.ts", 1));

    /**
     * A CAST TO A RECORD HOLDING THE BRAND in the product's TypeScript, by file (H5, 2026-11-12: {@code JSON.parse(text)
     * as Plan} makes a plan's statement of any text with no cast to PlannedSql to see). A record holds the brand when a
     * property's type names PlannedSql or another record that does (never a method's parameter or a function's, never
     * through an Omit), measured over the product and the tests: casts to one are refused but those pinned here.
     */
    static final Map<String, Integer> HOLDER_CASTS = Map.of();
    // 2026-11-12 (H5, measured): none, once python-host.ts's three model reads (Python's page answers, cast to
    // InferredModel, whose copySelectList is a PlannedSql) and wasm-grammar.ts's executePlan read (cast to ExecutePlan,
    // its run then minted) were typed without the brand

    /**
     * ONE CHOKE POINT PER TRANSPORT (the C final audit's H13, 2026-11-12; the user: "how do we stop this from actually
     * being able to be added without loud alarm bells?"). Each place the product's TypeScript could hand a database or a
     * SQL API text, by file, exactly -- a new one is red, naming this rule:
     * <ul>
     * <li>DATABASE_SENDS: a call of {@code .query(}, {@code .send(} or {@code .prepare(} (DuckDB-WASM's connection's
     * sends), a reference to one of DuckDB-WASM's own ingest or file members ({@code insertArrowFromIPCStream},
     * {@code registerFileHandle}, ...), and any of them by a bracketed name. DuckDB-WASM's connection is sent to in
     * engine-client/src/duckdb.ts alone (DuckDbEngine: ArrowishConnection takes a PlannedSql); the rest are named.</li>
     * <li>CONNECTS: a call of {@code .connect(} -- DuckDB-WASM's raw connection (whose own query takes any string) is
     * made in duckdb-tab.ts alone and handed to DuckDbEngine; the others connect to something else (named).</li>
     * <li>FETCHES: each {@code fetch(}: the HTTP a host sends; a SQL API's statement (the warehouse's {@code submit}) is
     * sent by engine-client/src/warehouse.ts alone (WarehouseEngine.arrowChunks, a PlannedSql: SUBMITS). Any
     * XMLHttpRequest, WebSocket or sendBeacon is refused.</li>
     * </ul>
     */
    static final Map<String, List<String>> DATABASE_SENDS = Map.ofEntries(
            // 2026-11-12 (the re-audit's S10, measured): by call site, the receiver and the member -- none but duckdb.ts's sends DuckDB-WASM a statement
            Map.entry("datacube/demo/verify-features.mjs", List.of("cdp.send(")),
            Map.entry("datacube/demo/verify-real-data.mjs", List.of("conn.query(", "conn.query(", "conn.query(")),
            Map.entry("datacube/demo/verify-remote.mjs", List.of("conn.query(", "conn.query(", "conn.query(", "db.copyFileToBuffer")),
            Map.entry("datacube/demo/widget-loader.ts", List.of("model.send(")),
            Map.entry("datacube/src/headless/host.ts", List.of("calls.query(")),
            Map.entry("engine-client/src/duckdb.ts", List.of("conn.insertArrowFromIPCStream", "conn.query(", "prepared.query(", "prepared.query(", "prepared.send(", "this.#conn.prepare(", "this.#conn.query(")),
            Map.entry("query/src/app/app.ts", List.of("recent.query(")),
            Map.entry("query/src/ui/queries.ts", List.of("recent.query(")));
    static final Map<String, List<String>> CONNECTS = Map.ofEntries(
            // 2026-11-12 (the re-audit's S10, measured): by call site, the receiver and the member -- the others connect a DuckDbEngine (DuckDbInTab.connect), a model home or a warehouse, or are harnesses' own fixture databases
            Map.entry("datacube/demo/torture.mjs", List.of("db.connect(")),
            Map.entry("datacube/demo/verify-engine-differential.mjs", List.of("db.connect(")),
            Map.entry("datacube/demo/verify-real-data.mjs", List.of("db.connect(")),
            Map.entry("datacube/demo/verify-remote.mjs", List.of("db.connect(")),
            Map.entry("datacube/src/app/datacube-app.ts", List.of("modelHome.connect(", "modelHome.connect(")),
            Map.entry("datacube/src/app/sources.ts", List.of(".connect(", "modelHome.connect(", "on.connect(", "on.connect(")),
            Map.entry("datacube/src/services/browser.ts", List.of("driver.connect(", "o.duckdb.connect(")),
            // THE raw connection, made once and handed to DuckDbEngine
            Map.entry("engine-client/src/duckdb-tab.ts", List.of("db.connect(")),
            Map.entry("engine-client/src/project-data.ts", List.of("this.#host.connect(")),
            Map.entry("query/demo/main.ts", List.of("tab.connect(")),
            Map.entry("studio/src/app/app.ts", List.of("tab.connect(")));
    static final Map<String, List<String>> SUBMITS = Map.ofEntries(
            // 2026-11-12 (the re-audit's S10, measured): by call site, the receiver and the member -- the warehouse's statement, a PlannedSql
            Map.entry("engine-client/src/warehouse.ts", List.of("this.#call<Step>('submit'")));
    /** 2026-11-12 (H13, measured): Node's XMLHttpRequest stand-in, which answers DuckDB's extension requests from the
     *  pinned files and sends nothing (node-require.ts answerExtensionRequests). */
    static final Set<String> OTHER_HTTP_KEPT = Set.of("engine-client/src/node-require.ts");
    static final Map<String, List<String>> FETCHES = Map.ofEntries(
            // 2026-11-12 (the re-audit's S10, measured): by call site, the receiver and the member -- a server's API (legend-lite's, an SDLC's, a depot's, a query store's, Python's page), a site's own assets, a harness's, and the warehouse's (warehouse.ts, whose statement is SUBMITS's)
            Map.entry("datacube/demo/boot.ts", List.of("fetch('./trades.pure'")),
            Map.entry("datacube/demo/chaos.mjs", List.of("fetch(`${ENGINE}/api/pure/v1/grammar/grammarToJson/lam")),
            Map.entry("datacube/demo/engine.ts", List.of("fetch(input")),
            Map.entry("datacube/demo/page-config.ts", List.of("fetch(url")),
            Map.entry("datacube/demo/planners.ts", List.of("fetch(health")),
            Map.entry("datacube/demo/verify-calc-vocabulary.mjs", List.of("fetch(`${ENGINE}/api/server/v1/info`")),
            Map.entry("datacube/demo/verify-engine-differential.mjs", List.of("fetch(`${ENGINE}/api/server/v1/info`")),
            Map.entry("datacube/demo/verify-engine.mjs", List.of("fetch(`${API}${path}`")),
            Map.entry("datacube/demo/verify-features.mjs", List.of("fetch(`${engineUrl}/api/pure/v1/compilation/lambdaRela")),
            Map.entry("datacube/demo/widget-loader.ts", List.of("fetch(`${BASE}/${name}`")),
            Map.entry("datacube/src/cube-store.ts", List.of("fetch(`${this.#root}${path}`")),
            Map.entry("datacube/src/headless/calls.ts", List.of("fetch(method: string")),
            Map.entry("datacube/src/headless/host-tables.ts", List.of("fetch('/warehouse.json'")),
            Map.entry("datacube/src/headless/host.ts", List.of("fetch(init?.method ?? 'GET'")),
            Map.entry("datacube/src/services/browser.ts", List.of("fetch(url")),
            Map.entry("datacube/src/services/python-host.ts", List.of("fetch(`${link.baseUrl}${path}`", "fetch(`${link.baseUrl}/${at}&version=${version}${as}`", "fetch(`${link.baseUrl}/${path}`", "fetch(`${link.baseUrl}/${path}`", "fetch(`${link.baseUrl}/plan-and-run.json`", "fetch(`${link.baseUrl}/version.json?${query}`", "fetch(input")),
            Map.entry("datacube/src/ui/timeseries-view.ts", List.of("fetch(", "fetch(", "fetch(", "fetch(")),
            Map.entry("depot-client/src/client.ts", List.of("fetch(`${this.#api}${path}`")),
            Map.entry("engine-client/src/legend/engine.ts", List.of("fetch(`${this.#base}${path}`")),
            Map.entry("engine-client/src/pure-v1.ts", List.of("fetch(url")),
            Map.entry("engine-client/src/warehouse.ts", List.of("fetch(baseUrl.replace(/\\/+$/, '') + call.path")),
            Map.entry("query-store/src/client.ts", List.of("fetch(`${this.#api}/pure/v1/query${path}`")),
            Map.entry("query/demo/main.ts", List.of("fetch(url")),
            Map.entry("query/demo/verify.mjs", List.of("fetch(`http://127.0.0.1:${port}/health`")),
            Map.entry("sdlc-client/src/client.ts", List.of("fetch(`${this.#api}${path}`")),
            Map.entry("studio/demo/main.ts", List.of("fetch(params.get('config') ?? './config.json'")),
            Map.entry("studio/src/app/app.ts", List.of("fetch(new URL('manifest.json', base)", "fetch(new URL(f, base)")));

    /**
     * THE AUTHORED DOOR (the C final audit's H1, 2026-11-12: "any product file can write
     * {@code runAuthored(await grammar.authored('DuckDB', text), ...)} and every guard stays green"): a person's own
     * statement, carried by the planner as written (Boundary.authored) and run by the hosts' runner (runAuthored), is the
     * SQL playground's alone. Every reference to it in the TypeScript -- the runner {@code runAuthored}, a member or key
     * {@code authored} or {@code authoredOrError}, and the request kind {@code 'authored'} as a string -- by file,
     * exactly: the playground (studio run.ts, which runs it; app.ts, which plans it), and the planner's plumbing
     * (wasm-grammar.ts's client, the protocol's kind, the tab's answer of it, Python's page refusing it unasked).
     */
    static final Map<String, Integer> AUTHORED = Map.ofEntries(
            // 2026-11-12 (H1, measured): the playground's run (its runner's import and call, the session's member, read
            // twice) and the app's planning of its statement (the member, the client's call)
            Map.entry("studio/src/backend/run.ts", 5),
            Map.entry("studio/src/app/app.ts", 2),
            // the runner's definition; the planner's client (its method and its request's kind); the protocol's kind (in
            // the kinds' list and the request's type); the tab's answer of it (the export's type, its call, the kind)
            Map.entry("engine-client/src/plan-runner.ts", 1),
            Map.entry("engine-client/src/legend/wasm-grammar.ts", 2),
            Map.entry("engine-client/src/legend/planner-protocol.ts", 2),
            Map.entry("engine-client/src/legend/planner-answer.ts", 3),
            // Python's page refuses it by name, unasked (python-host.ts NOT_ASKED)
            Map.entry("datacube/src/services/python-host.ts", 1),
            // the tests of it: the runner's (plan-runner.test.ts) and the tab planner's fake module
            Map.entry("engine-client/test/plan-runner.test.ts", 17),
            Map.entry("datacube/test/lite-planner.test.ts", 1));

    /**
     * THE AUTHORED DOOR in Java (H1): the planner's {@code Authored.plan} is reached by the planner boundary alone
     * (Boundary.authored), and that by the one export a host asks -- the tab's (TabExports.authoredOrError) -- by file,
     * exactly. (2026-11-12, the orchestrator's ruling: the native library's lite_authored, which no headless host
     * walked, deleted; Python refuses the kind by name, pinned by python/tests/test_planned_guard.py.)
     */
    static final Map<String, Integer> AUTHORED_JAVA = Map.of(
            "wasm/src/main/java/planner/Boundary.java", 1,
            "wasm/src/main/java/planner/TabExports.java", 2);
    static final List<String> JAVA_PRODUCT = List.of("core/src/main/java", "wasm/src/main/java/planner", "native/src/main/java");

    /** The tests' folders, read beside the product's (host_sql.txt's): where a cast would hide a host's own SQL. */
    static final List<String> TESTS = List.of("engine-client/test", "datacube/test", "query/test", "studio/test");

    /** The two files that may cast to PlannedSql: the type's own mints, and the tests' (engine-client/test). */
    static final Set<String> CASTS = Set.of("engine-client/src/planned-sql.ts", "engine-client/test/unplanned.ts");

    // (PYTHON_MINTS, BRIDGE and PYTHON_EXECUTES, the Python half by regular expression: MOVED 2026-11-12, the C
    // checkpoint's code audit SHOULD-FIX 2, to python/tests/test_planned_guard.py, which reads every module with
    // Python's own parser -- an alias, a qualified call, a subclass, a send on any receiver are what a regular
    // expression passed)

    /** What a mint file may import from planned-sql.ts: the functions that type a plan's text or a planner's answer. */
    // 2026-11-12 (the re-audit's M1): and planFromAnswer, the plan the planner answered, typed as its answer is read
    static final Set<String> MINT_NAMES = Set.of("fromPlan", "allFromPlan", "plannedProjectData", "planFromAnswer");

    /**
     * The test files that import a value of planned-sql.ts, each with what it imports. 2026-11-12 (the C checkpoint's
     * code audit, MUST-FIX 1): the mints' own test. Every other test types its own SQL with {@code unplanned}.
     */
    static final Map<String, Set<String>> TEST_IMPORTS = Map.of(
            "engine-client/test/planned-sql.test.ts", Set.of("fromPlan", "allFromPlan"));

    /** A quoted module path naming planned-sql (any quote, any extension or none): every one is a statement the census
     *  below reads, or the guard fails. */
    private static final Pattern PLANNED_PATH = Pattern.compile("(['\"`])((?:[^'\"`\\s]*/)?planned-sql(?:\\.[cm]?[jt]s)?)\\1");
    /** An import or export statement's clause up to its quoted module path ({@code from}), or a side-effect import, or
     *  a dynamic import. */
    private static final Pattern MODULE_STATEMENT = Pattern.compile(
            "\\b(import|export)\\b((?:[^;'\"`(]|\\([^)]*\\))*?)\\bfrom\\s*(['\"`])([^'\"`]+)\\3"
                    + "|\\bimport\\s*(['\"`])([^'\"`]+)\\5"
                    + "|\\bimport\\s*\\(\\s*(['\"`])([^'\"`]+)\\7");
    // 2026-11-12 (the re-audit's M1): PlanText, the plan the planner answered, as the statement is
    private static final Pattern CAST = Pattern.compile(
            "\\bas\\s+(?:PlannedSql|PlanText)\\b|(?<![\\w$])<(?:PlannedSql|PlanText)>|\\bis\\s+(?:PlannedSql|PlanText)\\b");
    private static final Pattern NOT_PLANNED_CALL = Pattern.compile("\\bnotPlanned\\s*\\(");
    private static final Pattern STRING_LITERAL = Pattern.compile("(['\"`])((?:(?!\\1)[^\\\\]|\\\\.)*)\\1");
    private static final Pattern MINT_IDENTIFIER = Pattern.compile(
            "(?<![\\w$])(?:fromPlan|allFromPlan|notPlanned|plannedProjectData|planFromAnswer)(?![\\w$])");
    private static final Pattern ESCAPE = Pattern.compile("\\bas\\s+(?:never|any)\\b");
    /** A reference to the authored door, in code: its runner, a member or key named for it, or its export. */
    private static final Pattern AUTHORED_IDENTIFIER = Pattern.compile("(?<![\\w$])(?:runAuthored|authored|authoredOrError)(?![\\w$])");
    /** The request kind {@code 'authored'} as a string, in code with its strings kept (any quote). */
    private static final Pattern AUTHORED_KIND = Pattern.compile("(['\"`])authored\\1");
    /** Java's reach of the door: the planner's Authored.plan, the boundary's authored, the tab's export. */
    private static final Pattern AUTHORED_JAVA_REFERENCE = Pattern.compile(
            // S10 (the re-audit, 2026-11-12): a method reference (Authored::plan) is a reach too
            "\\bAuthored\\s*(?:\\.|::)\\s*plan\\b|\\bBoundary\\s*(?:\\.|::)\\s*authored\\b"
                    + "|(?<![\\w$])authoredOrError(?![\\w$])");
    private static final Pattern SUPPRESSION = Pattern.compile("@ts-(?:ignore|expect-error|nocheck)");
    /** H13: DuckDB-WASM's sends by call, its own ingest and file members by any reference, any of them bracketed. */
    private static final Pattern DATABASE_SEND = Pattern.compile(
            // S10 (the re-audit, 2026-11-12): a call, an optional call, or the method handed on by call/apply/bind
            "\\.\\s*(?:query|send|prepare)\\s*(?:\\?\\.\\s*)?(?:\\(|\\.\\s*(?:call|apply|bind)(?![\\w$]))"
            + "|\\.\\s*(?:insertArrowFromIPCStream|insertArrowTable|insertCSVFromPath|insertJSONFromPath|registerFileHandle"
            + "|registerFileURL|copyFileToBuffer|copyFileToPath|exportFileStatistics|runTask|postTask)(?![\\w$])");
    private static final Pattern BRACKETED_SEND = Pattern.compile(
            "\\[\\s*(['\"`])(?:query|send|prepare|connect)\\1\\s*\\]\\s*\\("
            + "|\\[\\s*(['\"`])(?:insertArrow\\w*|insertCSV\\w*|insertJSON\\w*|registerFile\\w*|copyFile\\w*)\\2\\s*\\]");
    private static final Pattern CONNECT_CALL = Pattern.compile("\\.\\s*connect\\s*\\(");
    private static final Pattern FETCH_CALL = Pattern.compile("(?<![\\w$])fetch\\s*\\(");
    private static final Pattern OTHER_HTTP = Pattern.compile("(?<![\\w$])(?:XMLHttpRequest|WebSocket|EventSource)(?![\\w$])"
            + "|\\.\\s*sendBeacon\\s*\\(");
    private static final Pattern SUBMIT_CALL = Pattern.compile("\\bcall\\s*(?:<[^>()]*>)?\\s*\\(\\s*(['\"`])submit\\1");
    /** S6 (the re-audit, 2026-11-12): `any`, cast or not, satisfies every type -- the brand's among them. */
    private static final Pattern ANY = Pattern.compile("(?<![\\w$.])any(?![\\w$])(?!\\s*[:(?])");
    /** M1 (the re-audit, 2026-11-12): a call of one of the hosts' plan runners, and any reference to one. */
    private static final Pattern RUNNER_CALL = Pattern.compile("(?<![\\w$.])(?:runPlan|runLoad|establish|runAuthored|targetKey)\\s*\\(");
    private static final Pattern RUNNER_NAME = Pattern.compile("(?<![\\w$.])(?:runPlan|runLoad|establish|runAuthored|targetKey)(?![\\w$])");
    /** M1: a destructuring pattern ({@code const { runLoad } = await import(...)}): a lazy import's binding, no hand-on. */
    private static final Pattern DESTRUCTURED = Pattern.compile("\\{([^{}]*)\\}[\\s\\]]*=(?![=>])");
    /** M1: the plan format's name, in code with its strings kept: where a plan is read, never where one is written. */
    private static final Pattern PLAN_FORMAT = Pattern.compile("legend-lite-plan");
    /** H5: a cast through unknown, which satisfies any type. */
    private static final Pattern UNKNOWN_CAST = Pattern.compile("\\bas\\s+unknown\\s+as\\b");
    /** H5: the brand under another name -- a renamed import or export of it, or a type alias of it. */
    private static final Pattern BRAND_ALIAS = Pattern.compile(
            "\\b(?:PlannedSql|PlanText)\\s+as\\s+[\\w$]|\\btype\\s+[\\w$]+\\s*(?:<[^=;]*>)?\\s*=\\s*(?:PlannedSql|PlanText)\\s*;");
    /** A declaration of a type: an interface or class (its body braced), or a type alias (its body to its ';'). */
    private static final Pattern DECLARATION = Pattern.compile(
            "\\b(?:interface|class)\\s+([\\w$]+)[^{;]*\\{|\\btype\\s+([\\w$]+)\\s*(?:<[^=;]*>)?\\s*=");
    /** A property's name and its colon, a method's being followed by its parameters (marked, so never matched). */
    private static final Pattern PROPERTY = Pattern.compile("(?<![\\w$.])(?:readonly\\s+)?[\\w$]+\\??\\s*:\\s*");
    /** An Omit of a type (one level of nested generics): what it names is not all it holds. */
    private static final Pattern OMIT = Pattern.compile("\\bOmit\\s*<[^<>]*(?:<[^<>]*>[^<>]*)*>");
    /** A cast's start (never {@code as const}), and the angle-bracket assertion of a named type. */
    private static final Pattern AS = Pattern.compile("\\bas\\s+(?!const\\b)");
    private static final Pattern ANGLE_ASSERTION = Pattern.compile("(?<![\\w$])<([\\w$]+)>");
    private static final Pattern RUN_TS_IMPORT = Pattern.compile(
            "\\bimport\\s*(type\\s+)?\\{([^}]*)\\}\\s*from\\s*['\"`][^'\"`]*backend/run(?:\\.ts)?['\"`]");
    private static final Pattern PLAYGROUND_READ = Pattern.compile("\\.\\s*playground\\b|\\[\\s*['\"`]playground['\"`]\\s*\\]"
            + "|\\{[^{}]*\\bplayground\\b[^{}]*\\}\\s*=(?!=)");
    private static final Pattern UNPLANNED_IMPORT = Pattern.compile("from\\s*'[^']*test/unplanned\\.ts'");

    /** host_sql.txt's [typescript] and [python] folders: the product's, one list with the host-SQL guard. */
    private static Map<String, List<String>> folders() throws IOException {
        Map<String, List<String>> sections = new TreeMap<>();
        List<String> current = null;
        for (String line : Files.readAllLines(Runfile.property("host.sql.rule"), StandardCharsets.UTF_8)) {
            if (line.isBlank() || line.startsWith("#")) {
                continue;
            }
            if (line.startsWith("[") && line.endsWith("]")) {
                current = sections.computeIfAbsent(line.substring(1, line.length() - 1), k -> new ArrayList<>());
            } else if (current != null) {
                current.add(line.strip());
            }
        }
        return sections;
    }

    /** The checker census's planted violators (datacube/test/brand-casts.test.ts, the re-audit's S5, 2026-11-12): casts
     *  written to be read as violators by TypeScript's own checker, which reads them; never read as text here. */
    static final String PLANTED_CASTS = "datacube/test/fixtures/brand-casts/";

    private static List<String> inventory() throws IOException {
        return Files.readAllLines(Runfile.property("inventory.files")).stream()
                .filter(f -> !f.startsWith(PLANTED_CASTS)).toList();
    }

    /** Each file read once: a census runs twice in {@link #bites} (the tree, then a violator planted on it). */
    private static final Map<String, String> READ = new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * Each per-file scan a census makes, kept by its input: a census runs once over the tree in its own @Test, then
     * again over the tree with one violator planted on it for each of its {@link #bites} -- so the tree's files are
     * scanned once, the plant alone afresh (2026-11-13: the bites ran every census over every file again, 18 s of the
     * guard's 27 here, and the guard timed out at its 60 s on Windows' CI, gates run 43380062329). A key is the scan's
     * name and its inputs (a file's text is the one String {@link #READ} holds, so a repeat lookup compares it by
     * identity); a value is never changed after it is kept.
     */
    private static final Map<List<Object>, Object> SCANNED = new java.util.concurrent.ConcurrentHashMap<>();

    @SuppressWarnings("unchecked")
    private static <T> T scanned(java.util.function.Supplier<T> scan, Object... key) {
        List<Object> k = List.of(key);
        Object kept = SCANNED.get(k);
        if (kept == null) {
            kept = scan.get();
            SCANNED.put(k, kept);
        }
        return (T) kept;
    }

    /** Whether {@code p} is found in {@code text}, scanned once per text. */
    private static boolean finds(Pattern p, String text) {
        return scanned(() -> p.matcher(text).find(), "finds", p, text);
    }

    /** {@code code} with each import and export statement blanked to spaces (offsets kept), once per text. */
    private static String blankModules(String code) {
        return scanned(() -> MODULE_STATEMENT.matcher(code).replaceAll(r -> " ".repeat(r.group().length())), "blank", code);
    }

    private static String read(String file) throws IOException {
        try {
            return READ.computeIfAbsent(file, f -> {
                try {
                    return Files.readString(Runfile.of("_main/" + f), StandardCharsets.UTF_8);
                } catch (IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
            });
        } catch (java.io.UncheckedIOException e) {
            throw e.getCause();
        }
    }

    /**
     * The sources a census reads: the tree's (its inventory, each file as Bazel's runfiles hold it), and any file
     * {@code planted} on top of it -- a violator the census's own pinned comparison must FAIL on (check 6, the final
     * audit's item 5, 2026-11-12: a census that only "sees" a planted form proves its detector, not that its pin
     * refuses it).
     */
    record Sources(Map<String, String> planted) {
        static final Sources TREE = new Sources(Map.of());

        static Sources planting(String file, String text) {
            return new Sources(Map.of(file, text));
        }

        List<String> inventory() throws IOException {
            List<String> out = new ArrayList<>(PlannedSqlTest.inventory());
            out.addAll(new TreeSet<>(planted.keySet()));
            return out;
        }

        String read(String file) throws IOException {
            String text = planted.get(file);
            return text != null ? text : PlannedSqlTest.read(file);
        }
    }

    /** A census run over {@link Sources}. */
    interface Census {
        void run(Sources s) throws IOException;
    }

    /** A violator planted in the product's TypeScript: a path under a product folder no file of the tree has. */
    static final String PLANTED_VIOLATOR = "datacube/src/planted-violator.ts";

    /**
     * {@code census}'s pinned comparison FAILS over the tree with {@code text} planted at {@link #PLANTED_VIOLATOR},
     * naming it (check 6): never merely that its detector sees the form.
     */
    static void bites(String what, Census census, String text) throws IOException {
        // the tree alone passes each census: its own @Test runs it (a red one fails there, and its failure here names no
        // planted file)
        Throwable failed = null;
        try {
            census.run(Sources.planting(PLANTED_VIOLATOR, text));
        } catch (AssertionError e) {
            failed = e;
        }
        if (failed == null) {
            throw new AssertionError(what + ": its pinned comparison passed over a planted violator: " + text);
        }
        if (failed.getMessage() == null || !failed.getMessage().contains(PLANTED_VIOLATOR)) {
            throw new AssertionError(what + ": failed over the planted violator without naming it: " + failed.getMessage(),
                    failed);
        }
    }

    /** CHECK 6 (the final audit's item 5, 2026-11-12): every census's pinned comparison fails on an actual violation. */
    @Test
    void everyCensusFailsOnAPlantedViolator() throws IOException {
        String mint = "import { fromPlan } from '../../engine-client/src/planned-sql.ts';\n";
        bites("the mint census", PlannedSqlTest::everyUseOfAMintIsCounted_andNoNewEscapeFromTheTypeSystem,
                mint + "export const sql = fromPlan(text);");
        bites("the escape census", PlannedSqlTest::everyUseOfAMintIsCounted_andNoNewEscapeFromTheTypeSystem,
                "export const sql = text as never;");
        bites("the authored census", PlannedSqlTest::theAuthoredDoorIsReachedFromThePlaygroundAndItsPlumbingAlone,
                "export const plan = await grammar.authored('DuckDB', text);");
        bites("the import census", PlannedSqlTest::thePlannedStatementIsMadeOnlyWhereAPlanIsRead,
                mint + "export const sql = fromPlan(text);");
        bites("the cast census", PlannedSqlTest::thePlannedStatementIsMadeOnlyWhereAPlanIsRead,
                "import type { PlannedSql } from '../../engine-client/src/planned-sql.ts';\n"
                        + "export const sql = text as PlannedSql;");
        bites("the playground census", PlannedSqlTest::thePlaygroundSendsAPlansStatement_reachedFromThePlaygroundAlone,
                "export const send = context.playground;");
        bites("the brand alias census", PlannedSqlTest::theBrandIsNeverSatisfiedByAnotherName,
                "import type { PlannedSql } from '../../engine-client/src/planned-sql.ts';\n"
                        + "export type Statement = PlannedSql;");
        bites("the holder cast census", PlannedSqlTest::theBrandIsNeverSatisfiedByAnotherName,
                "export const plan = JSON.parse(text) as Plan;");
        bites("the unknown cast census", PlannedSqlTest::theBrandIsNeverSatisfiedByAnotherName,
                "export const when = text as unknown as Date;");
        bites("the send census", PlannedSqlTest::everyTransportHasOneChokePoint,
                "export const rows = await connection.query(text);");
        bites("the connect census", PlannedSqlTest::everyTransportHasOneChokePoint,
                "export const connection = await db.connect();");
        bites("the fetch census", PlannedSqlTest::everyTransportHasOneChokePoint,
                "export const sent = await fetch('https://warehouse.example/api/2.0/sql/statements', { method: 'POST' });");
        bites("the submit census", PlannedSqlTest::everyTransportHasOneChokePoint,
                "export const sent = await this.call('submit', { statement: text });");
        bites("the runner census", PlannedSqlTest::thePlanRunnersAreCalledWhereAPlanTheyAreHandedWasAnswered,
                "import { runPlan } from '../../engine-client/src/plan-runner.ts';\n"
                        + "export const answer = await runPlan(plan, session);");
        bites("the plan format census", PlannedSqlTest::thePlanRunnersAreCalledWhereAPlanTheyAreHandedWasAnswered,
                "export const plan = JSON.stringify({ format: 'legend-lite-plan', root: {} });");
        bites("the any census", PlannedSqlTest::noAnyInTheProduct, "export let value: any = text;");
        bites("the tests' statement census", PlannedSqlTest::noProductSourceReachesTheTestsOwnStatement,
                "import { unplanned } from '../test/unplanned.ts';\nexport const sql = unplanned(text);");
    }

    private static boolean under(String file, List<String> folders) {
        return folders.stream().anyMatch(d -> file.startsWith(d + "/"));
    }

    /** One source's references to planned-sql.ts, read statement by statement: the values it imports, and each form
     *  the census refuses (a namespace, default, side-effect or dynamic import, an alias, a value re-export, a path it
     *  cannot place). A type-only import or re-export is free: a type mints nothing. */
    record Imports(Set<String> values, List<String> refused) {
    }

    static Imports imports(String src) {
        return scanned(() -> {
            Imports found = importsOf(src);
            return new Imports(java.util.Collections.unmodifiableSet(new TreeSet<>(found.values())), List.copyOf(found.refused()));
        }, "imports", src);
    }

    private static Imports importsOf(String src) {
        Set<String> values = new TreeSet<>();
        List<String> refused = new ArrayList<>();
        Set<Integer> paths = new TreeSet<>();
        Matcher p = PLANNED_PATH.matcher(src);
        while (p.find()) {
            paths.add(p.start(2));
        }
        Matcher m = MODULE_STATEMENT.matcher(src);
        while (m.find()) {
            if (m.group(4) != null) {
                if (!paths.remove(m.start(4))) {
                    continue;
                }
                String clause = m.group(2).strip();
                boolean typeOnly = clause.startsWith("type ") || clause.startsWith("type{");
                if (m.group(1).equals("export")) {
                    if (!typeOnly) {
                        refused.add("a re-export (" + clause + "): the mints are planned-sql.ts's alone");
                    }
                    continue;
                }
                if (typeOnly) {
                    continue;
                }
                if (clause.contains("*")) {
                    refused.add("a namespace import (" + clause + ")");
                    continue;
                }
                int brace = clause.indexOf('{');
                String head = (brace < 0 ? clause : clause.substring(0, brace)).replace(",", "").strip();
                if (!head.isEmpty()) {
                    refused.add("a default import (" + head + ")");
                }
                if (brace >= 0) {
                    for (String spec : clause.substring(brace + 1, clause.lastIndexOf('}')).split(",")) {
                        String one = spec.strip();
                        if (one.isEmpty() || one.startsWith("type ")) {
                            continue;
                        }
                        if (one.matches(".*\\bas\\b.*")) {
                            refused.add("an alias (" + one + ")");
                        } else {
                            values.add(one);
                        }
                    }
                }
            } else if (m.group(6) != null) {
                if (paths.remove(m.start(6))) {
                    refused.add("a side-effect import");
                }
            } else if (paths.remove(m.start(8))) {
                refused.add("a dynamic import");
            }
        }
        for (int at : paths) {
            refused.add("a reference to planned-sql the census does not read as an import, at line "
                    + (src.substring(0, at).chars().filter(c -> c == '\n').count() + 1));
        }
        return new Imports(values, refused);
    }

    /** Each {@code notPlanned(} call's first argument: a literal's text, or the expression, marked. */
    static List<String> notPlannedReasons(String src) {
        return scanned(() -> List.copyOf(notPlannedReasonsOf(src)), "notPlanned", src);
    }

    private static List<String> notPlannedReasonsOf(String src) {
        List<String> reasons = new ArrayList<>();
        Matcher m = NOT_PLANNED_CALL.matcher(src);
        while (m.find()) {
            int at = m.end();
            while (at < src.length() && Character.isWhitespace(src.charAt(at))) {
                at++;
            }
            Matcher literal = STRING_LITERAL.matcher(src).region(at, src.length());
            if (literal.lookingAt()) {
                reasons.add(literal.group(2));
            } else {
                int stop = at;
                while (stop < src.length() && ",)".indexOf(src.charAt(stop)) < 0) {
                    stop++;
                }
                reasons.add("<not a literal: " + src.substring(at, stop).strip() + ">");
            }
        }
        return reasons;
    }

    /**
     * {@code src}'s code: each comment and each string's text blanked to spaces (newlines kept), a template's
     * {@code ${...}} kept as code -- what an identifier count and a cast count read, prose and messages left out.
     */
    static String code(String src) {
        return code(src, false);
    }

    /** {@link #code(String)}, its strings' text kept where {@code strings}: comments alone blanked. */
    static String code(String src, boolean strings) {
        return scanned(() -> codeOf(src, strings), "code", strings, src);
    }

    private static String codeOf(String src, boolean strings) {
        StringBuilder out = new StringBuilder(src.length());
        java.util.ArrayDeque<int[]> templates = new java.util.ArrayDeque<>();   // each open ${, its brace depth
        int i = 0;
        int n = src.length();
        while (i < n) {
            char c = src.charAt(i);
            char next = i + 1 < n ? src.charAt(i + 1) : '\0';
            if (c == '/' && next == '/') {
                while (i < n && src.charAt(i) != '\n') {
                    out.append(' ');
                    i++;
                }
            } else if (c == '/' && next == '*') {
                int end = src.indexOf("*/", i + 2);
                end = end < 0 ? n : end + 2;
                for (; i < end; i++) {
                    out.append(src.charAt(i) == '\n' ? '\n' : ' ');
                }
            } else if (c == '\'' || c == '"') {
                out.append(c);
                i++;
                while (i < n && src.charAt(i) != c && src.charAt(i) != '\n') {
                    if (src.charAt(i) == '\\' && i + 1 < n) {
                        out.append(strings ? src.charAt(i) : ' ');
                        i++;
                    }
                    out.append(strings ? src.charAt(i) : ' ');
                    i++;
                }
                if (i < n) {
                    out.append(src.charAt(i));
                    i++;
                }
            } else if (c == '`' || (c == '}' && !templates.isEmpty() && templates.peek()[0] == 0)) {
                if (c == '}') {
                    templates.pop();
                }
                out.append(c);
                i++;
                // a template's text, to its end or its next ${
                while (i < n && src.charAt(i) != '`' && !(src.charAt(i) == '$' && i + 1 < n && src.charAt(i + 1) == '{')) {
                    if (src.charAt(i) == '\\' && i + 1 < n) {
                        out.append(strings ? src.charAt(i) : ' ');
                        i++;
                    }
                    out.append(strings || src.charAt(i) == '\n' ? src.charAt(i) : ' ');
                    i++;
                }
                if (i < n && src.charAt(i) == '`') {
                    out.append('`');
                    i++;
                } else if (i < n) {
                    out.append("${");
                    i += 2;
                    templates.push(new int[] {0});
                }
            } else {
                if (!templates.isEmpty()) {
                    if (c == '{') {
                        templates.peek()[0]++;
                    } else if (c == '}') {
                        templates.peek()[0]--;
                    }
                }
                out.append(c);
                i++;
            }
        }
        return out.toString();
    }

    private static int count(Pattern p, String text) {
        return scanned(() -> countOf(p, text), "count", p, text);
    }

    private static int countOf(Pattern p, String text) {
        int found = 0;
        Matcher m = p.matcher(text);
        while (m.find()) {
            found++;
        }
        return found;
    }

    @Test
    void everyUseOfAMintIsCounted_andNoNewEscapeFromTheTypeSystem() throws IOException {
        everyUseOfAMintIsCounted_andNoNewEscapeFromTheTypeSystem(Sources.TREE);
    }

    static void everyUseOfAMintIsCounted_andNoNewEscapeFromTheTypeSystem(Sources s) throws IOException {
        List<String> product = folders().get("typescript");
        List<String> read = new ArrayList<>(product);
        read.addAll(TESTS);
        Map<String, Integer> references = new TreeMap<>();
        Map<String, Integer> escapes = new TreeMap<>();
        for (String file : s.inventory()) {
            if (!file.matches(".*\\.(?:[cm]?ts|tsx)$") || !under(file, read)) {
                continue;
            }
            String code = code(s.read(file));
            int refs = count(MINT_IDENTIFIER, code);
            if (refs > 0) {
                references.put(file, refs);
            }
            if (under(file, product)) {
                int e = count(ESCAPE, code) + count(SUPPRESSION, s.read(file));
                if (e > 0) {
                    escapes.put(file, e);
                }
            }
        }
        assertEquals(new TreeMap<>(MINT_REFERENCES), references, "each file's uses of a mint, by identifier, pinned"
                + " (PlannedSqlTest.MINT_REFERENCES): a new one is a statement typed where no plan was read");
        List<String> grew = new ArrayList<>();
        List<String> shrank = new ArrayList<>();
        for (Map.Entry<String, Integer> e : escapes.entrySet()) {
            int pinned = ESCAPES.getOrDefault(e.getKey(), 0);
            if (e.getValue() > pinned) {
                grew.add(e.getKey() + ": " + e.getValue() + " > " + pinned);
            }
        }
        for (Map.Entry<String, Integer> e : ESCAPES.entrySet()) {
            int now = escapes.getOrDefault(e.getKey(), 0);
            if (now < e.getValue()) {
                shrank.add(e.getKey() + ": " + now + " < " + e.getValue());
            }
        }
        assertEquals(List.of(), grew, "a new `as never`, `as any` or suppressed check in the product's TypeScript: it"
                + " satisfies the PlannedSql brand as a cast does (PlannedSqlTest.ESCAPES, shrink-only)");
        assertEquals(List.of(), shrank, "an escape gone: lower its file's count in PlannedSqlTest.ESCAPES, dated");
    }

    /** {@code src}'s references to the authored door (H1): identifiers in its code, the kind as a string in its code. */
    static int authoredReferences(String src) {
        return count(AUTHORED_IDENTIFIER, code(src)) + count(AUTHORED_KIND, code(src, true));
    }

    @Test
    void theAuthoredDoorIsReachedFromThePlaygroundAndItsPlumbingAlone() throws IOException {
        theAuthoredDoorIsReachedFromThePlaygroundAndItsPlumbingAlone(Sources.TREE);
    }

    static void theAuthoredDoorIsReachedFromThePlaygroundAndItsPlumbingAlone(Sources s) throws IOException {
        List<String> product = folders().get("typescript");
        List<String> read = new ArrayList<>(product);
        read.addAll(TESTS);
        Map<String, Integer> found = new TreeMap<>();
        Map<String, Integer> java = new TreeMap<>();
        for (String file : s.inventory()) {
            if (file.matches(".*\\.(?:[cm]?ts|tsx|[cm]?js)$") && under(file, read)) {
                int refs = authoredReferences(s.read(file));
                if (refs > 0) {
                    found.put(file, refs);
                }
            } else if (file.endsWith(".java") && under(file, JAVA_PRODUCT)) {
                int refs = count(AUTHORED_JAVA_REFERENCE, code(s.read(file)));
                if (refs > 0) {
                    java.put(file, refs);
                }
            }
        }
        assertEquals(new TreeMap<>(AUTHORED), found, "a person's own statement is the SQL playground's alone: every"
                + " reference to runAuthored, an `authored` member or the 'authored' request kind is pinned by file"
                + " (PlannedSqlTest.AUTHORED, the C final audit's H1)");
        assertEquals(new TreeMap<>(AUTHORED_JAVA), java, "the planner's Authored.plan is reached by Boundary.authored"
                + " alone, and that by the tab's export and the native library's (PlannedSqlTest.AUTHORED_JAVA)");
    }

    /** Each way of reaching the door is counted (H1's planted violators): a census that stops seeing one is red. */
    @Test
    void theAuthoredCensusSeesEveryForm() {
        for (String planted : List.of(
                "import { runAuthored } from '../../engine-client/src/plan-runner.ts';",
                "import { runAuthored as go } from './plan-runner.ts'; go(p, r);",
                "const plan = await grammar.authored('DuckDB', text);",
                "const { authored } = grammar;",
                "const ask = grammar['authored'];",
                "port.ask({ kind: \"authored\", request });",
                "port.ask({ kind: `authored`, request });",
                "m.exports.authoredOrError(r)")) {
            assertTrue(authoredReferences(planted) > 0, "the authored census does not see: " + planted);
        }
        // prose and messages are not references
        assertEquals(0, authoredReferences("// runAuthored, grammar.authored('x') in a comment\n"
                + "const m = 'an authored statement'; /* kind: 'authored' */"));
        assertTrue(count(AUTHORED_JAVA_REFERENCE, code("return Authored.plan(d, t, text);")) == 1);
        assertTrue(count(AUTHORED_JAVA_REFERENCE, code("a = Folded.of(() -> Boundary.authored(r));")) == 1);
        assertTrue(count(AUTHORED_JAVA_REFERENCE, code("Function<String, Plan> f = Authored::plan;")) == 1);
        assertTrue(count(AUTHORED_JAVA_REFERENCE, code("map(Boundary::authored)")) == 1);
        assertEquals(0, count(AUTHORED_JAVA_REFERENCE, code("/** {@code Boundary.authored} */ String s = \"Authored.plan\";")));
    }

    @Test
    void theCodeReaderBlanksProseAndText() {
        String src = "const a = fromPlan(x); // fromPlan in a comment\n/* as any */ const b = 'fromPlan as never';\n"
                + "const c = `${fromPlan(y)} as any ${`as never`}`; const d = z as any;";
        assertEquals(2, count(MINT_IDENTIFIER, code(src)), code(src));
        assertEquals(1, count(ESCAPE, code(src)), code(src));
    }

    @Test
    void thePlannedStatementIsMadeOnlyWhereAPlanIsRead() throws IOException {
        thePlannedStatementIsMadeOnlyWhereAPlanIsRead(Sources.TREE);
    }

    static void thePlannedStatementIsMadeOnlyWhereAPlanIsRead(Sources s) throws IOException {
        List<String> product = folders().get("typescript");
        List<String> read = new ArrayList<>(product);
        read.addAll(TESTS);
        List<String> ts = s.inventory().stream().filter(f -> f.matches(".*\\.(?:[cm]?ts|tsx|[cm]?js)$"))
                .filter(f -> under(f, read)).toList();
        assertTrue(ts.size() > 500, "the guard read too few .ts files to prove anything: " + ts.size());
        for (String folder : TESTS) {
            assertTrue(ts.stream().anyMatch(f -> f.startsWith(folder + "/")), folder + " holds no .ts: a folder moved");
        }
        String own = "engine-client/src/planned-sql.ts";
        assertTrue(s.read(own).contains("unique symbol"),
                "PlannedSql is a branded string (planned-sql.ts): a plain string is not one");
        Map<String, Set<String>> productImports = new TreeMap<>();
        Map<String, Set<String>> testImports = new TreeMap<>();
        List<String> refused = new ArrayList<>();
        List<String> casts = new ArrayList<>();
        Map<String, String> named = new TreeMap<>();
        for (String file : ts) {
            if (file.equals(own)) {
                continue;
            }
            String src = s.read(file);
            Imports im = imports(src);
            im.refused().forEach(r -> refused.add(file + ": " + r));
            if (!im.values().isEmpty()) {
                (under(file, product) ? productImports : testImports).put(file, im.values());
            }
            if (!CASTS.contains(file) && finds(CAST, src)) {
                casts.add(file);
            }
            for (String reason : notPlannedReasons(src)) {
                named.merge(file, reason, (a, b) -> a + " | " + b);
            }
        }
        assertEquals(List.of(), refused, "planned-sql.ts is imported by name, a value at a time, never aliased, by"
                + " namespace, by default, dynamically or re-exported: each importer and each name is pinned");
        assertEquals(new TreeSet<>(MINTS), productImports.keySet(), "a value of planned-sql.ts -- a mint -- is imported"
                + " only where a plan or the planner's answer is read (PlannedSqlTest.MINTS): a host's own text is not a"
                + " plan's");
        for (String mint : MINTS) {
            assertTrue(MINT_NAMES.containsAll(productImports.get(mint)), mint + " imports " + productImports.get(mint)
                    + ": a plan reader imports the mints alone (PlannedSqlTest.MINT_NAMES)");
        }
        assertEquals(new TreeMap<>(TEST_IMPORTS), testImports, "a test types its own SQL with `unplanned`"
                + " (engine-client/test/unplanned.ts); a test that imports a mint is pinned (PlannedSqlTest.TEST_IMPORTS)");
        assertEquals(List.of(), casts, "a cast to PlannedSql (or a guard narrowing to it) outside its mints: a test's own"
                + " SQL is `unplanned` (engine-client/test/unplanned.ts), a product's is planned");
        assertEquals(Map.of(), named, "notPlanned is deleted (2026-11-12): no host sends a statement outside a plan, so"
                + " none names one");
    }

    /** The census refuses each form it cannot pin: measured, so a change to the census that stops seeing one is red. */
    @Test
    void theImportCensusSeesEveryForm() {
        assertEquals(Set.of("fromPlan", "allFromPlan"), imports("import { allFromPlan, fromPlan, type PlannedSql } from"
                + " '../planned-sql.ts';").values());
        assertEquals(Set.of("notPlanned"), imports("import {\n  notPlanned,\n} from \"./planned-sql.ts\"").values());
        assertEquals(new Imports(Set.of(), List.of()), imports("import type { PlannedSql } from './planned-sql.ts';"));
        assertEquals(new Imports(Set.of(), List.of()), imports("import { type PlannedSql } from `./planned-sql`;"));
        assertEquals(new Imports(Set.of(), List.of()), imports("export type { PlannedSql } from './planned-sql.ts';"));
        for (String form : List.of(
                "import * as P from './planned-sql.ts';",
                "import P from './planned-sql.ts';",
                "import P, { fromPlan } from './planned-sql.ts';",
                "import { notPlanned as named } from './planned-sql.ts';",
                "import './planned-sql.ts';",
                "const p = await import('./planned-sql.ts');",
                "export * from './planned-sql.ts';",
                "export { fromPlan } from \"../src/planned-sql.ts\";",
                "const where = '../planned-sql.ts';")) {
            assertTrue(!imports(form).refused().isEmpty(), "the census does not refuse: " + form);
        }
        assertEquals(List.of("the SQL playground", "<not a literal: why>", "a ' b"),
                notPlannedReasons("notPlanned('the SQL playground', s); notPlanned(why, s); notPlanned( \"a ' b\", s)"));
    }

    @Test
    void thePlaygroundSendsAPlansStatement_reachedFromThePlaygroundAlone() throws IOException {
        thePlaygroundSendsAPlansStatement_reachedFromThePlaygroundAlone(Sources.TREE);
    }

    static void thePlaygroundSendsAPlansStatement_reachedFromThePlaygroundAlone(Sources s) throws IOException {
        String backend = s.read(PLAYGROUND);
        int factory = backend.indexOf("export function playground(session: RunSession): Playground {");
        int runner = backend.indexOf("export function runner(session: RunSession): Runner {");
        assertTrue(factory >= 0 && runner > factory, "run.ts's playground() and runner() moved: " + PLAYGROUND);
        int send = backend.indexOf("return runAuthored(plan, { databaseType, run: (sql) => engine.run(sql, 0) });");
        assertTrue(send > factory && send < runner, "the playground's send is the Playground object's, and no other's");
        int planned = backend.indexOf("const plan = await session.authored(statement);");
        assertTrue(planned > factory && planned < send, "the playground sends the plan's one statement: the person's"
                + " text as the planner carried it (Boundary.authored), never the text itself");
        // a type of planned-sql.ts names the plan it is handed (PlanText, the re-audit's M1); no value of it is imported
        assertTrue(imports(backend).values().isEmpty() && imports(backend).refused().isEmpty(), "the playground mints"
                + " nothing: the plan runner types the plan's"
                + " statement");
        String runnerType = backend.substring(backend.indexOf("export interface Runner {"),
                backend.indexOf("}", backend.indexOf("export interface Runner {")));
        assertTrue(!runnerType.contains("sql("), "the Runner every run is handed holds no way to send SQL");
        List<String> product = folders().get("typescript");
        Map<String, Set<String>> imports = new TreeMap<>();
        Map<String, List<String>> reads = new TreeMap<>();
        for (String file : s.inventory()) {
            if (!file.endsWith(".ts") || !under(file, product) || file.equals(PLAYGROUND)) {
                continue;
            }
            List<List<String>> uses = playgroundUses(s.read(file));
            if (!uses.get(0).isEmpty()) {
                imports.put(file, new TreeSet<>(uses.get(0)));
            }
            if (!uses.get(1).isEmpty()) {
                reads.put(file, uses.get(1));
            }
        }
        assertEquals(new TreeMap<>(PLAYGROUND_IMPORTS), imports, "the playground is made by the app and typed by the"
                + " editor's context alone (PlannedSqlTest.PLAYGROUND_IMPORTS)");
        assertEquals(Map.of("studio/src/ui/editor.ts", List.of(".playground.sql(sqlText.value, ws.model(")), reads,
                "the editor's context's playground is read once, by the SQL playground's run of its own text, and never"
                        + " destructured or passed on");
        assertTrue(s.read("studio/src/app/app.ts").split("\\bplayground\\(session\\)", -1).length == 2,
                "the app makes the playground once");
    }

    /** {@code src}'s playground: the names it imports from run.ts that are the playground, and each read of a
     *  {@code playground} member (its first 40 characters), once per text. */
    private static List<List<String>> playgroundUses(String src) {
        return scanned(() -> {
            List<String> named = new ArrayList<>();
            Matcher i = RUN_TS_IMPORT.matcher(src);
            while (i.find()) {
                for (String spec : i.group(2).split(",")) {
                    String name = spec.strip().replaceFirst("^type\\s+", "").replaceFirst("\\s+as\\s+.*", "");
                    if (name.equalsIgnoreCase("playground")) {
                        named.add(name);
                    }
                }
            }
            List<String> read = new ArrayList<>();
            Matcher r = PLAYGROUND_READ.matcher(src);
            while (r.find()) {
                read.add(src.substring(r.start(), Math.min(src.length(), r.start() + 40)).replaceAll("\\s+", " "));
            }
            return List.of(List.copyOf(named), List.copyOf(read));
        }, "playground", src);
    }

    /** Each type the TypeScript declares that holds the brand in a property, by name (H5): a fixed point over every
     *  declaration in {@code codes}, from PlannedSql. */
    static Set<String> brandHolders(List<String> codes) {
        Map<String, List<String>> braced = new TreeMap<>();
        Map<String, List<String>> aliased = new TreeMap<>();
        for (String s : codes) {
            List<Map<String, List<String>>> declared = declarations(s);
            declared.get(0).forEach((k, v) -> braced.computeIfAbsent(k, x -> new ArrayList<>()).addAll(v));
            declared.get(1).forEach((k, v) -> aliased.computeIfAbsent(k, x -> new ArrayList<>()).addAll(v));
        }
        Set<String> holders = new TreeSet<>(Set.of("PlannedSql"));
        for (boolean changed = true; changed; ) {
            changed = false;
            Pattern token = Pattern.compile("(?<![\\w$])(?:" + String.join("|", holders) + ")(?![\\w$])");
            for (Map<String, List<String>> kind : List.of(braced, aliased)) {
                for (Map.Entry<String, List<String>> e : kind.entrySet()) {
                    boolean alias = kind == aliased;
                    if (!holders.contains(e.getKey()) && e.getValue().stream().anyMatch(b -> scanned(
                            () -> holds(b, alias, token), "holds", b, alias, token.pattern()))) {
                        holders.add(e.getKey());
                        changed = true;
                    }
                }
            }
        }
        return holders;
    }

    /** {@code s}'s declarations, once per text: its interfaces' and classes' bodies (braced) and its type aliases'
     *  (aliased), each by its name. */
    private static List<Map<String, List<String>>> declarations(String s) {
        return scanned(() -> {
            Map<String, List<String>> braced = new TreeMap<>();
            Map<String, List<String>> aliased = new TreeMap<>();
            Matcher m = DECLARATION.matcher(s);
            while (m.find()) {
                boolean isBraced = m.group(1) != null;
                int depth = isBraced ? 1 : 0;
                int j = m.end();
                for (; j < s.length(); j++) {
                    char c = s.charAt(j);
                    if (c == '{' || c == '(' || c == '[') {
                        depth++;
                    } else if (c == '}' || c == ')' || c == ']') {
                        depth--;
                        if (depth == 0 && isBraced) {
                            break;
                        }
                    } else if (c == ';' && depth == 0 && !isBraced) {
                        break;
                    }
                }
                (isBraced ? braced : aliased).computeIfAbsent(isBraced ? m.group(1) : m.group(2), k -> new ArrayList<>())
                        .add(s.substring(m.end(), Math.min(j, s.length())));
            }
            Map<String, List<String>> b = new TreeMap<>();
            braced.forEach((k, v) -> b.put(k, List.copyOf(v)));
            Map<String, List<String>> a = new TreeMap<>();
            aliased.forEach((k, v) -> a.put(k, List.copyOf(v)));
            return List.of(java.util.Collections.unmodifiableMap(b), java.util.Collections.unmodifiableMap(a));
        }, "declarations", s);
    }

    /** A body holds the brand when a property's type names a holder (never a method's, never a function's, never a
     *  local's in a method's body: an interface's or class's properties are at its own top level), or -- a type alias --
     *  its own top level does, outside an Omit. */
    private static boolean holds(String body, boolean alias, Pattern token) {
        String flat = parametersMarked(body);
        int[] depth = new int[flat.length() + 1];
        for (int i = 0, d = 0; i < flat.length(); i++) {
            depth[i] = d;
            char c = flat.charAt(i);
            d += c == '{' || c == '[' ? 1 : c == '}' || c == ']' ? -1 : 0;
        }
        Matcher p = PROPERTY.matcher(flat);
        while (p.find()) {
            if (!alias && depth[p.start()] > 0) {
                continue;
            }
            String type = typeAt(flat, p.end(), ";,\n");
            if (!type.contains("=>") && token.matcher(OMIT.matcher(type).replaceAll(" ")).find()) {
                return true;
            }
        }
        if (alias) {
            String top = topLevel(OMIT.matcher(flat).replaceAll(" "));
            return !top.contains("=>") && token.matcher(top).find();
        }
        return false;
    }

    /** {@code body} with each top-level parameter list replaced by a mark: a method is then no property. */
    private static String parametersMarked(String body) {
        StringBuilder out = new StringBuilder();
        int depth = 0;
        for (char c : body.toCharArray()) {
            if (c == '(') {
                if (depth++ == 0) {
                    out.append('\u00a7');
                }
            } else if (c == ')') {
                depth--;
            } else if (depth == 0) {
                out.append(c);
            }
        }
        return out.toString();
    }

    private static String topLevel(String body) {
        StringBuilder out = new StringBuilder();
        int depth = 0;
        for (char c : body.toCharArray()) {
            if (c == '{' || c == '[' || c == '(') {
                depth++;
            } else if (c == '}' || c == ']' || c == ')') {
                depth--;
            } else if (depth == 0) {
                out.append(c);
            }
        }
        return out.toString();
    }

    /** The type text from {@code at} to the first of {@code ends} outside its brackets, or a bracket it did not open. */
    private static String typeAt(String s, int at, String ends) {
        int depth = 0;
        int j = at;
        for (; j < s.length(); j++) {
            char c = s.charAt(j);
            if ("{[(<".indexOf(c) >= 0) {
                depth++;
            } else if ("}])>".indexOf(c) >= 0) {
                if (depth == 0) {
                    break;
                }
                depth--;
            } else if (depth == 0 && ends.indexOf(c) >= 0) {
                break;
            }
        }
        return s.substring(at, j);
    }

    /** {@code code}'s casts to a holder (H5): each {@code as T} whose type names one outside an Omit (a cast through
     *  unknown, any or never counted once, at its last step), and each {@code <T>} assertion of one; its import and
     *  export statements' renames are no casts. */
    static int holderCasts(String code, Set<String> holders) {
        return holderCastTexts(code, holders).size();
    }

    /** {@code code}'s casts to a holder, each as its text (what a failure names). */
    static List<String> holderCastTexts(String code, Set<String> holders) {
        List<String> named = List.copyOf(new TreeSet<>(holders));
        return scanned(() -> List.copyOf(holderCastTextsOf(code, named)), "holderCasts", code, named);
    }

    private static List<String> holderCastTextsOf(String code, List<String> holders) {
        Set<String> records = new TreeSet<>(holders);
        records.remove("PlannedSql");
        List<String> found = new ArrayList<>();
        if (records.isEmpty()) {
            return found;
        }
        Pattern token = Pattern.compile("(?<![\\w$])(?:" + String.join("|", records) + ")(?![\\w$])");
        String s = blankModules(code);
        Matcher m = AS.matcher(s);
        while (m.find()) {
            String type = typeAt(s, m.end(), ";,=\n?:");
            Matcher named = token.matcher(OMIT.matcher(type).replaceAll(" "));
            if (!type.matches("(?s)\\s*(?:unknown|any|never)\\s+as\\b.*") && named.find()) {
                found.add("as " + type.strip().replaceAll("\\s+", " ") + " (" + named.group() + ")");
            }
        }
        Matcher a = ANGLE_ASSERTION.matcher(s);
        while (a.find()) {
            if (records.contains(a.group(1))) {
                found.add(a.group());
            }
        }
        return found;
    }

    @Test
    void theBrandIsNeverSatisfiedByAnotherName() throws IOException {
        theBrandIsNeverSatisfiedByAnotherName(Sources.TREE);
    }

    static void theBrandIsNeverSatisfiedByAnotherName(Sources s) throws IOException {
        List<String> product = folders().get("typescript");
        List<String> read = new ArrayList<>(product);
        read.addAll(TESTS);
        Map<String, String> codes = new TreeMap<>();
        for (String file : s.inventory()) {
            if (file.matches(".*\\.(?:[cm]?ts|tsx)$") && under(file, read)) {
                codes.put(file, code(s.read(file)));
            }
        }
        Set<String> holders = brandHolders(new ArrayList<>(codes.values()));
        // 2026-11-12 (the user's ruling, "that is a real off platform problem"): SnapStreamRequest holds no statement
        // any more -- it carries the pull, which the planner plans in the call -- so it left this list; and (the final
        // audit's item 1, 2026-11-12) HostOperation, HostSource and InferredModel hold none: a host operation takes no
        // SQL and a model carries no copy select list (convert)
        for (String record : List.of("Plan", "ExecutePlan")) {
            assertTrue(holders.contains(record), record + " holds a plan's statement, and the census does not see it");
        }
        Map<String, Integer> unknownCasts = new TreeMap<>();
        Map<String, Integer> holderCasts = new TreeMap<>();
        Map<String, List<String>> holderCastTexts = new TreeMap<>();
        List<String> aliases = new ArrayList<>();
        for (Map.Entry<String, String> e : codes.entrySet()) {
            String file = e.getKey();
            if (!file.equals("engine-client/src/planned-sql.ts") && finds(BRAND_ALIAS, e.getValue())) {
                aliases.add(file);
            }
            if (under(file, product)) {
                int u = count(UNKNOWN_CAST, e.getValue());
                if (u > 0) {
                    unknownCasts.put(file, u);
                }
                List<String> h = holderCastTexts(e.getValue(), holders);
                if (!h.isEmpty()) {
                    holderCasts.put(file, h.size());
                    holderCastTexts.put(file, h);
                }
            }
        }
        assertEquals(List.of(), aliases, "PlannedSql is named PlannedSql: a renamed import or export of it, or a type"
                + " alias of it, is a brand the cast census cannot see (the C final audit's H5)");
        assertEquals(new TreeMap<>(HOLDER_CASTS), holderCasts, "a cast to a record holding the brand makes a plan's"
                + " statement of any text: each is pinned by file (PlannedSqlTest.HOLDER_CASTS); read a planner's answer"
                + " into a type without the brand and mint its statement with fromPlan; seen: " + holderCastTexts);
        List<String> grew = new ArrayList<>();
        List<String> shrank = new ArrayList<>();
        for (Map.Entry<String, Integer> e : unknownCasts.entrySet()) {
            if (e.getValue() > UNKNOWN_CASTS.getOrDefault(e.getKey(), 0)) {
                grew.add(e.getKey() + ": " + e.getValue() + " > " + UNKNOWN_CASTS.getOrDefault(e.getKey(), 0));
            }
        }
        for (Map.Entry<String, Integer> e : UNKNOWN_CASTS.entrySet()) {
            if (unknownCasts.getOrDefault(e.getKey(), 0) < e.getValue()) {
                shrank.add(e.getKey() + ": " + unknownCasts.getOrDefault(e.getKey(), 0) + " < " + e.getValue());
            }
        }
        assertEquals(List.of(), grew, "a new `as unknown as` in the product's TypeScript (PlannedSqlTest.UNKNOWN_CASTS,"
                + " shrink-only); measured: " + unknownCasts);
        assertEquals(List.of(), shrank, "an `as unknown as` gone: lower its file's count in"
                + " PlannedSqlTest.UNKNOWN_CASTS, dated; measured: " + unknownCasts);
    }

    /** {@code src}'s sends to DuckDB-WASM (H13): its calls and members in code, and a bracketed name in code with its
     *  strings kept. */
    static int databaseSends(String src) {
        return count(DATABASE_SEND, code(src)) + count(BRACKETED_SEND, code(src, true));
    }

    /** Each match of {@code p} in {@code code} as its call site (S10): the receiver chain before it and the match,
     *  spaces dropped -- so one call is not swapped for another under a file's count. */
    static List<String> sites(Pattern p, String code) {
        return scanned(() -> List.copyOf(sitesOf(p, code)), "sites", p, code);
    }

    private static List<String> sitesOf(Pattern p, String code) {
        List<String> out = new ArrayList<>();
        Matcher m = p.matcher(code);
        while (m.find()) {
            int from = m.start();
            while (from > 0 && (Character.isJavaIdentifierPart(code.charAt(from - 1)) || ".#?$".indexOf(code.charAt(from - 1)) >= 0)) {
                from--;
            }
            out.add(code.substring(from, m.end()).replaceAll("\\s+", ""));
        }
        return out;
    }

    /** Each fetch of {@code code} (its strings kept) as its call site: {@code fetch(} and its first argument, cut short. */
    static List<String> fetchSites(String code) {
        return scanned(() -> List.copyOf(fetchSitesOf(code)), "fetchSites", code);
    }

    private static List<String> fetchSitesOf(String code) {
        List<String> out = new ArrayList<>();
        Matcher m = FETCH_CALL.matcher(code);
        while (m.find()) {
            String argument = typeAt(code, m.end(), ",)\n").strip().replaceAll("\\s+", " ");
            out.add("fetch(" + (argument.length() > 48 ? argument.substring(0, 48) : argument));
        }
        return out;
    }

    /** {@code src}'s DuckDB-WASM send sites (H13, S10). */
    static List<String> databaseSendSites(String src) {
        List<String> out = new ArrayList<>(sites(DATABASE_SEND, code(src)));
        out.addAll(sites(BRACKETED_SEND, code(src, true)));
        return out;
    }

    private static void put(Map<String, List<String>> into, String file, List<String> found) {
        if (!found.isEmpty()) {
            into.put(file, found.stream().sorted().toList());
        }
    }

    @Test
    void everyTransportHasOneChokePoint() throws IOException {
        everyTransportHasOneChokePoint(Sources.TREE);
    }

    static void everyTransportHasOneChokePoint(Sources s) throws IOException {
        List<String> product = folders().get("typescript");
        Map<String, List<String>> sends = new TreeMap<>();
        Map<String, List<String>> connects = new TreeMap<>();
        Map<String, List<String>> fetches = new TreeMap<>();
        Map<String, List<String>> submits = new TreeMap<>();
        List<String> otherHttp = new ArrayList<>();
        for (String file : s.inventory()) {
            // the product's TypeScript and the JavaScript beside it (S10: the harnesses' .mjs were not read)
            if (!file.matches(".*\\.(?:[cm]?ts|tsx|[cm]?js)$") || file.endsWith(".d.ts") || !under(file, product)) {
                continue;
            }
            String src = s.read(file);
            String code = code(src);
            put(sends, file, databaseSendSites(src));
            put(connects, file, sites(CONNECT_CALL, code));
            put(fetches, file, fetchSites(code(src, true)));
            put(submits, file, sites(SUBMIT_CALL, code(src, true)));
            if (finds(OTHER_HTTP, code) && !OTHER_HTTP_KEPT.contains(file)) {
                otherHttp.add(file);
            }
        }
        assertEquals(new TreeMap<>(DATABASE_SENDS), sends, "ONE CHOKE POINT (H13): DuckDB-WASM's connection is sent to in"
                + " engine-client/src/duckdb.ts alone, as a PlannedSql (PlannedSqlTest.DATABASE_SENDS, by call site): a new"
                + " send site is a statement no plan may carry");
        assertEquals(new TreeMap<>(CONNECTS), connects, "ONE CHOKE POINT (H13): DuckDB-WASM's raw connection is made in"
                + " duckdb-tab.ts alone and handed to DuckDbEngine (PlannedSqlTest.CONNECTS, by call site)");
        assertEquals(new TreeMap<>(SUBMITS), submits, "ONE CHOKE POINT (H13): a SQL API's statement is submitted by"
                + " engine-client/src/warehouse.ts alone, as a PlannedSql (PlannedSqlTest.SUBMITS, by call site)");
        assertEquals(new TreeMap<>(FETCHES), fetches, "ONE CHOKE POINT (H13): each HTTP send of the product's is pinned by"
                + " call site (PlannedSqlTest.FETCHES): a new one could carry a statement to a SQL API");
        assertEquals(List.of(), otherHttp, "ONE CHOKE POINT (H13): the product sends HTTP by fetch alone (no"
                + " XMLHttpRequest, WebSocket, EventSource or sendBeacon), each fetch pinned");
    }

    /**
     * THE PLAN RUNNERS' CALLERS (the re-audit's M1, 2026-11-12): runPlan, runLoad, establish, runAuthored and targetKey
     * take a PlanText alone -- a plan the planner answered -- and each place the product calls one is pinned by call site;
     * a runner handed on rather than called is refused (its every reference outside its imports is a call). And the plan
     * format's name appears only where a plan is read (PLAN_FORMAT_READERS): a product source spelling a plan is red.
     */
    static final Map<String, List<String>> RUNNER_CALLS = Map.ofEntries(
            // 2026-11-12 (M1, measured): each with the plan its planner client answered -- a write's (table-output.ts),
            // a load's: a person's file (model-data.ts, upload.ts), a remote mount (remote.ts), a snapshot's stream
            // (snap.ts), a project's database (project-data.ts; the demos' seeds, boot.ts and query/demo/main.ts), and
            // the playground's authored statement (run.ts)
            Map.entry("datacube/demo/boot.ts", List.of("runLoad(")),
            Map.entry("datacube/src/remote.ts", List.of("runLoad(")),
            Map.entry("datacube/src/table-output.ts", List.of("runPlan(")),
            Map.entry("datacube/src/upload.ts", List.of("runLoad(")),
            Map.entry("engine-client/src/model-data.ts", List.of("runLoad(")),
            Map.entry("engine-client/src/project-data.ts", List.of("runLoad(")),
            // 2026-11-12 (the final audit's item 1(b)): a snapshot's local copy, its plan the planner's of the pull
            // (snapCopy)
            Map.entry("engine-client/src/snap.ts", List.of("runLoad(", "runPlan(")),
            Map.entry("query/demo/main.ts", List.of("runLoad(")),
            Map.entry("studio/src/backend/run.ts", List.of("runAuthored(")));
    // 2026-11-12 (M1, measured): the runner's own read of a plan's format (planned-sql.ts names it in prose alone)
    static final Set<String> PLAN_FORMAT_READERS = Set.of("engine-client/src/plan-runner.ts");

    @Test
    void thePlanRunnersAreCalledWhereAPlanTheyAreHandedWasAnswered() throws IOException {
        thePlanRunnersAreCalledWhereAPlanTheyAreHandedWasAnswered(Sources.TREE);
    }

    static void thePlanRunnersAreCalledWhereAPlanTheyAreHandedWasAnswered(Sources s) throws IOException {
        List<String> product = folders().get("typescript");
        Map<String, List<String>> calls = new TreeMap<>();
        List<String> handedOn = new ArrayList<>();
        Set<String> formats = new TreeSet<>();
        for (String file : s.inventory()) {
            if (!file.matches(".*\\.(?:[cm]?ts|tsx|[cm]?js)$") || file.endsWith(".d.ts") || !under(file, product)) {
                continue;
            }
            String src = s.read(file);
            if (finds(PLAN_FORMAT, code(src, true))) {
                formats.add(file);
            }
            if (file.equals("engine-client/src/plan-runner.ts")) {
                continue;
            }
            String body = blankModules(code(src));
            List<String> found = sites(RUNNER_CALL, body);
            put(calls, file, found);
            if (runnerReferences(body) != found.size()) {
                handedOn.add(file);
            }
        }
        assertEquals(new TreeMap<>(RUNNER_CALLS), calls, "THE PLAN RUNNERS' CALLERS (the re-audit's M1): each call of a"
                + " runner is pinned by call site (PlannedSqlTest.RUNNER_CALLS), its plan the planner's answer (PlanText)");
        assertEquals(List.of(), handedOn, "a plan runner is called where it is named, never handed on");
        assertEquals(new TreeSet<>(PLAN_FORMAT_READERS), formats, "the plan format's name is spelled where a plan is read"
                + " (PlannedSqlTest.PLAN_FORMAT_READERS): a product source that spells it writes a plan");
    }

    /**
     * NO `any` IN THE PRODUCT'S TYPESCRIPT (the re-audit's S6, 2026-11-12): a type of `any` -- declared, cast to, or a
     * type argument -- satisfies every type, PlannedSql's brand among them, with no cast to see. The product's code holds
     * none (comments and strings aside); JSON.parse, a response's json() and Array.isArray's narrowing give `unknown`
     * (engine-client/src/json-unknown.ts).
     */
    @Test
    void noAnyInTheProduct() throws IOException {
        noAnyInTheProduct(Sources.TREE);
    }

    static void noAnyInTheProduct(Sources s) throws IOException {
        List<String> product = folders().get("typescript");
        Map<String, Integer> found = new TreeMap<>();
        for (String file : s.inventory()) {
            if (file.matches(".*\\.(?:[cm]?ts|tsx)$") && !file.endsWith(".d.ts") && under(file, product)) {
                int n = count(ANY, code(s.read(file)));
                if (n > 0) {
                    found.put(file, n);
                }
            }
        }
        assertEquals(Map.of(), found, "`any` in the product's TypeScript satisfies the PlannedSql brand as a cast does: type"
                + " it `unknown` and check it (the re-audit's S6)");
        for (String planted : List.of("let x: any = y;", "const v = y as any;", "f<any>(x);", "const xs: any[] = [];",
                "(a: any) => a")) {
            assertTrue(count(ANY, code(planted)) > 0, "the census does not see: " + planted);
        }
        // a name is no type: a key, a method, a member
        assertEquals(0, count(ANY, code("// any comment\nconst s = 'any text'; const anyEmpty = company;"
                + " const o = { any: 'x' }; static any(s: Signal[]): Signal {} AbortSignal.any([a]);")));
    }

    /** {@code body}'s references to a runner (M1), less the names a destructuring pattern binds (a lazy import's). */
    static int runnerReferences(String body) {
        return scanned(() -> runnerReferencesOf(body), "runnerReferences", body);
    }

    private static int runnerReferencesOf(String body) {
        int bound = 0;
        Matcher d = DESTRUCTURED.matcher(body);
        while (d.find()) {
            bound += count(RUNNER_NAME, d.group(1));
        }
        return count(RUNNER_NAME, body) - bound;
    }

    /** M1's forms, planted: a runner called and handed on, and a plan's format spelled. */
    @Test
    void theRunnerCensusSeesEveryForm() {
        String src = "import { runPlan, runLoad } from './plan-runner.ts';\nawait runPlan(plan, s); const go = runLoad; go(p, r);";
        String body = MODULE_STATEMENT.matcher(code(src)).replaceAll(r -> " ".repeat(r.group().length()));
        assertEquals(List.of("runPlan("), sites(RUNNER_CALL, body));
        assertEquals(2, runnerReferences(body), "a runner handed on is a reference that is no call");
        String lazy = "const { runLoad } = await import('./plan-runner.ts');\nawait runLoad(plan, run);";
        assertEquals(List.of("runLoad("), sites(RUNNER_CALL, code(lazy)));
        assertEquals(1, runnerReferences(code(lazy)), "a lazy import's binding is no hand-on");
        assertEquals(1, runnerReferences(code("const [plan, { runLoad }] = await Promise.all([a, b]);\nrunLoad(plan, r);")));
        assertTrue(PLAN_FORMAT.matcher(code("const p = `{\"format\":\"legend-lite-plan\",\"root\":{}}`;", true)).find());
        assertEquals(0, count(RUNNER_NAME, code("// runPlan in prose\nconst s = 'runPlan(x)'; o.runPlan;")));
    }

    /** H13's forms, planted: a census that stops seeing one is red. */
    @Test
    void theChokePointCensusSeesEveryForm() {
        for (String planted : List.of("await conn.query('DROP TABLE t');", "conn.send(sql);", "const p = await c.prepare(s);",
                "const insert = conn.insertArrowFromIPCStream;", "await conn['query']('x');", "db[\"registerFileHandle\"](n, h);",
                "await (await db.connect()).query(text);", "conn.query.call(conn, sql);", "const q = conn.query.bind(conn);",
                "await conn.query?.(sql);")) {
            assertTrue(databaseSends(planted) > 0, "the census does not see: " + planted);
        }
        assertEquals(0, databaseSends("// conn.query('x') in prose\nconst s = 'conn.query(x)'; const q = snapshot.query;"
                + " const r = o['query'];"));
        assertEquals(1, count(CONNECT_CALL, code("const raw = await db.connect();")));
        assertEquals(1, count(FETCH_CALL, code("await fetch(url, { method: 'POST', body });")));
        assertEquals(0, count(FETCH_CALL, code("const r = await this.#binding.fetchChunk(); prefetch(x);")));
        assertEquals(1, count(SUBMIT_CALL, code("await this.#call<Step>('submit', { sql }, signal);", true)));
        assertEquals(0, count(SUBMIT_CALL, code("form.addEventListener('submit', go);", true)));
        for (String planted : List.of("new XMLHttpRequest()", "new WebSocket(url)", "navigator.sendBeacon(url, body)")) {
            assertTrue(OTHER_HTTP.matcher(code(planted)).find(), "the census does not refuse: " + planted);
        }
    }

    /** Each form H5 named, planted: a census that stops seeing one is red. */
    @Test
    void theBrandCensusSeesEveryForm() {
        Set<String> holders = brandHolders(List.of(code(
                "export interface Plan { readonly sql: PlannedSql; readonly columns: readonly string[] }\n"
                + "interface Outer { readonly plans: readonly Plan[]; run(sql: PlannedSql): void }\n"
                + "interface Runner { run(sql: PlannedSql): Promise<Plan>; readonly go: (s: PlannedSql) => void }\n"
                + "type Rows = readonly Plan[];\ntype Without = Omit<Plan, 'sql'>;\n"
                + "type Op = { readonly op: 'count' } | { readonly query: PlannedSql };\n"
                + "class View { readonly cube: string; render(): void { const plan: Plan = read(); show(plan); } }")));
        assertEquals(Set.of("PlannedSql", "Plan", "Outer", "Rows", "Op"), holders);
        for (String planted : List.of("const p = JSON.parse(t) as Plan;", "const p = JSON.parse(t) as unknown as Plan;",
                "const ps = x as readonly Plan[];", "const ps = x as Array<Plan>;", "const p = <Plan>x;",
                "const o = x as { readonly inner: Op };", "const r = x as Rows | undefined;")) {
            assertEquals(1, holderCasts(code(planted), holders), "the census does not see: " + planted);
        }
        assertEquals(0, holderCasts(code("import { Plan as P } from './p.ts'; const w = x as Without;"
                + " const s = x as Omit<Plan, 'sql'> & { sql: string }; const r = x as Runner; const c = [1] as const;"),
                holders));
        for (String planted : List.of("import type { PlannedSql as Sql } from './planned-sql.ts';",
                "export type { PlannedSql as Sql } from './planned-sql.ts';", "type Sql = PlannedSql;")) {
            assertTrue(BRAND_ALIAS.matcher(code(planted)).find(), "the census does not refuse: " + planted);
        }
        assertEquals(1, count(UNKNOWN_CAST, code("const a = x as unknown as Foo; // as unknown as Bar")));
    }

    @Test
    void noProductSourceReachesTheTestsOwnStatement() throws IOException {
        noProductSourceReachesTheTestsOwnStatement(Sources.TREE);
    }

    static void noProductSourceReachesTheTestsOwnStatement(Sources s) throws IOException {
        List<String> product = folders().get("typescript");
        List<String> found = new ArrayList<>();
        for (String file : s.inventory()) {
            if (file.endsWith(".ts") && under(file, product) && UNPLANNED_IMPORT.matcher(s.read(file)).find()) {
                found.add(file);
            }
        }
        assertEquals(List.of(), found, "a product source imports the tests' `unplanned`: a host sends only what a plan"
                + " carries");
    }
}
