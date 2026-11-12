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
 * with the host-SQL guard) and every module of Python's host. Each list below moves only with a dated justification
 * naming the task (AGENTS.md, guard files), and shrinks: a site that is gone is taken out (the stale checks).
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

    /**
     * What a host still sends that no plan carries, NAMED (planned-sql.ts {@code notPlanned}): each file and the name it
     * gives. 2026-11-12 (C3), both reported to the orchestrator for a ruling (plan each, or keep it named): Studio's SQL
     * playground (a person's own SQL, run as typed) and Query's demo warehouse fixture (its seed files' statements).
     */
    static final Map<String, String> NOT_PLANNED = Map.of(
            "studio/src/backend/run.ts", "the SQL playground",
            "query/demo/main.ts", "the demo warehouse fixture");

    /**
     * THE PLAYGROUND'S ONE PATH (the orchestrator's ruling of 2026-11-12: Studio's SQL playground stays named -- a person's
     * own SQL run as typed is the author's statement -- and nothing but the playground reaches it). Its send sits in the
     * run backend's {@code sql} method, and the product's one caller of that method is the editor's SQL playground.
     */
    static final String PLAYGROUND = "studio/src/backend/run.ts";
    static final Set<String> PLAYGROUND_CALLERS = Set.of("studio/src/ui/editor.ts");

    /** The tests' folders, read beside the product's (host_sql.txt's): where a cast would hide a host's own SQL. */
    static final List<String> TESTS = List.of("engine-client/test", "datacube/test", "query/test", "studio/test");

    /** The two files that may cast to PlannedSql: the type's own mints, and the tests' (engine-client/test). */
    static final Set<String> CASTS = Set.of("engine-client/src/planned-sql.ts", "engine-client/test/unplanned.ts");

    /** The Python modules that make a PlannedSql from a plan's text (from_plan, all_from_plan). 2026-11-12 (C3): the
     *  compiler's readers (compiler.py), the load runners (projects.py: a load's steps), and execute's plan half read
     *  by the engine (engine.py: its {@code run}). */
    static final Set<String> PYTHON_MINTS = Set.of(
            "python/legend_lite/compiler.py",
            "python/legend_lite/projects.py",
            "python/legend_lite/engine.py");

    /**
     * The statements DataCube's hosted code hands Python's database across the V8 bridge ({@code across_the_bridge}):
     * PlannedSql on that side, text on the bridge. Each module and how many calls. 2026-11-12 (C3): the hosted
     * DataCube's query and statement (_headless.py), and a remote file's settings its plan runner kept (engine.py).
     * Reported to the orchestrator: typed again by name at the crossing, not checked against the planner's answers.
     */
    static final Map<String, Integer> BRIDGE = Map.of(
            "python/legend_lite/_headless.py", 2,
            "python/legend_lite/engine.py", 1);

    /**
     * Each {@code .execute(} a module of Python's host writes, by its receiver: the one send ({@code _planned.send}'s
     * {@code target}), a read's own method that calls it (frames.py {@code self.execute}, Reading.execute) and a
     * docstring's Pure query ({@code trades.execute}). 2026-11-12 (C3).
     */
    static final Set<String> PYTHON_EXECUTES = Set.of(
            "python/legend_lite/_planned.py target",
            "python/legend_lite/frames.py self",
            "python/legend_lite/frames.py trades");

    private static final Pattern MINT_IMPORT = Pattern.compile(
            "import\\s*(?:type\\s*)?\\{[^}]*\\b(?:fromPlan|allFromPlan)\\b[^}]*\\}\\s*from\\s*'[^']*planned-sql\\.ts'");
    private static final Pattern RE_EXPORT = Pattern.compile("export\\s*\\{[^}]*\\b(?:fromPlan|allFromPlan|notPlanned)\\b");
    private static final Pattern CAST = Pattern.compile("\\bas\\s+PlannedSql\\b|(?<![\\w$])<PlannedSql>");
    private static final Pattern NOT_PLANNED_CALL = Pattern.compile("\\bnotPlanned\\(\\s*'([^']*)'");
    private static final Pattern RUN_SQL = Pattern.compile("\\brun\\s*\\.\\s*sql\\(");
    private static final Pattern UNPLANNED_IMPORT = Pattern.compile("from\\s*'[^']*test/unplanned\\.ts'");
    private static final Pattern PY_MINT = Pattern.compile("(?<![\\w.])(?:from_plan|all_from_plan)\\(");
    private static final Pattern PY_BRIDGE = Pattern.compile("(?<![\\w.])across_the_bridge\\(");
    private static final Pattern PY_CONSTRUCT = Pattern.compile("(?<![\\w.])PlannedSql\\(");
    private static final Pattern PY_EXECUTE = Pattern.compile("(\\w+)\\s*\\.\\s*execute(?:many)?\\(");
    private static final Pattern PY_OTHER_SENDS = Pattern.compile("\\bduckdb\\s*\\.\\s*(?:sql|execute|query)\\(");

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

    private static List<String> inventory() throws IOException {
        return Files.readAllLines(Runfile.property("inventory.files"));
    }

    private static String read(String file) throws IOException {
        return Files.readString(Runfile.of("_main/" + file), StandardCharsets.UTF_8);
    }

    private static boolean under(String file, List<String> folders) {
        return folders.stream().anyMatch(d -> file.startsWith(d + "/"));
    }

    @Test
    void thePlannedStatementIsMadeOnlyWhereAPlanIsRead() throws IOException {
        List<String> product = folders().get("typescript");
        List<String> read = new ArrayList<>(product);
        read.addAll(TESTS);
        List<String> ts = inventory().stream().filter(f -> f.endsWith(".ts") && !f.endsWith(".d.ts"))
                .filter(f -> under(f, read)).toList();
        assertTrue(ts.size() > 500, "the guard read too few .ts files to prove anything: " + ts.size());
        for (String folder : TESTS) {
            assertTrue(ts.stream().anyMatch(f -> f.startsWith(folder + "/")), folder + " holds no .ts: a folder moved");
        }
        assertTrue(read("engine-client/src/planned-sql.ts").contains("unique symbol"),
                "PlannedSql is a branded string (planned-sql.ts): a plain string is not one");
        Set<String> mints = new TreeSet<>();
        List<String> casts = new ArrayList<>();
        List<String> reExports = new ArrayList<>();
        Map<String, String> named = new TreeMap<>();
        for (String file : ts) {
            String src = read(file);
            // a test is no host: it may make one (engine-client/test/planned-sql.test.ts tests the mints themselves)
            if (under(file, product) && MINT_IMPORT.matcher(src).find()) {
                mints.add(file);
            }
            if (!CASTS.contains(file) && CAST.matcher(src).find()) {
                casts.add(file);
            }
            if (!file.equals("engine-client/src/planned-sql.ts") && RE_EXPORT.matcher(src).find()) {
                reExports.add(file);
            }
            Matcher m = NOT_PLANNED_CALL.matcher(src);
            while (m.find()) {
                named.merge(file, m.group(1), (a, b) -> a + " | " + b);
            }
        }
        assertEquals(new TreeSet<>(MINTS), mints, "a PlannedSql is made only where a plan or the planner's answer is read"
                + " (PlannedSqlTest.MINTS): a host's own text is not a plan's");
        assertEquals(List.of(), casts, "a cast to PlannedSql outside its mints: a test's own SQL is `unplanned`"
                + " (engine-client/test/unplanned.ts), a product's is planned");
        assertEquals(List.of(), reExports, "the mints are planned-sql.ts's alone: re-exported, they are anyone's");
        assertEquals(new TreeMap<>(NOT_PLANNED), named, "a statement no plan carries is sent only by a name the guard"
                + " pins (PlannedSqlTest.NOT_PLANNED): a new one is reported, never added in passing");
    }

    @Test
    void thePlaygroundsUnplannedSendIsReachedFromThePlaygroundAlone() throws IOException {
        String backend = read(PLAYGROUND);
        int method = backend.indexOf("    async sql(statement, modelText) {");
        assertTrue(method >= 0, "the run backend's sql method moved: " + PLAYGROUND);
        int end = backend.indexOf("\n    },", method);
        int send = backend.indexOf("notPlanned('the SQL playground'");
        assertTrue(send > method && send < end, "the playground's send is the run backend's sql method's, and no other's");
        List<String> product = folders().get("typescript");
        Set<String> callers = new TreeSet<>();
        for (String file : inventory()) {
            if (file.endsWith(".ts") && under(file, product) && RUN_SQL.matcher(read(file)).find()) {
                callers.add(file);
            }
        }
        assertEquals(new TreeSet<>(PLAYGROUND_CALLERS), callers, "the run backend's sql method -- a person's own SQL,"
                + " sent unplanned -- is the SQL playground's alone (PlannedSqlTest.PLAYGROUND_CALLERS)");
        String editor = read("studio/src/ui/editor.ts");
        Matcher call = RUN_SQL.matcher(editor);
        assertTrue(call.find() && editor.substring(call.start(), Math.min(editor.length(), call.start() + 80))
                .contains("sqlText.value"), "the editor's one call sends the playground's own text");
        assertTrue(!call.find(), "the editor calls the run backend's sql method once: the playground's run");
    }

    @Test
    void noProductSourceReachesTheTestsOwnStatement() throws IOException {
        List<String> product = folders().get("typescript");
        List<String> found = new ArrayList<>();
        for (String file : inventory()) {
            if (file.endsWith(".ts") && under(file, product) && UNPLANNED_IMPORT.matcher(read(file)).find()) {
                found.add(file);
            }
        }
        assertEquals(List.of(), found, "a product source imports the tests' `unplanned`: a host sends only what a plan"
                + " carries");
    }

    @Test
    void pythonsHostSendsOnlyThroughTheOneSend() throws IOException {
        List<String> python = folders().get("python");
        List<String> modules = inventory().stream().filter(f -> f.endsWith(".py") && under(f, python)).toList();
        assertTrue(modules.size() > 10, "the guard read too few Python modules: " + modules.size());
        Set<String> executes = new TreeSet<>();
        Set<String> mints = new TreeSet<>();
        Map<String, Integer> bridge = new TreeMap<>();
        List<String> constructed = new ArrayList<>();
        List<String> otherSends = new ArrayList<>();
        for (String file : modules) {
            String src = read(file);
            Matcher e = PY_EXECUTE.matcher(src);
            while (e.find()) {
                executes.add(file + " " + e.group(1));
            }
            if (!file.endsWith("/_planned.py") && PY_MINT.matcher(src).find()) {
                mints.add(file);
            }
            Matcher b = PY_BRIDGE.matcher(src);
            int calls = 0;
            while (b.find()) {
                calls++;
            }
            if (!file.endsWith("/_planned.py") && calls > 0) {
                bridge.put(file, calls);
            }
            if (!file.endsWith("/_planned.py") && PY_CONSTRUCT.matcher(src).find()) {
                constructed.add(file);
            }
            if (PY_OTHER_SENDS.matcher(src).find()) {
                otherSends.add(file);
            }
        }
        assertEquals(new TreeSet<>(PYTHON_EXECUTES), executes, "a statement Python's host runs goes through"
                + " _planned.send, which refuses any text no plan carries (PlannedSqlTest.PYTHON_EXECUTES)");
        assertEquals(new TreeSet<>(PYTHON_MINTS), mints, "a PlannedSql is made only where a plan or the planner's answer"
                + " is read (PlannedSqlTest.PYTHON_MINTS)");
        assertEquals(new TreeMap<>(BRIDGE), bridge, "the statements the hosted DataCube hands across are typed at the"
                + " named crossings alone (PlannedSqlTest.BRIDGE)");
        assertEquals(List.of(), constructed, "PlannedSql is made by _planned.py's functions alone");
        assertEquals(List.of(), otherSends, "duckdb's module-level sends reach the default connection outside the one send");
    }
}
