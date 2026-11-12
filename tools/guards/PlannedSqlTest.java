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
     * playground (a person's own SQL, run as typed), kept named by the ruling (below), and Query's demo warehouse fixture
     * (its seed files' statements), planned by the ruling: 2026-11-12, the C3 follow-up, the warehouse is made by the
     * project's own Data load (query/demo/main.ts seed, Boundary.projectData), and its SQL file is deleted.
     */
    static final Map<String, String> NOT_PLANNED = Map.of(
            "studio/src/backend/run.ts", "the SQL playground");

    /**
     * THE PLAYGROUND'S ONE PATH (the orchestrator's ruling of 2026-11-12: Studio's SQL playground stays named -- a person's
     * own SQL run as typed is the author's statement -- and nothing but the playground reaches it). 2026-11-12, the C
     * checkpoint's code audit, SHOULD-FIX 5: by STRUCTURE, not by a call's spelling -- the send is the {@code Playground}
     * object's (run.ts {@code playground()}), apart from the Runner every run is handed; the app makes it (app.ts, the
     * factory's one importer) and hands it to the editor's context alone (editor.ts, the type's one importer), whose SQL
     * playground is its one reader.
     */
    static final String PLAYGROUND = "studio/src/backend/run.ts";
    static final Map<String, Set<String>> PLAYGROUND_IMPORTS = Map.of(
            "studio/src/app/app.ts", Set.of("playground"),
            "studio/src/ui/editor.ts", Set.of("Playground"));

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

    /** What a mint file may import from planned-sql.ts: the functions that type a plan's text or a planner's answer. */
    static final Set<String> MINT_NAMES = Set.of("fromPlan", "allFromPlan", "plannedProjectData");

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
    private static final Pattern CAST = Pattern.compile("\\bas\\s+PlannedSql\\b|(?<![\\w$])<PlannedSql>|\\bis\\s+PlannedSql\\b");
    private static final Pattern NOT_PLANNED_CALL = Pattern.compile("\\bnotPlanned\\s*\\(");
    private static final Pattern STRING_LITERAL = Pattern.compile("(['\"`])((?:(?!\\1)[^\\\\]|\\\\.)*)\\1");
    private static final Pattern RUN_TS_IMPORT = Pattern.compile(
            "\\bimport\\s*(type\\s+)?\\{([^}]*)\\}\\s*from\\s*['\"`][^'\"`]*backend/run(?:\\.ts)?['\"`]");
    private static final Pattern PLAYGROUND_READ = Pattern.compile("\\.\\s*playground\\b|\\[\\s*['\"`]playground['\"`]\\s*\\]"
            + "|\\{[^{}]*\\bplayground\\b[^{}]*\\}\\s*=(?!=)");
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

    /** One source's references to planned-sql.ts, read statement by statement: the values it imports, and each form
     *  the census refuses (a namespace, default, side-effect or dynamic import, an alias, a value re-export, a path it
     *  cannot place). A type-only import or re-export is free: a type mints nothing. */
    record Imports(Set<String> values, List<String> refused) {
    }

    static Imports imports(String src) {
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

    @Test
    void thePlannedStatementIsMadeOnlyWhereAPlanIsRead() throws IOException {
        List<String> product = folders().get("typescript");
        List<String> read = new ArrayList<>(product);
        read.addAll(TESTS);
        List<String> ts = inventory().stream().filter(f -> f.matches(".*\\.(?:[cm]?ts|tsx|[cm]?js)$"))
                .filter(f -> under(f, read)).toList();
        assertTrue(ts.size() > 500, "the guard read too few .ts files to prove anything: " + ts.size());
        for (String folder : TESTS) {
            assertTrue(ts.stream().anyMatch(f -> f.startsWith(folder + "/")), folder + " holds no .ts: a folder moved");
        }
        String own = "engine-client/src/planned-sql.ts";
        assertTrue(read(own).contains("unique symbol"),
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
            String src = read(file);
            Imports im = imports(src);
            im.refused().forEach(r -> refused.add(file + ": " + r));
            if (!im.values().isEmpty()) {
                (under(file, product) ? productImports : testImports).put(file, im.values());
            }
            if (!CASTS.contains(file) && CAST.matcher(src).find()) {
                casts.add(file);
            }
            for (String reason : notPlannedReasons(src)) {
                named.merge(file, reason, (a, b) -> a + " | " + b);
            }
        }
        assertEquals(List.of(), refused, "planned-sql.ts is imported by name, a value at a time, never aliased, by"
                + " namespace, by default, dynamically or re-exported: each importer and each name is pinned");
        Set<String> importers = new TreeSet<>(MINTS);
        importers.addAll(NOT_PLANNED.keySet());
        assertEquals(importers, productImports.keySet(), "a value of planned-sql.ts -- a mint, or notPlanned -- is"
                + " imported only where a plan or the planner's answer is read (PlannedSqlTest.MINTS) or a statement no"
                + " plan carries is named (NOT_PLANNED): a host's own text is not a plan's");
        for (String mint : MINTS) {
            assertTrue(MINT_NAMES.containsAll(productImports.get(mint)), mint + " imports " + productImports.get(mint)
                    + ": a plan reader imports the mints alone (PlannedSqlTest.MINT_NAMES), never notPlanned");
        }
        for (String file : NOT_PLANNED.keySet()) {
            assertEquals(Set.of("notPlanned"), productImports.get(file), file + " names what no plan carries, and"
                    + " imports nothing else of planned-sql.ts");
        }
        assertEquals(new TreeMap<>(TEST_IMPORTS), testImports, "a test types its own SQL with `unplanned`"
                + " (engine-client/test/unplanned.ts); a test that imports a mint is pinned (PlannedSqlTest.TEST_IMPORTS)");
        assertEquals(List.of(), casts, "a cast to PlannedSql (or a guard narrowing to it) outside its mints: a test's own"
                + " SQL is `unplanned` (engine-client/test/unplanned.ts), a product's is planned");
        assertEquals(new TreeMap<>(NOT_PLANNED), named, "a statement no plan carries is sent only by a name the guard"
                + " pins (PlannedSqlTest.NOT_PLANNED), each call's first argument read whatever it is: a new one is"
                + " reported, never added in passing");
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
    void thePlaygroundsUnplannedSendIsReachedFromThePlaygroundAlone() throws IOException {
        String backend = read(PLAYGROUND);
        int factory = backend.indexOf("export function playground(session: RunSession): Playground {");
        int runner = backend.indexOf("export function runner(session: RunSession): Runner {");
        assertTrue(factory >= 0 && runner > factory, "run.ts's playground() and runner() moved: " + PLAYGROUND);
        int send = backend.indexOf("notPlanned('the SQL playground'");
        assertTrue(send > factory && send < runner, "the playground's send is the Playground object's, and no other's");
        String runnerType = backend.substring(backend.indexOf("export interface Runner {"),
                backend.indexOf("}", backend.indexOf("export interface Runner {")));
        assertTrue(!runnerType.contains("sql("), "the Runner every run is handed holds no way to send SQL");
        List<String> product = folders().get("typescript");
        Map<String, Set<String>> imports = new TreeMap<>();
        Map<String, List<String>> reads = new TreeMap<>();
        for (String file : inventory()) {
            if (!file.endsWith(".ts") || !under(file, product) || file.equals(PLAYGROUND)) {
                continue;
            }
            String src = read(file);
            Matcher i = RUN_TS_IMPORT.matcher(src);
            while (i.find()) {
                for (String spec : i.group(2).split(",")) {
                    String name = spec.strip().replaceFirst("^type\\s+", "").replaceFirst("\\s+as\\s+.*", "");
                    if (name.equalsIgnoreCase("playground")) {
                        imports.computeIfAbsent(file, k -> new TreeSet<>()).add(name);
                    }
                }
            }
            Matcher r = PLAYGROUND_READ.matcher(src);
            while (r.find()) {
                reads.computeIfAbsent(file, k -> new ArrayList<>())
                        .add(src.substring(r.start(), Math.min(src.length(), r.start() + 40)).replaceAll("\\s+", " "));
            }
        }
        assertEquals(new TreeMap<>(PLAYGROUND_IMPORTS), imports, "the playground is made by the app and typed by the"
                + " editor's context alone (PlannedSqlTest.PLAYGROUND_IMPORTS)");
        assertEquals(Map.of("studio/src/ui/editor.ts", List.of(".playground.sql(sqlText.value, ws.model(")), reads,
                "the editor's context's playground is read once, by the SQL playground's run of its own text, and never"
                        + " destructured or passed on");
        assertTrue(read("studio/src/app/app.ts").split("\\bplayground\\(session\\)", -1).length == 2,
                "the app makes the playground once");
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
