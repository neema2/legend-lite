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
            // (the C final audit's H9): an authored plan's target's session statements, typed as they are read
            "engine-client/src/plan-runner.ts", 18,
            // the planner clients: Query's and Studio's, DataCube's, and the reader of a server's generatePlan
            "engine-client/src/legend/wasm-grammar.ts", 8,
            // 2026-11-12, 8 -> 9 (the C final audit's H2): DataCube's table model's copy select list, typed as its answer
            // is read, since a host operation's selectList takes a PlannedSql alone
            "datacube/src/lite-planner.ts", 9,
            "datacube/src/planner.ts", 2,
            // the definitions themselves, and the mints' own test
            "engine-client/src/planned-sql.ts", 7,
            "engine-client/test/planned-sql.test.ts", 5);
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
     * (Boundary.authored), and that by the two exports a host asks -- the tab's (TabExports.authoredOrError) and a
     * headless host's (the native library's lite_authored) -- by file, exactly. Python's request kind is pinned by
     * python/tests/test_planned_guard.py.
     */
    static final Map<String, Integer> AUTHORED_JAVA = Map.of(
            "wasm/src/main/java/planner/Boundary.java", 1,
            "wasm/src/main/java/planner/TabExports.java", 2,
            "native/src/main/java/com/legend/nativelib/Compiler.java", 1);
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
    private static final Pattern MINT_IDENTIFIER = Pattern.compile(
            "(?<![\\w$])(?:fromPlan|allFromPlan|notPlanned|plannedProjectData)(?![\\w$])");
    private static final Pattern ESCAPE = Pattern.compile("\\bas\\s+(?:never|any)\\b");
    /** A reference to the authored door, in code: its runner, a member or key named for it, or its export. */
    private static final Pattern AUTHORED_IDENTIFIER = Pattern.compile("(?<![\\w$])(?:runAuthored|authored|authoredOrError)(?![\\w$])");
    /** The request kind {@code 'authored'} as a string, in code with its strings kept (any quote). */
    private static final Pattern AUTHORED_KIND = Pattern.compile("(['\"`])authored\\1");
    /** Java's reach of the door: the planner's Authored.plan, the boundary's authored, the tab's export. */
    private static final Pattern AUTHORED_JAVA_REFERENCE = Pattern.compile(
            "\\bAuthored\\s*\\.\\s*plan\\b|\\bBoundary\\s*\\.\\s*authored\\b|(?<![\\w$])authoredOrError(?![\\w$])");
    private static final Pattern SUPPRESSION = Pattern.compile("@ts-(?:ignore|expect-error|nocheck)");
    /** H5: a cast through unknown, which satisfies any type. */
    private static final Pattern UNKNOWN_CAST = Pattern.compile("\\bas\\s+unknown\\s+as\\b");
    /** H5: the brand under another name -- a renamed import or export of it, or a type alias of it. */
    private static final Pattern BRAND_ALIAS = Pattern.compile(
            "\\bPlannedSql\\s+as\\s+[\\w$]|\\btype\\s+[\\w$]+\\s*(?:<[^=;]*>)?\\s*=\\s*PlannedSql\\s*;");
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

    /**
     * {@code src}'s code: each comment and each string's text blanked to spaces (newlines kept), a template's
     * {@code ${...}} kept as code -- what an identifier count and a cast count read, prose and messages left out.
     */
    static String code(String src) {
        return code(src, false);
    }

    /** {@link #code(String)}, its strings' text kept where {@code strings}: comments alone blanked. */
    static String code(String src, boolean strings) {
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
        int found = 0;
        Matcher m = p.matcher(text);
        while (m.find()) {
            found++;
        }
        return found;
    }

    @Test
    void everyUseOfAMintIsCounted_andNoNewEscapeFromTheTypeSystem() throws IOException {
        List<String> product = folders().get("typescript");
        List<String> read = new ArrayList<>(product);
        read.addAll(TESTS);
        Map<String, Integer> references = new TreeMap<>();
        Map<String, Integer> escapes = new TreeMap<>();
        for (String file : inventory()) {
            if (!file.matches(".*\\.(?:[cm]?ts|tsx)$") || !under(file, read)) {
                continue;
            }
            String code = code(read(file));
            int refs = count(MINT_IDENTIFIER, code);
            if (refs > 0) {
                references.put(file, refs);
            }
            if (under(file, product)) {
                int e = count(ESCAPE, code) + count(SUPPRESSION, read(file));
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
        List<String> product = folders().get("typescript");
        List<String> read = new ArrayList<>(product);
        read.addAll(TESTS);
        Map<String, Integer> found = new TreeMap<>();
        Map<String, Integer> java = new TreeMap<>();
        for (String file : inventory()) {
            if (file.matches(".*\\.(?:[cm]?ts|tsx|[cm]?js)$") && under(file, read)) {
                int refs = authoredReferences(read(file));
                if (refs > 0) {
                    found.put(file, refs);
                }
            } else if (file.endsWith(".java") && under(file, JAVA_PRODUCT)) {
                int refs = count(AUTHORED_JAVA_REFERENCE, code(read(file)));
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
        String backend = read(PLAYGROUND);
        int factory = backend.indexOf("export function playground(session: RunSession): Playground {");
        int runner = backend.indexOf("export function runner(session: RunSession): Runner {");
        assertTrue(factory >= 0 && runner > factory, "run.ts's playground() and runner() moved: " + PLAYGROUND);
        int send = backend.indexOf("return runAuthored(plan, { databaseType, run: (sql) => engine.run(sql, 0) });");
        assertTrue(send > factory && send < runner, "the playground's send is the Playground object's, and no other's");
        int planned = backend.indexOf("const plan = await session.authored(statement);");
        assertTrue(planned > factory && planned < send, "the playground sends the plan's one statement: the person's"
                + " text as the planner carried it (Boundary.authored), never the text itself");
        assertTrue(!backend.contains("planned-sql.ts"), "the playground mints nothing: the plan runner types the plan's"
                + " statement");
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

    /** Each type the TypeScript declares that holds the brand in a property, by name (H5): a fixed point over every
     *  declaration in {@code codes}, from PlannedSql. */
    static Set<String> brandHolders(List<String> codes) {
        Map<String, List<String>> braced = new TreeMap<>();
        Map<String, List<String>> aliased = new TreeMap<>();
        for (String s : codes) {
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
        }
        Set<String> holders = new TreeSet<>(Set.of("PlannedSql"));
        for (boolean changed = true; changed; ) {
            changed = false;
            Pattern token = Pattern.compile("(?<![\\w$])(?:" + String.join("|", holders) + ")(?![\\w$])");
            for (Map<String, List<String>> kind : List.of(braced, aliased)) {
                for (Map.Entry<String, List<String>> e : kind.entrySet()) {
                    if (!holders.contains(e.getKey())
                            && e.getValue().stream().anyMatch(b -> holds(b, kind == aliased, token))) {
                        holders.add(e.getKey());
                        changed = true;
                    }
                }
            }
        }
        return holders;
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
        Set<String> records = new TreeSet<>(holders);
        records.remove("PlannedSql");
        List<String> found = new ArrayList<>();
        if (records.isEmpty()) {
            return found;
        }
        Pattern token = Pattern.compile("(?<![\\w$])(?:" + String.join("|", records) + ")(?![\\w$])");
        String s = MODULE_STATEMENT.matcher(code).replaceAll(r -> " ".repeat(r.group().length()));
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
        List<String> product = folders().get("typescript");
        List<String> read = new ArrayList<>(product);
        read.addAll(TESTS);
        Map<String, String> codes = new TreeMap<>();
        for (String file : inventory()) {
            if (file.matches(".*\\.(?:[cm]?ts|tsx)$") && under(file, read)) {
                codes.put(file, code(read(file)));
            }
        }
        Set<String> holders = brandHolders(new ArrayList<>(codes.values()));
        for (String record : List.of("Plan", "HostOperation", "HostSource", "SnapStreamRequest", "InferredModel",
                "ExecutePlan")) {
            assertTrue(holders.contains(record), record + " holds a plan's statement, and the census does not see it");
        }
        Map<String, Integer> unknownCasts = new TreeMap<>();
        Map<String, Integer> holderCasts = new TreeMap<>();
        Map<String, List<String>> holderCastTexts = new TreeMap<>();
        List<String> aliases = new ArrayList<>();
        for (Map.Entry<String, String> e : codes.entrySet()) {
            String file = e.getKey();
            if (!file.equals("engine-client/src/planned-sql.ts") && BRAND_ALIAS.matcher(e.getValue()).find()) {
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
}
