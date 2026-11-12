package com.legend.tools.guards;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.testing.Runfile;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * THE HOST-SQL GUARD, its TypeScript half (host_sql.txt says the rule; docs/ONE_PLATFORM_PROGRAM_2026_10_12.md D5):
 * every product .ts of the tab's apps and the libraries they bundle -- the inventory's files under the rule's folders
 * -- read for its string literals, each held to the rule. A literal is a quoted string, or a template's text between
 * its substitutions; comments and regular expressions are skipped, so prose about SQL is free. Its Python half is
 * python/tests/test_host_sql.py.
 *
 * <p>A lexer of its own, not TypeScript's parser (2026-10-13): the repository's TypeScript is 7.0.2, whose JavaScript
 * API is an unstable bridge to its Go binary, and rules_js refuses another package's raw sources as a node_test's data,
 * where every package's all_files reaches this package (guards_package).
 *
 * <p>What it cannot see, by construction: a statement built from parts none of which begins one ({@code 'DROP' + ' T'},
 * {@code `${verb} TABLE`}), and a regular expression right after a closing bracket ({@code if (x) /re/}), read as a
 * division. The guard holds the literals a host writes; the operations' fields that carry SQL carry the planner's.
 */
class HostSqlTest {

    /** The rule: host_sql.txt's sections. */
    record Rule(List<String> typescript, List<String> python, Pattern statement, List<Kept> kept,
            List<Harness> harnesses) {

        /** Whether a literal's text begins a statement. */
        boolean isStatement(String text) {
            return statement.matcher(text).lookingAt();
        }

        static Rule read(Path file) throws IOException {
            Map<String, List<String>> sections = new LinkedHashMap<>();
            List<String> current = null;
            for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                if (line.isBlank() || line.startsWith("#")) {
                    continue;
                }
                if (line.startsWith("[") && line.endsWith("]")) {
                    current = sections.computeIfAbsent(line.substring(1, line.length() - 1), k -> new ArrayList<>());
                } else if (current != null) {
                    current.add(line.strip());
                }
            }
            List<String> keywords = sections.get("keywords").stream()
                    .flatMap(l -> List.of(l.split("\\s+")).stream()).toList();
            // a keyword, upper case, then a space (more text, or a name joined on); or a lower-case statement's shape
            String either = "\\s*(?:(?:" + String.join("|", keywords) + ")\\s|"
                    + String.join("|", sections.get("shapes").stream().map(s -> "(?:" + s + ")").toList()) + ")";
            List<Kept> kept = sections.get("kept").stream().map(l -> {
                String[] parts = l.split(" \\| ", 3);
                return new Kept(parts[0].strip(), quoted(parts[1].strip()), parts[2].strip());
            }).toList();
            List<Harness> harnesses = sections.getOrDefault("harnesses", List.of()).stream().map(l -> {
                String[] parts = l.split(" \\| ", 2);
                return new Harness(parts[0].strip(), parts[1].strip());
            }).toList();
            return new Rule(sections.get("typescript"), sections.get("python"), Pattern.compile(either), kept,
                    harnesses);
        }
    }

    /** A kept site's text, written in double quotes ({@code \"} and {@code \\} its escapes): the literal exactly. */
    static String quoted(String written) {
        if (written.length() < 2 || !written.startsWith("\"") || !written.endsWith("\"")) {
            throw new IllegalArgumentException("a kept site's text is written in double quotes: " + written);
        }
        return written.substring(1, written.length() - 1).replace("\\\"", "\"").replace("\\\\", "\\");
    }

    /** A site kept: its file, the literal's whole text as written, and why it is not a statement the host spells. */
    record Kept(String file, String text, String reason) {
    }

    /** A harness script kept whole (the C final audit's H12, 2026-11-12): a test in all but its folder, whose statements
     *  build its own fixture database or stand for a person's typing, and why. */
    record Harness(String file, String reason) {
    }

    /** A string literal of a source: its line, and its text (escapes as written). */
    record Literal(int line, String text) {
    }

    /** Keywords after which a {@code /} begins a regular expression, not a division. */
    private static final Set<String> BEFORE_EXPRESSION = Set.of("return", "typeof", "case", "do", "else", "in", "of",
            "new", "delete", "void", "throw", "instanceof", "yield", "await");

    /**
     * Each string literal of a TypeScript source: a quoted string, and each part of a template's text (before, between
     * and after its substitutions), the code in its substitutions read as code. A {@code /} begins a
     * regular expression where an expression may begin (after an operator, an opening bracket, a keyword above, or at
     * the start), and is a division after a name, a number or a closing bracket.
     */
    static List<Literal> literals(String src) {
        List<Literal> out = new ArrayList<>();
        // for each open template substitution, the brace depth that closes it
        Deque<Integer> substitutions = new ArrayDeque<>();
        int depth = 0;
        int line = 1;
        boolean expressionMayBegin = true;
        int i = 0;
        int n = src.length();
        while (i < n) {
            char c = src.charAt(i);
            if (c == '\n') {
                line++;
                i++;
            } else if (Character.isWhitespace(c)) {
                i++;
            } else if (c == '/' && i + 1 < n && src.charAt(i + 1) == '/') {
                while (i < n && src.charAt(i) != '\n') {
                    i++;
                }
            } else if (c == '/' && i + 1 < n && src.charAt(i + 1) == '*') {
                int end = src.indexOf("*/", i + 2);
                end = end < 0 ? n : end + 2;
                line += count(src, i, end);
                i = end;
            } else if (c == '\'' || c == '"') {
                int start = i;
                i = skipQuoted(src, i, c);
                out.add(new Literal(line, src.substring(start + 1, Math.max(start + 1, i - 1))));
                line += count(src, start, i);
                expressionMayBegin = false;
            } else if (c == '`' || (c == '}' && !substitutions.isEmpty() && substitutions.peek() == depth)) {
                // a template's text, from its start or from a substitution's end, to its end or its next substitution
                boolean head = c == '`';
                if (!head) {
                    substitutions.pop();
                }
                int start = ++i;
                while (i < n && src.charAt(i) != '`' && !(src.charAt(i) == '$' && i + 1 < n && src.charAt(i + 1) == '{')) {
                    i += src.charAt(i) == '\\' ? 2 : 1;
                }
                out.add(new Literal(line, src.substring(start, Math.min(i, n))));
                line += count(src, start, Math.min(i, n));
                if (i < n && src.charAt(i) == '$') {
                    substitutions.push(depth);
                    i += 2;
                    expressionMayBegin = true;
                } else {
                    i++;
                    expressionMayBegin = false;
                }
            } else if (c == '/' && expressionMayBegin) {
                i = skipRegex(src, i);
                expressionMayBegin = false;
            } else if (Character.isJavaIdentifierStart(c)) {
                int start = i;
                while (i < n && Character.isJavaIdentifierPart(src.charAt(i))) {
                    i++;
                }
                expressionMayBegin = BEFORE_EXPRESSION.contains(src.substring(start, i));
            } else if (Character.isDigit(c)) {
                while (i < n && (Character.isLetterOrDigit(src.charAt(i)) || src.charAt(i) == '.' || src.charAt(i) == '_')) {
                    i++;
                }
                expressionMayBegin = false;
            } else {
                if (c == '{') {
                    depth++;
                } else if (c == '}') {
                    depth--;
                }
                // after a closing bracket a value ends (a division may follow); after any other punctuation one begins
                expressionMayBegin = c != ')' && c != ']' && c != '}';
                i++;
            }
        }
        return out;
    }

    private static int skipQuoted(String src, int at, char quote) {
        int i = at + 1;
        while (i < src.length() && src.charAt(i) != quote && src.charAt(i) != '\n') {
            i += src.charAt(i) == '\\' ? 2 : 1;
        }
        return Math.min(i + 1, src.length());
    }

    private static int skipRegex(String src, int at) {
        int i = at + 1;
        boolean inClass = false;
        while (i < src.length() && src.charAt(i) != '\n') {
            char c = src.charAt(i);
            if (c == '\\') {
                i += 2;
                continue;
            }
            if (c == '[') {
                inClass = true;
            } else if (c == ']') {
                inClass = false;
            } else if (c == '/' && !inClass) {
                i++;
                break;
            }
            i++;
        }
        while (i < src.length() && Character.isLetter(src.charAt(i))) {
            i++;
        }
        return i;
    }

    private static int count(String src, int from, int to) {
        int lines = 0;
        for (int i = from; i < to && i < src.length(); i++) {
            if (src.charAt(i) == '\n') {
                lines++;
            }
        }
        return lines;
    }

    private static Rule rule() throws IOException {
        return Rule.read(Runfile.property("host.sql.rule"));
    }

    @Test
    void aStatementInALiteralIsFoundNeverOneInACommentOrARegularExpression() throws IOException {
        Rule rule = rule();
        String source = String.join("\n",
                "// SELECT * FROM T: prose about SQL",
                "/* CREATE TABLE t (x INT)",
                "   DROP TABLE t */",
                "const a = 'SELECT 1';",
                "const b = `DROP TABLE IF EXISTS ${name} AND ${`INSERT INTO x ${y}`}`;",
                "const c = 'select x from y';",
                "const d = \"insert into t values (1)\";",
                "const e = { label: 'Select a column', tag: 'SELECT', state: 'COMMITTED', kind: 'SELECTED' };",
                "const f = /^select\\s+'/i.test(s) ? a / b / c : 'SET x = 1';",
                "const g = `text ${'USE \"db\"'} tail SELECT 2`;",
                "const h = 'USE ' + quoted(name);",
                "const i = `${''}SELECT 1 FROM t`;",
                "const j = ['install ' + name, 'load ' + name, 'set threads = 4', 'pragma platform', 'use \"db\"'];",
                "const k = ['Install the app', 'load more rows', 'set', 'use the filter', 'begin'];");
        List<Integer> found = literals(source).stream().filter(l -> rule.isStatement(l.text()))
                .map(Literal::line).toList();
        assertEquals(List.of(4, 5, 5, 6, 7, 9, 10, 11, 12, 13, 13, 13, 13, 13), found);
    }

    @Test
    void noProductTypeScriptSpellsSqlButTheSitesKept() throws IOException {
        noProductTypeScriptSpellsSqlButTheSitesKept(Map.of());
    }

    /** CHECK 6 (the final audit's item 5, 2026-11-12): the census's own pinned comparison FAILS over the tree with a
     *  statement planted in a product source, naming it -- never merely that its reader finds the literal. */
    @Test
    void theCensusFailsOnAPlantedStatement() throws IOException {
        String planted = "datacube/src/planted-violator.ts";
        AssertionError failed = assertThrows(AssertionError.class, () -> noProductTypeScriptSpellsSqlButTheSitesKept(
                Map.of(planted, "export const sql = 'SELECT * FROM trades WHERE desk = ?';")));
        assertTrue(failed.getMessage().contains(planted), "failed without naming the planted file: " + failed.getMessage());
    }

    /** The census over the tree's product TypeScript, and any file {@code planted} on top of it (path to its text). */
    static void noProductTypeScriptSpellsSqlButTheSitesKept(Map<String, String> planted) throws IOException {
        Rule rule = rule();
        List<String> files = java.util.stream.Stream.concat(Files.readAllLines(Runfile.property("inventory.files")).stream(),
                        new TreeSet<>(planted.keySet()).stream())
                // and the JavaScript beside it (the C final audit's H12, 2026-11-12: a harness's .mjs sent its own
                // statements, which no census read)
                .filter(f -> f.endsWith(".ts") && !f.endsWith(".d.ts") || f.matches(".*\\.(?:mjs|cjs|js)$"))
                .filter(f -> rule.typescript().stream().anyMatch(d -> f.startsWith(d + "/")))
                .toList();
        for (String folder : rule.typescript()) {
            assertTrue(files.stream().anyMatch(f -> f.startsWith(folder + "/")),
                    folder + " holds no .ts: the rule names a folder that moved (host_sql.txt)");
        }
        List<String> found = new ArrayList<>();
        List<Kept> used = new ArrayList<>();
        Set<String> harnessed = new TreeSet<>();
        Set<String> harnesses = new TreeSet<>(rule.harnesses().stream().map(Harness::file).toList());
        for (String file : files) {
            String src = planted.containsKey(file) ? planted.get(file)
                    : Files.readString(Runfile.of("_main/" + file), StandardCharsets.UTF_8);
            for (Literal l : literals(src)) {
                if (!rule.isStatement(l.text())) {
                    continue;
                }
                if (harnesses.contains(file)) {
                    harnessed.add(file);
                    continue;
                }
                Kept kept = rule.kept().stream().filter(k -> k.file().equals(file) && l.text().equals(k.text()))
                        .findFirst().orElse(null);
                if (kept != null) {
                    used.add(kept);
                } else {
                    found.add(file + ":" + l.line() + ": " + l.text().substring(0, Math.min(80, l.text().length())));
                }
            }
        }
        assertEquals(List.of(), found, "SQL spelled in the tab: name the operation and its data instead (engine-client/"
                + "src/host-operations.ts), which the planner writes by the database's type (HostOperations.java)");
        List<String> stale = rule.kept().stream()
                .filter(k -> k.file().matches(".*\\.(?:ts|mjs|cjs|js)$") && !used.contains(k))
                .map(k -> k.file() + ": " + k.text()).toList();
        assertEquals(List.of(), stale, "a site kept is no longer there: take it out of host_sql.txt");
        List<String> idle = harnesses.stream().filter(h -> !harnessed.contains(h)).toList();
        assertEquals(List.of(), idle, "a harness kept spells no statement now: take it out of host_sql.txt's [harnesses]");
    }
}
