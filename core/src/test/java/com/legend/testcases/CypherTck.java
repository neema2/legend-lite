// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testcases;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * The openCypher TCK's feature files, read as test input (docs/CYPHER_STUDY_2026_10_15.md §8.2): the Gherkin subset
 * the TCK uses, each scenario (a Scenario Outline once per Examples row, its placeholders substituted) as its key,
 * its query, its setup statements, its parameters and what it expects. This reader MIRRORS the oracle's
 * ({@code tools/neo4j-oracle/snap.py}, {@code tck_cases}) step for step: the two must produce the same keys and the
 * same sha inputs (the query, the setup, the parameters as written), or every answer is STALE.
 */
public final class CypherTck {

    private CypherTck() {
    }

    /** What a scenario expects of its query. */
    public sealed interface Expect permits RowsExpected, EmptyExpected, ErrorExpected {
    }

    /** {@code Then the result should be, in any order / in order / (ignoring element order for lists)}: the columns
     *  and the rows in the value grammar. */
    public record RowsExpected(List<String> columns, List<List<String>> rows, boolean ordered,
            boolean ignoreListOrder) implements Expect {
    }

    /** {@code Then the result should be empty}. */
    public record EmptyExpected() implements Expect {
    }

    /** {@code Then a <Type> should be raised at <phase>: <detail>}. */
    public record ErrorExpected(String type, String phase, String detail) implements Expect {
    }

    /**
     * A scenario: its key ({@code tck/<feature path>/<name>}, {@code #<row>} appended for an outline's row), its
     * query, its setup statements (a named graph's, the {@code having executed} blocks), its parameters as written
     * (one {@code name=value} per line) and parsed, what it expects (null when it says nothing), and why the harness
     * does not run it (a control query, a procedure, a step it does not read; null when it runs).
     */
    public record Scenario(String key, String query, List<String> setup, String paramsText,
            Map<String, String> params, @com.legend.base.Nullable Expect expect,
            @com.legend.base.Nullable String skip, List<Procedure> procedures) {
    }

    /** A procedure the scenario declares ({@code there exists a procedure test.my.proc(in :: INTEGER?) :: (out ::
     *  STRING?):} and its rows, the header the inputs then the outputs): the harness declares it as a Pure function
     *  over its rows (the study's §12.60). The types are the TCK's ({@code INTEGER}, {@code STRING}, {@code NUMBER},
     *  {@code FLOAT}, {@code BOOLEAN}), each optional. */
    public record Procedure(String name, List<Param> inputs, List<Param> outputs, List<List<String>> rows) {
        public record Param(String name, String type) {
        }

        private static final Pattern DECLARATION = Pattern.compile(
                "(?:there exists a procedure )?(\\S+?)\\((.*?)\\)\\s*::\\s*\\((.*?)\\)\\s*:?\\s*$");

        /** The declaration's text with its table (the header row first; a table of one empty cell is none). */
        static Procedure parse(String text, @com.legend.base.Nullable List<List<String>> table) {
            Matcher m = DECLARATION.matcher(text.strip());
            if (!m.matches()) {
                throw new IllegalStateException("a procedure declaration the harness does not read: " + text);
            }
            // the table's first row is its header (the inputs' then the outputs' names), the rest the rows
            List<List<String>> rows = new ArrayList<>();
            if (table != null) {
                for (int i = 1; i < table.size(); i++) {
                    List<String> row = table.get(i);
                    if (!(row.size() == 1 && row.get(0).isEmpty())) {
                        rows.add(row);
                    }
                }
            }
            return new Procedure(m.group(1), params(m.group(2)), params(m.group(3)), rows);
        }

        private static List<Param> params(String text) {
            List<Param> out = new ArrayList<>();
            for (String p : text.split(",")) {
                if (p.isBlank()) {
                    continue;
                }
                String[] parts = p.split("::");
                if (parts.length != 2) {
                    throw new IllegalStateException("a procedure parameter the harness does not read: " + p);
                }
                out.add(new Param(parts[0].strip(), parts[1].strip().replace("?", "")));
            }
            return out;
        }

        /** The declarations of a lite case's {@code procedures:} block: each a declaration line followed by its
         *  table's lines. */
        public static List<Procedure> parseAll(String block) {
            List<Procedure> out = new ArrayList<>();
            String declaration = null;
            List<List<String>> table = new ArrayList<>();
            for (String line : block.split("\n")) {
                String l = line.strip();
                if (l.isEmpty()) {
                    continue;
                }
                if (l.startsWith("|")) {
                    table.add(cells(l));
                } else {
                    if (declaration != null) {
                        out.add(parse(declaration, table));
                    }
                    declaration = l;
                    table = new ArrayList<>();
                }
            }
            if (declaration != null) {
                out.add(parse(declaration, table));
            }
            return out;
        }

        /** What the answer is for: the declaration and its rows, as the oracle's snapper spells them. */
        public String text() {
            StringBuilder sb = new StringBuilder(name).append('(');
            sb.append(inputs.stream().map(p -> p.name() + " :: " + p.type() + "?").collect(
                    java.util.stream.Collectors.joining(", "))).append(") :: (");
            sb.append(outputs.stream().map(p -> p.name() + " :: " + p.type() + "?").collect(
                    java.util.stream.Collectors.joining(", "))).append(")");
            for (List<String> row : rows) {
                sb.append('\n').append(String.join("|", row));
            }
            return sb.toString();
        }

        public static String text(List<Procedure> procedures) {
            return procedures.stream().map(Procedure::text).collect(java.util.stream.Collectors.joining("\n"));
        }
    }

    /** A lite case's {@code expect:} block: {@code rows in order}, {@code rows in any order} or {@code rows
     *  (ignoring element order)} then the table, {@code empty}, or {@code error <type>}. */
    public static Expect expectOf(String block) {
        String[] lines = block.strip().split("\n");
        String head = lines[0].strip();
        if (head.equals("empty")) {
            return new EmptyExpected();
        }
        if (head.startsWith("error")) {
            return new ErrorExpected(head.substring(5).strip(), "compile time", "");
        }
        if (!head.startsWith("rows")) {
            throw new IllegalStateException("an expect block the harness does not read: " + head);
        }
        List<List<String>> table = new ArrayList<>();
        for (int i = 1; i < lines.length; i++) {
            if (lines[i].strip().startsWith("|")) {
                table.add(cells(lines[i].strip()));
            }
        }
        return new RowsExpected(table.isEmpty() ? List.of() : table.get(0),
                table.isEmpty() ? List.of() : table.subList(1, table.size()), head.contains("in order"),
                head.contains("ignoring element order"));
    }

    /** Every scenario under {@code tckDir} ({@code features/} and {@code graphs/}). */
    public static List<Scenario> read(Path tckDir) throws IOException {
        Map<String, String> graphs = new TreeMap<>();
        Path gdir = tckDir.resolve("graphs");
        if (Files.isDirectory(gdir)) {
            try (Stream<Path> dirs = Files.list(gdir)) {
                for (Path d : dirs.sorted().toList()) {
                    Path f = d.resolve(d.getFileName().toString() + ".cypher");
                    if (Files.isRegularFile(f)) {
                        graphs.put(d.getFileName().toString(), Files.readString(f, StandardCharsets.UTF_8)
                                .replace("\r\n", "\n").replace('\r', '\n'));
                    }
                }
            }
        }
        List<Scenario> out = new ArrayList<>();
        Path fdir = tckDir.resolve("features");
        try (Stream<Path> files = Files.walk(fdir)) {
            for (Path f : files.filter(p -> p.toString().endsWith(".feature")).sorted().toList()) {
                String rel = fdir.relativize(f).toString().replace('\\', '/');
                rel = rel.substring(0, rel.length() - ".feature".length());
                // the oracle reads the files in Python's text mode, which turns CRLF into LF: so here
                out.addAll(feature(Files.readString(f, StandardCharsets.UTF_8).replace("\r\n", "\n")
                        .replace('\r', '\n').split("\n", -1), "tck/" + rel, graphs));
            }
        }
        return out;
    }

    private record Step(String text, @com.legend.base.Nullable String doc,
            @com.legend.base.Nullable List<List<String>> table) {
    }

    private static final Pattern SCENARIO = Pattern.compile("(Scenario Outline|Scenario):\\s*(.*)");
    private static final Pattern STEP = Pattern.compile("(Given|And|When|Then|But)\\s+(.*)");
    private static final Pattern NAMED_GRAPH = Pattern.compile("the (\\S+) graph$");
    private static final Pattern ERROR = Pattern.compile(
            "an? (\\w+) should be raised at (compile time|runtime|any time): (\\S+)");

    /** The scenarios of one feature file, as the oracle reads them. */
    static List<Scenario> feature(String[] lines, String prefix, Map<String, String> graphs) {
        List<Scenario> out = new ArrayList<>();
        int i = 0;
        int n = lines.length;
        while (i < n) {
            String line = lines[i].strip();
            Matcher m = SCENARIO.matcher(line);
            if (!m.matches()) {
                i++;
                continue;
            }
            boolean outline = m.group(1).equals("Scenario Outline");
            String name = m.group(2).strip();
            i++;
            List<Step> steps = new ArrayList<>();
            List<List<List<String>>> examples = new ArrayList<>();
            while (i < n) {
                line = lines[i].strip();
                if (SCENARIO.matcher(line).lookingAt() || line.startsWith("Feature:")) {
                    break;
                }
                if (line.startsWith("Examples:")) {
                    i++;
                    List<List<String>> table = new ArrayList<>();
                    while (i < n && lines[i].strip().startsWith("|")) {
                        table.add(cells(lines[i].strip()));
                        i++;
                    }
                    examples.add(table);
                    continue;
                }
                Matcher s = STEP.matcher(line);
                if (s.matches()) {
                    String text = s.group(2).strip();
                    i++;
                    String doc = null;
                    List<List<String>> table = null;
                    if (i < n && lines[i].strip().equals("\"\"\"")) {
                        int indent = lines[i].length() - lines[i].stripLeading().length();
                        i++;
                        List<String> body = new ArrayList<>();
                        while (i < n && !lines[i].strip().equals("\"\"\"")) {
                            String l = lines[i];
                            String lead = l.length() >= indent ? l.substring(0, indent) : l;
                            body.add(lead.strip().isEmpty() && l.length() >= indent ? l.substring(indent)
                                    : l.stripLeading());
                            i++;
                        }
                        i++;
                        doc = String.join("\n", body).replaceAll("\n+$", "");
                    } else if (i < n && lines[i].strip().startsWith("|")) {
                        table = new ArrayList<>();
                        while (i < n && lines[i].strip().startsWith("|")) {
                            table.add(cells(lines[i].strip()));
                            i++;
                        }
                    }
                    steps.add(new Step(text, doc, table));
                    continue;
                }
                i++;
            }
            if (outline) {
                for (List<List<String>> table : examples) {
                    List<String> header = table.get(0);
                    for (int r = 1; r < table.size(); r++) {
                        Map<String, String> subst = new LinkedHashMap<>();
                        for (int c = 0; c < header.size() && c < table.get(r).size(); c++) {
                            subst.put(header.get(c), table.get(r).get(c));
                        }
                        out.add(scenario(prefix, name + " #" + r, substitute(steps, subst), graphs));
                    }
                }
            } else {
                out.add(scenario(prefix, name, steps, graphs));
            }
        }
        return out;
    }

    /** The cells of a table row, as the oracle splits them: the line's text between its outer bars, split at
     *  every bar, each cell stripped. */
    private static List<String> cells(String line) {
        String s = line.strip();
        int a = 0;
        int b = s.length();
        while (a < b && s.charAt(a) == '|') {
            a++;
        }
        while (b > a && s.charAt(b - 1) == '|') {
            b--;
        }
        List<String> out = new ArrayList<>();
        for (String part : s.substring(a, b).split("\\|", -1)) {
            out.add(part.strip());
        }
        return out;
    }

    private static List<Step> substitute(List<Step> steps, Map<String, String> subst) {
        List<Step> out = new ArrayList<>();
        for (Step s : steps) {
            List<List<String>> table = null;
            if (s.table() != null) {
                table = new ArrayList<>();
                for (List<String> row : s.table()) {
                    List<String> cells = new ArrayList<>();
                    for (String c : row) {
                        cells.add(sub(c, subst));
                    }
                    table.add(cells);
                }
            }
            String doc = s.doc();
            out.add(new Step(sub(s.text(), subst), doc == null ? null : sub(doc, subst), table));
        }
        return out;
    }

    private static String sub(String s, Map<String, String> subst) {
        String out = s;
        for (Map.Entry<String, String> e : subst.entrySet()) {
            out = out.replace("<" + e.getKey() + ">", e.getValue());
        }
        return out;
    }

    private static Scenario scenario(String prefix, String name, List<Step> steps, Map<String, String> graphs) {
        List<String> setup = new ArrayList<>();
        Map<String, String> params = new LinkedHashMap<>();
        List<String> paramsText = new ArrayList<>();
        List<Procedure> procedures = new ArrayList<>();
        String query = null;
        Expect expect = null;
        String skip = null;
        for (Step st : steps) {
            String text = st.text();
            Matcher g = NAMED_GRAPH.matcher(text);
            if (text.equals("an empty graph") || text.equals("any graph")) {
                continue;
            }
            if (g.matches()) {
                String graph = graphs.get(g.group(1));
                if (graph == null) {
                    skip = skip == null ? "an unknown named graph: " + g.group(1) : skip;
                } else {
                    setup.add(statement(graph));
                }
            } else if (text.equals("having executed:")) {
                setup.add(statement(st.doc() == null ? "" : st.doc()));
            } else if (text.equals("parameters are:")) {
                for (List<String> row : st.table() == null ? List.<List<String>>of() : st.table()) {
                    params.put(row.get(0), row.get(1));
                    paramsText.add(row.get(0) + "=" + row.get(1));
                }
            } else if (text.equals("executing query:")) {
                if (query != null) {
                    skip = skip == null ? "two queries" : skip;
                }
                query = st.doc();
            } else if (text.equals("executing control query:")) {
                skip = skip == null ? "a control query" : skip;
            } else if (text.startsWith("the result should be, in any order") || text.startsWith(
                    "the result should be, in order") || text.startsWith("the result should be (ignoring")) {
                List<List<String>> table = st.table() == null ? List.of() : st.table();
                expect = new RowsExpected(table.isEmpty() ? List.of() : table.get(0),
                        table.isEmpty() ? List.of() : table.subList(1, table.size()), text.contains("in order"),
                        text.contains("ignoring element order"));
            } else if (text.equals("the result should be empty")) {
                expect = new EmptyExpected();
            } else if (ERROR.matcher(text).matches()) {
                Matcher e = ERROR.matcher(text);
                e.matches();
                expect = new ErrorExpected(e.group(1), e.group(2), e.group(3));
            } else if (text.startsWith("there exists a procedure")) {
                procedures.add(Procedure.parse(text, st.table()));
            } else if (text.startsWith("having defined")) {
                skip = skip == null ? "a defined function" : skip;
            } else if (text.equals("no side effects") || text.startsWith("the side effects should be")) {
                continue;
            } else {
                skip = skip == null ? "an unread step: " + text : skip;
            }
        }
        if (query == null) {
            skip = skip == null ? "no query" : skip;
        }
        return new Scenario(prefix + "/" + name, query == null ? "" : query, List.copyOf(setup),
                String.join("\n", paramsText), params, expect, skip, List.copyOf(procedures));
    }

    /** A setup block as one statement: trailing whitespace and a trailing semicolon dropped, as the oracle does. */
    private static String statement(String text) {
        String s = text.stripTrailing();
        while (s.endsWith(";")) {
            s = s.substring(0, s.length() - 1);
        }
        return s;
    }
}
