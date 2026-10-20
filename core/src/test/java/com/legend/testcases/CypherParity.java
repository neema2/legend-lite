// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testcases;

import com.legend.exec.ExecutionResult;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * The Cypher parity harness (docs/CYPHER_STUDY_2026_10_15.md §8): every openCypher TCK scenario and every case of
 * lite's own ({@code cypher-parity/lite/*.yaml}) run as a {@code #Cypher{...}#} island over the model its graph
 * implies ({@link CypherGraph}), judged against Neo4j's snapped answer ({@link CypherAnswers}) in the TCK's value
 * grammar ({@link CypherValues}), with {@link SqlParity}'s statuses and ratchet: PASS, FAIL, REFUSED, ERROR,
 * BOTH_ERROR, BUG, SKIP (a case the harness cannot model yet, or Neo4j could not set up), per lane in
 * {@code ledger.tsv}. Zero FAIL, BUG and ERROR is the rule; a refusal by name is the only other way a case ends.
 */
public final class CypherParity {

    private CypherParity() {
    }

    /** The Neo4j Community release the answers are snapped on (C9: the newest at each landing; a file snapped on
     *  another is a VERSION problem until the pin moves with a dated reason). */
    public static final String PINNED_SERVER = "2026.09.0";

    /** lite's default Cypher version for an island without a {@code CYPHER n} prefix (C1, C9). */
    public static final String DEFAULT_CYPHER = "25";

    private static final Pattern PREFIX = Pattern.compile("\\s*CYPHER\\s+(5|25)\\b");

    /**
     * A case: its key ({@code file/id}), its query, its setup statements, its parameters as written, the Cypher
     * version it names (null for the default), what the TCK expects (null for a lite case), why the harness skips it
     * (null to run), and a lite case's decision and expect.
     */
    public record Case(String file, String id, String query, List<String> setup, String paramsText,
            @com.legend.base.Nullable String version, CypherTck.@com.legend.base.Nullable Expect expect,
            @com.legend.base.Nullable String skip, @com.legend.base.Nullable String decision,
            @com.legend.base.Nullable String expectText, List<CypherTck.Procedure> procedures) {

        public String key() {
            return file + "/" + id;
        }

        /** The Cypher version the case runs under: its query's prefix, else the one it names, else the default. */
        public String cypherVersion() {
            Matcher m = PREFIX.matcher(query);
            if (m.lookingAt()) {
                return m.group(1);
            }
            return version == null ? DEFAULT_CYPHER : version;
        }

        public String sha() {
            return CypherAnswers.sha(query, cypherVersion(), setup, paramsText, decision, expectText,
                    CypherTck.Procedure.text(procedures));
        }

        /** The island as the lane runs it: the parameters bound into the query's text, the version named outside
         *  the query made its prefix. */
        public String island() {
            String q = bindParameters(implicitArguments(query), paramsText);
            if (version != null && !PREFIX.matcher(q).lookingAt()) {
                q = "CYPHER " + version + " " + q;
            }
            // the graph: the harness's DataSpace, named after the version prefix (the study's §12.45)
            java.util.regex.Matcher m = PREFIX.matcher(q);
            q = m.lookingAt() ? q.substring(0, m.end()) + " USE " + CypherGraph.DATASPACE + q.substring(m.end())
                    : "USE " + CypherGraph.DATASPACE + " " + q;
            return "#Cypher{" + q + "}#";
        }

        SqlParity.Case asSql() {
            return new SqlParity.Case(file, id, query, skip);
        }

        /** The parameters bound into the query's text (the island has no parameter channel; the study's §12.15): a
         *  TCK scenario's {@code name=value} lines, or a lite case's flow map in the value grammar, each
         *  {@code $name} replaced by its value's literal text -- the grammar's spelling of a list, map, string,
         *  number, boolean or null is Cypher's. The oracle passes the same values to Neo4j as parameters. */
        /** {@code CALL test.my.proc} with no parentheses calls the procedure with its inputs' namesakes among the
         *  query's parameters (Cypher's implicit arguments): spelled as the explicit call of those parameters,
         *  which the binding below fills as it fills every parameter (the study's §12.60). */
        private String implicitArguments(String query) {
            for (CypherTck.Procedure p : procedures) {
                // the standalone call alone (Cypher allows implicit arguments nowhere else: inside a query the
                // island raises Neo4j's error)
                if (!p.inputs().isEmpty() && query.strip().equals("CALL " + p.name())) {
                    return "CALL " + p.name() + "(" + p.inputs().stream().map(i -> "$" + i.name())
                            .collect(java.util.stream.Collectors.joining(", ")) + ")";
                }
            }
            return query;
        }

        static String bindParameters(String query, String paramsText) {
            if (paramsText == null || paramsText.isBlank()) {
                return query;
            }
            Map<String, String> values = new LinkedHashMap<>();
            if (paramsText.strip().startsWith("{")) {
                CypherValues.Value v = CypherValues.parse(paramsText.strip());
                ((CypherValues.Mp) v).entries().forEach((k, x) -> values.put(k, CypherValues.text(x)));
            } else {
                for (String line : paramsText.split("\n")) {
                    int eq = line.indexOf('=');
                    if (eq > 0) {
                        values.put(line.substring(0, eq).strip(), line.substring(eq + 1).strip());
                    }
                }
            }
            String out = query;
            for (Map.Entry<String, String> e : values.entrySet()) {
                out = out.replaceAll("\\$" + java.util.regex.Pattern.quote(e.getKey()) + "\\b",
                        java.util.regex.Matcher.quoteReplacement(e.getValue()));
            }
            return out;
        }
    }

    // ------------------------------------------------------------------ the corpus

    /** The TCK's scenarios ({@code tckDir}: features and graphs) and lite's cases ({@code liteDir}: the yaml
     *  files, with {@code ../graphs} for named graphs), in that order. */
    public static List<Case> cases(Path tckDir, Path liteDir) throws IOException {
        List<Case> out = new ArrayList<>();
        for (CypherTck.Scenario s : CypherTck.read(tckDir)) {
            int slash = s.key().lastIndexOf('/');
            out.add(new Case(s.key().substring(0, slash), s.key().substring(slash + 1), s.query(), s.setup(),
                    s.paramsText(), null, s.expect(), s.skip(), null, null, s.procedures()));
        }
        out.addAll(liteCases(liteDir));
        return out;
    }

    /** lite's own cases, read as the oracle reads them ({@code snap.py}, {@code lite_cases}): the YAML subset of
     *  {@code - id:} items whose values are a quoted line or a {@code |} block. */
    public static List<Case> liteCases(Path liteDir) throws IOException {
        List<Case> out = new ArrayList<>();
        if (!Files.isDirectory(liteDir)) {
            return out;
        }
        Map<String, String> graphs = new LinkedHashMap<>();
        Path gdir = liteDir.resolveSibling("graphs");
        if (Files.isDirectory(gdir)) {
            try (Stream<Path> files = Files.list(gdir)) {
                for (Path f : files.sorted().toList()) {
                    String name = f.getFileName().toString();
                    if (name.endsWith(".cypher")) {
                        graphs.put(name.substring(0, name.length() - ".cypher".length()),
                                Files.readString(f, StandardCharsets.UTF_8));
                    }
                }
            }
        }
        List<Path> files;
        try (Stream<Path> s = Files.list(liteDir)) {
            files = s.filter(f -> f.getFileName().toString().endsWith(".yaml")).sorted().toList();
        }
        for (Path f : files) {
            String fileName = f.getFileName().toString();
            String file = "lite/" + fileName.substring(0, fileName.length() - ".yaml".length());
            for (Map<String, String> item : items(Files.readString(f, StandardCharsets.UTF_8).split("\n", -1))) {
                String graph = item.getOrDefault("graph", "");
                List<String> setup = new ArrayList<>();
                if (graphs.containsKey(graph)) {
                    setup.add(statement(graphs.get(graph)));
                } else if (!graph.isBlank()) {
                    setup.add(statement(graph));
                }
                // a lite case declaring procedures (the study's §12.60) is judged by its own expect block, as the
                // TCK's are by theirs: Neo4j runs no procedure of the harness's
                String procedures = item.get("procedures");
                String expect = item.get("expect");
                out.add(new Case(file, item.get("id"), item.getOrDefault("cypher", ""), setup,
                        item.getOrDefault("params", ""), item.get("version"),
                        procedures != null && expect != null ? CypherTck.expectOf(expect) : null, item.get("skip"),
                        item.get("decision"), expect,
                        procedures == null ? List.of() : CypherTck.Procedure.parseAll(procedures)));
            }
        }
        return out;
    }

    private static final Pattern ITEM = Pattern.compile("\\s*-\\s+id:\\s*(\\S+)\\s*$");
    private static final Pattern FIELD = Pattern.compile("\\s*([a-z]+):\\s*(.*)$");

    private static List<Map<String, String>> items(String[] lines) {
        List<Map<String, String>> items = new ArrayList<>();
        int i = 0;
        while (i < lines.length) {
            Matcher m = ITEM.matcher(lines[i]);
            if (!m.matches()) {
                i++;
                continue;
            }
            Map<String, String> item = new LinkedHashMap<>();
            item.put("id", m.group(1));
            int indent = lines[i].length() - lines[i].stripLeading().length() + 2;
            i++;
            while (i < lines.length) {
                String l2 = lines[i];
                if (l2.strip().isEmpty() || l2.strip().startsWith("#")) {
                    i++;
                    continue;
                }
                if (l2.length() - l2.stripLeading().length() < indent) {
                    break;
                }
                Matcher f = FIELD.matcher(l2);
                if (!f.matches()) {
                    throw new IllegalStateException("cypher-parity yaml: unexpected line " + l2);
                }
                String key = f.group(1);
                String rest = f.group(2).strip();
                i++;
                if (rest.equals("|")) {
                    List<String> body = new ArrayList<>();
                    while (i < lines.length && (lines[i].strip().isEmpty()
                            || lines[i].length() - lines[i].stripLeading().length() > indent)) {
                        body.add(lines[i].length() >= indent + 2 ? lines[i].substring(indent + 2) : "");
                        i++;
                    }
                    while (!body.isEmpty() && body.get(body.size() - 1).strip().isEmpty()) {
                        body.remove(body.size() - 1);
                    }
                    item.put(key, String.join("\n", body));
                } else {
                    if (rest.length() >= 2 && (rest.startsWith("\"") && rest.endsWith("\"")
                            || rest.startsWith("'") && rest.endsWith("'"))) {
                        rest = rest.substring(1, rest.length() - 1);
                    }
                    item.put(key, rest);
                }
            }
            items.add(item);
        }
        return items;
    }

    private static String statement(String text) {
        String s = text.stripTrailing();
        while (s.endsWith(";")) {
            s = s.substring(0, s.length() - 1);
        }
        return s;
    }

    // ------------------------------------------------------------------ running a lane

    /** A lane: a database the cases' tables are seeded into, and lite run over them. */
    public interface Lane {
        /** The connection's text in the model. */
        String connection();

        /** Creates and fills the tables (dropping their earlier versions). */
        void seed(List<SqlParity.Table> tables) throws Exception;

        /** Runs {@code island} over {@code model} on this lane's database. */
        ExecutionResult run(String model, String island) throws Exception;
    }

    /** A decided case ({@code cypher-parity/decisions.tsv}) on one lane: the register row that decides it, and
     *  what lite gives under it: rows in the value grammar, or an error the database raises ({@code rows} null). */
    public record Decision(String register, @com.legend.base.Nullable List<List<String>> rows) {
    }

    /**
     * The decisions file: {@code key<TAB>register row<TAB>duckdb<TAB>postgres}, each lane's cell a JSON list of
     * rows of grammar text, {@code ERROR} (the database raises where Neo4j answers), or {@code -} (Neo4j's answer
     * judges that lane). The decisions for {@code lane}.
     */
    public static Map<String, Decision> decisions(Path file, String lane) throws IOException {
        Map<String, Decision> out = new LinkedHashMap<>();
        if (!Files.exists(file)) {
            return out;
        }
        int column = 2 + SqlParity.LANES.indexOf(lane);
        for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
            if (line.isBlank() || line.startsWith("#")) {
                continue;
            }
            String[] parts = line.split("\t", -1);
            if (parts.length != 2 + SqlParity.LANES.size()) {
                throw new IllegalStateException("decisions.tsv: expected key, register row and a cell per lane: "
                        + line);
            }
            String cell = parts[column];
            if (cell.equals("-")) {
                continue;
            }
            if (cell.equals("ERROR")) {
                out.put(parts[0], new Decision(parts[1], null));
                continue;
            }
            List<List<String>> rows = new ArrayList<>();
            for (com.legend.json.Json.Node r : ((com.legend.json.Json.Arr) com.legend.json.Json.parse(cell)).items()) {
                List<String> cells = new ArrayList<>();
                for (com.legend.json.Json.Node c : ((com.legend.json.Json.Arr) r).items()) {
                    cells.add(((com.legend.json.Json.Str) c).value());
                }
                rows.add(cells);
            }
            out.put(parts[0], new Decision(parts[1], rows));
        }
        return out;
    }

    /** The decisions that decide no case of the corpus (every lane's, read from the file). */
    public static List<String> decisionGuards(List<Case> cases, Path file) throws IOException {
        java.util.Set<String> keys = new java.util.HashSet<>();
        cases.forEach(c -> keys.add(c.key()));
        List<String> problems = new ArrayList<>();
        if (!Files.exists(file)) {
            return problems;
        }
        for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
            if (line.isBlank() || line.startsWith("#")) {
                continue;
            }
            String key = line.split("\t", -1)[0];
            if (!keys.contains(key)) {
                problems.add("GONE: the decision for " + key + " names no case");
            }
        }
        return problems;
    }

    /** Every case's outcome on {@code lane}: the cases grouped by their graph, each graph seeded once. */
    public static Map<String, SqlParity.Outcome> run(List<Case> cases, Map<String, CypherAnswers.Answer> answers,
            Map<String, Decision> decisions, Lane lane) throws Exception {
        Map<String, SqlParity.Outcome> outcomes = new LinkedHashMap<>();
        Map<String, List<Case>> byGraph = new LinkedHashMap<>();
        Map<String, CypherGraph.Inferred> inferred = new LinkedHashMap<>();
        for (Case c : cases) {
            if (c.skip() != null) {
                outcomes.put(c.key(), new SqlParity.Outcome(SqlParity.Status.SKIP, c.skip()));
                continue;
            }
            CypherAnswers.Answer a = answers.get(c.key());
            if (a == null) {
                throw new IllegalStateException("no snapped Neo4j answer for " + c.key());
            }
            if (a.setupError() != null) {
                outcomes.put(c.key(), new SqlParity.Outcome(SqlParity.Status.SKIP, "Neo4j refused the setup: "
                        + a.setupError().code() + ": " + a.setupError().message()));
                continue;
            }
            CypherGraph.Inferred inf;
            try {
                inf = CypherGraph.infer(java.util.Objects.requireNonNull(a.graph()));
            } catch (CypherGraph.Unsupported u) {
                outcomes.put(c.key(), new SqlParity.Outcome(SqlParity.Status.SKIP, u.getMessage()));
                continue;
            }
            inferred.putIfAbsent(inf.graphKey(), inf);
            byGraph.computeIfAbsent(inf.graphKey(), k -> new ArrayList<>()).add(c);
        }
        for (Map.Entry<String, List<Case>> e : byGraph.entrySet()) {
            CypherGraph.Inferred inf = inferred.get(e.getKey());
            try {
                lane.seed(inf.tables());
            } catch (Exception seeding) {
                // the graph's tables could not be made: every case over it is an error of the harness, named (the
                // harness's last column a dialect refused, TIMESTAMP_NS on a microsecond engine, went with W13 leg 6)
                for (Case c : e.getValue()) {
                    outcomes.put(c.key(), new SqlParity.Outcome(SqlParity.Status.ERROR, "seeding: "
                            + oneLine(seeding)));
                }
                continue;
            }
            String model = CypherGraph.model(inf, lane.connection());
            for (Case c : e.getValue()) {
                outcomes.put(c.key(), outcome(c, java.util.Objects.requireNonNull(answers.get(c.key())),
                        decisions.get(c.key()), model, lane));
            }
        }
        return outcomes;
    }

    /** One case on one lane, its graph seeded: its island run through lite, judged against Neo4j's answer, or
     *  against the rows its decision names. */
    public static SqlParity.Outcome outcome(Case c, CypherAnswers.Answer a, @com.legend.base.Nullable Decision decided,
            String model, Lane lane) {
        if (!c.procedures().isEmpty()) {
            return procedureOutcome(c, model + CypherGraph.procedures(c.procedures()), lane);
        }
        ExecutionResult result;
        try {
            result = lane.run(model, c.island());
        } catch (Exception | StackOverflowError e) {
            if (decided != null && decided.rows() == null) {
                return new SqlParity.Outcome(SqlParity.Status.PASS, "decided: " + decided.register() + ", the"
                        + " database raises: " + oneLine(e));
            }
            return failed(e, a);
        }
        if (decided != null && decided.rows() == null) {
            return new SqlParity.Outcome(SqlParity.Status.FAIL, "the decision " + decided.register() + " says the"
                    + " database raises, and lite answered rows");
        }
        SqlParity.Grid actual = SqlParity.grid(result);
        if (a.error() != null) {
            return new SqlParity.Outcome(SqlParity.Status.BUG, "Neo4j rejects it (" + a.error().code() + ": "
                    + a.error().message() + ") and legend-lite answered " + actual.rows().size() + " rows");
        }
        // Neo4j names an unaliased item by its text, a parameter as written ($age); the lane binds the parameters
        // into the query's text (Case.island), so the island names it by the bound text: Neo4j's names read
        // through the same binding
        List<String> columns = java.util.Objects.requireNonNull(a.columns()).stream()
                .map(name -> Case.bindParameters(name, c.paramsText())).toList();
        if (!columns.equals(actual.columns())) {
            return new SqlParity.Outcome(SqlParity.Status.FAIL, "columns: Neo4j " + columns + ", lite "
                    + actual.columns());
        }
        List<List<CypherValues.Value>> expected = new ArrayList<>();
        for (List<String> row : decided != null ? java.util.Objects.requireNonNull(decided.rows())
                : java.util.Objects.requireNonNull(a.rows())) {
            expected.add(CypherValues.row(row));
        }
        List<List<CypherValues.Value>> got = new ArrayList<>();
        // the cells as the database returned them (a list or map is its JSON, read as a value by the judge; the
        // grid's canonical JSON text would lose a float's spelling: 3.0 is a float in Cypher, 3 an integer)
        for (var row : result.rows()) {
            List<CypherValues.Value> cells = new ArrayList<>();
            for (Object cell : row.values()) {
                cells.add(CypherValues.ofCell(cell));
            }
            got.add(cells);
        }
        boolean ordered;
        boolean bagLists = false;
        if (c.expect() instanceof CypherTck.RowsExpected r) {
            ordered = r.ordered();
            bagLists = r.ignoreListOrder();
        } else {
            ordered = c.query().toUpperCase(java.util.Locale.ROOT).contains("ORDER BY");
        }
        if (CypherValues.sameRows(expected, got, ordered, bagLists)) {
            return new SqlParity.Outcome(SqlParity.Status.PASS, decided == null ? "" : "decided: " + decided.register());
        }
        return new SqlParity.Outcome(SqlParity.Status.FAIL, (decided == null ? "Neo4j " : "the decision "
                + decided.register() + " ") + expected.size() + " rows "
                + firstRows(expected) + (ordered ? " in order" : " in any order") + ", lite " + got.size() + " rows "
                + firstRows(got));
    }


    /** The outcome of a run that threw: refused by name, lite's compile error, or another error, each BOTH_ERROR
     *  where Neo4j errs too. */
    /** A case declaring procedures (the study's §12.60): Neo4j runs none of the harness's, so the TCK's own
     *  expectation judges (its rows, an empty result, or an error: any error of lite's but a refusal by name). */
    private static SqlParity.Outcome procedureOutcome(Case c, String model, Lane lane) {
        CypherTck.Expect expect = c.expect();
        if (expect == null) {
            return new SqlParity.Outcome(SqlParity.Status.ERROR, "a procedure case with no expectation to judge by");
        }
        ExecutionResult result;
        try {
            result = lane.run(model, c.island());
        } catch (Exception | StackOverflowError e) {
            boolean refusal = e instanceof com.legend.error.NotImplementedException;
            for (Throwable t = e; t != null; t = t.getCause()) {
                refusal |= t instanceof com.legend.sql.dialect.DialectCapability;
            }
            if (refusal) {
                return new SqlParity.Outcome(SqlParity.Status.REFUSED, oneLine(e));
            }
            if (expect instanceof CypherTck.ErrorExpected err) {
                return new SqlParity.Outcome(SqlParity.Status.PASS, "the TCK expects " + err.type() + " ("
                        + err.detail() + "); lite: " + oneLine(e));
            }
            return new SqlParity.Outcome(SqlParity.Status.ERROR, oneLine(e));
        }
        SqlParity.Grid actual = SqlParity.grid(result);
        List<List<CypherValues.Value>> got = new ArrayList<>();
        for (var row : result.rows()) {
            List<CypherValues.Value> cells = new ArrayList<>();
            for (Object cell : row.values()) {
                cells.add(CypherValues.ofCell(cell));
            }
            got.add(cells);
        }
        return switch (expect) {
            case CypherTck.ErrorExpected err -> new SqlParity.Outcome(SqlParity.Status.FAIL, "the TCK expects "
                    + err.type() + " (" + err.detail() + ") and lite answered " + got.size() + " rows");
            case CypherTck.EmptyExpected ignored -> got.isEmpty()
                    ? new SqlParity.Outcome(SqlParity.Status.PASS, "")
                    : new SqlParity.Outcome(SqlParity.Status.FAIL, "the TCK expects no row, lite answered "
                            + got.size());
            case CypherTck.RowsExpected r -> {
                if (!r.columns().equals(actual.columns())) {
                    yield new SqlParity.Outcome(SqlParity.Status.FAIL, "columns: the TCK " + r.columns() + ", lite "
                            + actual.columns());
                }
                List<List<CypherValues.Value>> expected = new ArrayList<>();
                for (List<String> row : r.rows()) {
                    expected.add(CypherValues.row(row));
                }
                yield CypherValues.sameRows(expected, got, r.ordered(), r.ignoreListOrder())
                        ? new SqlParity.Outcome(SqlParity.Status.PASS, "")
                        : new SqlParity.Outcome(SqlParity.Status.FAIL, "the TCK " + expected.size() + " rows "
                                + firstRows(expected) + (r.ordered() ? " in order" : " in any order") + ", lite "
                                + got.size() + " rows " + firstRows(got));
            }
        };
    }

    private static SqlParity.Outcome failed(Throwable e, CypherAnswers.Answer a) {
        // a dialect's refusal by name (DialectCapability, at render: a microsecond engine asked for nanoseconds)
        // is a refusal, wherever it sits in the cause chain
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof com.legend.sql.dialect.DialectCapability) {
                return a.error() != null ? new SqlParity.Outcome(SqlParity.Status.BOTH_ERROR, oneLine(e))
                        : new SqlParity.Outcome(SqlParity.Status.REFUSED, oneLine(e));
            }
        }
        if (e instanceof com.legend.error.NotImplementedException) {
            return a.error() != null ? new SqlParity.Outcome(SqlParity.Status.BOTH_ERROR, oneLine(e))
                    : new SqlParity.Outcome(SqlParity.Status.REFUSED, oneLine(e));
        }
        if (e instanceof com.legend.compiler.spec.TypeInferenceException) {
            // C4 (docs/SEMANTICS_REGISTER.md S58): over a CLOSED graph a label, property or relationship type the
            // model does not declare is lite's compile error where Neo4j answers no rows or null -- a decided
            // refusal, by name. The harness's graphs are OPEN (the island's USE names its DataSpace, marked
            // <<graph::graph.Open>>, the study's §12.45), so the lanes read Neo4j's answers; the classification
            // stays for a reading the open graph does not cover yet
            boolean c4 = String.valueOf(e.getMessage()).contains("the graph is the model");
            return a.error() != null ? new SqlParity.Outcome(SqlParity.Status.BOTH_ERROR, oneLine(e))
                    : new SqlParity.Outcome(c4 ? SqlParity.Status.REFUSED : SqlParity.Status.ERROR, oneLine(e));
        }
        return a.error() != null ? new SqlParity.Outcome(SqlParity.Status.BOTH_ERROR, oneLine(e))
                : new SqlParity.Outcome(SqlParity.Status.ERROR, oneLine(e));
    }

    private static String firstRows(List<List<CypherValues.Value>> rows) {
        List<String> out = new ArrayList<>();
        for (int i = 0; i < Math.min(3, rows.size()); i++) {
            out.add(CypherValues.rowText(rows.get(i)));
        }
        return String.join(" ", out) + (rows.size() > 3 ? " ..." : "");
    }

    private static String oneLine(Throwable e) {
        String m = e.getClass().getSimpleName() + ": " + e.getMessage();
        m = m.replace('\n', ' ').replace('\t', ' ');
        return m.length() > 300 ? m.substring(0, 300) + "..." : m;
    }

    // ------------------------------------------------------------------ the refusal census

    /**
     * The REFUSED and SKIP outcomes by their reason, the specifics (names, positions) folded away, most frequent
     * first: the burn-down list (docs/CYPHER_STUDY_2026_10_15.md §12.3). A case counts once, under the FIRST
     * construct the translator refused, so attacking a category uncovers the next refusal in its queries. Written to
     * the test's outputs as {@code cypher-parity-<lane>-census.tsv} and printed.
     */
    public static String census(Map<String, SqlParity.Outcome> outcomes, String lane) throws IOException {
        Map<String, Integer> counts = new java.util.HashMap<>();
        for (SqlParity.Outcome o : outcomes.values()) {
            if (o.status() == SqlParity.Status.REFUSED || o.status() == SqlParity.Status.SKIP) {
                counts.merge(o.status() + "\t" + reason(o.detail()), 1, Integer::sum);
            }
        }
        StringBuilder out = new StringBuilder("status\treason\tcases\n");
        counts.entrySet().stream().sorted((x, y) -> y.getValue().compareTo(x.getValue()) != 0
                ? y.getValue().compareTo(x.getValue()) : x.getKey().compareTo(y.getKey()))
                .forEach(e -> out.append(e.getKey()).append('\t').append(e.getValue()).append('\n'));
        String text = out.toString();
        String outputs = System.getenv("TEST_UNDECLARED_OUTPUTS_DIR");
        if (outputs != null) {
            Files.writeString(Path.of(outputs, "cypher-parity-" + lane + "-census.tsv"), text, StandardCharsets.UTF_8);
            // and every case with its folded reason and its detail, for attacking one category's queries
            StringBuilder each = new StringBuilder("case\tstatus\treason\tdetail\n");
            outcomes.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(e -> each.append(e.getKey())
                    .append('\t').append(e.getValue().status()).append('\t').append(reason(e.getValue().detail()))
                    .append('\t').append(e.getValue().detail().replace('\n', ' ').replace('\t', ' ')).append('\n'));
            Files.writeString(Path.of(outputs, "cypher-parity-" + lane + "-cases.tsv"), each.toString(),
                    StandardCharsets.UTF_8);
        }
        System.out.println("the refusal census, " + lane + " lane:\n" + text);
        return text;
    }

    /** A refusal's or skip's reason with its specifics folded away: one line per construct. */
    static String reason(String detail) {
        String m = detail.replaceAll("^\\w+: #Cypher\\{\\.\\.\\.\\}# at line \\d+, column \\d+( of the SQL)?: ", "");
        m = m.replaceAll("\\(.*?\\)", "");
        m = m.replaceAll("the (function|list predicate) \\S+", "the $1 X");
        m = m.replaceAll("the label \\S+ names no class.*", "a label naming no class of the model (C4)");
        m = m.replaceAll("the class \\S+ has no property \\S+.*", "a property no class declares (C4)");
        m = m.replaceAll("the relationship type \\S+ names no property.*",
                "a relationship type naming no property or association end (C4)");
        m = m.replaceAll("the relationship \\S+ (holds|reaches|is a property).*",
                "a relationship that fans out or has no inverse end");
        m = m.replaceAll("a (node|relationship) as a value \\S*", "a node or relationship as a value");
        m = m.replaceAll("a float literal beyond.*", "a float literal beyond 1e15 or below 1e-15");
        m = m.replaceAll("harness: the property \\S+ holds a (\\w+).*", "harness: a property holding a $1");
        m = m.replaceAll("harness: the relationship type \\S+ joins several class pairs.*",
                "harness: a relationship type joining several class pairs");
        m = m.replaceAll("harness: a node with several labels.*", "harness: a node with several labels");
        m = m.replaceAll("Neo4j refused the setup.*", "Neo4j refused the setup");
        m = m.replaceAll("^(\\w+) is not supported yet$", "the clause $1");
        return m.replaceAll("\\s+", " ").strip().replaceAll("\\.$", "");
    }

    // ------------------------------------------------------------------ the ledger

    private static final String LEDGER_HEADER = "# the Cypher parity corpus (the openCypher TCK and lite's cases,"
            + " cypher-parity/), each case's status per lane: the ratchet\n# CypherParityTest and"
            + " CypherParityPostgresTest hold (core/src/test/java/com/legend/testcases/CypherParity.java; the"
            + " statuses are SqlParity's)\n";

    public static List<String> ratchet(List<Case> cases, Map<String, SqlParity.Outcome> outcomes, Path ledger,
            String lane, @com.legend.base.Nullable Path update) throws IOException {
        return SqlParity.ratchet("the Cypher parity corpus (the openCypher TCK and lite's cases)", "cypher-parity",
                LEDGER_HEADER, cases.stream().map(Case::asSql).toList(), outcomes, ledger, lane, update);
    }
}
