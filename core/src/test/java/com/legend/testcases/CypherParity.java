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
import java.util.Set;
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
     * (null to run), a lite case's decision and expect, the procedures it declares, its control query (null when
     * none: a query run after a write, reading the graph it left), and the TCK's side-effect counts (null when the
     * scenario states none; Neo4j's own diff judges, and is checked against these, the writes design's §6).
     */
    public record Case(String file, String id, String query, List<String> setup, String paramsText,
            @com.legend.base.Nullable String version, CypherTck.@com.legend.base.Nullable Expect expect,
            @com.legend.base.Nullable String skip, @com.legend.base.Nullable String decision,
            @com.legend.base.Nullable String expectText, List<CypherTck.Procedure> procedures,
            @com.legend.base.Nullable String control, @com.legend.base.Nullable Map<String, Integer> sideEffects) {

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
                    CypherTck.Procedure.text(procedures), control);
        }

        /** The island as the lane runs it: the parameters bound into the query's text, the version named outside
         *  the query made its prefix. */
        public String island() {
            return island(bindParameters(implicitArguments(query), paramsText));
        }

        /** The control query's island, under the case's version (the oracle runs it so). */
        public String controlIsland() {
            return island(java.util.Objects.requireNonNull(control, "no control query"));
        }

        private String island(String text) {
            String q = text;
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
                    s.paramsText(), null, s.expect(), s.skip(), null, null, s.procedures(), s.control(),
                    s.sideEffects()));
        }
        out.addAll(liteCases(liteDir));
        return out;
    }

    /** lite's own cases, read as the oracle reads them ({@code snap.py}, {@code lite_cases}): the YAML subset of
     *  {@code - id:} items whose values are a quoted line or a {@code |} block ({@code control:} a write case's
     *  control query). */
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
                        procedures == null ? List.of() : CypherTck.Procedure.parseAll(procedures),
                        item.get("control"), null));
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

        /** Opens a transaction the harness owns around one write case (its statement, the graph read back after it,
         *  its control query); {@link #rollback} ends it, so that no case's writes reach the next case over the same
         *  seeded tables (the writes design's §6). */
        void begin() throws Exception;

        /** Rolls back the transaction {@link #begin} opened. */
        void rollback() throws Exception;

        /** Runs one statement of the harness's own on this lane's database: a native store's view (the native
         *  realization, {@link CypherGraph.Native}). */
        default void execute(String sql) throws Exception {
            throw new IllegalStateException("this lane runs no native realization: " + sql);
        }

        /** The rows of one query of the harness's own on this lane's database: a native store's tables read after
         *  a write ({@link #storeGraph}). */
        default List<List<Object>> query(String sql) throws Exception {
            throw new IllegalStateException("this lane runs no native realization: " + sql);
        }
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

    /** Every case's outcome on {@code lane}: the cases grouped by their graph, each graph seeded once. A graph some
     *  write case runs over is read back through the island once it is seeded ({@link #readBack}): the state lite's
     *  side effects are counted from, which must be the graph Neo4j's dump holds, or the judge is broken there (a
     *  problem added to {@code problems}, naming the graph's first case). */
    /** -Dcypher.parity.trace: each case printed as it finishes, with its time (a development run's aid). */
    private static final boolean TRACE = System.getProperty("cypher.parity.trace") != null;

    public static Map<String, SqlParity.Outcome> run(List<Case> cases, Map<String, CypherAnswers.Answer> answers,
            Map<String, Decision> decisions, Lane lane, List<String> problems) throws Exception {
        return run(cases, answers, decisions, lane, problems, CypherGraph.Realization.DOMAIN);
    }

    /** {@link #run} with the graphs realized as {@code realization} says: the domain model's own tables, or the
     *  native store's with the model's tables views over them (the writes design's §3 and §8, leg 3). */
    public static Map<String, SqlParity.Outcome> run(List<Case> cases, Map<String, CypherAnswers.Answer> answers,
            Map<String, Decision> decisions, Lane lane, List<String> problems, CypherGraph.Realization realization)
            throws Exception {
        Map<String, SqlParity.Outcome> outcomes = new LinkedHashMap<>();
        // (the native store's views live from the graph seeded before: dropped before its tables are made again)
        List<String> liveViews = new ArrayList<>();
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
                // (a write over a domain model: the schema of the graphs before and after, the writes design's leg
                // 5; over the native store the schema is the store's own)
                inf = realization == CypherGraph.Realization.DOMAIN && a.after() != null
                        ? writeSchema(java.util.Objects.requireNonNull(a.graph()), a.after(), c.query())
                        : CypherGraph.infer(java.util.Objects.requireNonNull(a.graph()), realization);
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
                CypherGraph.Native store = inf.nativeStore();
                if (store == null) {
                    lane.seed(inf.tables());
                } else {
                    for (String v : liveViews) {
                        lane.execute("DROP VIEW IF EXISTS " + v);
                    }
                    liveViews.clear();
                    lane.seed(store.tables());
                    for (int i = 0; i < store.views().size(); i++) {
                        lane.execute("DROP VIEW IF EXISTS " + store.viewNames().get(i));
                        lane.execute(store.views().get(i));
                        liveViews.add(store.viewNames().get(i));
                    }
                }
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
            Read before = null;
            String unread = null;
            Case first = e.getValue().stream().filter(c -> writes(c, answers.get(c.key()))).findFirst().orElse(null);
            if (first != null) {
                CypherAnswers.Answer a = java.util.Objects.requireNonNull(answers.get(first.key()));
                try {
                    before = readBack(model, lane, realization == CypherGraph.Realization.NATIVE);
                    if (!sameGraph(before, Read.of(java.util.Objects.requireNonNull(a.graph())))) {
                        unread = "the graph read back through the island is not Neo4j's: " + before.describe()
                                + ", Neo4j " + Read.of(a.graph()).describe();
                    }
                } catch (Exception | StackOverflowError unreadable) {
                    unread = "the graph could not be read back through the island: " + oneLine(unreadable);
                }
                if (unread != null) {
                    problems.add("JUDGE: " + first.key() + ": " + unread);
                    before = null;
                }
            }
            for (Case c : e.getValue()) {
                long started = System.nanoTime();
                if (TRACE) {
                    System.err.println("START\t" + c.key());
                }
                outcomes.put(c.key(), outcome(c, java.util.Objects.requireNonNull(answers.get(c.key())),
                        decisions.get(c.key()), model, lane, before, unread, realization, liveViews));
                if (TRACE) {
                    // (a development run's trace: each case as it finishes, and its time)
                    System.err.println("TRACE\t" + (System.nanoTime() - started) / 1_000_000 + "ms\t" + c.key()
                            + "\t" + outcomes.get(c.key()).status());
                }
            }
        }
        return outcomes;
    }

    /** A write case's domain model: the schema of the graphs before and after it ({@link CypherGraph#inferWrite});
     *  where the harness cannot model the graph after (a value it holds no column for: a list of temporals), the
     *  graph before's, so the island's own refusal of the write is what the case measures. */
    private static CypherGraph.Inferred writeSchema(CypherAnswers.Graph before, CypherAnswers.Graph after,
            String query) throws CypherGraph.Unsupported {
        try {
            return CypherGraph.inferWrite(before, after, query);
        } catch (CypherGraph.Unsupported afterUnmodelled) {
            return CypherGraph.infer(before, CypherGraph.Realization.DOMAIN);
        }
    }

    /** Whether a case writes (the writes design's §6): Neo4j's summary marked its query as writing, or it reads the
     *  graph back by a control query. */
    static boolean writes(Case c, CypherAnswers.@com.legend.base.Nullable Answer a) {
        return a != null && (a.after() != null || c.control() != null);
    }

    /** Whether a case's query has a clause that updates the graph, as lite reads it: run in a transaction rolled
     *  back after, though Neo4j raised (no graph after to judge), so the cases after over the same seeded graph
     *  read it as seeded. A query lite cannot read writes nothing. */
    static boolean updatesGraph(Case c) {
        try {
            return com.legend.protocol.cypher.CypherToPure.updates(com.legend.protocol.cypher.CypherParser.parse(
                    c.query()));
        } catch (RuntimeException unreadable) {
            return false;
        }
    }

    /** One case on one lane, its graph seeded: its island run through lite, judged against Neo4j's answer, or
     *  against the rows its decision names. A write (the writes design's §6) runs in a transaction the harness owns
     *  and rolls back, and passes only on all four of its verdicts: the rows, the graph it left, its side effects
     *  and its control query's rows ({@link #writeOutcome}); {@code before} is the seeded graph as the island reads
     *  it, null when it could not be read ({@code unread} says why). */
    public static SqlParity.Outcome outcome(Case c, CypherAnswers.Answer a, @com.legend.base.Nullable Decision decided,
            String model, Lane lane, @com.legend.base.Nullable Read before, @com.legend.base.Nullable String unread) {
        return outcome(c, a, decided, model, lane, before, unread, CypherGraph.Realization.DOMAIN, List.of());
    }

    /** {@link #outcome} over a graph realized as {@code realization} says; over the native store, the graph a write
     *  left is read back through a model of the store's own rows ({@link #storeGraph}: the writes design's §6, B's
     *  model is its store's), the views {@code liveViews} names made again for it inside the case's transaction. */
    public static SqlParity.Outcome outcome(Case c, CypherAnswers.Answer a, @com.legend.base.Nullable Decision decided,
            String model, Lane lane, @com.legend.base.Nullable Read before, @com.legend.base.Nullable String unread,
            CypherGraph.Realization realization, List<String> liveViews) {
        if (!c.procedures().isEmpty()) {
            return procedureOutcome(c, model + CypherGraph.procedures(c.procedures()), lane);
        }
        boolean judged = writes(c, a);
        if (!judged && !updatesGraph(c)) {
            return rowsOutcome(c, a, decided, model, lane);
        }
        try {
            lane.begin();
        } catch (Exception e) {
            return new SqlParity.Outcome(SqlParity.Status.ERROR, "harness: the transaction: " + oneLine(e));
        }
        try {
            SqlParity.Outcome rows = rowsOutcome(c, a, decided, model, lane);
            if (rows.status() != SqlParity.Status.PASS || !judged) {
                return rows;
            }
            if (before == null) {
                return new SqlParity.Outcome(SqlParity.Status.ERROR, "harness: " + unread);
            }
            String after = model;
            if (realization == CypherGraph.Realization.NATIVE) {
                try {
                    after = storeModel(lane, liveViews);
                } catch (Exception e) {
                    return new SqlParity.Outcome(SqlParity.Status.ERROR, "harness: the model of the store after the"
                            + " write: " + oneLine(e));
                }
            }
            SqlParity.Outcome wrote = writeOutcome(c, a, after, lane, before,
                    realization == CypherGraph.Realization.NATIVE);
            return wrote == null ? rows : wrote;
        } finally {
            try {
                lane.rollback();
            } catch (Exception e) {
                throw new IllegalStateException("harness: the rollback after " + c.key(), e);
            }
        }
    }

    /** A case's rows judged against Neo4j's answer, or against the rows its decision names. */
    private static SqlParity.Outcome rowsOutcome(Case c, CypherAnswers.Answer a,
            @com.legend.base.Nullable Decision decided, String model, Lane lane) {
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
        if (a.columns() != null && a.columns().isEmpty() && !actual.columns().isEmpty()
                && actual.columns().stream().allMatch(n -> n.startsWith(HIDDEN_COLUMN))) {
            // a statement that ends in an update returns no column (Neo4j's); lite's relation holds one, hidden
            // (docs/SEMANTICS_REGISTER.md: a relation has a column at least)
            List<List<Object>> none = new ArrayList<>();
            actual.rows().forEach(r -> none.add(List.of()));
            actual = new SqlParity.Grid(List.of(), none);
        }
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

    /** The outcome of a run that threw: refused by name (REFUSED, whatever Neo4j does), lite's compile error or
     *  another error, each BOTH_ERROR where Neo4j errs too. */
    private static SqlParity.Outcome failed(Throwable e, CypherAnswers.Answer a) {
        // a dialect's refusal by name (DialectCapability, at render: a microsecond engine asked for nanoseconds)
        // is a refusal, wherever it sits in the cause chain; a refusal is REFUSED where Neo4j raises too, as the
        // TCK's lane judges it: lite does not read the statement, so it does not catch Neo4j's error (PR #67's
        // review, 2026-10-22: eight cases had passed for BOTH_ERROR on a refusal)
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof com.legend.sql.dialect.DialectCapability) {
                return new SqlParity.Outcome(SqlParity.Status.REFUSED, oneLine(e));
            }
        }
        if (e instanceof com.legend.error.NotImplementedException) {
            return new SqlParity.Outcome(SqlParity.Status.REFUSED, oneLine(e));
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
        if (TRACE && e.getClass().getName().startsWith("java.lang.")) {
            // (a development run's trace: a Java failure's stack)
            e.printStackTrace();
        }
        String m = e.getClass().getSimpleName() + ": " + e.getMessage();
        m = m.replace('\n', ' ').replace('\t', ' ');
        return m.length() > 300 ? m.substring(0, 300) + "..." : m;
    }

    // ------------------------------------------------------------------ writes (the writes design's §6)

    /** A relationship of a read graph: its ends' identities and its value. */
    public record Edge(String start, String end, CypherValues.Rel rel) {
    }

    /** A graph as the write judge compares it: each node and relationship by its identity (an ordinal of Neo4j's
     *  dump, an {@code elementId} of lite's read-back), each a value of the TCK's grammar. */
    public record Read(Map<String, CypherValues.Node> nodes, Map<String, Edge> rels) {

        /** Neo4j's dump: its ordinals as the identities (a write's {@code after} keeps the ordinals of the elements
         *  the graph before held), its properties read in the value grammar. */
        public static Read of(CypherAnswers.Graph g) {
            Map<String, CypherValues.Node> nodes = new LinkedHashMap<>();
            for (CypherAnswers.GraphNode n : g.nodes()) {
                Map<String, CypherValues.Value> props = new java.util.TreeMap<>();
                n.props().forEach((k, v) -> props.put(k, CypherValues.parse(v)));
                nodes.put(String.valueOf(n.id()), new CypherValues.Node(new java.util.TreeSet<>(n.labels()), props));
            }
            Map<String, Edge> rels = new LinkedHashMap<>();
            for (CypherAnswers.GraphRel r : g.rels()) {
                Map<String, CypherValues.Value> props = new java.util.TreeMap<>();
                r.props().forEach((k, v) -> props.put(k, CypherValues.parse(v)));
                rels.put(String.valueOf(r.id()), new Edge(String.valueOf(r.start()), String.valueOf(r.end()),
                        new CypherValues.Rel(r.type(), props)));
            }
            return new Read(nodes, rels);
        }

        /** The graph's size, for a verdict's message. */
        public String size() {
            return nodes.size() + " nodes, " + rels.size() + " relationships";
        }

        /** The graph's elements, for a verdict's message: the first few nodes and relationships. */
        public String describe() {
            List<String> out = new ArrayList<>();
            nodes.values().stream().limit(4).forEach(n -> out.add(CypherValues.text(n)));
            rels.values().stream().limit(4).forEach(r -> out.add(CypherValues.text(nodes.get(r.start()))
                    + "-" + CypherValues.text(r.rel()) + "->" + CypherValues.text(nodes.get(r.end()))));
            return nodes.size() + " nodes, " + rels.size() + " relationships " + out;
        }
    }

    /** The prefix of the island's own column names (CypherToPure's synthetic names): never a name Neo4j returns. */
    private static final String HIDDEN_COLUMN = "_cypher_";

    /** The graph a native store's tables hold (the writes design's §3 B): each node by its id, its labels (the label
     *  set's text, {@code :A:B:}) and its properties (a row per slot, its value in the column of its type; a scalar
     *  slot's value the property), each relationship alike; the values in the dump's grammar, so the model of the
     *  store is inferred as a seeded graph's ({@link CypherGraph#infer}). A property of slots (a duration, a zoned or
     *  a nanosecond value) is not read back yet. */
    public static CypherAnswers.Graph storeGraph(Lane lane) throws Exception {
        Map<Long, Map<String, String>> nodeProps = storeProps(lane, "node_props", "node_id");
        Map<Long, Map<String, String>> relProps = storeProps(lane, "rel_props", "rel_id");
        List<CypherAnswers.GraphNode> nodes = new ArrayList<>();
        for (List<Object> row : lane.query("SELECT id, labels FROM nodes ORDER BY id")) {
            long id = ((Number) row.get(0)).longValue();
            List<String> labels = new ArrayList<>();
            for (String l : String.valueOf(row.get(1)).split(":")) {
                if (!l.isEmpty()) {
                    labels.add(l);
                }
            }
            nodes.add(new CypherAnswers.GraphNode(Math.toIntExact(id), labels, nodeProps.getOrDefault(id, Map.of())));
        }
        List<CypherAnswers.GraphRel> rels = new ArrayList<>();
        for (List<Object> row : lane.query("SELECT id, type, src_id, dst_id FROM rels ORDER BY id")) {
            long id = ((Number) row.get(0)).longValue();
            rels.add(new CypherAnswers.GraphRel(Math.toIntExact(id), Math.toIntExact(((Number) row.get(2)).longValue()),
                    Math.toIntExact(((Number) row.get(3)).longValue()), String.valueOf(row.get(1)),
                    relProps.getOrDefault(id, Map.of())));
        }
        return new CypherAnswers.Graph(nodes, rels);
    }

    /** The scalar properties of a native store's property table, by owner, each value in the dump's grammar. */
    private static Map<Long, Map<String, String>> storeProps(Lane lane, String table, String owner)
            throws Exception {
        Map<Long, Map<String, String>> out = new LinkedHashMap<>();
        Map<Long, Map<String, Map<String, Object>>> slotted = new LinkedHashMap<>();
        for (List<Object> row : lane.query("SELECT " + owner + ", key, slot, v_int, v_float, v_str, v_bool, v_date,"
                + " v_ts, v_time, v_json FROM " + table + " ORDER BY " + owner + ", key")) {
            String slot = String.valueOf(row.get(2));
            if (!slot.isEmpty()) {
                // (a property of slots: a zoned value's, a duration's; its value spelled once all its slots are read)
                Object cell = row.get(3) != null ? row.get(3) : row.get(8) != null ? localDateTime(row.get(8))
                        : row.get(7) != null ? row.get(7) : row.get(5);
                slotted.computeIfAbsent(((Number) row.get(0)).longValue(), k -> new LinkedHashMap<>())
                        .computeIfAbsent(String.valueOf(row.get(1)), k -> new LinkedHashMap<>()).put(slot, cell);
                continue;
            }
            CypherValues.Value v = row.get(3) != null ? new CypherValues.Int(java.math.BigInteger.valueOf(
                    ((Number) row.get(3)).longValue()))
                    : row.get(4) != null ? new CypherValues.Flt(((Number) row.get(4)).doubleValue())
                    : row.get(5) != null ? new CypherValues.Str(String.valueOf(row.get(5)))
                    : row.get(6) != null ? new CypherValues.Bool((Boolean) row.get(6))
                    : row.get(7) != null ? new CypherValues.Opaque("Date('" + row.get(7) + "')")
                    // (a local date-time and a local time as Neo4j's snapshot spells them: nine digits of the second)
                    : row.get(8) != null ? new CypherValues.Opaque("DateTime('" + localDateTime(row.get(8)).format(
                            java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS", java.util.Locale.ROOT)) + "')")
                    : row.get(9) != null ? new CypherValues.Opaque("Time('" + localTime(row.get(9)).format(
                            java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSSSSS", java.util.Locale.ROOT)) + "')")
                    : row.get(10) != null ? dumped(CypherValues.ofCell(row.get(10)))
                    : null;
            if (v == null) {
                throw new IllegalStateException("harness: a property of the native store holds no value it reads"
                        + " back yet: " + row);
            }
            out.computeIfAbsent(((Number) row.get(0)).longValue(), k -> new java.util.TreeMap<>())
                    .put(String.valueOf(row.get(1)), CypherValues.text(v));
        }
        for (Map.Entry<Long, Map<String, Map<String, Object>>> o : slotted.entrySet()) {
            for (Map.Entry<String, Map<String, Object>> p : o.getValue().entrySet()) {
                out.computeIfAbsent(o.getKey(), k -> new java.util.TreeMap<>()).put(p.getKey(),
                        slottedText(p.getKey(), p.getValue()));
            }
        }
        return out;
    }

    /** A stored list as the dump spells it: a local time's or date-time's text, the island's (to the minute, or
     *  the fraction trimmed: {@code Time('13:00')}), to nine digits of the second as the dump writes one; any other
     *  element as it is. */
    private static CypherValues.Value dumped(CypherValues.Value v) {
        if (!(v instanceof CypherValues.Lst l)) {
            return v;
        }
        List<CypherValues.Value> items = new ArrayList<>();
        for (CypherValues.Value item : l.items()) {
            String text = item instanceof CypherValues.Opaque o ? o.text() : null;
            if (text != null && text.startsWith("Time('") && text.endsWith("')")) {
                try {
                    item = new CypherValues.Opaque("Time('" + java.time.LocalTime.parse(text.substring(6,
                            text.length() - 2)).format(java.time.format.DateTimeFormatter.ofPattern(
                            "HH:mm:ss.SSSSSSSSS", java.util.Locale.ROOT)) + "')");
                } catch (java.time.format.DateTimeParseException e) {
                    // a zoned time: as the store holds it
                }
            } else if (text != null && text.startsWith("DateTime('") && text.endsWith("')")) {
                try {
                    item = new CypherValues.Opaque("DateTime('" + java.time.LocalDateTime.parse(text.substring(10,
                            text.length() - 2)).format(java.time.format.DateTimeFormatter.ofPattern(
                            "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS", java.util.Locale.ROOT)) + "')");
                } catch (java.time.format.DateTimeParseException e) {
                    // a zoned date-time: as the store holds it
                }
            }
            items.add(item);
        }
        return new CypherValues.Lst(items);
    }

    /** A property of slots as Neo4j's dump spells its value: a zoned date-time or time (its instant and offset; a
     *  time's slots {@link CypherGraph#TIME_SLOT_PREFIX}ed) at its offset, nine digits of the second; a duration
     *  (its four slots) as Neo4j prints one. Any other set of slots is the harness's error. */
    private static String slottedText(String key, Map<String, Object> slots) {
        String time = CypherGraph.TIME_SLOT_PREFIX;
        if (slots.keySet().equals(java.util.Set.of("instant", "offsetSeconds"))
                || slots.keySet().equals(java.util.Set.of(time + "instant", time + "offsetSeconds"))) {
            boolean ofDay = slots.containsKey(time + "instant");
            java.time.LocalDateTime instant = (java.time.LocalDateTime) slots.get(ofDay ? time + "instant" : "instant");
            int offset = Math.toIntExact(((Number) slots.get(ofDay ? time + "offsetSeconds" : "offsetSeconds"))
                    .longValue());
            java.time.LocalDateTime wall = instant.plusSeconds(offset);
            String nine = String.format(java.util.Locale.ROOT, "%02d:%02d:%02d.%09d", wall.getHour(), wall.getMinute(),
                    wall.getSecond(), wall.getNano());
            return ofDay ? "Time('" + nine + offsetText(offset) + "')"
                    : "DateTime('" + wall.toLocalDate() + "T" + nine + offsetText(offset) + "')";
        }
        if (slots.keySet().equals(java.util.Set.of("months", "days", "seconds", "nanoseconds"))) {
            return "Duration('" + durationText(((Number) slots.get("months")).longValue(),
                    ((Number) slots.get("days")).longValue(), ((Number) slots.get("seconds")).longValue(),
                    ((Number) slots.get("nanoseconds")).longValue()) + "')";
        }
        throw new IllegalStateException("harness: a property of slots " + slots.keySet() + " (" + key + ") read back"
                + " from the native store, no value the harness spells");
    }

    /** An offset as Neo4j's dump spells it: {@code +00:00}, {@code -05:30}, {@code +02:05:59}. */
    private static String offsetText(int seconds) {
        int abs = Math.abs(seconds);
        String text = (seconds < 0 ? "-" : "+") + String.format(java.util.Locale.ROOT, "%02d:%02d", abs / 3600, abs / 60 % 60);
        return abs % 60 == 0 ? text : text + String.format(java.util.Locale.ROOT, ":%02d", abs % 60);
    }

    /** A duration's four slots as Neo4j prints the duration: {@code P} and its years, months and days, then
     *  {@code T} and its hours, minutes and seconds (a fraction of the second after the point, its trailing zeros
     *  dropped), each part that is zero left out; {@code PT0S} for none. Seconds and nanoseconds of opposite signs
     *  print as their sum. */
    static String durationText(long months, long days, long seconds, long nanos) {
        if (months == 0 && days == 0 && seconds == 0 && nanos == 0) {
            return "PT0S";
        }
        StringBuilder out = new StringBuilder("P");
        part(out, months / 12, 'Y');
        part(out, months % 12, 'M');
        part(out, days, 'D');
        if (seconds != 0 || nanos != 0) {
            if (seconds < 0 && nanos > 0) {
                seconds++;
                nanos -= 1_000_000_000L;
            } else if (seconds > 0 && nanos < 0) {
                seconds--;
                nanos += 1_000_000_000L;
            }
            out.append('T');
            part(out, seconds / 3600, 'H');
            part(out, seconds % 3600 / 60, 'M');
            long s = seconds % 60;
            if (s != 0 || nanos != 0) {
                if (s == 0 && nanos < 0) {
                    out.append('-');
                }
                out.append(s);
                if (nanos != 0) {
                    String fraction = String.format(java.util.Locale.ROOT, "%09d", Math.abs(nanos)).replaceAll("0+$", "");
                    out.append('.').append(fraction);
                }
                out.append('S');
            }
        }
        return out.toString();
    }

    private static void part(StringBuilder out, long n, char unit) {
        if (n != 0) {
            out.append(n).append(unit);
        }
    }

    /** A TIMESTAMP cell as the driver reads it, a local date-time. */
    private static java.time.LocalDateTime localDateTime(Object cell) {
        return cell instanceof java.sql.Timestamp ts ? ts.toLocalDateTime() : (java.time.LocalDateTime) cell;
    }

    /** A TIME cell as the driver reads it, a local time. */
    private static java.time.LocalTime localTime(Object cell) {
        return cell instanceof java.sql.Time t ? t.toLocalTime() : (java.time.LocalTime) cell;
    }

    /** The model of a native store's graph as its tables hold it now ({@link #storeGraph}), its views made again
     *  for it (the ones {@code liveViews} names dropped first, the names added to it): the model a write's
     *  graph is read back through. */
    private static String storeModel(Lane lane, List<String> liveViews) throws Exception {
        CypherGraph.Inferred inf = CypherGraph.infer(storeGraph(lane), CypherGraph.Realization.NATIVE);
        CypherGraph.Native store = java.util.Objects.requireNonNull(inf.nativeStore());
        for (String v : liveViews) {
            lane.execute("DROP VIEW IF EXISTS " + v);
        }
        liveViews.clear();
        for (int i = 0; i < store.views().size(); i++) {
            lane.execute("DROP VIEW IF EXISTS " + store.viewNames().get(i));
            lane.execute(store.views().get(i));
            liveViews.add(store.viewNames().get(i));
        }
        return CypherGraph.model(inf, lane.connection());
    }

    /** The two queries the oracle dumps a graph by ({@code snap.py}, {@code dump}), run through the island. */
    private static final String READ_NODES = "MATCH (n) RETURN elementId(n) AS id, labels(n) AS labels,"
            + " properties(n) AS props";
    private static final String READ_RELS = "MATCH (a)-[r]->(b) RETURN elementId(r) AS id, elementId(a) AS a,"
            + " elementId(b) AS b, type(r) AS type, properties(r) AS props";

    /** The lane's graph read back through the island, by the oracle's own dump queries: each node by its
     *  {@code elementId}, its labels and properties; each relationship by its {@code elementId}, its ends', its type
     *  and properties. Two nodes read back with one identity is the harness's error (the side effects count
     *  identities). Two relationships with one are two links of an association between the same two nodes (S74:
     *  its identity is its type and its ends'), which nothing else tells apart: no property (that would be an edge
     *  class, keyed by its own row), so they are numbered by occurrence ({@code #2}, ...), and any pairing of them
     *  between two reads is the same. */
    public static Read readBack(String model, Lane lane) throws Exception {
        return readBack(model, lane, false);
    }

    /** {@link #readBack}; {@code storeIds} reads each identity as the native store's id, what its elementId ends in
     *  (an elementId is its class's path and its key, and a node's class is its label set's, which a write may
     *  change: the store's id is the node's across them). */
    public static Read readBack(String model, Lane lane, boolean storeIds) throws Exception {
        String use = "USE " + CypherGraph.DATASPACE + " ";
        Map<String, CypherValues.Node> nodes = new LinkedHashMap<>();
        for (var row : lane.run(model, "#Cypher{" + use + READ_NODES + "}#").rows()) {
            List<Object> v = row.values();
            String id = storeId(String.valueOf(v.get(0)), storeIds);
            java.util.TreeSet<String> labels = new java.util.TreeSet<>();
            for (CypherValues.Value l : ((CypherValues.Lst) CypherValues.ofCell(v.get(1))).items()) {
                labels.add(((CypherValues.Str) l).value());
            }
            if (nodes.put(id, new CypherValues.Node(labels, props(v.get(2)))) != null) {
                throw new IllegalStateException("harness: two nodes read back with the identity " + id);
            }
        }
        Map<String, Edge> rels = new LinkedHashMap<>();
        for (var row : lane.run(model, "#Cypher{" + use + READ_RELS + "}#").rows()) {
            List<Object> v = row.values();
            String id = storeId(String.valueOf(v.get(0)), storeIds);
            Edge e = new Edge(storeId(String.valueOf(v.get(1)), storeIds), storeId(String.valueOf(v.get(2)), storeIds),
                    new CypherValues.Rel(String.valueOf(v.get(3)), props(v.get(4))));
            Edge first = rels.get(id);
            if (first != null && (!first.start().equals(e.start()) || !first.end().equals(e.end())
                    || !CypherValues.equal(first.rel(), e.rel(), false))) {
                throw new IllegalStateException("harness: two relationships read back with the identity " + id
                        + " that differ: " + CypherValues.text(first.rel()) + ", " + CypherValues.text(e.rel()));
            }
            String key = id;
            for (int k = 2; rels.containsKey(key); k++) {
                key = id + "#" + k;
            }
            rels.put(key, e);
        }
        return new Read(nodes, rels);
    }

    /** An identity as read back: as the island spells it, or the native store's id it ends in. */
    private static String storeId(String identity, boolean storeIds) {
        return storeIds ? identity.substring(identity.lastIndexOf(':') + 1) : identity;
    }

    /** A {@code properties(x)} cell as its entries, a null among them dropped (a stored null is no property). */
    private static Map<String, CypherValues.Value> props(@com.legend.base.Nullable Object cell) {
        Map<String, CypherValues.Value> out = new java.util.TreeMap<>();
        ((CypherValues.Mp) CypherValues.ofCell(cell)).entries().forEach((k, v) -> {
            if (!(v instanceof CypherValues.Null)) {
                out.put(k, v);
            }
        });
        return out;
    }

    /** The first few of {@code x}'s nodes and relationships (by shape) that {@code y} has no match for. */
    private static String alone(Read x, Read y) {
        List<String> out = new ArrayList<>();
        List<CypherValues.Value> left = new ArrayList<>(y.nodes().values());
        for (CypherValues.Node n : x.nodes().values()) {
            int i = indexOf(left, n);
            if (i < 0) {
                out.add(CypherValues.text(n));
            } else {
                left.remove(i);
            }
        }
        left = new ArrayList<>(edges(y));
        for (CypherValues.Value e : edges(x)) {
            int i = indexOf(left, e);
            if (i < 0) {
                out.add(CypherValues.text(e));
            } else {
                left.remove(i);
            }
        }
        return out.stream().limit(4).toList().toString();
    }

    private static int indexOf(List<CypherValues.Value> values, CypherValues.Value v) {
        for (int i = 0; i < values.size(); i++) {
            if (CypherValues.equal(v, values.get(i), false)) {
                return i;
            }
        }
        return -1;
    }

    /** Whether two read graphs are the same graph by shape: the same bag of nodes (labels, properties), and the same
     *  bag of relationships (type, properties, the two nodes' labels and properties); the identities are each
     *  side's own. */
    public static boolean sameGraph(Read x, Read y) {
        return CypherValues.sameBag(new ArrayList<>(x.nodes().values()), new ArrayList<>(y.nodes().values()), false)
                && CypherValues.sameBag(edges(x), edges(y), false);
    }

    private static List<CypherValues.Value> edges(Read g) {
        List<CypherValues.Value> out = new ArrayList<>();
        for (Edge e : g.rels().values()) {
            out.add(new CypherValues.Lst(List.of(java.util.Objects.requireNonNull(g.nodes().get(e.start())),
                    e.rel(), java.util.Objects.requireNonNull(g.nodes().get(e.end())))));
        }
        return out;
    }

    /**
     * The TCK's side effects as the diff of two reads of one graph (the oracle's {@code side_effects} computes the
     * same, checked against every TCK table by {@code snap.sh --check}): the nodes and relationships created and
     * deleted (by identity), the label names the graph gained and lost (distinct names, not per node), and the
     * (element, key, value) triples it gained and lost (an overwrite is one of each). The non-zero counts, keyed as
     * the TCK keys them ({@code +nodes}, {@code -properties}, ...).
     */
    public static Map<String, Integer> sideEffects(Read before, Read after) {
        Map<String, Set<List<String>>> b = state(before);
        Map<String, Set<List<String>>> a = state(after);
        Map<String, Integer> out = new java.util.TreeMap<>();
        for (String k : List.of("nodes", "relationships", "labels", "properties")) {
            long plus = a.get(k).stream().filter(x -> !b.get(k).contains(x)).count();
            long minus = b.get(k).stream().filter(x -> !a.get(k).contains(x)).count();
            if (plus > 0) {
                out.put("+" + k, (int) plus);
            }
            if (minus > 0) {
                out.put("-" + k, (int) minus);
            }
        }
        return out;
    }

    private static Map<String, Set<List<String>>> state(Read g) {
        Set<List<String>> labels = new java.util.HashSet<>();
        Set<List<String>> props = new java.util.HashSet<>();
        g.nodes().forEach((id, n) -> {
            n.labels().forEach(l -> labels.add(List.of(l)));
            n.props().forEach((k, v) -> props.add(List.of("n", id, k, CypherValues.text(v))));
        });
        g.rels().forEach((id, r) -> r.rel().props().forEach((k, v) -> props.add(List.of("r", id, k,
                CypherValues.text(v)))));
        Set<List<String>> nodes = new java.util.HashSet<>();
        g.nodes().keySet().forEach(id -> nodes.add(List.of(id)));
        Set<List<String>> rels = new java.util.HashSet<>();
        g.rels().keySet().forEach(id -> rels.add(List.of(id)));
        return Map.of("nodes", nodes, "relationships", rels, "labels", labels, "properties", props);
    }

    /** A write's verdicts past its rows: the graph it left, read back through the island, against Neo4j's by shape;
     *  its side effects, lite's diff against Neo4j's; its control query's rows against Neo4j's. Null when all agree. */
    private static SqlParity.@com.legend.base.Nullable Outcome writeOutcome(Case c, CypherAnswers.Answer a,
            String model, Lane lane, Read before, boolean storeIds) {
        Read neoBefore = Read.of(java.util.Objects.requireNonNull(a.graph()));
        Read neoAfter = a.after() == null ? neoBefore : Read.of(a.after());
        Read after;
        try {
            after = readBack(model, lane, storeIds);
        } catch (Exception | StackOverflowError e) {
            return new SqlParity.Outcome(SqlParity.Status.ERROR, "harness: the graph after the write could not be"
                    + " read back through the island: " + oneLine(e));
        }
        if (!sameGraph(after, neoAfter)) {
            return new SqlParity.Outcome(SqlParity.Status.FAIL, "the graph after: Neo4j " + neoAfter.size()
                    + ", lite " + after.size() + "; Neo4j's alone " + alone(neoAfter, after) + ", lite's alone "
                    + alone(after, neoAfter));
        }
        Map<String, Integer> neo = sideEffects(neoBefore, neoAfter);
        Map<String, Integer> lite = sideEffects(before, after);
        if (!neo.equals(lite)) {
            return new SqlParity.Outcome(SqlParity.Status.FAIL, "side effects: Neo4j " + neo + ", lite " + lite);
        }
        return c.control() == null ? null : controlOutcome(c, java.util.Objects.requireNonNull(a.control(),
                "no snapped answer to the control query of " + c.key()), model, lane);
    }

    /** The control query's rows against Neo4j's (in order where it orders), or both raising; a refusal by name is
     *  the case's refusal. Null when they agree. */
    private static SqlParity.@com.legend.base.Nullable Outcome controlOutcome(Case c, CypherAnswers.Control control,
            String model, Lane lane) {
        ExecutionResult result;
        try {
            result = lane.run(model, c.controlIsland());
        } catch (Exception | StackOverflowError e) {
            if (control.error() != null) {
                return null;
            }
            boolean refusal = e instanceof com.legend.error.NotImplementedException;
            for (Throwable t = e; t != null; t = t.getCause()) {
                refusal |= t instanceof com.legend.sql.dialect.DialectCapability;
            }
            return new SqlParity.Outcome(refusal ? SqlParity.Status.REFUSED : SqlParity.Status.ERROR,
                    "the control query: " + oneLine(e));
        }
        if (control.error() != null) {
            return new SqlParity.Outcome(SqlParity.Status.BUG, "the control query: Neo4j rejects it ("
                    + control.error().code() + ") and legend-lite answered rows");
        }
        SqlParity.Grid actual = SqlParity.grid(result);
        if (!java.util.Objects.equals(control.columns(), actual.columns())) {
            return new SqlParity.Outcome(SqlParity.Status.FAIL, "the control query's columns: Neo4j "
                    + control.columns() + ", lite " + actual.columns());
        }
        List<List<CypherValues.Value>> expected = new ArrayList<>();
        for (List<String> row : java.util.Objects.requireNonNull(control.rows())) {
            expected.add(CypherValues.row(row));
        }
        List<List<CypherValues.Value>> got = new ArrayList<>();
        for (var row : result.rows()) {
            List<CypherValues.Value> cells = new ArrayList<>();
            for (Object cell : row.values()) {
                cells.add(CypherValues.ofCell(cell));
            }
            got.add(cells);
        }
        boolean ordered = java.util.Objects.requireNonNull(c.control()).toUpperCase(java.util.Locale.ROOT)
                .contains("ORDER BY");
        return CypherValues.sameRows(expected, got, ordered, false) ? null : new SqlParity.Outcome(
                SqlParity.Status.FAIL, "the control query: Neo4j " + expected.size() + " rows " + firstRows(expected)
                        + ", lite " + got.size() + " rows " + firstRows(got));
    }

    /**
     * The write cases by what a graph over a domain model (graph A, the writes design's §3) needs to hold them: the
     * graph after the write inside the schema the harness infers from the graph before it (the two graphs together
     * imply the same model), outside it (a label, relationship type or property the graph before lacks, a value of
     * another type, more relationships than a storage chosen from the graph before holds), or a node's labels
     * changed (on A a node's label is its class), or a graph the harness cannot model at all. Counted over the cases
     * Neo4j's summary marks as writing (the writes design's §8, leg 1).
     */
    public static Map<String, Integer> writeGroups(List<Case> cases, Map<String, CypherAnswers.Answer> answers) {
        Map<String, Integer> out = new java.util.TreeMap<>();
        for (Case c : cases) {
            CypherAnswers.Answer a = answers.get(c.key());
            if (a == null || a.after() == null || a.graph() == null) {
                continue;
            }
            out.merge(writeGroup(a.graph(), a.after()), 1, Integer::sum);
        }
        return out;
    }

    static String writeGroup(CypherAnswers.Graph before, CypherAnswers.Graph after) {
        Map<Integer, List<String>> labels = new java.util.HashMap<>();
        before.nodes().forEach(n -> labels.put(n.id(), n.labels()));
        for (CypherAnswers.GraphNode n : after.nodes()) {
            if (labels.containsKey(n.id()) && !labels.get(n.id()).equals(n.labels())) {
                return "a node's labels changed";
            }
        }
        // the two graphs as one (the graph after renumbered past the graph before): the schema that holds both
        int offset = 1 + Math.max(before.nodes().stream().mapToInt(CypherAnswers.GraphNode::id).max().orElse(0),
                after.nodes().stream().mapToInt(CypherAnswers.GraphNode::id).max().orElse(0));
        int relOffset = 1 + Math.max(before.rels().stream().mapToInt(CypherAnswers.GraphRel::id).max().orElse(0),
                after.rels().stream().mapToInt(CypherAnswers.GraphRel::id).max().orElse(0));
        List<CypherAnswers.GraphNode> nodes = new ArrayList<>(before.nodes());
        after.nodes().forEach(n -> nodes.add(new CypherAnswers.GraphNode(n.id() + offset, n.labels(), n.props())));
        List<CypherAnswers.GraphRel> rels = new ArrayList<>(before.rels());
        after.rels().forEach(r -> rels.add(new CypherAnswers.GraphRel(r.id() + relOffset, r.start() + offset,
                r.end() + offset, r.type(), r.props())));
        try {
            String model = CypherGraph.infer(before).model();
            return CypherGraph.infer(new CypherAnswers.Graph(nodes, rels)).model().equals(model)
                    ? "inside the graph before's schema" : "outside the graph before's schema";
        } catch (CypherGraph.Unsupported u) {
            return "a graph the harness cannot model";
        }
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

    /** The native store's ledger (cypher-parity/native-ledger.tsv): each case's status over the native store, a
     *  lane of its own beside the domain lanes (the writes design's §3 B). */
    public static List<String> nativeRatchet(List<Case> cases, Map<String, SqlParity.Outcome> outcomes, Path ledger,
            @com.legend.base.Nullable Path update) throws IOException {
        return SqlParity.ratchet("the Cypher parity corpus over the native store", "cypher-parity",
                NATIVE_LEDGER_HEADER, cases.stream().map(Case::asSql).toList(), outcomes, ledger, List.of("native"),
                "native", update);
    }

    private static final String NATIVE_LEDGER_HEADER = "# the Cypher parity corpus over the native store (the"
            + " writes design's graph B: nodes, node_props, rels, rel_props, the model's tables views over them),\n#"
            + " each case's status: the ratchet CypherParityNativeTest holds (core/src/test/java/com/legend/testcases/"
            + "CypherParity.java; the statuses are SqlParity's)\n";

    public static List<String> ratchet(List<Case> cases, Map<String, SqlParity.Outcome> outcomes, Path ledger,
            String lane, @com.legend.base.Nullable Path update) throws IOException {
        return SqlParity.ratchet("the Cypher parity corpus (the openCypher TCK and lite's cases)", "cypher-parity",
                LEDGER_HEADER, cases.stream().map(Case::asSql).toList(), outcomes, ledger, lane, update);
    }
}
