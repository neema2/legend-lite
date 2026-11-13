// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.json.Json;
import com.legend.model.ConnectionDefinition.DatabaseType;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

/**
 * INJECTION FUZZING OF THE PLANNER'S REQUEST FIELDS (the next PR's item 3, "separation made foolproof", its second
 * half, 2026-11-13). Every request door of {@code planner.Boundary} -- the hosts' loads, mounts, snapshots, outputs,
 * synthesized tables, frames, catalogs, table models and named operations -- is given a request it plans, and then the
 * same request with each payload of the injection fuzz ({@link InjectionFuzzTest#PAYLOADS}) in each of its text fields
 * in turn (a table's, a schema's, a column's, a view's, a file's or a frame's name, a URL, a key, a value), on every
 * relational database. Each answer is refused by name, or plans statements the oracle reads by that database's own
 * lexical rules ({@link InjectionFuzzTest#finding}): the marker inside a literal or a quoted identifier, nothing left
 * open. A table model's answer is a model: it is compiled and its table read through its accessor, and the statement
 * read the same way, each of its columns named exactly as the request named it.
 *
 * <p>The findings are pinned by door and field (KNOWN, shrink-only): each is a defect named until it is fixed, and a new
 * one fails the test.
 */
class BoundaryRequestFieldsFuzzTest {

    private static final String MARK = InjectionFuzzTest.MARK;

    /** The defects found, by database and door's field: shrink-only, each named until fixed. Empty since 2026-11-13: its
     *  first finding (a column whose name holds a line break, declared in a schema, declared under another name:
     *  CatalogModel.text indented a schema's blocks by replacing each line break of their text) went with the user's
     *  ruling (a), each block written at its indent as it is built. */
    static final Map<String, Set<String>> KNOWN = Map.of();

    private static final String TABLES = """
            ###Relational
            Database x::DB ( Table people ( name VARCHAR(10), n INTEGER ) )
            ###Connection
            RelationalDatabaseConnection x::Conn { type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime x::RT { mappings: []; connections: [ x::DB: [ c1: x::Conn ] ]; }
            """;

    private static Map<String, Object> obj(Object... kv) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            out.put((String) kv[i], kv[i + 1]);
        }
        return out;
    }

    private static Map<String, Object> column(String name, String dataType) {
        return obj("name", name, "dataType", dataType, "logicalType", dataType, "notNull", false);
    }

    private static List<Object> people() {
        return List.of(column("name", "VARCHAR"), column("n", "INTEGER"));
    }

    /** What a door's answer is: a plan or statements, or a table model to compile and read. */
    enum Answer { STATEMENTS, TABLE_MODEL }

    /** A door's request: its name, the door (a request's text in, its answer out), a request it plans, and its
     *  answer's kind. Its {@code databaseType} is each database's in turn, where it names one. */
    private record Case(String name, Function<String, String> door, Map<String, Object> request, Answer answer) {
    }

    private static Case statements(String name, Function<String, String> door, Map<String, Object> request) {
        return new Case(name, door, request, Answer.STATEMENTS);
    }

    /** A host operation, its database the request's. */
    private static Case operation(Map<String, Object> op) {
        return statements("hostOperations " + op.get("op"), json -> {
            Map<String, Object> in = new LinkedHashMap<>(asMap(Json.parse(json)));
            String type = (String) in.remove("databaseType");
            return planner.Boundary.hostOperations(type, Json.toCompact(List.of(in)));
        }, with(obj("databaseType", "DuckDB"), op));
    }

    private static Map<String, Object> with(Map<String, Object> a, Map<String, Object> b) {
        Map<String, Object> out = new LinkedHashMap<>(a);
        out.putAll(b);
        return out;
    }

    private static List<Case> cases() {
        String digest = "ab".repeat(32);
        String lambda = com.legend.parser.SpecParser.lambdaJson("|#>{x::DB.people}#->select(~[name])", false);
        String rows = com.legend.parser.SpecParser.lambdaJson("|#>{local::DB.people}#->filter(r|$r.n > 1)", false);
        Map<String, Object> peopleTable = obj("table", "people", "convertible", true, "columns", people());
        Map<String, Object> file = obj("name", "people.csv", "digest", digest, "size", 10, "format", "csv");
        Map<String, Object> target = obj("database", "db", "schema", "main", "name", "t", "declared", false);
        Map<String, Object> source = obj("relation", obj("database", "db", "schema", "main", "name", "s"));
        List<Case> out = new ArrayList<>(List.of(
                statements("fileLoad, a table of its own", planner.Boundary::fileLoad, obj("databaseType", "DuckDB",
                        "file", file, "into", obj("table", "people"))),
                statements("fileLoad, a declared table", planner.Boundary::fileLoad, obj("databaseType", "DuckDB",
                        "file", file, "into", obj("model", TABLES, "database", "x::DB", "schema", "default",
                                "table", "people"))),
                statements("databaseFromCatalog", planner.Boundary::databaseFromCatalog, obj("path", "x::DB",
                        "schema", "main", "table", "people", "convertible", true, "databaseType", "DuckDB",
                        "columns", List.of(column("name", "VARCHAR"), column("n", "INTEGER"),
                                column("j", "JSON")))),
                new Case("tableModel, one table", planner.Boundary::tableModel, obj("table", "people", "schema",
                        "main", "pkg", "local", "convertible", true, "databaseType", "DuckDB", "columns",
                        List.of(column("name", "VARCHAR"), column("n", "INTEGER"), column("j", "JSON"))),
                        Answer.TABLE_MODEL),
                new Case("tableModel, several tables", planner.Boundary::tableModel, obj("databaseType", "DuckDB",
                        "connection", "host", "tables", List.of(peopleTable, obj("table", "more", "schema", "main",
                                "pkg", "other", "convertible", false, "columns", people()))), Answer.TABLE_MODEL),
                statements("outputPlan, a made table", planner.Boundary::outputPlan, obj("databaseType", "DuckDB",
                        "tables", List.of(peopleTable), "output", obj("name", "out", "mode", "create"),
                        "rows", rows)),
                statements("outputPlan, an appended table", planner.Boundary::outputPlan, obj("databaseType",
                        "DuckDB", "tables", List.of(peopleTable), "output", obj("name", "out", "mode", "append"),
                        "rows", rows, "target", people())),
                statements("outputPlan, a merged table", planner.Boundary::outputPlan, obj("databaseType", "DuckDB",
                        "tables", List.of(peopleTable), "output", obj("name", "out", "mode", "merge",
                                "keys", List.of("name")), "rows", rows, "target", people())),
                statements("remoteMount", planner.Boundary::remoteMount, obj("databaseType", "DuckDB",
                        "view", "remote", "location", obj("url", "https://example.com/a.parquet"),
                        "format", "parquet", "credentials", obj("name", "s3", "fields",
                                List.of("KEY_ID", "SECRET")))),
                statements("remoteMount, a bearer URL", planner.Boundary::remoteMount, obj("databaseType", "DuckDB",
                        "view", "remote", "location", obj("bearer", "signed", "shown",
                                "https://example.com/a.parquet"), "format", "csv")),
                statements("snapStream", planner.Boundary::snapStream, obj("databaseType", "DuckDB", "plane",
                        "warehouse", "model", TABLES, "runtime", "x::RT", "lambda", lambda, "schema", "snaps",
                        "table", "copy")),
                statements("snapCopy", planner.Boundary::snapCopy, obj("databaseType", "DuckDB", "model", TABLES,
                        "runtime", "x::RT", "lambda", lambda, "schema", "snaps", "table", "copy")),
                statements("synthesize", planner.Boundary::synthesize, obj("databaseType", "DuckDB", "model",
                        TABLES, "database", "x::DB", "schema", "default", "table", "people", "count", 3,
                        "columns", obj("name", obj("values", List.of("a", "b")), "n", obj("range",
                                List.of(1, 5))))),
                statements("frameLoad", planner.Boundary::frameLoad, obj("databaseType", "DuckDB", "held", "trades",
                        "as", "__legend_lite_trades", "live", true, "schema", "main", "relation", "trades")),
                statements("frameLoad, a table", planner.Boundary::frameLoad, obj("databaseType", "DuckDB",
                        "held", "trades", "as", "__legend_lite_trades", "live", false, "relation", "trades")),
                statements("catalogColumnsSql", json -> {
                    Json.Obj in = Json.parseObject(json);
                    return Json.toCompact(List.of(planner.Boundary.catalogColumnsSql(in.getString("schema"),
                            in.getString("table"))));
                }, obj("schema", "main", "table", "people"))));
        for (Map<String, Object> op : List.of(
                obj("op", "relation", "kind", "table", "target", target, "replace", true, "from", source,
                        "convert", List.of(column("a", "INTEGER"), column("j", "JSON"))),
                obj("op", "relation", "kind", "view", "target", target, "replace", false, "from", source),
                obj("op", "drop", "kind", "table", "target", target),
                obj("op", "drop", "kind", "view", "target", target),
                obj("op", "schema", "database", "db", "name", "s"),
                obj("op", "dropSchema", "database", "db", "name", "s"),
                obj("op", "count", "from", source),
                obj("op", "dropSecret", "name", "s3"),
                obj("op", "resetVariable", "held", "v"),
                obj("op", "extensions", "names", List.of("httpfs", "iceberg")),
                obj("op", "extensionRepository", "url", "https://extensions.example.com"),
                obj("op", "extensionDirectory", "folder", "/tmp/extensions"),
                obj("op", "fileSearchPath", "folder", "/tmp/files"),
                obj("op", "nameTaken", "name", "t"),
                obj("op", "relationExists", "name", target),
                obj("op", "tablesIn", "database", "db", "schema", "main"),
                obj("op", "schemaExists", "database", "db", "schema", "main"),
                obj("op", "icebergManifestLists", "url", "https://example.com/t"),
                obj("op", "icebergManifests", "lists", List.of("https://example.com/l1")),
                obj("op", "icebergDataFiles", "url", "https://example.com/m1"))) {
            out.add(operation(op));
        }
        return out;
    }

    // ---- the plantings

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asMap(Object node) {
        return (Map<String, Object>) toPlain(node);
    }

    /** A JSON node as plain maps, lists, texts, numbers and Booleans. */
    private static Object toPlain(Object node) {
        if (node instanceof Json.Obj o) {
            Map<String, Object> out = new LinkedHashMap<>();
            o.fields().forEach((k, v) -> out.put(k, toPlain(v)));
            return out;
        }
        if (node instanceof Json.Arr a) {
            List<Object> out = new ArrayList<>();
            a.items().forEach(n -> out.add(toPlain(n)));
            return out;
        }
        // a text as a text; a number, a Boolean or a null as its own node, written back as it was
        return node instanceof Json.Str s ? s.value() : node;
    }

    /** Every text field of {@code node} by its path, and every key of an object whose keys are names (a synthesized
     *  table's columns: {@code path#key}) -- but a database's type, a model's text and a lambda's JSON, which are not
     *  names or values (the database's type is each database's in turn; a model and a lambda are compiled). */
    private static void texts(Object node, String at, String key, List<String> out) {
        if (node instanceof Map<?, ?> m) {
            m.forEach((k, v) -> {
                if (at.endsWith(".columns") && v instanceof Map) {
                    out.add(at + "#" + k);
                }
                texts(v, at + "." + k, (String) k, out);
            });
        } else if (node instanceof List<?> l) {
            for (int i = 0; i < l.size(); i++) {
                texts(l.get(i), at + "[" + i + "]", key, out);
            }
        } else if (node instanceof String && !Set.of("databaseType", "model", "lambda", "rows").contains(key)) {
            out.add(at);
        }
    }

    /** {@code node} copied, the text at {@code target} (or the key, {@code path#key}) the payload. */
    private static Object planted(Object node, String at, String target, String payload) {
        if (node instanceof Map<?, ?> m) {
            Map<String, Object> copy = new LinkedHashMap<>();
            m.forEach((k, v) -> {
                String key = target.equals(at + "#" + k) ? payload : (String) k;
                copy.put(key, planted(v, at + "." + k, target, payload));
            });
            return copy;
        }
        if (node instanceof List<?> l) {
            List<Object> copy = new ArrayList<>();
            for (int i = 0; i < l.size(); i++) {
                copy.add(planted(l.get(i), at + "[" + i + "]", target, payload));
            }
            return copy;
        }
        return at.equals(target) ? payload : node;
    }

    // ---- the statements

    /** Every statement {@code answer} carries: a plan's, a host operation's lists, a conversion's SQL. */
    private static List<String> statementsOf(String answer) {
        List<String> out = new ArrayList<>(InjectionFuzzTest.statements(answer));
        Object parsed = toPlain(Json.parse(answer));
        if (parsed instanceof List<?> lists) {
            // a host operation's answer, [[statement, ...], ...], or the catalog question's [statement]
            for (Object l : lists) {
                if (l instanceof List<?> statements) {
                    statements.forEach(s -> out.add((String) s));
                } else if (l instanceof String s) {
                    out.add(s);
                }
            }
        }
        return out;
    }

    /** A table model compiled and each of its tables read through its accessor: the statements, and each table's
     *  columns as the plan names them. */
    private static List<String> readThrough(String answer, List<List<String>> columns) {
        Json.Obj a = Json.parseObject(answer);
        List<String> accessors = new ArrayList<>();
        if (a.has("accessor")) {
            accessors.add(a.getString("accessor"));
        } else {
            for (Json.Node t : a.getArr("tables").items()) {
                accessors.add(((Json.Obj) t).getString("accessor"));
            }
        }
        var ctx = com.legend.Compiler.compileModel(a.getString("model"));
        List<String> out = new ArrayList<>(statementsOf(answer));
        for (String accessor : accessors) {
            var plan = com.legend.Compiler.query(ctx, "|" + accessor).executionPlan(a.getString("runtime"));
            out.addAll(InjectionFuzzTest.statements(com.legend.executionplan.PlanJson.write(plan)));
            columns.add(((com.legend.executionplan.ExecutionPlan.TdsResult) plan.root()).columns().stream()
                    .map(com.legend.executionplan.ExecutionPlan.Column::name).toList());
        }
        return out;
    }

    /** The columns a table model's request names, table by table, less those its answer excludes. */
    private static List<List<String>> requested(Map<String, Object> request, String answer) {
        Json.Obj a = Json.parseObject(answer);
        List<Map<String, Object>> tables = new ArrayList<>();
        List<Set<String>> excluded = new ArrayList<>();
        if (request.containsKey("tables")) {
            for (Object t : (List<?>) request.get("tables")) {
                tables.add(asMap(Json.parse(Json.toCompact(t))));
            }
            for (Json.Node t : a.getArr("tables").items()) {
                excluded.add(Set.copyOf(((Json.Obj) t).getStringArray("excluded")));
            }
        } else {
            tables.add(request);
            excluded.add(Set.copyOf(a.getStringArray("excluded")));
        }
        List<List<String>> out = new ArrayList<>();
        for (int i = 0; i < tables.size(); i++) {
            List<String> names = new ArrayList<>();
            for (Object c : (List<?>) tables.get(i).get("columns")) {
                String name = (String) ((Map<?, ?>) c).get("name");
                if (!excluded.get(i).contains(name)) {
                    names.add(name);
                }
            }
            out.add(names);
        }
        return out;
    }

    // ---- the fuzz

    /** Every door's field where a payload changed a statement, by database: {@code cases} fuzzed, the counts of
     *  plantings in {@code counts}, one example per finding in {@code examples}. */
    static Map<String, Set<String>> fuzz(List<Case> cases, Map<String, Integer> counts, List<String> examples) {
        Map<String, Set<String>> found = new TreeMap<>();
        List<DatabaseType> types = com.legend.database.Databases.relational().stream()
                .filter(t -> !com.legend.database.Databases.dialect(t).getClass().getSimpleName().equals("MongoDb"))
                .toList();
        for (Case c : cases) {
            List<String> fields = new ArrayList<>();
            texts(c.request(), "$", null, fields);
            for (DatabaseType type : types) {
                Map<String, Object> base = new LinkedHashMap<>(c.request());
                if (base.containsKey("databaseType")) {
                    base.put("databaseType", type.name());
                } else if (type != com.legend.database.Databases.PLATFORM) {
                    // a door that names no database answers for the platform's
                    continue;
                }
                InjectionFuzzTest.Rules rules = InjectionFuzzTest.rules(type);
                try {
                    assertTrue(!c.door().apply(Json.toCompact(base)).isEmpty(), c.name());
                } catch (RuntimeException refusal) {
                    // a door that plans for one database alone refuses the others BY NAME (anything else is a finding)
                    if (!InjectionFuzzTest.namedRefusal(refusal)
                            && found.computeIfAbsent(type.name(), k -> new TreeSet<>()).add(c.name())) {
                        examples.add(type.name() + ", " + c.name() + ": " + InjectionFuzzTest.unnamed(refusal));
                    }
                    counts.merge("a database a door does not plan for", 1, Integer::sum);
                    continue;
                }
                for (String field : fields) {
                    for (String payload : InjectionFuzzTest.PAYLOADS) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> request = (Map<String, Object>) planted(base, "$", field, payload);
                        String json = Json.toCompact(request);
                        List<String> statements = List.of();
                        List<String> wrong = new ArrayList<>();
                        String place = c.name() + " " + field;
                        String answer;
                        try {
                            answer = c.door().apply(json);
                        } catch (RuntimeException refusal) {
                            // refused BY NAME, or a finding naming the payload, the place and the database (the C4
                            // audit's M3, 2026-11-13: until then any RuntimeException counted as refused)
                            if (!InjectionFuzzTest.namedRefusal(refusal)) {
                                String unnamed = place + " (" + InjectionFuzzTest.unnamed(refusal).split(":")[0] + ")";
                                if (found.computeIfAbsent(type.name(), k -> new TreeSet<>()).add(unnamed)) {
                                    examples.add(type.name() + ", " + place + ", " + payload.replace("\n", "\\n")
                                            + ": " + InjectionFuzzTest.unnamed(refusal));
                                }
                            }
                            counts.merge("refused", 1, Integer::sum);
                            continue;
                        }
                        counts.merge("planned", 1, Integer::sum);
                        counts.merge("planned on " + type.name(), 1, Integer::sum);
                        counts.merge("planned at " + place, 1, Integer::sum);
                        // the door's answer read OUTSIDE the refusal (the C4 audit's M3, 2026-11-13): a table model
                        // the door answered that does not compile, or whose table does not plan through its accessor,
                        // is a finding -- until then it counted as refused, so a broken model passed
                        try {
                            if (c.answer() == Answer.TABLE_MODEL) {
                                List<List<String>> columns = new ArrayList<>();
                                statements = readThrough(answer, columns);
                                List<List<String>> asked = requested(request, answer);
                                if (!columns.equals(asked)) {
                                    wrong.add("the model's columns " + columns + ", not the request's " + asked);
                                }
                            } else {
                                statements = statementsOf(answer);
                            }
                        } catch (RuntimeException broken) {
                            wrong.add("the door's answer does not read back (" + broken.getClass().getSimpleName()
                                    + ": " + String.valueOf(broken.getMessage()).lines().findFirst().orElse("") + ")");
                        }
                        if (statements.stream().anyMatch(s -> s.contains(MARK))) {
                            counts.merge("carried as data", 1, Integer::sum);
                        }
                        for (String statement : statements) {
                            String w = InjectionFuzzTest.finding(statement, rules, payload.equals(MARK));
                            if (w != null) {
                                wrong.add(w + ": " + statement.replace("\n", " "));
                            }
                        }
                        if (!wrong.isEmpty()) {
                            if (found.computeIfAbsent(type.name(), k -> new TreeSet<>()).add(place)) {
                                examples.add(type.name() + ", " + place + ", " + payload.replace("\n", "\\n") + ": "
                                        + wrong.get(0));
                            }
                        }
                    }
                }
            }
        }
        return found;
    }

    @Test
    void noRequestFieldChangesAStatementsStructureOnAnyDatabase() {
        Map<String, Integer> counts = new TreeMap<>();
        List<String> examples = new ArrayList<>();
        Map<String, Set<String>> found = fuzz(cases(), counts, examples);
        assertTrue(counts.getOrDefault("carried as data", 0) > 0, "no field reached a statement: " + counts);
        assertEquals(KNOWN, found, counts + "; one example each:\n" + String.join("\n", examples));
        // the coverage, pinned (the C4 audit's M3, 2026-11-13): the plantings each database and each door's field
        // PLANS, so a change that refuses a field's payloads wholesale -- and so finds nothing there -- fails here
        assertEquals(PLANNED_BY_DATABASE, within(counts, "planned on "), "the plantings planned, by database");
        assertEquals(PLANNED_BY_PLACE, within(counts, "planned at "), "the plantings planned, by door and field");
    }

    /** The counts under {@code prefix}, keyed by what follows it. */
    private static Map<String, Integer> within(Map<String, Integer> counts, String prefix) {
        Map<String, Integer> out = new TreeMap<>();
        counts.forEach((k, v) -> {
            if (k.startsWith(prefix)) {
                out.put(k.substring(prefix.length()), v);
            }
        });
        return out;
    }

    /** The plantings each database plans (measured 2026-11-13, the C4 audit's M3; a move is dated and names its task). */
    static final Map<String, Integer> PLANNED_BY_DATABASE = Map.of("DuckDB", 2475);

    /** The plantings each door's field plans, on every database (measured 2026-11-13, the C4 audit's M3). */
    static final Map<String, Integer> PLANNED_BY_PLACE = java.util.Map.ofEntries(
            Map.entry("catalogColumnsSql $.schema", 24),
            Map.entry("catalogColumnsSql $.table", 24),
            Map.entry("databaseFromCatalog $.columns[0].dataType", 24),
            Map.entry("databaseFromCatalog $.columns[0].name", 24),
            Map.entry("databaseFromCatalog $.columns[1].dataType", 24),
            Map.entry("databaseFromCatalog $.columns[1].name", 24),
            Map.entry("databaseFromCatalog $.columns[2].logicalType", 24),
            Map.entry("databaseFromCatalog $.columns[2].name", 24),
            Map.entry("databaseFromCatalog $.path", 23),
            Map.entry("databaseFromCatalog $.schema", 23),
            Map.entry("databaseFromCatalog $.table", 23),
            Map.entry("fileLoad, a declared table $.file.name", 14),
            Map.entry("fileLoad, a table of its own $.file.name", 14),
            Map.entry("fileLoad, a table of its own $.into.table", 24),
            Map.entry("frameLoad $.as", 1),
            Map.entry("frameLoad $.held", 1),
            Map.entry("frameLoad $.relation", 24),
            Map.entry("frameLoad $.schema", 24),
            Map.entry("frameLoad, a table $.as", 1),
            Map.entry("frameLoad, a table $.held", 1),
            Map.entry("frameLoad, a table $.relation", 24),
            Map.entry("hostOperations count $.from.relation.database", 24),
            Map.entry("hostOperations count $.from.relation.name", 24),
            Map.entry("hostOperations count $.from.relation.schema", 24),
            Map.entry("hostOperations drop $.target.database", 48),
            Map.entry("hostOperations drop $.target.name", 48),
            Map.entry("hostOperations drop $.target.schema", 48),
            Map.entry("hostOperations dropSchema $.database", 24),
            Map.entry("hostOperations dropSchema $.name", 24),
            Map.entry("hostOperations dropSecret $.name", 24),
            Map.entry("hostOperations extensionDirectory $.folder", 24),
            Map.entry("hostOperations extensionRepository $.url", 24),
            Map.entry("hostOperations fileSearchPath $.folder", 24),
            Map.entry("hostOperations icebergDataFiles $.url", 24),
            Map.entry("hostOperations icebergManifestLists $.url", 24),
            Map.entry("hostOperations icebergManifests $.lists[0]", 24),
            Map.entry("hostOperations nameTaken $.name", 24),
            Map.entry("hostOperations relation $.convert[0].dataType", 24),
            Map.entry("hostOperations relation $.convert[0].name", 24),
            Map.entry("hostOperations relation $.convert[1].logicalType", 24),
            Map.entry("hostOperations relation $.convert[1].name", 24),
            Map.entry("hostOperations relation $.from.relation.database", 48),
            Map.entry("hostOperations relation $.from.relation.name", 48),
            Map.entry("hostOperations relation $.from.relation.schema", 48),
            Map.entry("hostOperations relation $.target.database", 48),
            Map.entry("hostOperations relation $.target.name", 48),
            Map.entry("hostOperations relation $.target.schema", 48),
            Map.entry("hostOperations relationExists $.name.database", 24),
            Map.entry("hostOperations relationExists $.name.name", 24),
            Map.entry("hostOperations relationExists $.name.schema", 24),
            Map.entry("hostOperations resetVariable $.held", 1),
            Map.entry("hostOperations schema $.database", 24),
            Map.entry("hostOperations schema $.name", 24),
            Map.entry("hostOperations schemaExists $.database", 24),
            Map.entry("hostOperations schemaExists $.schema", 24),
            Map.entry("hostOperations tablesIn $.database", 24),
            Map.entry("hostOperations tablesIn $.schema", 24),
            Map.entry("outputPlan, a made table $.output.name", 24),
            Map.entry("outputPlan, a made table $.tables[0].columns[0].dataType", 24),
            Map.entry("outputPlan, a made table $.tables[0].columns[0].name", 24),
            Map.entry("outputPlan, a made table $.tables[0].columns[1].dataType", 24),
            Map.entry("outputPlan, a merged table $.output.name", 23),
            Map.entry("outputPlan, a merged table $.tables[0].columns[0].dataType", 24),
            Map.entry("outputPlan, a merged table $.tables[0].columns[1].dataType", 24),
            Map.entry("outputPlan, a merged table $.target[0].dataType", 24),
            Map.entry("outputPlan, a merged table $.target[1].dataType", 24),
            Map.entry("outputPlan, an appended table $.output.name", 23),
            Map.entry("outputPlan, an appended table $.tables[0].columns[0].dataType", 24),
            Map.entry("outputPlan, an appended table $.tables[0].columns[1].dataType", 24),
            Map.entry("outputPlan, an appended table $.target[0].dataType", 24),
            Map.entry("outputPlan, an appended table $.target[1].dataType", 24),
            Map.entry("remoteMount $.credentials.name", 1),
            Map.entry("remoteMount $.location.url", 23),
            Map.entry("remoteMount $.view", 24),
            Map.entry("remoteMount, a bearer URL $.location.bearer", 1),
            Map.entry("remoteMount, a bearer URL $.location.shown", 23),
            Map.entry("remoteMount, a bearer URL $.view", 24),
            Map.entry("snapCopy $.schema", 24),
            Map.entry("snapCopy $.table", 24),
            Map.entry("snapStream $.plane", 1),
            Map.entry("snapStream $.schema", 24),
            Map.entry("snapStream $.table", 24),
            Map.entry("synthesize $.columns.name.values[0]", 24),
            Map.entry("synthesize $.columns.name.values[1]", 24),
            Map.entry("tableModel, one table $.columns[0].dataType", 24),
            Map.entry("tableModel, one table $.columns[0].name", 24),
            Map.entry("tableModel, one table $.columns[1].dataType", 24),
            Map.entry("tableModel, one table $.columns[1].name", 24),
            Map.entry("tableModel, one table $.columns[2].logicalType", 24),
            Map.entry("tableModel, one table $.columns[2].name", 24),
            Map.entry("tableModel, one table $.pkg", 1),
            Map.entry("tableModel, one table $.schema", 23),
            Map.entry("tableModel, one table $.table", 23),
            Map.entry("tableModel, several tables $.connection", 1),
            Map.entry("tableModel, several tables $.tables[0].columns[0].dataType", 24),
            Map.entry("tableModel, several tables $.tables[0].columns[0].name", 24),
            Map.entry("tableModel, several tables $.tables[0].columns[1].dataType", 24),
            Map.entry("tableModel, several tables $.tables[0].columns[1].name", 24),
            Map.entry("tableModel, several tables $.tables[0].table", 23),
            Map.entry("tableModel, several tables $.tables[1].columns[0].dataType", 24),
            Map.entry("tableModel, several tables $.tables[1].columns[0].name", 24),
            Map.entry("tableModel, several tables $.tables[1].columns[1].dataType", 24),
            Map.entry("tableModel, several tables $.tables[1].columns[1].name", 24),
            Map.entry("tableModel, several tables $.tables[1].pkg", 1),
            Map.entry("tableModel, several tables $.tables[1].schema", 23),
            Map.entry("tableModel, several tables $.tables[1].table", 23));

    /** The fuzz bites: a door planted to write its table's name into its statement as given, and its view's name
     *  doubled-quoted as ANSI writes one (no backslash escaped), is named at both fields -- on DuckDB the table's name
     *  opens a string or a comment, and the view's name holds nothing open (DuckDB reads no backslash in an
     *  identifier), so only the table's is a finding. */
    @Test
    void theFuzzNamesAPlantedDoorThatSplicesAName() {
        Case planted = statements("planted", json -> {
            Json.Obj in = Json.parseObject(json);
            if (!"DuckDB".equals(in.getString("databaseType"))) {
                throw new IllegalArgumentException("the planted door plans for DuckDB alone");
            }
            return Json.toCompact(List.of(List.of("CREATE VIEW \"" + in.getString("view").replace("\"", "\"\"")
                    + "\" AS SELECT * FROM " + in.getString("table"))));
        }, obj("databaseType", "DuckDB", "view", "v", "table", "t"));
        Map<String, Integer> counts = new TreeMap<>();
        assertEquals(Map.of("DuckDB", Set.of("planted $.table")), fuzz(List.of(planted), counts, new ArrayList<>()),
                counts.toString());
    }

    /** A host operation no platform names -- none declares its keys -- is refused by name, never read unclosed (the C4
     *  audit's M3, 2026-11-13: until then it was passed over, and its arm read whatever it was handed); one the platform
     *  refuses by its own words (a secret) keeps its words. */
    @Test
    void aHostOperationNoPlatformNamesIsRefusedByName() {
        IllegalArgumentException e = org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> planner.Boundary.hostOperations("DuckDB", Json.toCompact(List.of(obj("op", "frobnicate")))));
        assertTrue(e.getMessage().contains("a host's operation is not one this platform names (none declares its keys):"
                + " frobnicate"), e.getMessage());
        RuntimeException secret = org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class,
                () -> planner.Boundary.hostOperations("DuckDB", Json.toCompact(List.of(obj("op", "secret")))));
        assertTrue(!secret.getMessage().contains("none declares its keys"), secret.getMessage());
    }

    /** A table model the door answers is read OUTSIDE the refusal (the C4 audit's M3, 2026-11-13): a door planted to
     *  answer the real door's model with its table's name written into the model's text as given -- a model that no
     *  longer compiles once the name holds a quote -- is a finding at that field, never a refusal; and a door planted
     *  to fail with a NullPointerException is a finding too, never a refusal (lite's named refusals alone count). */
    @Test
    void theFuzzNamesABrokenModelAndAnUnnamedRefusal() {
        Map<String, Object> request = obj("table", "people", "schema", "main", "pkg", "x", "databaseType", "DuckDB",
                "convertible", false, "columns", people());
        Case broken = new Case("brokenModel", json -> {
            Map<String, Object> in = new LinkedHashMap<>(asMap(Json.parse(json)));
            String table = (String) in.put("table", "people");
            Map<String, Object> out = asMap(Json.parse(planner.Boundary.tableModel(Json.toCompact(in))));
            out.put("model", ((String) out.get("model")).replace("people", table));
            return Json.toCompact(out);
        }, request, Answer.TABLE_MODEL);
        Map<String, Integer> counts = new TreeMap<>();
        List<String> examples = new ArrayList<>();
        Map<String, Set<String>> found = fuzz(List.of(broken), counts, examples);
        assertTrue(found.getOrDefault("DuckDB", Set.of()).contains("brokenModel $.table"), found + " " + examples);
        assertTrue(examples.stream().anyMatch(e -> e.contains("does not read back")), String.join("\n", examples));
        Case unnamed = statements("unnamed", json -> {
            if (json.contains(MARK)) {
                throw new NullPointerException("planted");
            }
            return Json.toCompact(List.of(List.of("SELECT 1")));
        }, obj("view", "v"));
        examples.clear();
        assertEquals(Map.of(com.legend.database.Databases.PLATFORM.name(), Set.of("unnamed $.view (refused, but not by name)")),
                fuzz(List.of(unnamed), new TreeMap<>(), examples), String.join("\n", examples));
    }
}
