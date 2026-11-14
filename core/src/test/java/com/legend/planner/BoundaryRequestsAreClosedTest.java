// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

/**
 * EVERY REQUEST DOOR CLOSES EVERY OBJECT IT READS (the next PR's item 1, "separation made foolproof", the orchestrator's
 * go of 2026-11-13: an unknown key is refused by name, G7, so no field can ride in a request unread). For each door of
 * {@code planner.Boundary} that takes a request, a request it plans, and then the same request with one key it has not
 * ({@code extra}) put in each of its JSON objects in turn -- the request, each nested object, each element of each
 * array of objects -- each refused with that object's name and the key. Until 2026-11-13 fileLoad, databaseFromCatalog,
 * tableModel (its tables and their catalog rows, which outputPlan's tables and target are) and every host operation
 * read what they knew and dropped the rest.
 */
class BoundaryRequestsAreClosedTest {

    private static final String UNKNOWN = "extra";
    private static final String REFUSED = " has no key " + UNKNOWN + " (names and fields only, never a value)";

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

    /** Each door's requests: a name, the door, a request it plans. */
    private record Case(String name, Function<String, String> door, Object request) {
    }

    private static List<Case> cases() {
        String digest = "ab".repeat(32);
        String lambda = com.legend.parser.SpecParser.lambdaJson("|#>{x::DB.people}#->select(~[name])", false);
        String rows = com.legend.parser.SpecParser.lambdaJson("|#>{local::DB.people}#->filter(r|$r.n > 1)", false);
        Map<String, Object> peopleTable = obj("table", "people", "convertible", true, "columns", people());
        return List.of(
                new Case("fileLoad, a table of its own", planner.Boundary::fileLoad, obj("databaseType", "DuckDB",
                        "file", obj("name", "people.csv", "digest", digest, "size", 10, "format", "csv"),
                        "into", obj("table", "people"))),
                new Case("fileLoad, a declared table", planner.Boundary::fileLoad, obj("databaseType", "DuckDB",
                        "file", obj("name", "people.csv", "digest", digest, "size", 10, "format", "csv"),
                        "into", obj("model", TABLES, "database", "x::DB", "schema", "default", "table", "people"))),
                new Case("databaseFromCatalog", planner.Boundary::databaseFromCatalog, obj("path", "x::DB",
                        "table", "people", "convertible", false, "databaseType", "DuckDB", "columns", people())),
                new Case("tableModel, one table", planner.Boundary::tableModel, obj("table", "people",
                        "convertible", false, "databaseType", "DuckDB", "columns", people())),
                new Case("tableModel, several tables", planner.Boundary::tableModel, obj("databaseType", "DuckDB",
                        "tables", List.of(peopleTable, obj("table", "more", "pkg", "other", "convertible", false,
                                "columns", people())))),
                new Case("outputPlan, a made table", planner.Boundary::outputPlan, obj("databaseType", "DuckDB",
                        "tables", List.of(peopleTable), "output", obj("name", "out", "mode", "create"),
                        "rows", rows)),
                new Case("outputPlan, a written table", planner.Boundary::outputPlan, obj("databaseType", "DuckDB",
                        "tables", List.of(peopleTable), "output", obj("name", "out", "mode", "append"),
                        "rows", rows, "target", people())),
                new Case("hostOperations", ops -> planner.Boundary.hostOperations("DuckDB", ops), List.of(
                        obj("op", "drop", "kind", "table", "target", obj("name", "t", "declared", false)),
                        obj("op", "relation", "kind", "table", "target", obj("name", "c"), "replace", true,
                                "from", obj("relation", obj("schema", "main", "name", "t")),
                                "convert", List.of(column("a", "INTEGER"))),
                        obj("op", "schema", "name", "s"),
                        obj("op", "count", "from", obj("relation", obj("name", "t"))),
                        obj("op", "transaction", "step", "begin"),
                        obj("op", "tablesIn", "schema", "main"))),
                new Case("remoteMount", planner.Boundary::remoteMount, obj("databaseType", "DuckDB",
                        "location", obj("url", "https://example.com/a.parquet"), "format", "parquet",
                        "credentials", obj("name", "s3", "fields", List.of("KEY_ID", "SECRET")))),
                new Case("remoteMount, a bearer URL", planner.Boundary::remoteMount, obj("databaseType", "DuckDB",
                        "location", obj("bearer", "signed", "shown", "https://example.com/a.parquet"),
                        "format", "parquet")),
                new Case("snapCopy", planner.Boundary::snapCopy, obj("databaseType", "DuckDB", "model", TABLES,
                        "runtime", "x::RT", "lambda", lambda, "table", "copy")),
                new Case("frameLoad", planner.Boundary::frameLoad, obj("databaseType", "DuckDB", "held", "trades",
                        "as", "__legend_lite_trades", "live", true, "schema", "main", "relation", "trades")),
                new Case("authored", planner.Boundary::authored, obj("databaseType", "DuckDB", "text", "SELECT 1")));
    }

    /** Every JSON object of {@code node}, by its path from {@code at}. */
    private static void objects(Object node, String at, List<String> out) {
        if (node instanceof Map<?, ?> m) {
            out.add(at);
            m.forEach((k, v) -> objects(v, at + "." + k, out));
        } else if (node instanceof List<?> l) {
            for (int i = 0; i < l.size(); i++) {
                objects(l.get(i), at + "[" + i + "]", out);
            }
        }
    }

    /** {@code node} copied, the object at {@code target} given the key it has not. */
    private static Object withUnknownKey(Object node, String at, String target) {
        if (node instanceof Map<?, ?> m) {
            Map<String, Object> copy = new LinkedHashMap<>();
            m.forEach((k, v) -> copy.put((String) k, withUnknownKey(v, at + "." + k, target)));
            if (at.equals(target)) {
                copy.put(UNKNOWN, 1);
            }
            return copy;
        }
        if (node instanceof List<?> l) {
            List<Object> copy = new ArrayList<>();
            for (int i = 0; i < l.size(); i++) {
                copy.add(withUnknownKey(l.get(i), at + "[" + i + "]", target));
            }
            return copy;
        }
        return node;
    }

    @Test
    void everyRequestDoorPlansItsRequestAndRefusesAnUnknownKeyInEachOfItsObjectsByName() {
        List<String> wrong = new ArrayList<>();
        int refused = 0;
        for (Case c : cases()) {
            String json = com.legend.json.Json.toCompact(c.request());
            try {
                assertNotNull(c.door().apply(json), c.name());
            } catch (RuntimeException e) {
                wrong.add(c.name() + ": its request is refused: " + e.getMessage());
                continue;
            }
            List<String> paths = new ArrayList<>();
            objects(c.request(), "$", paths);
            for (String path : paths) {
                String extra = com.legend.json.Json.toCompact(withUnknownKey(c.request(), "$", path));
                try {
                    String answered = c.door().apply(extra);
                    wrong.add(c.name() + " at " + path + ": an unknown key is read past (" + answered.length()
                            + " characters answered)");
                } catch (IllegalArgumentException e) {
                    if (e.getMessage() == null || !e.getMessage().endsWith(REFUSED)) {
                        wrong.add(c.name() + " at " + path + ": refused, but not by its name: " + e.getMessage());
                    } else {
                        refused++;
                    }
                } catch (RuntimeException e) {
                    wrong.add(c.name() + " at " + path + ": " + e);
                }
            }
        }
        assertEquals(List.of(), wrong);
        // the count pins the walk itself: an object dropped from a case would shrink it
        assertEquals(52, refused, "the objects walked");
    }
}
