// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.Compiler;
import com.legend.TypedQuery;
import com.legend.executionplan.ExecutionPlan;
import com.legend.testing.EmbeddedMongoDb;
import com.mongodb.client.MongoClient;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.StringWriter;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * v0 end to end (docs/MONGODB_PLAN_2026_10_16.md §11, 2026-10-17): a model with a {@code ###MongoDB} store and a
 * {@code MongoDBConnection}, planned by the one compiler into an aggregate command and run by the plan runner through
 * the MongoDB connector on a real MongoDB 8.0 ({@code EmbeddedMongoDb}). The documents are the test's own, written
 * through the driver; the platform reads them through the store's declared schema alone.
 */
class MongoDbPlanTest {

    private static final String PASSWORD_PROPERTY = "legend.mongodb.pct.password";

    private static String model;

    @BeforeAll
    static void documents() {
        EmbeddedMongoDb db = EmbeddedMongoDb.shared();
        System.setProperty(PASSWORD_PROPERTY, EmbeddedMongoDb.PASSWORD);
        try (MongoClient client = db.client()) {
            var people = client.getDatabase(EmbeddedMongoDb.DATABASE).getCollection("people");
            people.drop();
            people.insertMany(List.of(
                    new Document("name", "Ann").append("age", 34L).append("city", "Oslo"),
                    new Document("name", "Bob").append("age", 28L).append("city", "Oslo"),
                    new Document("name", "Cy").append("age", 41L).append("city", "Rome"),
                    new Document("name", "Di").append("city", "Rome")));
        }
        model = String.format(java.util.Locale.ROOT, """
                ###MongoDB
                Database test::Store
                (
                  Collection people
                  (
                    jsonSchema: {
                      "bsonType": "object",
                      "properties": {
                        "name": { "bsonType": "string" },
                        "age": { "bsonType": "long" },
                        "city": { "bsonType": "string" }
                      },
                      "required": ["name"]
                    };
                  )
                  Collection big
                  (
                    jsonSchema: {
                      "bsonType": "object",
                      "properties": {
                        "n": { "bsonType": "long" },
                        "s": { "bsonType": "string" }
                      }
                    };
                  )
                )
                ###Connection
                MongoDBConnection test::Conn
                {
                  database: %s;
                  store: test::Store;
                  serverURLs: [127.0.0.1:%d];
                  authentication: # UserPassword {
                    username: '%s';
                    password: SystemPropertiesSecret
                    {
                      systemPropertyName: '%s';
                    };
                  }#;
                }
                ###Runtime
                Runtime test::RT
                {
                  mappings: [];
                  connections: [ test::Store: [ session: test::Conn ] ];
                }
                """, EmbeddedMongoDb.DATABASE, db.port(), EmbeddedMongoDb.USER, PASSWORD_PROPERTY);
    }

    private static String run(String query, TypedQuery.Output output, Map<String, ?> values) throws Exception {
        ExecutionPlan plan = Compiler.query(Compiler.compileModel(model), query).executionPlan("test::RT", output);
        com.legend.testcases.PlanRoundTrip.assertRoundTrips(plan, query);
        StringWriter out = new StringWriter();
        PlanRunner.run(plan, values, PlanSessions.shared(), out);
        return out.toString();
    }

    @Test
    void aFilteredSortedSelectStreamsItsRows() throws Exception {
        assertEquals("[{\"name\":\"Ann\",\"age\":34},{\"name\":\"Cy\",\"age\":41}]",
                run("#>{test::Store.people}#->filter(x | $x.age > 30)->select(~[name, age])->sort(~name->ascending())",
                        TypedQuery.Output.STREAMED_JSON, Map.of()));
    }

    @Test
    void anAbsentFieldReadsAsNull() throws Exception {
        assertEquals("[{\"name\":\"Di\",\"age\":null}]",
                run("#>{test::Store.people}#->filter(x | $x.age->isEmpty())->select(~[name, age])",
                        TypedQuery.Output.STREAMED_JSON, Map.of()));
    }

    @Test
    void aParameterIsBoundInPlace() throws Exception {
        assertEquals("[{\"name\":\"Cy\"}]",
                run("{n: Integer[1] | #>{test::Store.people}#->filter(x | $x.age > $n)->select(~[name])}",
                        TypedQuery.Output.STREAMED_JSON, Map.of("n", 40L)));
    }

    @Test
    void aGroupCountsAndSorts() throws Exception {
        assertEquals("[{\"city\":\"Oslo\",\"n\":2},{\"city\":\"Rome\",\"n\":2}]",
                run("#>{test::Store.people}#->groupBy(~[city], ~[n: x | $x.name : y | $y->count()])"
                        + "->sort(~city->ascending())", TypedQuery.Output.STREAMED_JSON, Map.of()));
    }

    @Test
    void theWholeResultAsJsonAndAsCsv() throws Exception {
        String json = run("#>{test::Store.people}#->filter(x | $x.city == 'Oslo')->select(~[name, age])"
                + "->sort(~age->descending())", TypedQuery.Output.JSON, Map.of());
        assertTrue(json.contains("\"name\":\"Ann\"") && json.contains("\"name\":\"Bob\""), json);
        String csv = run("#>{test::Store.people}#->filter(x | $x.city == 'Oslo')->select(~[name, age])"
                + "->sort(~age->descending())", TypedQuery.Output.CSV, Map.of());
        assertTrue(csv.startsWith("name,age") && csv.contains("Ann,34") && csv.contains("Bob,28"), csv);
    }

    /**
     * Homework 4 (docs/MONGODB_PLAN_2026_10_16.md §9, 2026-10-17): the whole-text wires (CSV, the whole JSON) gather
     * every row into one string inside one document, which MongoDB caps at 16 MB; the streamed JSON wire does not.
     * Two facts measured here, each printed: (1) the string aggregation the wires use on MongoDB is a {@code $reduce}
     * with {@code $concat}, which copies the whole accumulated text at every step -- quadratic in the row count (the
     * first draft, 240,000 rows of 100 characters, ran past the test's 300 s); so the rows here are FEW and LARGE
     * (100 KB each): 100 rows, about 10 MB, pass; 200 rows, about 20 MB, meet the bound, and the error's text is
     * recorded. (2) The streamed JSON wire reads the same collections row by row.
     */
    @Test
    void theWholeTextWiresAreBoundedBySixteenMegabytes() throws Exception {
        String filler = "x".repeat(100_000);
        loadBig(100, filler);
        long t0 = System.nanoTime();
        String small = run("#>{test::Store.big}#->select(~[n, s])", TypedQuery.Output.CSV, Map.of());
        System.out.println("[mongodb-16mb] 100 rows x 100 KB: the CSV wire passed, " + small.length()
                + " characters in " + (System.nanoTime() - t0) / 1_000_000 + " ms");
        assertTrue(small.length() > 10_000_000 && small.startsWith("n,s"), "the 10 MB CSV: " + small.length());
        assertEquals("[{\"n\":99}]", run("#>{test::Store.big}#->select(~n)->sort(~n->descending())->limit(1)",
                TypedQuery.Output.STREAMED_JSON, Map.of()));
        loadBig(200, filler);
        t0 = System.nanoTime();
        try {
            String large = run("#>{test::Store.big}#->select(~[n, s])", TypedQuery.Output.CSV, Map.of());
            System.out.println("[mongodb-16mb] 200 rows x 100 KB: the CSV wire PASSED with " + large.length()
                    + " characters in " + (System.nanoTime() - t0) / 1_000_000 + " ms");
        } catch (com.legend.error.DataError e) {
            System.out.println("[mongodb-16mb] 200 rows x 100 KB: the CSV wire failed after "
                    + (System.nanoTime() - t0) / 1_000_000 + " ms: " + e.getMessage());
            assertTrue(e.getMessage() != null && !e.getMessage().isEmpty(), "an error with no text");
        }
        assertEquals("[{\"n\":199}]", run("#>{test::Store.big}#->select(~n)->sort(~n->descending())->limit(1)",
                TypedQuery.Output.STREAMED_JSON, Map.of()));
        // the quadratic cost, measured on small rows: 2,000 and 8,000 rows of 100 characters
        for (int rows : new int[] {2_000, 8_000}) {
            loadBig(rows, "x".repeat(100));
            t0 = System.nanoTime();
            String csv = run("#>{test::Store.big}#->select(~[n, s])", TypedQuery.Output.CSV, Map.of());
            System.out.println("[mongodb-16mb] " + rows + " rows x 100 B: the CSV wire passed, " + csv.length()
                    + " characters in " + (System.nanoTime() - t0) / 1_000_000 + " ms");
        }
    }

    private static void loadBig(int rows, String filler) {
        try (MongoClient client = EmbeddedMongoDb.shared().client()) {
            var big = client.getDatabase(EmbeddedMongoDb.DATABASE).getCollection("big");
            big.drop();
            List<Document> batch = new java.util.ArrayList<>();
            for (int i = 0; i < rows; i++) {
                batch.add(new Document("n", (long) i).append("s", filler));
                if (batch.size() == 50) {
                    big.insertMany(batch);
                    batch.clear();
                }
            }
            if (!batch.isEmpty()) {
                big.insertMany(batch);
            }
        }
    }

    @Test
    void aStorelessExpressionRunsOnTheDatabase() throws Exception {
        assertTrue(run("1 + 2", TypedQuery.Output.JSON, Map.of()).contains("3"));
    }

    /**
     * The server's texts of a number (docs/MONGODB_PLAN_2026_10_16.md §9 item 4, measured 2026-10-17): what
     * {@code $toString} and a Decimal128 spell, raw, and what the target's spelling makes of them -- each printed, and
     * the three the differential turns on asserted (a Float's text with its fraction digit, a big double in plain
     * digits, a small one in fixed point).
     */
    @Test
    void theServersNumberTexts() {
        EmbeddedMongoDb db = EmbeddedMongoDb.shared();
        try (MongoClient client = db.client()) {
            var database = client.getDatabase(EmbeddedMongoDb.DATABASE);
            Map<String, Object> raw = new java.util.LinkedHashMap<>();
            raw.put("$toString 1e20", new Document("$toString", 1e20));
            raw.put("$toString 2.0", new Document("$toString", 2.0));
            raw.put("$toString 1e15", new Document("$toString", 1e15));
            raw.put("$toString 1e16", new Document("$toString", 1e16));
            raw.put("$toString 1.5e-8", new Document("$toString", 1.5e-8));
            raw.put("$toString 0.0001", new Document("$toString", 0.0001));
            raw.put("$toString 0.1+0.2", new Document("$toString", new Document("$add", List.of(0.1, 0.2))));
            raw.put("$toString decimal(1e20)", new Document("$toString",
                    new Document("$convert", new Document("input", 1e20).append("to", "decimal"))));
            raw.put("$toString decimal(1.5e-8) at 18", new Document("$toString", new Document("$round", List.of(
                    new Document("$convert", new Document("input", 1.5e-8).append("to", "decimal")), 18))));
            raw.put("$toString decimal(2.0)", new Document("$toString",
                    new Document("$convert", new Document("input", 2.0).append("to", "decimal"))));
            raw.put("$type 1e20", new Document("$type", 1e20));
            raw.put("long of '+20'", new Document("$convert", new Document("input", "+20").append("to", "long")));
            for (var e : raw.entrySet()) {
                Object r;
                try {
                    r = first(database.runCommand(new Document("aggregate", 1).append("pipeline", List.of(
                            new Document("$documents", List.of(new Document())),
                            new Document("$replaceWith", new Document("r", e.getValue()))))
                            .append("cursor", new Document())));
                } catch (RuntimeException x) {
                    r = "ERROR " + x.getMessage();
                }
                System.out.println("[mongodb-number-text] raw " + e.getKey() + " -> " + r);
            }
            com.legend.sql.dialect.SqlDialect target = new com.legend.sql.dialect.MongoDb();
            Map<String, com.legend.sql.SqlExpr> spelled = new java.util.LinkedHashMap<>();
            com.legend.sql.SqlExpr big = new com.legend.sql.SqlExpr.FloatLit(1e20);
            spelled.put("text of 1e20", text(big));
            spelled.put("text of hugeint(1e20)", text(new com.legend.sql.SqlExpr.Cast(big,
                    com.legend.sql.SqlType.Scalar.HUGEINT)));
            spelled.put("text of hugeint(1e20) + '.0'", com.legend.sql.SqlExpr.Call.of(com.legend.sql.SqlFn.CONCAT,
                    text(new com.legend.sql.SqlExpr.Cast(big, com.legend.sql.SqlType.Scalar.HUGEINT)),
                    new com.legend.sql.SqlExpr.StringLit(".0")));
            spelled.put("Pure's print of 1e20", com.legend.lowering.LiteralSpelling.floatPrint(big));
            spelled.put("text of 2.0", text(new com.legend.sql.SqlExpr.FloatLit(2.0)));
            spelled.put("text of 1.5e-8", text(new com.legend.sql.SqlExpr.FloatLit(1.5e-8)));
            spelled.put("text of decimal(38,18) of 1.5e-8", text(new com.legend.sql.SqlExpr.Cast(
                    new com.legend.sql.SqlExpr.FloatLit(1.5e-8), new com.legend.sql.SqlType.Decimal(38, 18))));
            spelled.put("Pure's print of 1.5e-8", com.legend.lowering.LiteralSpelling.floatPrint(
                    new com.legend.sql.SqlExpr.FloatLit(1.5e-8)));
            Map<String, Object> got = new java.util.LinkedHashMap<>();
            for (var e : spelled.entrySet()) {
                com.legend.sql.SqlSelect select = new com.legend.sql.SqlSelect(List.of(
                        new com.legend.sql.SqlSelect.Projection(e.getValue(), "r", new com.legend.sql.OutputCol("r",
                                com.legend.sql.SqlType.Scalar.VARCHAR, true))), false,
                        new com.legend.sql.SqlSource.Dual(), null, List.of(), null, null, List.of(), null, null,
                        List.of());
                Object r;
                try {
                    r = first(database.runCommand(Document.parse(target.render(select))));
                } catch (RuntimeException x) {
                    r = "ERROR " + x.getMessage();
                }
                got.put(e.getKey(), r);
                System.out.println("[mongodb-number-text] spelled " + e.getKey() + " -> " + r);
            }
            assertEquals("2.0", got.get("text of 2.0"));
            assertEquals("100000000000000000000.0", got.get("Pure's print of 1e20"));
            assertEquals("0.000000015", got.get("Pure's print of 1.5e-8"));
        }
    }

    private static com.legend.sql.SqlExpr text(com.legend.sql.SqlExpr e) {
        return new com.legend.sql.SqlExpr.Cast(e, com.legend.sql.SqlType.Scalar.VARCHAR);
    }

    private static Object first(Document commandResult) {
        List<?> batch = commandResult.get("cursor", Document.class).getList("firstBatch", Document.class);
        return batch.isEmpty() ? "(no row)" : ((Document) batch.get(0)).get("r");
    }
}
