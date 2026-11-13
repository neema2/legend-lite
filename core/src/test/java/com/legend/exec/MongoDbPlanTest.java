// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.Compiler;
import com.legend.executionplan.ExecutionPlan;
import com.legend.testcases.PlanCases;
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

    /** {@code query}'s typed plan run with {@code values} on the shared sessions, its rows written in {@code written}
     *  (lite's CSV, its JSON rows: docs/RESULT_BOUNDARY_PROGRAM_2026_10_26.md, the end state of 2026-10-27). */
    private static String run(String query, PlanCases.Written written, Map<String, ?> values) throws Exception {
        ExecutionPlan plan = Compiler.query(Compiler.compileModel(model), query).executionPlan("test::RT");
        com.legend.testcases.PlanRoundTrip.assertRoundTrips(plan, query);
        StringWriter out = new StringWriter();
        PlanRunner.stream(plan, values, PlanSessions.shared(), written.writer(out, plan.answerShape()), java.util.UUID.randomUUID());
        return out.toString();
    }

    @Test
    void aFilteredSortedSelectStreamsItsRows() throws Exception {
        assertEquals("[{\"name\":\"Ann\",\"age\":34},{\"name\":\"Cy\",\"age\":41}]",
                run("#>{test::Store.people}#->filter(x | $x.age > 30)->select(~[name, age])->sort(~name->ascending())",
                        PlanCases.Written.JSON, Map.of()));
    }

    @Test
    void anAbsentFieldReadsAsNull() throws Exception {
        assertEquals("[{\"name\":\"Di\",\"age\":null}]",
                run("#>{test::Store.people}#->filter(x | $x.age->isEmpty())->select(~[name, age])",
                        PlanCases.Written.JSON, Map.of()));
    }

    /**
     * WHAT MONGODB IS SENT IS THE PLAN'S COMMAND (the C checkpoint's MUST-FIX 1 and its NOTE, 2026-11-12): the aggregate
     * command the server receives -- read by the driver's own command listener -- carries the plan's pipeline EXACTLY,
     * each plan parameter the pipeline variable {@code "$$lpN"} the dialect wrote, and the parameters' values in the
     * command's {@code let} alone ({@code lpN} the N-th slot's, as a {@code $literal}): the session writes nothing into
     * the pipeline (AGENTS.md invariant 3; until 2026-11-12 it replaced a marker document by each value). The
     * session's statements take the plan's text by type ({@code PlannedSql}); this is the run's witness of what it
     * sends.
     */
    @Test
    void theCommandSentIsThePlans_itsParametersBoundByTheLet() throws Exception {
        String query = "{n: Integer[1], s: String[1] | #>{test::Store.people}#->filter(x | ($x.age > $n) &&"
                + " ($x.name != $s))->select(~[name])}";
        ExecutionPlan plan = Compiler.query(Compiler.compileModel(model), query).executionPlan("test::RT");
        ExecutionPlan.Sql sql = plan.answerSql();
        assertEquals(List.of("n", "s"), sql.slots().stream().map(ExecutionPlan.Slot::parameter).toList(),
                "a slot for each parameter, in document order");
        assertTrue(sql.statement().contains("\"$$lp1\"") && sql.statement().contains("\"$$lp2\""), sql.statement());
        assertTrue(!sql.statement().contains("$legendParam"), "no marker in the plan: " + sql.statement());
        // EVERY command the driver sends is recorded (the C final audit's J3, 2026-11-12: until then the aggregate alone,
        // so a command exec wrote itself went unseen)
        List<String> names = java.util.Collections.synchronizedList(new java.util.ArrayList<>());
        List<org.bson.BsonDocument> commands = java.util.Collections.synchronizedList(new java.util.ArrayList<>());
        com.mongodb.event.CommandListener listener = new com.mongodb.event.CommandListener() {
            @Override
            public void commandStarted(com.mongodb.event.CommandStartedEvent e) {
                names.add(e.getCommandName());
                commands.add(e.getCommand().clone());
            }
        };
        EmbeddedMongoDb db = EmbeddedMongoDb.shared();
        com.mongodb.MongoClientSettings settings = com.mongodb.MongoClientSettings.builder()
                .applyToClusterSettings(c -> c.hosts(List.of(new com.mongodb.ServerAddress("127.0.0.1", db.port()))))
                .credential(com.mongodb.MongoCredential.createCredential(EmbeddedMongoDb.USER, EmbeddedMongoDb.DATABASE,
                        EmbeddedMongoDb.PASSWORD.toCharArray()))
                .addCommandListener(listener)
                .build();
        Map<String, PlanParameters.Checked> checked = PlanParameters.check(plan.parameters(),
                Map.of("n", 30L, "s", "Ann"));
        List<List<Object>> rows = new java.util.ArrayList<>();
        List<String> sentByTheRun;
        try (MongoClient client = com.mongodb.client.MongoClients.create(settings);
                var run = com.legend.base.RunScope.enter()) {
            Statements st = new MongoDbStatements(client.getDatabase(EmbeddedMongoDb.DATABASE));
            // checked as the runner checks every session first (J3: until then this witness skipped it)
            st.check(sql.target());
            st.rows(sql.planned(), sql.slots(), List.of(checked.get("n"), checked.get("s")),
                    ((ExecutionPlan.TdsResult) plan.root()).columns(), rows::add);
            // what the run sent, before the client's own close
            sentByTheRun = List.copyOf(names);
        }
        assertEquals(List.of(List.of("Cy")), rows);
        // the plan's command and nothing else: a command the plan does not carry fails here (the user's ruling of
        // 2026-11-12, J3: the session's own buildInfo, sent by its check until then, is deleted)
        org.bson.BsonDocument planned = org.bson.BsonDocument.parse(sql.statement());
        assertEquals(List.of(planned.getFirstKey()), sentByTheRun, "every command the run sent is the plan's");
        org.bson.BsonDocument aggregate = commands.get(0);
        assertEquals(planned.get("aggregate"), aggregate.get("aggregate"));
        assertEquals(planned.get("pipeline"), aggregate.get("pipeline"), "the plan's pipeline, untouched");
        assertEquals(org.bson.BsonDocument.parse("{\"lp1\": {\"$literal\": {\"$numberLong\": \"30\"}},"
                + " \"lp2\": {\"$literal\": \"Ann\"}}"), aggregate.get("let"), "the values, in the let alone");
    }

    /**
     * A dynamic pivot's keys and a plan's parameters bind apart (the keyed plan of 2026-10-20 on MongoDB): each copy of
     * the per-key region binds its key at the key's marker, as it did, and the parameters -- before the region and
     * after it in document order -- are the command's let variables, numbered over the parameters alone. Swapped,
     * {@code lp1} and {@code lp2} would filter every row out.
     */
    @Test
    void aDynamicPivotsKeysAndParametersBindApart() throws Exception {
        String query = "{lo: Integer[1], hi: Integer[1] | #>{test::Store.people}#->filter(x | ($x.age > $lo) &&"
                + " ($x.age < $hi))->select(~[city, age])->pivot(~city, ~[s: x | $x.age : y | $y->plus()])}";
        ExecutionPlan plan = Compiler.query(Compiler.compileModel(model), query).executionPlan("test::RT");
        String body = plan.answerSql().statement();
        assertTrue(body.contains("\"$$lp1\"") && body.contains("\"$$lp2\"") && body.contains("$legendParam"),
                "the parameters' variables and the keys' markers: " + body);
        assertEquals("[{\"'Oslo__|__s'\":62}]", run(query, PlanCases.Written.JSON, Map.of("lo", 20L, "hi", 40L)));
        assertEquals("[{\"'Oslo__|__s'\":34,\"'Rome__|__s'\":41}]",
                run(query, PlanCases.Written.JSON, Map.of("lo", 30L, "hi", 50L)));
    }

    /** A field named with a quote and a backslash is written as JSON's lexer reads a string (the user's ruling of
     *  2026-11-13, every dialect's identifier escaped for its own lexer): a dynamic pivot's per-key aggregate so named
     *  answers that name exactly, its pipeline whole. (Measured that day: a person's name reaches the pipeline through
     *  MongoJson's escaping alone; the per-key writers that splice a name around the runner's counter carry the
     *  plan's own names, {@code _u0} and {@code __agg0}.) */
    @Test
    void aFieldNamedWithAQuoteAndABackslashIsItsOwnName() throws Exception {
        String query = "|#>{test::Store.people}#->select(~[city, age])->pivot(~city, ~['a\"b\\\\c': x | $x.age : y"
                + " | $y->plus()])";
        String answered = run(query, PlanCases.Written.JSON, Map.of());
        assertTrue(answered.contains("\"'Oslo__|__a\\\"b\\\\c'\":"), answered);
    }

    /** A parameter inside the per-key region is refused by name: each copy of the region would bind it again, and a
     *  pipeline variable is bound once. */
    @Test
    void aParameterInsideAPerKeyRegionIsRefusedByName() {
        String query = "{n: Integer[1] | #>{test::Store.people}#->select(~[city, age])"
                + "->pivot(~city, ~[s: x | $x.age->toOne() + $n : y | $y->plus()])}";
        RuntimeException refused = org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class,
                () -> Compiler.query(Compiler.compileModel(model), query).executionPlan("test::RT"));
        assertTrue(String.valueOf(refused.getMessage()).contains("inside a dynamic pivot's per-key region"),
                String.valueOf(refused.getMessage()));
    }

    @Test
    void aParameterFilters() throws Exception {
        assertEquals("[{\"name\":\"Cy\"}]",
                run("{n: Integer[1] | #>{test::Store.people}#->filter(x | $x.age > $n)->select(~[name])}",
                        PlanCases.Written.JSON, Map.of("n", 40L)));
    }

    @Test
    void aParameterInAnExtend() throws Exception {
        assertEquals("[{\"name\":\"Ann\",\"next\":35},{\"name\":\"Bob\",\"next\":29},{\"name\":\"Cy\",\"next\":42}]",
                run("{n: Integer[1] | #>{test::Store.people}#->filter(x | $x.age->isNotEmpty())"
                        + "->extend(~next: x | $x.age->toOne() + $n)->select(~[name, next])->sort(~name->ascending())}",
                        PlanCases.Written.JSON, Map.of("n", 1L)));
    }

    /** A join's condition is matched inside its {@code $lookup}'s sub-pipeline, where the command's {@code let}
     *  variables are read as anywhere in the pipeline. */
    @Test
    void aParameterInsideALookupsSubPipeline() throws Exception {
        String query = "{n: Integer[1] | #>{test::Store.people}#->select(~[name, city])"
                + "->join(#>{test::Store.people}#->select(~[name, age])->rename(~name, ~other), JoinKind.INNER,"
                + " {a, b | ($a.name == $b.other) && ($b.age > $n)})->select(~[name])->sort(~name->ascending())}";
        String statement = Compiler.query(Compiler.compileModel(model), query).executionPlan("test::RT").answerSql()
                .statement();
        int lookup = statement.indexOf("\"$lookup\"");
        assertTrue(lookup >= 0 && statement.indexOf("\"$$lp1\"", lookup) > lookup,
                "the parameter inside the lookup: " + statement);
        assertEquals("[{\"name\":\"Ann\"},{\"name\":\"Cy\"}]", run(query, PlanCases.Written.JSON, Map.of("n", 30L)));
    }

    @Test
    void aListParameterIsBoundWhole() throws Exception {
        String query = "{names: String[*] | #>{test::Store.people}#->filter(x | $names->contains($x.name))"
                + "->select(~[name])->sort(~name->ascending())}";
        assertEquals("[{\"name\":\"Ann\"},{\"name\":\"Di\"}]",
                run(query, PlanCases.Written.JSON, Map.of("names", List.of("Ann", "Di"))));
        assertEquals("[]", run(query, PlanCases.Written.JSON, Map.of("names", List.of())));
    }

    @Test
    void anAbsentParameterIsBoundAsNull() throws Exception {
        assertEquals("[{\"name\":\"Ann\",\"given\":null}]",
                run("{c: String[0..1] | #>{test::Store.people}#->filter(x | $x.name == 'Ann')"
                        + "->extend(~given: x | $c)->select(~[name, given])}", PlanCases.Written.JSON, Map.of()));
    }

    @Test
    void aDecimalParameterKeepsItsDigits() throws Exception {
        assertEquals("[{\"name\":\"Ann\",\"d\":12.345}]",
                run("{d: Decimal[1] | #>{test::Store.people}#->filter(x | $x.name == 'Ann')"
                        + "->extend(~d: x | $d)->select(~[name, d])}", PlanCases.Written.JSON,
                        Map.of("d", new java.math.BigDecimal("12.345"))));
    }

    @Test
    void aDateParameter() throws Exception {
        assertEquals("[{\"name\":\"Ann\",\"t\":\"2026-11-12\"}]",
                run("{t: StrictDate[1] | #>{test::Store.people}#->filter(x | $x.name == 'Ann')"
                        + "->extend(~t: x | $t)->select(~[name, t])}", PlanCases.Written.JSON,
                        Map.of("t", java.time.LocalDate.of(2026, 11, 12))));
    }

    /** A text is bound as its text, a {@code $literal}: quotes stay quotes, and a leading {@code $} is no field path
     *  (bare, {@code "$name"} would read each document's own name, and every row would match). */
    @Test
    void aStringIsBoundAsItsText() throws Exception {
        String echo = "{s: String[1] | #>{test::Store.people}#->filter(x | $x.name == 'Ann')"
                + "->extend(~s: x | $s)->select(~[name, s])}";
        assertEquals("[{\"name\":\"Ann\",\"s\":\"it's \\\"quoted\\\"\"}]",
                run(echo, PlanCases.Written.JSON, Map.of("s", "it's \"quoted\"")));
        assertEquals("[{\"name\":\"Ann\",\"s\":\"$name\"}]", run(echo, PlanCases.Written.JSON, Map.of("s", "$name")));
        assertEquals("[{\"name\":\"Ann\",\"s\":\"$$ROOT\"}]", run(echo, PlanCases.Written.JSON, Map.of("s", "$$ROOT")));
        assertEquals("[]", run("{s: String[1] | #>{test::Store.people}#->filter(x | $x.name == $s)->select(~[name])}",
                PlanCases.Written.JSON, Map.of("s", "$name")));
    }

    @Test
    void aGroupCountsAndSorts() throws Exception {
        assertEquals("[{\"city\":\"Oslo\",\"n\":2},{\"city\":\"Rome\",\"n\":2}]",
                run("#>{test::Store.people}#->groupBy(~[city], ~[n: x | $x.name : y | $y->count()])"
                        + "->sort(~city->ascending())", PlanCases.Written.JSON, Map.of()));
    }

    @Test
    void theWholeResultAsJsonAndAsCsv() throws Exception {
        String json = run("#>{test::Store.people}#->filter(x | $x.city == 'Oslo')->select(~[name, age])"
                + "->sort(~age->descending())", PlanCases.Written.JSON, Map.of());
        assertTrue(json.contains("\"name\":\"Ann\"") && json.contains("\"name\":\"Bob\""), json);
        String csv = run("#>{test::Store.people}#->filter(x | $x.city == 'Oslo')->select(~[name, age])"
                + "->sort(~age->descending())", PlanCases.Written.CSV, Map.of());
        assertTrue(csv.startsWith("name,age") && csv.contains("Ann,34") && csv.contains("Bob,28"), csv);
    }

    /**
     * Homework 4 (docs/MONGODB_PLAN_2026_10_16.md §9, 2026-10-17) measured the whole-text wires: every row gathered
     * into one string inside one document, which MongoDB caps at 16 MB, by a {@code $reduce} quadratic in the row
     * count. Those wires are gone (the result boundary program's phase 3, 2026-10-27): a result is its typed rows, read
     * document by document and written at the edge, so 200 rows of 100 KB -- about 20 MB, past the old bound -- answer
     * whole.
     */
    @Test
    void aResultPastSixteenMegabytesStreams() throws Exception {
        String filler = "x".repeat(100_000);
        loadBig(200, filler);
        long t0 = System.nanoTime();
        String large = run("#>{test::Store.big}#->select(~[n, s])", PlanCases.Written.CSV, Map.of());
        System.out.println("[mongodb-16mb] 200 rows x 100 KB: the CSV passed, " + large.length() + " characters in "
                + (System.nanoTime() - t0) / 1_000_000 + " ms");
        assertTrue(large.length() > 20_000_000 && large.startsWith("n,s"), "the 20 MB CSV: " + large.length());
        assertEquals("[{\"n\":199}]", run("#>{test::Store.big}#->select(~n)->sort(~n->descending())->limit(1)",
                PlanCases.Written.JSON, Map.of()));
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
        assertTrue(run("1 + 2", PlanCases.Written.JSON, Map.of()).contains("3"));
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
