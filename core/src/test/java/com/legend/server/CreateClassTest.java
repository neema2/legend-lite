// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import com.legend.database.Databases;
import com.legend.testbackend.TestBackends;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.json.Json;
import com.legend.model.ConnectionDefinition;
import com.legend.model.ServiceDefinition;
import com.legend.test.ServiceTestRunner;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * DataCube's Create a Class… (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §15.7, the routes): the class, its function and
 * mapping, the store, its connection and runtime, the model's mapping, the sample rows -- named, typed and laid out as
 * upstream's promotion lays them out; a second source joining the one store; the same class made again changing
 * nothing; and what cannot be a class, refused by name.
 */
@DisplayName("createClass -- a relation made a class, with everything it needs to run")
class CreateClassTest {

    /**
     * The path the project's connection names for an in-memory database is its declared database's (the dialect
     * program's Phase 7, 2026-10-18): the platform's own names one where legend-engine needs it; a specification that
     * names a path, a server's coordinates, or a database the platform does not run on is written as it is.
     */
    @Test
    void anInMemoryDatabaseIsWrittenWithItsDatabasesPath() {
        String platform = Databases.PLATFORM.name();
        assertEquals(Databases.dialect(Databases.PLATFORM).inMemoryPath(), CreateClass.inMemoryPath(connection(platform,
                "{\"_type\":\"duckDB\"}")));
        assertEquals(null, CreateClass.inMemoryPath(connection(platform, "{\"_type\":\"duckDB\",\"path\":\"/d.db\"}")));
        for (String server : List.of("Postgres", "SqlServer", "Oracle")) {
            assertEquals(null, CreateClass.inMemoryPath(connection(server,
                    "{\"_type\":\"static\",\"host\":\"db\",\"port\":5432,\"databaseName\":\"d\"}")), server);
        }
        assertEquals(null, CreateClass.inMemoryPath((Json.Obj) Json.parse("{\"databaseType\":\"" + platform + "\"}")));
    }

    private static Json.Obj connection(String databaseType, String specification) {
        return (Json.Obj) Json.parse("{\"_type\":\"RelationalDatabaseConnection\",\"databaseType\":\"" + databaseType
                + "\",\"type\":\"" + databaseType + "\",\"datasourceSpecification\":" + specification + "}");
    }

    /** A source as the tab's table model writes it: a Database, its connection, a runtime. */
    private static final String TAB = """
            ###Relational
            Database local::DB
            (
              Table trades_2
              (
                trade_id INTEGER NOT NULL,
                customer_id INTEGER,
                region VARCHAR(10),
                "Trade Note" VARCHAR(20),
                notional DECIMAL(18,2)
              )
            )
            ###Connection
            RelationalDatabaseConnection local::Conn { type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime local::RT { mappings: []; connections: [ local::DB: [ c1: local::Conn ] ]; }
            """;

    /** A second source, as its own grid's table model writes it. */
    private static final String CUSTOMERS = """
            ###Relational
            Database local::DB ( Table customers ( id INTEGER NOT NULL, name VARCHAR(20) ) )
            ###Connection
            RelationalDatabaseConnection local::Conn { type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime local::RT { mappings: []; connections: [ local::DB: [ c1: local::Conn ] ]; }
            """;

    private static final String QUERY =
            "|#>{local::DB.trades_2}#->filter(x|$x.region != 'APAC')->extend(~big: x|$x.notional > 100)";

    private static final String WHERE = """
            "where": {"from": "local::DB", "runtime": "local::RT", "as": "Browser",
              "samples": {"trades_2": {"columns": ["trade_id", "customer_id", "region", "Trade Note", "notional"],
                "rows": [["1", "10", "EMEA", "first, of two", "150.50"], ["2", null, "APAC", "say \\"hi\\"", "20"],
                         ["3", "11", "AMER", null, "300"]]}}}""";

    private static final String KEYED = "\"columns\": {\"customer_id\": {\"as\": \"key\"}}";

    /** A createClass request: the project's files (`project`), and the class over the grid's own model (`source`). */
    private static String body(String project, String source, String query, String name, String extra) {
        return """
                {"project": {"_type": "text", "code": %s}, "package": "trades",
                 "class": {"source": {"_type": "text", "code": %s}, "lambda": %s, "name": "%s", %s}}"""
                .formatted(Json.toCompact(project), Json.toCompact(source),
                        com.legend.parser.SpecParser.lambdaJson(query, false), name, extra);
    }

    private static Map<String, Object> make(String project, String source, String query, String name, String extra) {
        return ModelChanges.createClass(Json.parseObject(body(project, source, query, name, extra)));
    }

    /** The answer's one class made. */
    private static Map<?, ?> made(Map<String, Object> answer) {
        return (Map<?, ?>) ((List<?>) answer.get("classes")).get(0);
    }

    /** The answer's changes, each {@code "TYPE path"}. */
    private static List<String> changes(Map<String, Object> answer) {
        List<String> out = new ArrayList<>();
        for (Object c : (List<?>) answer.get("changes")) {
            Map<?, ?> m = (Map<?, ?>) c;
            out.add(m.get("type") + " " + m.get("path"));
        }
        return out;
    }

    private static String file(Map<String, Object> answer, String path) {
        for (Object c : (List<?>) answer.get("changes")) {
            Map<?, ?> m = (Map<?, ?>) c;
            if (path.equals(m.get("path"))) {
                return (String) m.get("pureCode");
            }
        }
        throw new AssertionError("no change at " + path + ": " + changes(answer));
    }

    /** The project the answers' files make, each file as the SDLC holds it: the later answer's file wins. */
    @SafeVarargs
    private static String project(Map<String, Object>... answers) {
        Map<String, String> files = new java.util.LinkedHashMap<>();
        for (Map<String, Object> a : answers) {
            for (Object c : (List<?>) a.get("changes")) {
                Map<?, ?> m = (Map<?, ?>) c;
                files.put((String) m.get("path"), (String) m.get("pureCode"));
            }
        }
        StringBuilder out = new StringBuilder();
        for (String code : files.values()) {
            out.append(code.startsWith("###") ? "\n" : "\n###Pure\n").append(code);
        }
        return out.toString();
    }

    private static final String FUNCTION = "trades::function::TradeFn__Relation_1_";

    @Test
    @DisplayName("writes the class, its function and mapping, the store, its connection and runtime, the sample")
    void writesEverythingTheClassNeeds() {
        Map<String, Object> a = make("", TAB, QUERY, "Trade", KEYED + ", " + WHERE);
        assertEquals(List.of("CREATE trades::store::Browser", "CREATE trades::connection::Browser",
                "CREATE trades::runtime::Browser", "CREATE " + FUNCTION, "CREATE trades::domain::Trade",
                "CREATE trades::mapping::TradeMapping", "CREATE trades::mapping::Model",
                "CREATE trades::data::Trades_2Sample"), changes(a));
        assertEquals("""
                Class trades::domain::Trade
                {
                  tradeId: Integer[1];
                  region: String[0..1];
                  tradeNote: String[0..1];
                  notional: Decimal[0..1];
                  big: Boolean[1];
                }
                """, file(a, "trades::domain::Trade"), "each column a property, named by upstream's rule; customer_id a "
                + "key only");
        assertEquals("""
                ###Mapping
                Mapping trades::mapping::TradeMapping
                (
                  *trades::domain::Trade: Relation
                  {
                    ~func trades::function::TradeFn():Relation<Any>[1]
                    tradeId: trade_id,
                    +customerId: Integer[0..1]: customer_id,
                    region: region,
                    tradeNote: 'Trade Note',
                    notional: notional,
                    big: big
                  }
                )
                """, file(a, "trades::mapping::TradeMapping"));
        assertEquals("""
                function trades::function::TradeFn(): meta::pure::metamodel::relation::Relation<(trade_id:Integer, \
                customer_id:Integer, region:String, 'Trade Note':String, notional:Decimal, big:Boolean)>[1]
                {
                  #>{trades::store::Browser.trades_2}#->filter(x|$x.region != 'APAC')->extend(~big:x|$x.notional > 100)
                }
                """, file(a, FUNCTION), "the grid's query, reading the project's store");
        String store = file(a, "trades::store::Browser");
        assertTrue(store.contains("Table trades_2") && store.contains("\"Trade Note\" VARCHAR(20)"), store);
        String connection = file(a, "trades::connection::Browser");
        assertTrue(connection.contains("store: trades::store::Browser;") && connection.contains("path: ':memory:';"),
                connection);
        String runtime = file(a, "trades::runtime::Browser");
        assertTrue(runtime.contains("trades::mapping::Model") && runtime.contains("browser: trades::connection::Browser"),
                runtime);
        assertEquals("trades::domain::Trade.all()->project(~[trade_id: x|$x.tradeId, region: x|$x.region, "
                + "'Trade Note': x|$x.tradeNote, notional: x|$x.notional, big: x|$x.big])", made(a).get("query"));
        assertEquals("trades::mapping::Model", made(a).get("mapping"));
        assertEquals("trades::runtime::Browser", made(a).get("runtime"));

        // the project alone -- no tab model -- compiles, and the class reads its relation through the mapping
        String project = project(a);
        assertEquals(List.of(), com.legend.Compiler.compileErrors(project));
        String sql = com.legend.Compiler.query(com.legend.Compiler.compileModel(project), "|" + made(a).get("query"))
                .plan("trades::runtime::Browser").sql();
        assertTrue(sql.contains("FROM trades_2") && sql.contains("'APAC'"), sql);
    }

    @Test
    @DisplayName("the sample rows load as test data, and the class reads them")
    void theSampleRowsLoad() {
        Map<String, Object> a = make("", TAB, QUERY, "Trade", KEYED + ", " + WHERE);
        assertEquals("""
                ###Data
                Data trades::data::Trades_2Sample
                {
                  Relational
                  #{
                    default.trades_2:
                      'trade_id,customer_id,region,Trade Note,notional\\n'+
                      '1,10,EMEA,"first, of two",150.50\\n'+
                      '2,,APAC,"say \"\"hi\"\"\",20\\n'+
                      '3,11,AMER,,300\\n';
                  }#
                }
                """, file(a, "trades::data::Trades_2Sample"));
        // a service over the class, its test's data the sample: run by lite's service test runner on DuckDB
        String model = project(a) + """
                ###Service
                Service trades::Check
                {
                  pattern: '/check';
                  documentation: '';
                  execution: Single
                  {
                    query: |%s;
                    mapping: trades::mapping::Model;
                    runtime: trades::runtime::Browser;
                  }
                  testSuites:
                  [
                    sample:
                    {
                      data:
                      [
                        connections:
                        [
                          browser:
                            Reference
                            #{
                              trades::data::Trades_2Sample
                            }#
                        ]
                      ]
                      tests:
                      [
                        rows:
                        {
                          serializationFormat: PURE_TDSOBJECT;
                          asserts:
                          [
                            rows:
                              EqualToJson
                              #{
                                expected:
                                  ExternalFormat
                                  #{
                                    contentType: 'application/json';
                                    data: '[{"trade_id":1,"region":"EMEA","Trade Note":"first, of two","notional":150.50,\
                "big":true},{"trade_id":3,"region":"AMER","Trade Note":null,"notional":300,"big":true}]';
                                  }#;
                              }#
                          ]
                        }
                      ]
                    }
                  ]
                }
                """.formatted(made(a).get("query"));
        ServiceDefinition service = com.legend.testing.Own.model(model).elements().stream()
                .filter(e -> e instanceof ServiceDefinition s && s.qualifiedName().equals("trades::Check"))
                .map(e -> (ServiceDefinition) e).findFirst().orElseThrow();
        List<ServiceTestRunner.Result> results = new ServiceTestRunner(com.legend.Compiler.compileModel(model),
                () -> TestBackends.platform().open(), ServiceTestRunner.Sessions.FRESH_PER_TEST,
                Databases.PLATFORM).run(service);
        assertEquals(1, results.size(), results.toString());
        assertTrue(results.get(0).pass(), results.toString());
    }

    @Test
    @DisplayName("a column a property, a key only, or left out; a property named and documented as asked")
    void columnsAsAsked() {
        Map<String, Object> a = make("", TAB, QUERY, "Trade", """
                "documentation": "A trade, as the desk books it",
                "columns": {"customer_id": {"as": "key", "name": "customer"}, "region": {"as": "omit"},
                            "Trade Note": {"name": "note", "documentation": "What the trader wrote"}}, """ + WHERE);
        assertEquals("""
                Class {meta::pure::profiles::doc.doc = 'A trade, as the desk books it'} trades::domain::Trade
                {
                  tradeId: Integer[1];
                  {meta::pure::profiles::doc.doc = 'What the trader wrote'} note: String[0..1];
                  notional: Decimal[0..1];
                  big: Boolean[1];
                }
                """, file(a, "trades::domain::Trade"));
        String mapping = file(a, "trades::mapping::TradeMapping");
        assertTrue(mapping.contains("+customer: Integer[0..1]: customer_id,"), mapping);
        assertFalse(mapping.contains("    region: region"), "a column left out is mapped to nothing: " + mapping);
        assertEquals(List.of(), com.legend.Compiler.compileErrors(project(a)));
    }

    @Test
    @DisplayName("names by upstream's rule, made total: camel case, a character a name cannot hold, a leading "
            + "digit, a name two columns would share")
    void upstreamsNamingRule() {
        assertEquals("tradeId", ModelChanges.camelCase("TRADE_ID", false));
        assertEquals("customerId", ModelChanges.camelCase("customerId", false));
        assertEquals("tradeNote", ModelChanges.camelCase("Trade Note", false));
        assertEquals("TradeLines", ModelChanges.camelCase("trade_lines", true));
        assertEquals("_1st", ModelChanges.identifier(ModelChanges.camelCase("1st", false)));
        assertEquals("price_", ModelChanges.identifier(ModelChanges.camelCase("price$", false)));
        String model = TAB.replace("region VARCHAR(10),", "region VARCHAR(10),\n    \"trade id\" INTEGER,\n"
                + "    \"function\" INTEGER,\n    \"Class\" INTEGER,\n    \"TRUE\" INTEGER,");
        Map<String, Object> a = make("", model, "|#>{local::DB.trades_2}#", "Trade",
                WHERE.replace("\"samples\"", "\"none\""));
        String cls = file(a, "trades::domain::Trade");
        assertTrue(cls.contains("  tradeId: Integer[1];") && cls.contains("  tradeId2: Integer[0..1];"), cls);
        // a word the grammar reads in another sense is a name in a class; one Pure reads as a literal is not
        assertTrue(cls.contains("  function: Integer[0..1];") && cls.contains("  class: Integer[0..1];")
                && cls.contains("  true_: Integer[0..1];"), cls);
        assertEquals(List.of(), com.legend.Compiler.compileErrors(project(a)), "and the files read back");
    }

    @Test
    @DisplayName("a second source joins the one store; the connection and runtime stay; the model's mapping "
            + "includes both")
    void aSecondSourceJoinsTheStore() {
        Map<String, Object> first = make("", TAB, QUERY, "Trade", KEYED + ", " + WHERE);
        String model = project(first);
        Map<String, Object> second = make(model, CUSTOMERS, "|#>{local::DB.customers}#", "Customer", """
                "where": {"from": "local::DB", "runtime": "local::RT", "as": "Browser",
                  "samples": {"customers": {"columns": ["id", "name"], "rows": [["10", "Ann"], ["11", "Bo"]]}}}""");
        // the project's files changed, in its order, then the new ones, as made
        assertEquals(List.of("MODIFY trades::store::Browser", "MODIFY trades::mapping::Model",
                "CREATE trades::function::CustomerFn__Relation_1_", "CREATE trades::domain::Customer",
                "CREATE trades::mapping::CustomerMapping", "CREATE trades::data::CustomersSample"), changes(second));
        String store = file(second, "trades::store::Browser");
        assertTrue(store.contains("Table trades_2") && store.contains("Table customers"), store);
        assertEquals("""
                ###Mapping
                Mapping trades::mapping::Model
                (
                  include mapping trades::mapping::TradeMapping
                  include mapping trades::mapping::CustomerMapping
                )
                """, file(second, "trades::mapping::Model"));
        String project = project(first, second);
        assertEquals(List.of(), com.legend.Compiler.compileErrors(project));
        var ctx = com.legend.Compiler.compileModel(project);
        for (Map<String, Object> a : List.of(first, second)) {
            com.legend.Compiler.query(ctx, "|" + made(a).get("query")).plan("trades::runtime::Browser");
        }
    }

    @Test
    @DisplayName("the same class made again changes nothing; made again renamed, only its class and mapping")
    void madeAgain() {
        Map<String, Object> first = make("", TAB, QUERY, "Trade", KEYED + ", " + WHERE);
        String model = project(first);
        assertEquals(List.of(), changes(make(model, TAB, QUERY, "Trade", KEYED + ", " + WHERE)));
        Map<String, Object> renamed = make(model, TAB, QUERY, "Trade",
                "\"columns\": {\"customer_id\": {\"as\": \"key\"}, \"Trade Note\": {\"name\": \"note\"}}, " + WHERE);
        assertEquals(List.of("MODIFY trades::domain::Trade", "MODIFY trades::mapping::TradeMapping"), changes(renamed));
        assertEquals(List.of(), com.legend.Compiler.compileErrors(project(first, renamed)));
    }

    @Test
    @DisplayName("made again with another filter: its function alone changes, cut out of the model and written anew")
    void madeAgainItsFunctionChanges() {
        Map<String, Object> first = make("", TAB, QUERY, "Trade", KEYED + ", " + WHERE);
        String model = project(first);
        Map<String, Object> again = make(model, TAB, QUERY.replace("'APAC'", "'EMEA'"), "Trade", KEYED + ", " + WHERE);
        assertEquals(List.of("MODIFY " + FUNCTION), changes(again));
        assertTrue(file(again, FUNCTION).contains("$x.region != 'EMEA'"), file(again, FUNCTION));
        String project = project(first, again);
        assertEquals(List.of(), com.legend.Compiler.compileErrors(project));
        String sql = com.legend.Compiler.query(com.legend.Compiler.compileModel(project), "|" + made(again).get("query"))
                .plan("trades::runtime::Browser").sql();
        assertTrue(sql.contains("'EMEA'") && !sql.contains("'APAC'"), sql);
    }

    @Test
    @DisplayName("a row whose value holds a line break is left out of the sample, and said")
    void aLineBreakRowIsLeftOut() {
        Map<String, Object> a = make("", TAB, QUERY, "Trade", KEYED + ", " + WHERE.replace("\"first, of two\"",
                "\"two\\nlines\""));
        String data = file(a, "trades::data::Trades_2Sample");
        assertFalse(data.contains("'1,10"), data);
        assertEquals(List.of("1 of trades_2's sample rows left out: a value holds a line break, which relational "
                + "test data cannot"), a.get("notes"));
    }

    // ---------------------------------------------------------------------
    // Through the route, and what cannot be a class
    // ---------------------------------------------------------------------

    private static final PureV1Api.Runner NO_RUN = (query, runtime, sinkOf) -> {
        throw new AssertionError("nothing is run here");
    };

    private static PureV1Api.Answer route(String body) {
        return PureV1Api.route("/api/lite/v1/model/createClass", null, body, NO_RUN);
    }

    private static void refused(String body, String expected) {
        PureV1Api.Answer a = route(body);
        assertEquals(400, a.status(), a.json());
        Json.Obj error = Json.parseObject(a.json());
        assertEquals("COMPILATION", error.getString("errorType"));
        assertTrue(error.getString("message").contains(expected), error.getString("message"));
    }

    @Test
    @DisplayName("answered through pure/v1's routing, as the tab and Python ask it")
    void throughTheRoute() {
        PureV1Api.Answer a = route(body("", TAB, QUERY, "Trade", KEYED + ", " + WHERE));
        assertEquals(200, a.status(), a.json());
        assertEquals(8, Json.parseObject(a.json()).getArr("changes").items().size());
    }

    @Test
    @DisplayName("what cannot be a class is refused by name, as a model that does not compile is")
    void refusals() {
        refused(body("", TAB, "|1", "Trade", WHERE), "a class is made of a relation; this query returns Integer");
        refused(body("", TAB, QUERY, "Trade", "\"columns\": {\"trade_id\": {\"as\": \"key\"}, \"region\": {\"as\": "
                + "\"omit\"}, \"customer_id\": {\"as\": \"key\"}, \"Trade Note\": {\"as\": \"omit\"}, \"notional\": "
                + "{\"as\": \"omit\"}, \"big\": {\"as\": \"omit\"}}, " + WHERE), "a class needs at least one property");
        refused(body("", TAB, QUERY, "Trade", "\"columns\": {\"nope\": {}}, " + WHERE), "the relation has no column 'nope'");
        refused(body("", TAB, "|#>{local::DB.trades_2}#->select(~[region, notional])->pivot(~[region], "
                + "~[total: x|$x.notional : y|$y->plus()])", "Trade", WHERE), "take the pivot off first");
        refused(body("", TAB, QUERY, "1Trade", WHERE), "a class's name is a Pure name");
        // a column Pure reads as a word of its own: its files would not read back
        for (String word : List.of("let", "true")) {
            String model = TAB.replace("region VARCHAR(10),", "region VARCHAR(10),\n    \"" + word + "\" INTEGER,");
            refused(body("", model, "|#>{local::DB.trades_2}#", "Trade", WHERE.replace("\"samples\"", "\"none\"")),
                    "would not read back");
        }
        refused(body("", TAB, QUERY, "Trade", "\"columns\": {\"region\": {\"name\": \"tradeId\"}, \"trade_id\": "
                + "{\"name\": \"tradeId\"}}, " + WHERE), "two columns are named 'tradeId'");
        // the store declares the source's table otherwise: never overwritten
        Map<String, Object> first = make("", TAB, QUERY, "Trade", KEYED + ", " + WHERE);
        String changed = TAB.replace("notional DECIMAL(18,2)", "notional DECIMAL(18,2),\n"
                + "    extra INTEGER");
        refused(body(project(first), changed, QUERY, "Trade", KEYED + ", " + WHERE),
                "declares the table default.trades_2 otherwise");
        // a relation reading a store the class's place does not write: its file would read what the project lacks
        String two = TAB + "###Relational\nDatabase other::DB ( Table desks ( desk VARCHAR(10) ) )\n";
        refused(body("", two, "|#>{other::DB.desks}#", "Desk", WHERE.replace("\"samples\"", "\"none\"")),
                "the relation reads the store 'other::DB'");
        // a class's name taken by a class of another table: not written over
        refused(body(project(first), CUSTOMERS, "|#>{local::DB.customers}#", "Trade", WHERE.replace("\"samples\"", "\"none\"")),
                "'trades::domain::Trade' is already the class of trades::store::Browser.default.trades_2");
        // a sample's name taken by another table's sample: `a_b` and `A_b` are both A_bSample
        String sample = """
                "where": {"from": "local::DB", "runtime": "local::RT", "as": "Browser", "samples": {"%s": {"columns":
                  ["trade_id", "customer_id", "region", "Trade Note", "notional"], "rows": [["1", null, "EMEA", null, "1"]]}}}""";
        Map<String, Object> ab = make("", TAB.replace("Table trades_2", "Table a_b"), "|#>{local::DB.a_b}#", "Ab",
                sample.formatted("a_b"));
        assertTrue(changes(ab).contains("CREATE trades::data::A_bSample"), changes(ab).toString());
        refused(body(project(ab), TAB.replace("Table trades_2", "Table A_b"), "|#>{local::DB.A_b}#", "Ab2",
                sample.formatted("A_b")), "the sample 'trades::data::A_bSample' holds the rows of default.a_b");
        // a request without the place is malformed: 500, as the routes answer one
        assertEquals(500, route(body("", TAB, QUERY, "Trade", "\"x\": 1")).status());
    }
}
