// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.error.ModelException;
import com.legend.json.Json;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The model's writer (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §15.6a): Create an Association… -- Pure's own shape,
 * its ModelJoin in the model's mapping, walked both ways through a key the class does not show, how many from the
 * facts, the refusals -- the associations the model suggests, and Create a Data Space… in one go: two classes, their
 * association and the data space in one request, all or nothing.
 */
@DisplayName("ModelChanges -- an association, a data space, and the whole page in one go")
class ModelChangesTest {

    /** Two grids' sources, each as the tab's table model writes it: each its own `local::DB`. */
    private static final String TRADES = """
            ###Relational
            Database local::DB ( Table trades ( trade_id INTEGER NOT NULL, customer_id INTEGER, notional DOUBLE ) )
            ###Connection
            RelationalDatabaseConnection local::Conn { type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime local::RT { mappings: []; connections: [ local::DB: [ c1: local::Conn ] ]; }
            """;
    private static final String CUSTOMERS = TRADES.replace(
            "Table trades ( trade_id INTEGER NOT NULL, customer_id INTEGER, notional DOUBLE )",
            "Table customers ( id INTEGER NOT NULL, name VARCHAR(20) )");

    private static String where() {
        return "\"where\": {\"from\": \"local::DB\", \"runtime\": \"local::RT\", \"as\": \"Browser\"}";
    }

    /** A class action: its source, its relation, its name, its columns' choices. */
    private static String cls(String source, String table, String name, String columns) {
        return """
                {"source": {"_type": "text", "code": %s}, "lambda": %s, "name": "%s", "columns": %s, %s}"""
                .formatted(Json.toCompact(source), com.legend.parser.SpecParser.lambdaJson("|#>{local::DB." + table + "}#",
                        false), name, columns, where());
    }

    private static final String TRADE = cls(TRADES, "trades", "Trade", "{\"customer_id\": {\"as\": \"key\"}}");
    private static final String CUSTOMER = cls(CUSTOMERS, "customers", "Customer", "{}");
    private static final String TRADE_CUSTOMER = """
            {"ends": [{"class": "trades::domain::Customer", "on": "id", "unique": true},
                      {"class": "trades::domain::Trade", "on": "customerId", "unique": false}]}""";

    private static String request(String project, String rest) {
        return """
                {"project": {"_type": "text", "code": %s}, "package": "trades", %s}""".formatted(Json.toCompact(project), rest);
    }

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

    /** The project the answers' files make, the later answer's file winning. */
    @SafeVarargs
    private static String project(Map<String, Object>... answers) {
        Map<String, String> files = new LinkedHashMap<>();
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

    /** The two classes, made one call each (the page's first two): their answers, the later's files winning. */
    @SuppressWarnings("unchecked")
    private static Map<String, Object>[] twoClassesMade() {
        Map<String, Object> trade = ModelChanges.createClass(Json.parseObject(request("", "\"class\": " + TRADE)));
        Map<String, Object> customer = ModelChanges.createClass(Json.parseObject(request(project(trade),
                "\"class\": " + CUSTOMER)));
        return new Map[] {trade, customer};
    }

    private static String twoClasses() {
        return project(twoClassesMade());
    }

    @Test
    @DisplayName("an association: Pure's own shape, its ModelJoin in the model's mapping, walked both ways through a key")
    void anAssociation() {
        Map<String, Object>[] two = twoClassesMade();
        String project = project(two);
        Map<String, Object> a = ModelChanges.createAssociation(Json.parseObject(request(project,
                "\"association\": " + TRADE_CUSTOMER)));
        assertEquals(List.of("MODIFY trades::mapping::Model", "CREATE trades::domain::Customer_Trade"), changes(a));
        assertEquals("""
                Association trades::domain::Customer_Trade
                {
                  customer: trades::domain::Customer[0..1];
                  trades: trades::domain::Trade[*];
                }
                """, file(a, "trades::domain::Customer_Trade"), "named after the other class, plural when many");
        assertTrue(file(a, "trades::mapping::Model").contains(
                "{customer: trades::domain::Customer[1], trades: trades::domain::Trade[1]|$customer.id == $trades.customerId}"),
                file(a, "trades::mapping::Model"));
        // the project alone walks it, both ways: Trade to its customer (to one), Customer to its trades (to many)
        var ctx = com.legend.Compiler.compileModel(project(two[0], two[1], a));
        String toOne = com.legend.Compiler.query(ctx, "|trades::domain::Trade.all()->project(~[name: x|$x.customer.name])")
                .plan("trades::runtime::Browser").sql();
        assertTrue(toOne.contains("LEFT OUTER JOIN customers") && toOne.contains("customer_id"), toOne);
        String toMany = com.legend.Compiler.query(ctx, "|trades::domain::Customer.all()->project(~[n: x|$x.trades.notional])")
                .plan("trades::runtime::Browser").sql();
        assertTrue(toMany.contains("LEFT OUTER JOIN trades"), toMany);
        assertEquals("trades::domain::Customer_Trade",
                ((Map<?, ?>) ((List<?>) a.get("associations")).get(0)).get("association"));
    }

    @Test
    @DisplayName("suggested, never made: a key named after another class and its property, of its type")
    void suggestions() {
        String project = twoClasses();
        Map<String, Object> before = ModelChanges.createAssociation(Json.parseObject(request(project,
                "\"association\": " + TRADE_CUSTOMER)));
        // the answer's suggestions are the model's AFTER: this pair is related now, so none is suggested
        assertEquals(List.of(), before.get("suggestions"));
        // asked over the two classes alone (a data space of them, nothing related yet): the pair is suggested
        Map<String, Object> asked = ModelChanges.createDataSpace(Json.parseObject(request(project,
                "\"dataSpace\": {\"name\": \"Trades\"}")));
        assertEquals(List.of(Map.of("ends", List.of(Map.of("class", "trades::domain::Customer", "on", "id"),
                Map.of("class", "trades::domain::Trade", "on", "customerId")))), asked.get("suggestions"));
    }

    @Test
    @DisplayName("suggested by the same name too: Trade.customerId names Customer.customerId, the other class's own key")
    void suggestedByTheSameName() {
        String customers = TRADES.replace(
                "Table trades ( trade_id INTEGER NOT NULL, customer_id INTEGER, notional DOUBLE )",
                "Table customers ( customer_id INTEGER NOT NULL, name VARCHAR(20) )");
        Map<String, Object> trade = ModelChanges.createClass(Json.parseObject(request("", "\"class\": " + TRADE)));
        Map<String, Object> both = ModelChanges.createClass(Json.parseObject(request(project(trade),
                "\"class\": " + cls(customers, "customers", "Customer", "{}"))));
        assertEquals(List.of(Map.of("ends", List.of(Map.of("class", "trades::domain::Customer", "on", "customerId"),
                Map.of("class", "trades::domain::Trade", "on", "customerId")))), both.get("suggestions"));
    }

    @Test
    @DisplayName("an association of nothing makes nothing: what is suggested, and what each class is joined on, from what")
    void theWindowsStart() {
        Map<String, Object> start = ModelChanges.createAssociation(Json.parseObject(request(twoClasses(), "\"classes\": []")));
        assertEquals(List.of(), start.get("changes"));
        assertEquals(1, ((List<?>) start.get("suggestions")).size());
        Map<?, ?> trade = (Map<?, ?>) ((List<?>) start.get("joinable")).stream()
                .filter(c -> "trades::domain::Trade".equals(((Map<?, ?>) c).get("class"))).findFirst().orElseThrow();
        assertEquals("trades::function::TradeFn", trade.get("function"));
        assertEquals(List.of(
                Map.of("name", "tradeId", "type", "Integer", "shown", true, "column", "trade_id"),
                Map.of("name", "notional", "type", "Float", "shown", true, "column", "notional"),
                Map.of("name", "customerId", "type", "Integer", "shown", false, "column", "customer_id")), trade.get("keys"));
    }

    @Test
    @DisplayName("the whole page in one go: two classes, their association and the data space, one request, all new")
    void inOneGo() {
        Map<String, Object> all = ModelChanges.createDataSpace(Json.parseObject(request("",
                "\"classes\": [" + TRADE + ", " + CUSTOMER + "], \"associations\": [" + TRADE_CUSTOMER + "], "
                        + "\"dataSpace\": {\"name\": \"Trades\", \"title\": \"Trades\", \"description\": \"The desk's trades\"}")));
        List<String> made = changes(all);
        assertTrue(made.stream().allMatch(c -> c.startsWith("CREATE ")), made.toString());
        assertTrue(made.containsAll(List.of("CREATE trades::domain::Trade", "CREATE trades::domain::Customer",
                "CREATE trades::domain::Customer_Trade", "CREATE trades::Trades")), made.toString());
        assertEquals("""
                ###DataSpace
                DataSpace trades::Trades
                {
                  executionContexts:
                  [
                    {
                      name: 'Browser';
                      mapping: trades::mapping::Model;
                      defaultRuntime: trades::runtime::Browser;
                    }
                  ];
                  defaultExecutionContext: 'Browser';
                  title: 'Trades';
                  description: 'The desk\\'s trades';
                  elements:
                  [
                    trades::domain::Trade,
                    trades::domain::Customer
                  ];
                }
                """, file(all, "trades::Trades"));
        assertEquals("trades::Trades", all.get("dataSpace"));
        assertEquals(List.of(), com.legend.Compiler.compileErrors(project(all)), "the project alone compiles");
    }

    @Test
    @DisplayName("all or nothing: one action refused, the request writes nothing, and says which")
    void allOrNothing() {
        String badAssociation = TRADE_CUSTOMER.replace("\"on\": \"customerId\"", "\"on\": \"notional\"");
        ModelException e = assertThrows(ModelException.class, () -> ModelChanges.createDataSpace(Json.parseObject(request("",
                "\"classes\": [" + TRADE + ", " + CUSTOMER + "], \"associations\": [" + badAssociation + "], "
                        + "\"dataSpace\": {\"name\": \"Trades\"}"))));
        assertTrue(e.getMessage().contains("an association joins equal values: trades::domain::Customer.id is Integer, "
                + "trades::domain::Trade.notional Float"), e.getMessage());
    }

    @Test
    @DisplayName("refused by name: a property or key of neither, how many unsaid, a name the class has, another package")
    void refusals() {
        Map<String, Object>[] two = twoClassesMade();
        String project = project(two);
        refused(project, TRADE_CUSTOMER.replace("\"on\": \"customerId\"", "\"on\": \"nope\""),
                "trades::domain::Trade has no property or key 'nope'");
        refused(project, TRADE_CUSTOMER.replace(", \"unique\": true", ""),
                "how many of trades::domain::Customer one of the other has is said");
        refused(project, TRADE_CUSTOMER.replace("\"unique\": true", "\"unique\": true, \"name\": \"tradeId\""),
                "trades::domain::Trade has a property 'tradeId' already");
        refused(project, TRADE_CUSTOMER.replace("trades::domain::Customer", "other::domain::Customer"),
                "the project has no class other::domain::Customer");
        // a class of the project, of another package: an association relates one package's classes
        Map<String, Object> other = ModelChanges.createClass(Json.parseObject(request(project, "\"class\": " + CUSTOMER)
                .replace("\"package\": \"trades\"", "\"package\": \"other\"")));
        refused(project(two[0], two[1], other), TRADE_CUSTOMER.replace("trades::domain::Customer", "other::domain::Customer"),
                "an association relates classes of one package, trades's: other::domain::Customer is not one");
        // made again: the same association, not a second one
        Map<String, Object> once = ModelChanges.createAssociation(Json.parseObject(request(project,
                "\"association\": " + TRADE_CUSTOMER)));
        Map<String, Object> again = ModelChanges.createAssociation(Json.parseObject(request(project(two[0], two[1], once),
                "\"association\": " + TRADE_CUSTOMER)));
        assertEquals(List.of(), changes(again), "made again over itself: nothing to change");
    }

    private static void refused(String project, String association, String expected) {
        refusedBy("createAssociation", request(project, "\"association\": " + association), expected);
    }

    private static void refusedBy(String route, String body, String expected) {
        PureV1Api.Answer a = PureV1Api.route("/api/lite/v1/model/" + route, null, body, (q, r, sinkOf) -> {
            throw new AssertionError("nothing is run here");
        });
        assertEquals(400, a.status(), a.json());
        Json.Obj error = Json.parseObject(a.json());
        assertEquals("COMPILATION", error.getString("errorType"));
        assertTrue(error.getString("message").contains(expected), error.getString("message"));
    }

    @Test
    @DisplayName("not written over: an element of another kind, a package, or one written in Studio rather than made here")
    void notWrittenOver() {
        String project = twoClasses();
        // a data space named as a package of the project (legend-engine refuses the project)
        refusedBy("createDataSpace", request(project, "\"dataSpace\": {\"name\": \"domain\"}"),
                "'trades::domain' is a package of the project (trades::domain::Trade): name it otherwise");
        // an association named as a class
        refused(project, TRADE_CUSTOMER.replace("{\"ends\"", "{\"name\": \"Trade\", \"ends\""),
                "'trades::domain::Trade' is already the project's class: name it otherwise");
        // a class, an association and a data space written in Studio: no grid's function, no ModelJoin, another mapping
        String studio = """
                ###Pure
                Class trades::domain::Desk { name: String[1]; }
                Association trades::domain::Customer_Trade { customer: trades::domain::Customer[0..1]; trades: trades::domain::Trade[*]; }
                ###Mapping
                Mapping trades::mapping::Other ( )
                ###DataSpace
                DataSpace trades::Trades { executionContexts: [ { name: 'Other'; mapping: trades::mapping::Other;
                  defaultRuntime: trades::runtime::Browser; } ]; defaultExecutionContext: 'Other'; }
                """;
        refusedBy("createClass", request(project + studio, "\"class\": " + cls(TRADES, "trades", "Desk", "{}")),
                "'trades::domain::Desk' is already in the project, not made of a grid's rows: name this class otherwise");
        refused(project + studio, TRADE_CUSTOMER,
                "'trades::domain::Customer_Trade' is already in the project, not made here: name this association otherwise");
        refusedBy("createDataSpace", request(project + studio, "\"dataSpace\": {\"name\": \"Trades\"}"),
                "'trades::Trades' is already in the project, not made here: name this data space otherwise");
    }

    @Test
    @DisplayName("made again over its own files: the same is nothing, ends the other way round too; a change is only its own")
    void madeAgain() {
        String sampled = ",\n \"samples\": {\"trades\": {\"columns\": [\"trade_id\", \"customer_id\", \"notional\"], "
                + "\"rows\": [[\"1\", \"10\", \"150.5\"]]}}}";
        String trade = TRADE.substring(0, TRADE.length() - 2) + sampled + "}";
        String request = "\"classes\": [" + trade + ", " + CUSTOMER + "], \"associations\": [" + TRADE_CUSTOMER + "], "
                + "\"dataSpace\": {\"name\": \"Trades\", \"title\": \"Trades\"}";
        Map<String, Object> all = ModelChanges.createDataSpace(Json.parseObject(request("", request)));
        assertTrue(changes(all).contains("CREATE trades::data::TradesSample"), changes(all).toString());
        String project = project(all);
        assertEquals(List.of(), changes(ModelChanges.createDataSpace(Json.parseObject(request(project, request)))),
                "the same again, over its own files (the data and the data space among them)");
        String swapped = """
                {"ends": [{"class": "trades::domain::Trade", "on": "customerId", "unique": false},
                          {"class": "trades::domain::Customer", "on": "id", "unique": true}]}""";
        assertEquals(List.of(), changes(ModelChanges.createAssociation(Json.parseObject(request(project,
                "\"association\": " + swapped)))), "the same association, its ends asked the other way round");
        // each element changed in place, cut out of the text where it was: the project after compiles, and only it moved
        Map<String, Object> retitled = ModelChanges.createDataSpace(Json.parseObject(request(project,
                request.replace("\"title\": \"Trades\"", "\"title\": \"The desk's trades\""))));
        assertEquals(List.of("MODIFY trades::Trades"), changes(retitled));
        Map<String, Object> renamed = ModelChanges.createAssociation(Json.parseObject(request(project,
                "\"association\": " + TRADE_CUSTOMER.replace("\"unique\": true", "\"unique\": true, \"name\": \"client\""))));
        assertEquals(List.of("MODIFY trades::mapping::Model", "MODIFY trades::domain::Customer_Trade"), changes(renamed));
        assertEquals(List.of(), com.legend.Compiler.compileErrors(project(all, retitled, renamed)));
    }
}
