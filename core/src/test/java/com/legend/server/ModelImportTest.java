// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.error.ModelException;
import com.legend.exec.ExecutionResult;
import com.legend.json.Json;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A model from a schema, mapped with SQL (docs/MODEL_IMPORT_STUDY_2026_10_20.md): an OpenAPI document's schemas
 * imported as Pure ({@code importSchema}), each class given its rows by a Postgres SELECT ({@code mapClass}), the
 * association the schema's reference made joined on a key the SELECT keeps ({@code joinAssociation}) -- and the model,
 * written by nothing but those three calls, queried on DuckDB, through the association both ways. What does not fit
 * is refused, every misfit named at once, and nothing is written.
 */
@DisplayName("Model import -- a schema imported, its classes mapped with SQL, queried end to end")
class ModelImportTest {

    /** The schema: an enumeration, two classes, a reference from one to the other, a list. */
    private static final String OPENAPI = """
            {"openapi": "3.0.3", "info": {"title": "Trades", "version": "1"}, "paths": {},
             "components": {"schemas": {
               "Side": {"type": "string", "enum": ["BUY", "SELL"], "description": "Which way the trade goes"},
               "Customer": {"type": "object", "required": ["id", "name"], "properties": {
                   "id": {"type": "integer", "format": "int64"},
                   "name": {"type": "string", "maxLength": 100},
                   "since": {"type": "string", "format": "date", "nullable": true}}},
               "Trade": {"type": "object", "description": "A trade booked with a customer",
                 "required": ["id", "quantity", "side", "customer"], "properties": {
                   "id": {"type": "integer"},
                   "quantity": {"type": "number"},
                   "side": {"$ref": "#/components/schemas/Side"},
                   "tradeDate": {"type": "string", "format": "date"},
                   "customer": {"$ref": "#/components/schemas/Customer"},
                   "tags": {"type": "array", "items": {"type": "string"}}}}}}}
            """;

    /** The project before: the tables, their connection, a runtime naming no mapping yet -- each a file. */
    private static Map<String, String> start() {
        Map<String, String> files = new LinkedHashMap<>();
        files.put("store::DB", """
                ###Relational
                Database store::DB
                (
                  Table TRADES (trade_id INTEGER, qty DOUBLE, side_code VARCHAR(4), trade_date DATE, cust INTEGER)
                  Table CUSTOMERS (id INTEGER NOT NULL, name VARCHAR(100), since DATE)
                )
                """);
        files.put("store::Conn", """
                ###Connection
                RelationalDatabaseConnection store::Conn { type: DuckDB; specification: DuckDB { }; auth: Test; }
                """);
        files.put("store::RT", """
                ###Runtime
                Runtime store::RT { mappings: []; connections: [ store::DB: [ environment: store::Conn ] ]; }
                """);
        return files;
    }

    private static String text(Map<String, String> files) {
        StringBuilder out = new StringBuilder();
        for (String code : files.values()) {
            out.append(code.startsWith("###") ? "\n" : "\n###Pure\n").append(code);
        }
        return out.toString();
    }

    private static String request(Map<String, String> files, String rest) {
        return """
                {"project": {"_type": "text", "code": %s}, "package": "trades", %s}"""
                .formatted(Json.toCompact(text(files)), rest);
    }

    /** Each change of {@code answer} written into {@code files}, as the SDLC would: the later file wins. */
    private static Map<String, Object> wrote(Map<String, String> files, Map<String, Object> answer) {
        for (Object c : (List<?>) answer.get("changes")) {
            Map<?, ?> m = (Map<?, ?>) c;
            files.put((String) m.get("path"), (String) m.get("pureCode"));
        }
        return answer;
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

    private static Map<String, Object> importSchema(Map<String, String> files, String schema) {
        return ModelChanges.importSchema(Json.parseObject(request(files, "\"schema\": {\"format\": \"openApi\", "
                + "\"content\": " + Json.toCompact(schema) + "}")));
    }

    private static Map<String, Object> mapClass(Map<String, String> files, String rest) {
        return ModelChanges.mapClass(Json.parseObject(request(files, rest)));
    }

    private static final String MAP_CUSTOMER = """
            "class": "trades::domain::Customer", "runtime": "store::RT",
            "sql": "SELECT id, upper(name) AS name, since FROM tb('store::DB.CUSTOMERS')\"""";

    /** Trade's SELECT: its side a code the enumeration's values are read from, trade_date read by upstream's camel case
     *  as tradeDate, cust kept as the key the association joins on; its tags, a list, left unmapped. */
    private static final String MAP_TRADE = """
            "class": "trades::domain::Trade", "unmapped": ["tags"],
            "enumerations": {"trades::domain::Side": {"BUY": ["B"], "SELL": ["S"]}},
            "sql": "SELECT trade_id AS id, qty AS quantity, side_code AS side, trade_date, cust AS customer_id \
            FROM tb('store::DB.TRADES') WHERE qty > 0\"""";

    private static final String JOIN = """
            "association": "trades::domain::Trade_Customer",
            "ends": [{"class": "trades::domain::Customer", "on": "id"}, {"class": "trades::domain::Trade", "on": "customerId"}]""";

    @Test
    @DisplayName("importSchema: each schema a class or enumeration, the reference an association, written as Pure")
    void importsAnOpenApiDocument() {
        Map<String, Object> a = importSchema(start(), OPENAPI);
        assertEquals(List.of("CREATE trades::domain::Side", "CREATE trades::domain::Customer",
                "CREATE trades::domain::Trade", "CREATE trades::domain::Trade_Customer"), changes(a));
        assertEquals("""
                Enum {meta::pure::profiles::doc.doc = 'Which way the trade goes'} trades::domain::Side
                {
                  BUY,
                  SELL
                }
                """, file(a, "trades::domain::Side"));
        assertEquals("""
                Class {meta::pure::profiles::doc.doc = 'A trade booked with a customer'} trades::domain::Trade
                {
                  id: Integer[1];
                  quantity: Float[1];
                  side: trades::domain::Side[1];
                  tradeDate: StrictDate[0..1];
                  tags: String[*];
                }
                """, file(a, "trades::domain::Trade"));
        assertEquals("""
                Association trades::domain::Trade_Customer
                {
                  customer: trades::domain::Customer[1];
                  trades: trades::domain::Trade[*];
                }
                """, file(a, "trades::domain::Trade_Customer"), "the reference one end, the way back the other");
        assertEquals(List.of("#/components/schemas/Customer/properties/name: maxLength is not carried (a validation, "
                + "not a shape)"), a.get("notes"));

        // imported again, over what it wrote: nothing changes
        Map<String, String> files = start();
        wrote(files, a);
        assertEquals(List.of(), changes(importSchema(files, OPENAPI)));
    }

    @Test
    @DisplayName("end to end: imported, mapped with SQL, joined -- and queried on DuckDB through the association")
    void endToEnd() throws Exception {
        Map<String, String> files = start();
        wrote(files, importSchema(files, OPENAPI));

        Map<String, Object> customer = wrote(files, mapClass(files, MAP_CUSTOMER));
        assertEquals(List.of("MODIFY store::RT", "CREATE trades::function::CustomerFn__Relation_1_",
                "CREATE trades::mapping::CustomerMapping", "CREATE trades::mapping::Model"), changes(customer),
                "the runtime named is made to name the model's mapping");
        assertEquals("""
                function trades::function::CustomerFn(): meta::pure::metamodel::relation::Relation<(id:Integer, \
                name:String, since:StrictDate)>[1]
                {
                  #SQL{SELECT id, upper(name) AS name, since FROM tb('store::DB.CUSTOMERS')}#
                }
                """, file(customer, "trades::function::CustomerFn__Relation_1_"));
        assertEquals(List.of("trades::domain::Customer.name is [1], and its column 'name' may be empty: a row without "
                + "one reads as no value"), customer.get("notes"), "id is NOT NULL in its table; upper(name) is not");

        Map<String, Object> trade = wrote(files, mapClass(files, MAP_TRADE));
        assertEquals("""
                ###Mapping
                Mapping trades::mapping::TradeMapping
                (
                  *trades::domain::Trade: Relation
                  {
                    ~func trades::function::TradeFn():Relation<Any>[1]
                    id: id,
                    quantity: quantity,
                    side: EnumerationMapping TradeSide : side,
                    tradeDate: trade_date,
                    +customerId: Integer[0..1]: customer_id
                  }

                  trades::domain::Side: EnumerationMapping TradeSide
                  {
                    BUY: ['B'],
                    SELL: ['S']
                  }
                )
                """, file(trade, "trades::mapping::TradeMapping"));
        assertEquals(List.of(Map.of("key", "customerId", "column", "customer_id", "type", "Integer")), trade.get("keys"));

        Map<String, Object> join = wrote(files, ModelChanges.joinAssociation(Json.parseObject(request(files, JOIN))));
        assertEquals(List.of("MODIFY trades::mapping::Model"), changes(join));
        assertTrue(file(join, "trades::mapping::Model").contains("{customer: trades::domain::Customer[1], trades: "
                + "trades::domain::Trade[1]|$customer.id == $trades.customerId}"), file(join, "trades::mapping::Model"));

        // the model, written by the three calls alone, run
        String model = text(files);
        QueryService qs = new QueryService();
        try (Connection conn = TestBackends.platform().open(); Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE TRADES (trade_id INTEGER, qty DOUBLE, side_code VARCHAR, trade_date DATE, "
                    + "cust INTEGER)");
            s.execute("INSERT INTO TRADES VALUES (1, 10.0, 'B', DATE '2026-01-05', 100), (2, 5.0, 'S', DATE "
                    + "'2026-01-06', 100), (3, 7.5, 'S', NULL, 200), (4, 0, 'B', NULL, 200), (5, 3.0, 'B', NULL, NULL)");
            s.execute("CREATE TABLE CUSTOMERS (id INTEGER NOT NULL, name VARCHAR, since DATE)");
            s.execute("INSERT INTO CUSTOMERS VALUES (100, 'acme', DATE '2020-01-01'), (200, 'globex', NULL), "
                    + "(300, 'initech', NULL)");
            // to one: each trade (the SELECT's WHERE leaves trade 4 out), its side from its code, its customer's name
            assertEquals(List.of("1 BUY ACME", "2 SELL ACME", "3 SELL GLOBEX", "5 BUY null"), rows(qs.execute(model,
                    "trades::domain::Trade.all()->project(~[id: x|$x.id, side: x|$x.side, customer: x|$x.customer.name])"
                            + "->sort(~id->ascending())", "store::RT", conn)));
            // to many: the customers with a trade selling
            assertEquals(List.of("ACME", "GLOBEX"), rows(qs.execute(model, "trades::domain::Customer.all()"
                    + "->filter(c|$c.trades->exists(t|$t.side == trades::domain::Side.SELL))"
                    + "->project(~[name: x|$x.name])->sort(~name->ascending())", "store::RT", conn)));
        }
    }

    private static List<String> rows(ExecutionResult r) {
        List<String> out = new ArrayList<>();
        for (var row : r.rows()) {
            List<String> cells = new ArrayList<>();
            for (Object v : row.values()) {
                cells.add(String.valueOf(v));
            }
            out.add(String.join(" ", cells));
        }
        return out;
    }

    @Test
    @DisplayName("a SELECT that does not fit the class is refused, every misfit named, nothing written")
    void aSelectThatDoesNotFitIsRefused() {
        Map<String, String> files = start();
        wrote(files, importSchema(files, OPENAPI));
        ModelException e = assertThrows(ModelException.class, () -> mapClass(files, """
                "class": "trades::domain::Trade", "runtime": "store::RT",
                "sql": "SELECT trade_id AS id, side_code AS quantity, cust AS side, trade_date AS \\"tradeDate\\" \
                FROM tb('store::DB.TRADES')\""""));
        assertEquals("the SQL does not fit trades::domain::Trade:\n"
                + "- trades::domain::Trade.quantity is Float, its column 'quantity' is String: cast it in the SQL\n"
                + "- trades::domain::Trade.side is the enumeration trades::domain::Side, read from text; its column "
                + "'side' is Integer\n"
                + "- trades::domain::Trade.tags holds many values, which one column does not: leave it unmapped",
                e.getMessage().substring(e.getMessage().indexOf("the SQL")));
        assertTrue(assertThrows(ModelException.class, () -> mapClass(files, """
                "class": "trades::domain::Trade", "runtime": "store::RT", "unmapped": ["tags"],
                "sql": "SELECT trade_id AS id FROM tb('store::DB.TRADES')\"""")).getMessage().contains(
                "- trades::domain::Trade.quantity has no column (the SELECT's: id)"));
        assertTrue(assertThrows(ModelException.class, () -> mapClass(files, """
                "class": "trades::domain::Trade", "runtime": "store::RT",
                "sql": "SELECT t.nope AS id FROM tb('store::DB.TRADES') t\"""")).getMessage().contains(
                "column t.nope does not exist"));
    }

    @Test
    @DisplayName("each reference's way back is named apart: two to one class, a class to itself, a value its owner holds")
    void waysBackAreNamedApart() {
        Map<String, Object> a = importSchema(start(), """
                {"openapi": "3.1.0", "components": {"schemas": {
                  "Party": {"type": "object", "properties": {"id": {"type": "integer"}}},
                  "Trade": {"type": "object", "properties": {
                    "buyer": {"$ref": "#/components/schemas/Party"},
                    "seller": {"$ref": "#/components/schemas/Party"},
                    "settlement": {"type": "object", "properties": {"date": {"type": "string", "format": "date"}}}}},
                  "Employee": {"type": "object", "properties": {"manager": {"$ref": "#/components/schemas/Employee"}}}}}}
                """);
        assertTrue(file(a, "trades::domain::Trade_Buyer").contains("buyer: trades::domain::Party[0..1];\n  tradesAsBuyer: "
                + "trades::domain::Trade[*];"), file(a, "trades::domain::Trade_Buyer"));
        assertTrue(file(a, "trades::domain::Trade_Seller").contains("tradesAsSeller: trades::domain::Trade[*];"),
                file(a, "trades::domain::Trade_Seller"));
        assertTrue(file(a, "trades::domain::Trade_Settlement").contains("settlement: trades::domain::TradeSettlement[0..1];"
                + "\n  trade: trades::domain::Trade[0..1];"), file(a, "trades::domain::Trade_Settlement"));
        assertTrue(file(a, "trades::domain::Employee_Manager").contains("employeesAsManager: trades::domain::Employee[*];"),
                file(a, "trades::domain::Employee_Manager"));
    }

    @Test
    @DisplayName("a subclass's SELECT reads its superclass's properties too; an enumeration's values by their own names")
    void inheritedPropertiesAndPlainEnumerations() {
        Map<String, String> files = start();
        wrote(files, importSchema(files, """
                {"openapi": "3.1.0", "components": {"schemas": {
                  "Party": {"type": "object", "required": ["id"], "properties": {"id": {"type": "integer"}}},
                  "Firm": {"allOf": [{"$ref": "#/components/schemas/Party"}, {"type": "object", "properties": {
                    "kind": {"type": "string", "enum": ["BANK", "FUND"]}}}]}}}}
                """));
        assertTrue(files.get("trades::domain::Firm").startsWith("Class trades::domain::Firm extends "
                + "trades::domain::Party"), files.get("trades::domain::Firm"));
        Map<String, Object> firm = mapClass(files, """
                "class": "trades::domain::Firm", "runtime": "store::RT",
                "sql": "SELECT id, CASE WHEN id > 150 THEN 'FUND' ELSE 'BANK' END AS kind FROM tb('store::DB.CUSTOMERS')\"""");
        String mapping = file(firm, "trades::mapping::FirmMapping");
        assertTrue(mapping.contains("id: id,\n    kind: EnumerationMapping FirmKind : kind"), mapping);
        assertTrue(mapping.contains("trades::domain::FirmKind: EnumerationMapping FirmKind\n  {\n    BANK: ['BANK'],\n"
                + "    FUND: ['FUND']\n  }"), mapping);
    }

    @Test
    @DisplayName("joinAssociation: an end joined on what its class does not have, or on values of another type, is refused")
    void aJoinThatCannotBeIsRefused() {
        Map<String, String> files = start();
        wrote(files, importSchema(files, OPENAPI));
        wrote(files, mapClass(files, MAP_CUSTOMER));
        wrote(files, mapClass(files, MAP_TRADE));
        assertTrue(assertThrows(ModelException.class, () -> ModelChanges.joinAssociation(Json.parseObject(request(files,
                JOIN.replace("\"on\": \"customerId\"", "\"on\": \"cust\""))))).getMessage().contains(
                "trades::domain::Trade has no property or key 'cust' (a key is a column its SQL keeps)"));
        assertTrue(assertThrows(ModelException.class, () -> ModelChanges.joinAssociation(Json.parseObject(request(files,
                JOIN.replace("\"on\": \"customerId\"", "\"on\": \"quantity\""))))).getMessage().contains(
                "an association joins equal values: trades::domain::Customer.id is Integer, trades::domain::Trade.quantity "
                        + "Float"));
    }

    private static final PureV1Api.Runner NO_RUN = (model, lambda, runtime, rows) -> {
        throw new AssertionError("nothing is run here");
    };

    @Test
    @DisplayName("through pure/v1's routing: answered as the SDLC's changes; refused as a model that does not compile")
    void throughTheRoutes() {
        PureV1Api.Answer ok = PureV1Api.route("/api/lite/v1/model/importSchema", null, request(start(),
                "\"schema\": {\"format\": \"openApi\", \"content\": " + Json.toCompact(OPENAPI) + "}"), NO_RUN);
        assertEquals(200, ok.status(), ok.json());
        assertEquals(4, Json.parseObject(ok.json()).getArr("changes").items().size());
        PureV1Api.Answer refused = PureV1Api.route("/api/lite/v1/model/importSchema", null, request(start(),
                "\"schema\": {\"format\": \"openApi\", \"content\": \"openapi: 3.0.0\"}"), NO_RUN);
        assertEquals(400, refused.status(), refused.json());
        Json.Obj error = Json.parseObject(refused.json());
        assertEquals("COMPILATION", error.getString("errorType"));
        assertTrue(error.getString("message").contains("a YAML document is not read yet"), error.getString("message"));
    }
}
