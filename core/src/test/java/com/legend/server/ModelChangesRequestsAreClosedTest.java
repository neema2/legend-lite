// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.legend.json.Json;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * THE LITE ROUTES' BODIES ARE CLOSED (the orchestrator's ruling (a) of 2026-11-13, on the C worker's finding: these
 * shapes are lite's own, so nothing upstream keeps them open). Each of {@code /api/lite/v1/model/createClass},
 * {@code createAssociation} and {@code createDataSpace} is given a request carrying EVERY key the model writer's wire
 * sends ({@code model-changes/src/wire.ts}'s {@code modelRequestBody}: the one body DataCube, the tab and Python's host
 * send) -- and answers it. Then the same request with one key it has not ({@code extra}) put in each of its objects in
 * turn -- the request, the project's and each source's model, each class, its place, each column's choice, each table's
 * sample, the association, each end, the data space -- is refused, by that object's name and the key. A sample naming a
 * table the relation does not read is refused too (until then its rows were passed over).
 */
class ModelChangesRequestsAreClosedTest {

    private static final String UNKNOWN = "extra";
    private static final String REFUSED = " has no key " + UNKNOWN + " (names and fields only, never a value)";

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

    private static Map<String, Object> obj(Object... kv) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            out.put((String) kv[i], kv[i + 1]);
        }
        return out;
    }

    private static Map<String, Object> text(String code) {
        return obj("_type", "text", "code", code);
    }

    /** A class action with every key the wire writes: its relation, source, name, documentation, columns' choices
     *  (each with every key), and its place with a sample of the table it reads. */
    private static Map<String, Object> cls(String source, String table, String name, Map<String, Object> columns,
            List<String> sampleColumns, List<Object> sampleRow) {
        return obj("lambda", Json.parse(com.legend.parser.SpecParser.lambdaJson("|#>{local::DB." + table + "}#", false)),
                "source", text(source), "name", name, "documentation", "the " + name + "s", "columns", columns,
                "where", obj("from", "local::DB", "as", "Browser", "samples", obj(table, obj("columns", sampleColumns,
                        "rows", List.of(sampleRow))), "runtime", "local::RT"));
    }

    private static Map<String, Object> trade() {
        // a key's documentation is refused by name (F1, 2026-11-13): the documentation is a property's
        return cls(TRADES, "trades", "Trade", obj("customer_id", obj("as", "key", "name", "customerId"), "notional",
                obj("as", "property", "name", "amount", "documentation", "the amount")),
                List.of("trade_id", "customer_id", "notional"), List.of(1, 7, 2.5));
    }

    private static Map<String, Object> customer() {
        return cls(CUSTOMERS, "customers", "Customer", obj("name", obj("as", "property", "name", "fullName",
                "documentation", "the customer's name")), List.of("id", "name"), List.of(7, "Ann"));
    }

    private static Map<String, Object> association() {
        return obj("name", "Customer_Trade", "ends", List.of(
                obj("class", "trades::domain::Customer", "on", "id", "name", "customer", "multiplicity", "[0..1]",
                        "unique", true),
                obj("class", "trades::domain::Trade", "on", "customerId", "name", "trades", "multiplicity", "[*]",
                        "unique", false)));
    }

    private static Map<String, Object> request(Object... rest) {
        Map<String, Object> out = obj("project", text(""), "package", "trades");
        out.putAll(obj(rest));
        return out;
    }

    /** Each route: its name, and a request carrying every key its wire sends. */
    private record Case(String route, Map<String, Object> request) {
    }

    private static List<Case> cases() {
        return List.of(
                new Case("createClass", request("class", trade())),
                new Case("createAssociation", request("classes", List.of(trade(), customer()),
                        "association", association())),
                new Case("createDataSpace", request("classes", List.of(trade(), customer()),
                        "associations", List.of(association()), "dataSpace", obj("name", "Trades",
                                "title", "Trades", "description", "the desk's trades",
                                "classes", List.of("trades::domain::Trade")))));
    }

    private static PureV1Api.Answer route(String route, Object request) {
        return PureV1Api.route("/api/lite/v1/model/" + route, null, Json.toCompact(request), (q, r, o) -> {
            throw new AssertionError("a model change runs nothing");
        });
    }

    /** Every JSON object of {@code node} whose keys are a reader's (not a map by name: columns' choices, samples), by
     *  its path from {@code at}. */
    private static void objects(Object node, String at, List<String> out) {
        if (node instanceof Map<?, ?> m) {
            if (!at.endsWith(".columns") && !at.endsWith(".samples")) {
                out.add(at);
            }
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
    void everyRouteAnswersEveryKeyItsWireSendsAndRefusesAnUnknownKeyInEachOfItsObjectsByName() {
        List<String> wrong = new ArrayList<>();
        int refused = 0;
        for (Case c : cases()) {
            PureV1Api.Answer whole = route(c.route(), c.request());
            if (whole.status() != 200) {
                wrong.add(c.route() + ": its request with every key is refused: " + whole.json());
                continue;
            }
            List<String> paths = new ArrayList<>();
            objects(c.request(), "$", paths);
            for (String path : paths) {
                PureV1Api.Answer a = route(c.route(), withUnknownKey(c.request(), "$", path));
                String message = a.status() == 200 ? "" : Json.parseObject(a.json()).getStringOr("message", "");
                if (a.status() == 200) {
                    wrong.add(c.route() + " at " + path + ": an unknown key is read past");
                } else if (!message.endsWith(REFUSED)) {
                    wrong.add(c.route() + " at " + path + ": refused, but not by its name: " + message);
                } else {
                    refused++;
                }
            }
        }
        assertEquals(List.of(), wrong);
        // the count pins the walk itself: an object dropped from a case would shrink it
        assertEquals(EXPECTED_OBJECTS, refused, "the objects walked");
    }

    /** The objects walked: createClass 8 (the request, the project, and the class's 6: itself, its source, its two
     *  columns' choices, its place, its sample); createAssociation 16 (the request, the project, the trade class's 6 and
     *  the customer's 5, the association and its two ends); createDataSpace 17 (those, the association one of a list,
     *  and the data space). */
    private static final int EXPECTED_OBJECTS = 8 + 16 + 17;

    /** A sample of a table the relation does not read is refused by name: its rows would be read by nothing. */
    @Test
    void aSampleOfATableTheRelationDoesNotReadIsRefused() {
        Map<String, Object> cls = trade();
        @SuppressWarnings("unchecked")
        Map<String, Object> where = (Map<String, Object>) cls.get("where");
        where.put("samples", obj("other", obj("columns", List.of("x"), "rows", List.of())));
        PureV1Api.Answer a = route("createClass", request("class", cls));
        assertEquals(400, a.status(), a.json());
        assertEquals("a sample of the table 'other', which the relation does not read",
                Json.parseObject(a.json()).getString("message"));
    }
}
