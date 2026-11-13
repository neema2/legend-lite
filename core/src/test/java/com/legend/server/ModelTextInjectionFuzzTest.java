// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.json.Json;
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
 * INJECTION FUZZING OF THE MODEL TEXT THE LITE ROUTES WRITE (the orchestrator's go of 2026-11-13: a class of its own,
 * beside the SQL fuzz). {@code /api/lite/v1/model/createClass}, {@code createAssociation} and {@code createDataSpace}
 * write Pure model TEXT from a request's names and values -- a package, a class's, a property's, an association's, an
 * end's and a data space's name, a title, a description, a documentation, a sample's rows, and the source model's own
 * table and column names. Each payload is planted in each such place in turn; the route answers it, or refuses it by
 * name. An answer's files are read back by lite's own parser, and held to the request:
 *
 * <ul>
 *   <li>every file parses;</li>
 *   <li>no element is added, removed or reshaped: the files' shape (each element's kind, every key, every list's
 *       length, and a sample's CSV rows and cells) is the unplanted request's;</li>
 *   <li>the payload reads back as it was written: a name, a text, or a sample's cell, exactly (a title and a description
 *       as the route strips them); a sample row holding a line break is left out and said so.</li>
 * </ul>
 *
 * <p>The findings are pinned by route and place (KNOWN, shrink-only): each is a defect named until it is fixed.
 */
class ModelTextInjectionFuzzTest {

    private static final String MARK = "ZQXMARK";

    /** The defects found, by route and place: shrink-only, each named until fixed. Empty since 2026-11-13: its two
     *  findings (a key's documentation read and written nowhere; a source column whose name holds a line break splitting
     *  its sample's CSV header) went with the user's rulings F1 (refused by name) and F2 (the sample left out, said). */
    static final Map<String, Set<String>> KNOWN = Map.of();

    /** The payloads: the SQL fuzz's (com.legend.sql.dialect.InjectionFuzzTest), and Pure's own -- a quote, a
     *  backslash, an island's or a block's close, a section's header, a path's separator, white space. */
    static final List<String> PAYLOADS = List.of(
            MARK, "a'" + MARK, "a''" + MARK, "a\\'" + MARK, "a\\\\'" + MARK, "a\"" + MARK, "a\\\"" + MARK,
            "a`" + MARK, MARK + "\\", "a -- " + MARK, "a /* " + MARK + " */", "a // " + MARK, "a\n" + MARK,
            "a\r\n" + MARK, "a\t" + MARK, "a $$ " + MARK, "a\\0" + MARK, "a\\n" + MARK,
            "a';\n}\nClass x::Injected {}\n" + MARK, "a}#\n###Pure\nClass x::Injected {}\n" + MARK,
            "a)\n" + MARK, "a]; " + MARK, "a{" + MARK + "}", "a::" + MARK, "a, " + MARK, "a.b " + MARK,
            "  " + MARK + "  ", "a$" + MARK, "a#" + MARK);

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

    /** A name in a Database's text: bare when it is a plain identifier, else quoted, its quote and backslash escaped
     *  (a quoted name in a Database keeps what the database's catalog names: CatalogModel.ident's rule). */
    private static String quotedName(String name) {
        return name.matches("[A-Za-z_][A-Za-z0-9_]*") ? name
                : "\"" + name.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    /** The trades table's source, a column or the table named {@code column} or {@code table} where given. */
    private static String trades(String table, String column) {
        return TRADES.replace("Table trades", "Table " + quotedName(table)).replace("notional DOUBLE",
                quotedName(column) + " DOUBLE");
    }

    /** A relation reading {@code table} of {@code local::DB}, as protocol JSON (the path carries any name). */
    private static Object relation(String table) {
        Json.Obj lambda = Json.parseObject(com.legend.parser.SpecParser.lambdaJson("|#>{local::DB.trades}#", false));
        return Json.parse(Json.toCompact(lambda).replace("\"trades\"", Json.toCompact(Json.str(table))));
    }

    private static Map<String, Object> trade(String table, String column, List<Object> row) {
        Map<String, Object> columns = obj("customer_id", obj("as", "key", "name", "customerId"));
        columns.put(column, obj("as", "property", "name", "amount", "documentation", "the amount"));
        return obj("lambda", relation(table), "source", obj("_type", "text", "code", trades(table, column)),
                "name", "Trade", "documentation", "the trades", "columns", columns,
                "where", obj("from", "local::DB", "as", "Browser", "runtime", "local::RT", "samples", obj(table,
                        obj("columns", List.of("trade_id", "customer_id", column), "rows", List.of(row)))));
    }

    private static Map<String, Object> customer() {
        return obj("lambda", Json.parse(com.legend.parser.SpecParser.lambdaJson("|#>{local::DB.customers}#", false)),
                "source", obj("_type", "text", "code", CUSTOMERS), "name", "Customer", "where", obj("from",
                        "local::DB", "as", "Browser", "runtime", "local::RT"));
    }

    private static Map<String, Object> association() {
        return obj("name", "Customer_Trade", "ends", List.of(
                obj("class", "trades::domain::Customer", "on", "id", "name", "customer", "multiplicity", "[0..1]"),
                obj("class", "trades::domain::Trade", "on", "customerId", "name", "trades", "multiplicity", "[*]")));
    }

    private static Map<String, Object> request(Object... rest) {
        Map<String, Object> out = obj("project", obj("_type", "text", "code", ""), "package", "trades");
        out.putAll(obj(rest));
        return out;
    }

    /** A place a payload is planted: its name, and the request with it. */
    private record Place(String name, Function<String, Map<String, Object>> request) {
    }

    /** A route, its places. */
    private record Route(String route, Map<String, Object> base, List<Place> places) {
    }

    private static final List<Object> ROW = List.of(1, 7, 2.5);

    @SuppressWarnings("unchecked")
    private static Map<String, Object> at(Map<String, Object> o, String... path) {
        Map<String, Object> m = o;
        for (String p : path) {
            Object next = m.get(p);
            m = next instanceof List<?> l ? (Map<String, Object>) l.get(0) : (Map<String, Object>) next;
        }
        return m;
    }

    /** {@code base()} with the text at {@code path}'s {@code key} the payload. */
    private static Place set(String name, java.util.function.Supplier<Map<String, Object>> base, String key,
            String... path) {
        return new Place(name, p -> {
            Map<String, Object> r = base.get();
            at(r, path).put(key, p);
            return r;
        });
    }

    private static List<Route> routes() {
        java.util.function.Supplier<Map<String, Object>> cls = () -> request("class", trade("trades", "notional", ROW));
        List<Place> classPlaces = new ArrayList<>(List.of(
                set("the package", cls, "package"),
                set("the class's name", cls, "name", "class"),
                set("the class's documentation", cls, "documentation", "class"),
                set("a property's name", cls, "name", "class", "columns", "notional"),
                set("a property's documentation", cls, "documentation", "class", "columns", "notional"),
                set("a key's name", cls, "name", "class", "columns", "customer_id"),
                set("a key's documentation", cls, "documentation", "class", "columns", "customer_id"),
                set("the place's name", cls, "as", "class", "where"),
                new Place("the source's column", p -> request("class", trade("trades", p, ROW))),
                new Place("the source's table", p -> request("class", trade(p, "notional", ROW))),
                new Place("a sample's text", p -> request("class", trade("trades", "notional", List.of(1, 7, p))))));
        java.util.function.Supplier<Map<String, Object>> assoc = () -> request("classes", List.of(
                trade("trades", "notional", ROW), customer()), "association", association());
        List<Place> associationPlaces = List.of(
                set("the association's name", assoc, "name", "association"),
                set("an end's name", assoc, "name", "association", "ends"));
        java.util.function.Supplier<Map<String, Object>> space = () -> request("classes", List.of(
                trade("trades", "notional", ROW), customer()), "dataSpace", obj("name", "Trades", "title", "Trades",
                "description", "the desk's trades"));
        List<Place> spacePlaces = List.of(
                set("the data space's name", space, "name", "dataSpace"),
                set("the data space's title", space, "title", "dataSpace"),
                set("the data space's description", space, "description", "dataSpace"));
        return List.of(new Route("createClass", cls.get(), classPlaces),
                new Route("createAssociation", assoc.get(), associationPlaces),
                new Route("createDataSpace", space.get(), spacePlaces));
    }

    // ---- reading an answer back

    /** An answer's files, each read back by lite's parser: its elements, as protocol JSON; a file that does not parse
     *  is named in {@code wrong}. */
    private static List<Object> readBack(String answer, List<String> wrong) {
        List<Object> out = new ArrayList<>();
        for (Json.Node c : Json.parseObject(answer).getArr("changes").items()) {
            String code = ((Json.Obj) c).getString("pureCode");
            String text = code.startsWith("###") ? code : "###Pure\n" + code;
            try {
                Json.Obj parsed = Json.parseObject(com.legend.parser.PmcdParser.parseDocument(text, false));
                for (Json.Node e : parsed.getArr("elements").items()) {
                    if (!"sectionIndex".equals(((Json.Obj) e).getStringOr("_type", ""))) {
                        out.add(e);
                    }
                }
            } catch (RuntimeException e) {
                wrong.add("the file " + ((Json.Obj) c).getString("path") + " does not parse: " + e.getMessage());
            }
        }
        return out;
    }

    /** A sample's CSV, as RFC 4180 reads it: its rows, each its cells. */
    static List<List<String>> csv(String text) {
        List<List<String>> rows = new ArrayList<>();
        List<String> row = new ArrayList<>();
        StringBuilder cell = new StringBuilder();
        boolean quoted = false;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (quoted) {
                if (c == '"' && i + 1 < text.length() && text.charAt(i + 1) == '"') {
                    cell.append('"');
                    i++;
                } else if (c == '"') {
                    quoted = false;
                } else {
                    cell.append(c);
                }
            } else if (c == '"') {
                quoted = true;
            } else if (c == ',') {
                row.add(cell.toString());
                cell.setLength(0);
            } else if (c == '\n') {
                row.add(cell.toString());
                cell.setLength(0);
                rows.add(row);
                row = new ArrayList<>();
            } else {
                cell.append(c);
            }
        }
        if (cell.length() > 0 || !row.isEmpty()) {
            row.add(cell.toString());
            rows.add(row);
        }
        return rows;
    }

    /** The shape of read-back elements: every kind ({@code _type}) as it is, every other string a placeholder, a
     *  sample's CSV its rows' lengths. */
    private static Object shape(Object node, String key) {
        return switch (node) {
            case Json.Obj o -> {
                Map<String, Object> out = new TreeMap<>();
                o.fields().forEach((k, v) -> out.put(k, shape(v, k)));
                yield out;
            }
            case Json.Arr a -> a.items().stream().map(n -> shape(n, key)).toList();
            case List<?> l -> l.stream().map(n -> shape(n, key)).toList();
            case Json.Str s when "values".equals(key) -> csv(s.value()).stream().map(List::size).toList();
            // an element's or a node's kind is its shape
            case Json.Str s when "_type".equals(key) -> s.value();
            case Json.Str s -> "S";
            default -> node instanceof Json.Node n ? Json.toCompact(n) : String.valueOf(node);
        };
    }

    /** Every string of read-back elements, a sample's CSV as its cells. */
    private static void strings(Object node, String key, Set<String> out) {
        switch (node) {
            case Json.Obj o -> o.fields().forEach((k, v) -> strings(v, k, out));
            case Json.Arr a -> a.items().forEach(n -> strings(n, key, out));
            case List<?> l -> l.forEach(n -> strings(n, key, out));
            case Json.Str s when "values".equals(key) -> csv(s.value()).forEach(out::addAll);
            case Json.Str s -> out.add(s.value());
            default -> {
            }
        }
    }

    // ---- the fuzz

    private static PureV1Api.Answer route(String route, Object request) {
        return PureV1Api.route("/api/lite/v1/model/" + route, null, Json.toCompact(request), (q, r, o) -> {
            throw new AssertionError("a model change runs nothing");
        });
    }

    /** Every place where a payload changed the model's text, by route; the counts in {@code counts}. */
    static Map<String, Set<String>> fuzz(List<Route> routes, Map<String, Integer> counts, List<String> examples) {
        Map<String, Set<String>> found = new TreeMap<>();
        for (Route r : routes) {
            PureV1Api.Answer base = route(r.route(), r.base());
            assertEquals(200, base.status(), r.route() + ": " + base.json());
            List<String> baseWrong = new ArrayList<>();
            Object baseShape = shape(readBack(base.json(), baseWrong), null);
            assertEquals(List.of(), baseWrong, r.route());
            for (Place place : r.places()) {
                for (String payload : PAYLOADS) {
                    PureV1Api.Answer a = route(r.route(), place.request().apply(payload));
                    if (a.status() != 200) {
                        counts.merge("refused", 1, Integer::sum);
                        continue;
                    }
                    counts.merge("written", 1, Integer::sum);
                    List<String> wrong = new ArrayList<>();
                    List<Object> elements = readBack(a.json(), wrong);
                    // a line break in a sample's cell leaves its row out; in a column's name, the sample (F2)
                    boolean leftOut = (place.name().equals("a sample's text") || place.name().equals("the source's column"))
                            && (payload.indexOf('\n') >= 0 || payload.indexOf('\r') >= 0);
                    if (wrong.isEmpty() && !leftOut && !shape(elements, null).equals(baseShape)) {
                        wrong.add("an element added, removed or reshaped");
                    }
                    Set<String> read = new TreeSet<>();
                    strings(elements, null, read);
                    boolean stripped = place.name().endsWith("title") || place.name().endsWith("description");
                    if (leftOut) {
                        if (Json.parseObject(a.json()).getArr("notes").items().isEmpty()) {
                            wrong.add("a sample row or a sample holding a line break left out, unsaid");
                        } else if (place.name().equals("the source's column") && !read.contains(payload)) {
                            wrong.add("the column's name does not read back as written");
                        }
                    } else if (!read.contains(stripped ? payload.strip() : payload) && !(place.name().equals(
                            "the package") && read.stream().anyMatch(l -> l.startsWith(payload + "::")))) {
                        wrong.add("the payload does not read back as written");
                    } else {
                        counts.merge("read back exactly", 1, Integer::sum);
                    }
                    if (!wrong.isEmpty()) {
                        if (found.computeIfAbsent(r.route(), k -> new TreeSet<>()).add(place.name())) {
                            examples.add(r.route() + ", " + place.name() + ", " + payload.replace("\n", "\\n")
                                    .replace("\r", "\\r").replace("\t", "\\t") + ": " + wrong.get(0));
                        }
                    }
                }
            }
        }
        return found;
    }

    @Test
    void noNameOrTextChangesTheModelsShape() {
        Map<String, Integer> counts = new TreeMap<>();
        List<String> examples = new ArrayList<>();
        Map<String, Set<String>> found = fuzz(routes(), counts, examples);
        assertTrue(counts.getOrDefault("read back exactly", 0) > 0, "nothing was written: " + counts);
        assertEquals(KNOWN, found, counts + "; one example each:\n" + String.join("\n", examples));
    }

    /** F1 (the user's ruling of 2026-11-13): a key's documentation, which no file would hold, is refused by name --
     *  until then read and dropped unsaid. */
    @Test
    void aKeysDocumentationIsRefusedByName() {
        Map<String, Object> cls = trade("trades", "notional", ROW);
        at(cls, "columns", "customer_id").put("documentation", "the customer's id");
        PureV1Api.Answer a = route("createClass", request("class", cls));
        assertEquals(400, a.status(), a.json());
        assertEquals("the column 'customer_id' is taken as a key only, and a documentation is written on a property"
                + " alone: take it off, or take the column as a property", Json.parseObject(a.json()).getString("message"));
    }

    /** F3 (the user's ruling of 2026-11-13): a source column whose name holds a quote and a backslash -- declared
     *  quoted, its escapes the lexer's -- takes its sample: the route reads the protocol's name as the compiled model
     *  does, and the sample's header names the column exactly. Until then the route kept the escapes, and the sample was
     *  refused as naming another column. */
    @Test
    void aColumnNamedWithAQuoteAndABackslashTakesItsSample() {
        String name = "a\"b\\c";
        PureV1Api.Answer a = route("createClass", request("class", trade("trades", name, ROW)));
        assertEquals(200, a.status(), a.json());
        List<String> wrong = new ArrayList<>();
        List<Object> elements = readBack(a.json(), wrong);
        assertEquals(List.of(), wrong);
        Set<String> read = new TreeSet<>();
        strings(elements, null, read);
        assertTrue(read.contains(name), "the sample's header and the mapping name the column exactly: " + read);
        assertTrue(Json.parseObject(a.json()).getArr("notes").items().isEmpty(), a.json());
    }

    /** The read-back bites: a sample CSV read as its rows and cells, and one that gained a row is another shape. */
    @Test
    void theReadBackHoldsASamplesRowsAndCells() {
        assertEquals(List.of(List.of("a", "b,c"), List.of("d", "e\"f")), csv("a,\"b,c\"\nd,\"e\"\"f\"\n"));
        assertTrue(!shape(List.of(Json.parse("{\"values\":\"a,b\\nc,d\\n\"}")), null)
                .equals(shape(List.of(Json.parse("{\"values\":\"a,b\\nc,d\\ne,f\\n\"}")), null)));
    }
}
