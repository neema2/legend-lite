// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.deephaven;

import com.legend.Compiler;
import com.legend.json.Json;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deephaven's wire (docs/DEEPHAVEN_PLAN_2026_10_22.md DH11, DH22): a plan's batch encodes to the bytes protobuf's
 * own library writes for it; a field with a zigzag or a negative number is written as protobuf writes it; what the
 * target writes for DataCube's shapes encodes and reads back; and what the table does not hold is refused by name.
 */
class DeephavenWireTest {

    private static final HexFormat HEX = HexFormat.of();

    private record Vector(String query, String plan, String hex) {
    }

    private static List<Vector> vectors() throws IOException {
        String text;
        try (InputStream in = Objects.requireNonNull(
                DeephavenWireTest.class.getResourceAsStream("/deephaven/wire-vectors.txt"))) {
            text = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        List<Vector> out = new ArrayList<>();
        String query = null;
        String plan = null;
        for (String line : text.split("\n")) {
            if (line.startsWith("QUERY ")) {
                query = line.substring(6);
            } else if (line.startsWith("PLAN ")) {
                plan = line.substring(5);
            } else if (line.startsWith("HEX ")) {
                out.add(new Vector(Objects.requireNonNull(query), Objects.requireNonNull(plan), line.substring(4)));
            }
        }
        return out;
    }

    @Test
    void aPlanEncodesToTheBytesProtobufsOwnLibraryWrites() throws IOException {
        List<Vector> vectors = vectors();
        assertEquals(4, vectors.size());
        for (Vector v : vectors) {
            assertEquals(v.hex(), HEX.formatHex(DeephavenWire.batch(v.plan(), 7)), v.query());
        }
    }

    @Test
    void zigzagAndNegativeNumbersAreWrittenAsProtobufWritesThem() {
        // a table reference's offset is a sint32 (the browser check's first failure): 1 is 2 on the wire, and 0 is
        // written, a oneof's member having presence
        assertEquals("1002", hex("TableReference", "{\"batchOffset\": 1}"));
        assertEquals("1000", hex("TableReference", "{\"batchOffset\": 0}"));
        // a sint64, given as the JSON form's string
        assertEquals("1002", hex("EmptyTableRequest", "{\"size\": \"1\"}"));
        assertEquals("10fe07", hex("EmptyTableRequest", "{\"size\": \"511\"}"));
        // an enum at -1 is its 64-bit two's complement, ten bytes
        assertEquals("0a0178" + "18ffffffffffffffffff01",
                hex("SortDescriptor", "{\"columnName\": \"x\", \"direction\": \"DESCENDING\"}"));
        // a field without presence at its default is left out
        assertEquals("", hex("AggregateRequest", "{\"preserveEmpty\": false, \"groupByColumns\": []}"));
    }

    @Test
    void publishAndReleaseNameTheirTickets() {
        assertEquals("0a070a05650700000012070a05682f616263", HEX.formatHex(DeephavenWire.publish(7, "abc")));
        assertEquals("0a070a056507000000", HEX.formatHex(DeephavenWire.releaseExport(7)));
    }

    @Test
    void aCreationAnswerIsReadAndAFieldLiteDoesNotReadIsSkipped() {
        assertEquals(new DeephavenWire.Created(true, ""), DeephavenWire.created(HEX.parseHex("1001")));
        // success left out (false), the error, then a schema (field 4) lite does not read
        DeephavenWire.Created failed = DeephavenWire.created(HEX.parseHex("1a03626164" + "2203010203"));
        assertEquals(new DeephavenWire.Created(false, "bad"), failed);
    }

    /** What the target writes for DataCube's shapes on a Deephaven runtime encodes, and reads back to the same bytes. */
    @Test
    void whatTheTargetWritesEncodesAndReadsBack() {
        String model = """
                ###Relational
                Database risk::Ref
                (
                  Table trades (id INTEGER PRIMARY KEY, region VARCHAR(16), country VARCHAR(16), desk VARCHAR(16),
                                year INTEGER, qty BIGINT, notional DOUBLE, pnl DOUBLE, instrument VARCHAR(16))
                  Table instrument (iid VARCHAR(16) PRIMARY KEY, sector VARCHAR(16), ccy VARCHAR(3) NOT NULL)
                )
                ###Connection
                RelationalDatabaseConnection risk::Conn
                {
                  store: risk::Ref;
                  type: DuckDB;
                  specification: DuckDB { };
                  auth: Test;
                }
                ###Runtime
                Runtime risk::RT
                {
                  mappings: [];
                  connections: [ risk::Ref: [ c: risk::Conn ] ];
                }
                """;
        // the connection made a Deephaven one here, so the text the own-corpus census harvests is the engine's grammar
        // (lite's Deephaven words are parser/DeephavenConnectionTest's to hold)
        String deephaven = model.replace("type: DuckDB;\n  specification: DuckDB { };\n  auth: Test;",
                "type: Deephaven;\n  specification: Deephaven { host: 'localhost'; port: 8443; useTls: true; };\n"
                        + "  auth: PreSharedKey { keyVaultReference: 'deephaven.psk'; };");
        assertTrue(deephaven.contains("type: Deephaven;"), deephaven);
        var ctx = Compiler.compileModel(deephaven);
        List<String> queries = List.of(
                "#>{risk::Ref.trades}#->groupBy(~[region, country], ~['2023__|__total':x|if($x.year == 2023,"
                        + " |$x.notional, |[]):y|$y->sum(), total:x|$x.notional:y|$y->sum()])"
                        + "->sort([~region->ascending(), ~country->ascending()])",
                "#>{risk::Ref.trades}#->extend(~[__root__:x|'[ROOT]'])->groupBy(~[__root__], ~[total:x|$x.notional:y|"
                        + "$y->sum(), n:x|$x.id:y|$y->count()])",
                "#>{risk::Ref.trades}#->groupBy(~[desk], ~[s:x|$x.pnl:y|$y->stdDevSample(), m:x|$x.pnl:y|$y->median(),"
                        + " v:x|$x.pnl:y|$y->varianceSample()])->sort(~desk->ascending())",
                "#>{risk::Ref.trades}#->join(#>{risk::Ref.instrument}#, meta::pure::functions::relation::JoinKind.INNER,"
                        + " {a, b | $a.instrument == $b.iid})->select(~[iid, sector, id, qty])->sort([~id->ascending()])",
                "#>{risk::Ref.trades}#->select(~[id, pnl])->sort([~pnl->ascending(), ~id->ascending()])->slice(5, 15)",
                "#>{risk::Ref.trades}#->filter(x|$x.qty > 1000000000)->groupBy(~[], ~[t:x|$x.notional:y|$y->sum()])",
                "#>{risk::Ref.trades}#->select(~[region, desk])->distinct()"
                        + "->sort([~region->ascending(), ~desk->descending()])",
                "#>{risk::Ref.trades}#->select(~[id, region])->concatenate(#>{risk::Ref.trades}#->select(~[id, region]))");
        for (String query : queries) {
            String plan = Compiler.query(ctx, query).plan("risk::RT").sql();
            byte[] bytes = DeephavenWire.batch(plan, 3);
            Json.Obj read = ProtoWire.read("BatchTableRequest", bytes);
            assertArrayEquals(bytes, ProtoWire.write("BatchTableRequest", read, b -> b), query);
            assertTrue(Json.toCompact(read).contains("\"resultId\""), query);
        }
    }

    /** Two grid queries over one source, as the target writes them on a Deephaven runtime. */
    private static final String SOURCE = "#>{risk::Ref.trades}#->join(#>{risk::Ref.instrument}#,"
            + " meta::pure::functions::relation::JoinKind.LEFT, {t, i | $t.instrument == $i.iid})";

    private static String plan(String query) {
        String model = """
                ###Relational
                Database risk::Ref
                (
                  Table trades (id INTEGER PRIMARY KEY, desk VARCHAR(16), qty BIGINT, pnl DOUBLE, instrument VARCHAR(16))
                  Table instrument (iid VARCHAR(16) PRIMARY KEY, sector VARCHAR(16), ccy VARCHAR(3) NOT NULL)
                )
                ###Connection
                RelationalDatabaseConnection risk::Conn
                {
                  store: risk::Ref;
                  type: DuckDB;
                  specification: DuckDB { };
                  auth: Test;
                }
                ###Runtime
                Runtime risk::RT
                {
                  mappings: [];
                  connections: [ risk::Ref: [ c: risk::Conn ] ];
                }
                """.replace("type: DuckDB;\n  specification: DuckDB { };\n  auth: Test;",
                "type: Deephaven;\n  specification: Deephaven { host: 'localhost'; port: 8443; useTls: true; };\n"
                        + "  auth: PreSharedKey { keyVaultReference: 'deephaven.psk'; };");
        return Compiler.query(Compiler.compileModel(model), query).plan("risk::RT").sql();
    }

    /** The operations a request sends: each one's name, its result's export id, and the references it reads. */
    private static List<String> sent(byte[] request) {
        List<String> out = new ArrayList<>();
        for (Json.Node n : ProtoWire.read("BatchTableRequest", request).getArrOr("ops", new Json.Arr(List.of())).items()) {
            Json.Obj op = (Json.Obj) n;
            String name = op.fields().keySet().iterator().next();
            Json.Obj body = op.getObj(name);
            byte[] result = java.util.Base64.getDecoder().decode(body.getObj("resultId").getString("ticket"));
            StringBuilder reads = new StringBuilder();
            ProtoWire.references("BatchTableRequest.Operation", op, ref -> {
                if (ref.has("batchOffset")) {
                    reads.append(" @").append(ref.getLong("batchOffset"));
                } else {
                    byte[] t = java.util.Base64.getDecoder().decode(ref.getObj("ticket").getString("ticket"));
                    reads.append(' ').append(t[0] == 'e' ? "e" + (t[1] & 0xFF) : new String(t, StandardCharsets.UTF_8));
                }
                return ref;
            });
            out.add(name + " e" + (result[1] & 0xFF) + reads);
        }
        return out;
    }

    /** A key names the table an operation makes: two queries over one source share the source's keys, and part at
     *  the first operation that differs, and every one after it differs too (it reads a different table). */
    @Test
    void queriesOverOneSourceShareTheSourcesKeys() {
        String byDesk = plan(SOURCE + "->groupBy(~[desk], ~[pnl:x|$x.pnl:y|$y->sum()])->sort(~desk->ascending())");
        String bySector = plan(SOURCE + "->filter(x|$x.qty > 10)->groupBy(~[sector], ~[pnl:x|$x.pnl:y|$y->sum()])");
        List<String> a = DeephavenWire.keys(byDesk);
        List<String> b = DeephavenWire.keys(bySector);
        int shared = 0;
        while (shared < Math.min(a.size(), b.size()) && a.get(shared).equals(b.get(shared))) {
            shared++;
        }
        // the two sides' views and their join
        assertEquals(3, shared, a + "\n" + b);
        for (int i = shared; i < b.size(); i++) {
            assertFalse(a.contains(b.get(i)), "operation " + i + " of the second plan");
        }
        assertEquals(a, DeephavenWire.keys(byDesk));
        assertTrue(a.stream().allMatch(k -> k.matches("[0-9a-f]{64}")), a.toString());
    }

    /** A send exports every operation it sends; a later send of a query over the same source sends only what the
     *  session does not keep alive, reading the join it keeps by its ticket; a result kept alive sends nothing. */
    @Test
    void aSendSendsOnlyWhatTheSessionDoesNotKeepAlive() {
        String byDesk = plan(SOURCE + "->groupBy(~[desk], ~[pnl:x|$x.pnl:y|$y->sum()])->sort(~desk->ascending())");
        String bySector = plan(SOURCE + "->filter(x|$x.qty > 10)->groupBy(~[sector], ~[pnl:x|$x.pnl:y|$y->sum()])");
        DeephavenWire.Send first = DeephavenWire.send(byDesk, Map.of(), Map.of(), 1);
        List<String> keys = DeephavenWire.keys(byDesk);
        assertEquals(keys.size(), first.exports().size());
        assertEquals(keys.get(keys.size() - 1), first.exports().get(first.exports().size() - 1).key());
        assertEquals(first.exports().size(), first.result());
        List<String> ops = sent(first.request());
        assertEquals("view e1 s/trades", ops.get(0));
        assertEquals("view e2 s/instrument", ops.get(1));
        assertEquals("naturalJoin e3 @0 @1", ops.get(2));

        Map<String, Integer> alive = new LinkedHashMap<>();
        for (DeephavenWire.Export e : first.exports()) {
            alive.put(e.key(), e.exportId());
        }
        DeephavenWire.Send second = DeephavenWire.send(bySector, alive, Map.of(), 100);
        List<String> tail = sent(second.request());
        assertEquals(DeephavenWire.keys(bySector).size() - 3, tail.size(), tail.toString());
        assertEquals("unstructuredFilter e100 e3", tail.get(0));
        assertTrue(tail.get(1).endsWith(" @0"), tail.toString());
        assertEquals(99 + tail.size(), second.result());

        DeephavenWire.Send again = DeephavenWire.send(byDesk, alive, Map.of(), 200);
        assertEquals(0, again.request().length);
        assertEquals(List.of(), again.exports());
        assertEquals(first.result(), again.result());
    }

    /** The same table twice in one plan is sent once, every reader of either reading the one sent. */
    @Test
    void theSameTableTwiceInOnePlanIsSentOnce() {
        String twice = "{\"ops\": [{\"emptyTable\": {\"size\": \"3\"}}, {\"emptyTable\": {\"size\": \"3\"}},"
                + " {\"merge\": {\"sourceIds\": [{\"batchOffset\": 0}, {\"batchOffset\": 1}]}}]}";
        List<String> keys = DeephavenWire.keys(twice);
        assertEquals(keys.get(0), keys.get(1));
        DeephavenWire.Send s = DeephavenWire.send(twice, Map.of(), Map.of(), 1);
        assertEquals(List.of("emptyTable e1", "merge e2 @0 @0"), sent(s.request()));
        assertEquals(2, s.result());
    }

    /** A grid's base (B1): its plan's result stored -- its view a select of the same columns, named as the plan names
     *  them -- and a query that reads the base by its name reads the session's export of it, the rest as written. */
    @Test
    void aGridsBaseIsStoredAndReadByTheExportThatIsIt() {
        DeephavenWire.Stored base = DeephavenWire.stored(plan(SOURCE + "->extend(~[v:x|$x.id * 2])"));
        List<String> ops = sent(DeephavenWire.send(base.plan(), Map.of(), Map.of(), 1).request());
        assertTrue(ops.get(ops.size() - 1).startsWith("select "), ops.toString());
        assertEquals(List.of("id", "desk", "qty", "pnl", "instrument", "iid", "sector", "ccy", "v"), base.columns());
        String named = java.util.Base64.getEncoder().encodeToString("s/legend_base_ab12".getBytes(StandardCharsets.UTF_8));
        String other = java.util.Base64.getEncoder().encodeToString("s/trades".getBytes(StandardCharsets.UTF_8));
        String query = "{\"ops\": [{\"view\": {\"sourceId\": {\"ticket\": {\"ticket\": \"" + named + "\"}},"
                + " \"columnSpecs\": [\"t0__v = v\"]}}, {\"unstructuredFilter\": {\"sourceId\": {\"batchOffset\": 0},"
                + " \"filters\": [\"t0__v > 1\"]}}]}";
        assertEquals(List.of("view e10 e7", "unstructuredFilter e11 @0"),
                sent(DeephavenWire.send(query, Map.of(), Map.of("legend_base_ab12", 7), 10).request()));
        // a table the session does not supply is read by its own name
        String server = "{\"ops\": [{\"view\": {\"sourceId\": {\"ticket\": {\"ticket\": \"" + other + "\"}},"
                + " \"columnSpecs\": [\"t1__id = id\"]}}]}";
        assertEquals(List.of("view e20 s/trades"),
                sent(DeephavenWire.send(server, Map.of(), Map.of("legend_base_ab12", 7), 20).request()));
        IllegalArgumentException sorted = assertThrows(IllegalArgumentException.class,
                () -> DeephavenWire.stored(plan("#>{risk::Ref.trades}#->sort(~id->ascending())")));
        assertTrue(sorted.getMessage().contains("has no stored form"), sorted.getMessage());
    }

    @Test
    void theRowsTicket() {
        assertEquals("0a056507000000", HEX.formatHex(DeephavenWire.doGet(7)));
    }

    @Test
    void whatTheTableDoesNotHoldIsRefusedByName() {
        IllegalArgumentException key = assertThrows(IllegalArgumentException.class,
                () -> DeephavenWire.batch("{\"ops\": [{\"timeTable\": {\"periodString\": \"PT1S\"}}]}", 1));
        assertTrue(key.getMessage().contains("no field 'timeTable'"), key.getMessage());
        IllegalArgumentException named = assertThrows(IllegalArgumentException.class, () -> DeephavenWire.batch(
                "{\"ops\": [{\"emptyTable\": {\"size\": \"1\", \"resultId\": {\"ticket\": \"ZQEAAAA=\"}}}]}", 1));
        assertTrue(named.getMessage().contains("names no result of its own"), named.getMessage());
        String parameters = java.util.Base64.getEncoder().encodeToString(
                "legend/parameters".getBytes(StandardCharsets.UTF_8));
        UnsupportedOperationException params = assertThrows(UnsupportedOperationException.class,
                () -> DeephavenWire.batch("{\"ops\": [{\"view\": {\"sourceId\": {\"ticket\": {\"ticket\": \""
                        + parameters + "\"}}, \"columnSpecs\": [\"a = __p0\"]}}]}", 1));
        assertTrue(params.getMessage().contains("with parameters is not sent yet"), params.getMessage());
        IllegalArgumentException kind = assertThrows(IllegalArgumentException.class,
                () -> ProtoWire.write("SortDescriptor", Json.parseObject("{\"columnName\": 3}"), b -> b));
        assertTrue(kind.getMessage().contains("SortDescriptor.columnName is a STRING"), kind.getMessage());
        IllegalArgumentException value = assertThrows(IllegalArgumentException.class, () -> ProtoWire.write(
                "SortDescriptor", Json.parseObject("{\"direction\": \"SIDEWAYS\"}"), b -> b));
        assertTrue(value.getMessage().contains("has no value 'SIDEWAYS'"), value.getMessage());
        IllegalArgumentException forward = assertThrows(IllegalArgumentException.class, () -> DeephavenWire.keys(
                "{\"ops\": [{\"emptyTable\": {\"size\": \"1\"}}, {\"dropColumns\": {\"sourceId\": {\"batchOffset\": 1},"
                        + " \"columnNames\": []}}]}"));
        assertTrue(forward.getMessage().contains("reads operation 1, which is not before it"), forward.getMessage());
        IllegalArgumentException named2 = assertThrows(IllegalArgumentException.class, () -> DeephavenWire.send(
                "{\"ops\": [{\"emptyTable\": {\"size\": \"1\", \"resultId\": {\"ticket\": \"ZQEAAAA=\"}}}]}", Map.of(), Map.of(),
                1));
        assertTrue(named2.getMessage().contains("names no result of its own"), named2.getMessage());
        assertNotEquals(DeephavenWire.keys("{\"ops\": [{\"emptyTable\": {\"size\": \"1\"}}]}"),
                DeephavenWire.keys("{\"ops\": [{\"emptyTable\": {\"size\": \"2\"}}]}"));
        assertThrows(IllegalArgumentException.class, () -> DeephavenWire.publish(0, "abc"));
        assertThrows(IllegalArgumentException.class, () -> DeephavenWire.publish(1, "a/b"));
        assertFalse(DeephavenMessages.all().isEmpty());
    }

    private static String hex(String type, String json) {
        return HEX.formatHex(ProtoWire.write(type, Json.parseObject(json), b -> b));
    }
}
