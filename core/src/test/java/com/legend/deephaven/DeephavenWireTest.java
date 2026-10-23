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
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
                  type: Deephaven;
                  specification: Deephaven { host: 'localhost'; port: 8443; useTls: true; };
                  auth: PreSharedKey { keyVaultReference: 'deephaven.psk'; };
                }
                ###Runtime
                Runtime risk::RT
                {
                  mappings: [];
                  connections: [ risk::Ref: [ c: risk::Conn ] ];
                }
                """;
        var ctx = Compiler.compileModel(model);
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
        assertThrows(IllegalArgumentException.class, () -> DeephavenWire.publish(0, "abc"));
        assertThrows(IllegalArgumentException.class, () -> DeephavenWire.publish(1, "a/b"));
        assertFalse(DeephavenMessages.all().isEmpty());
    }

    private static String hex(String type, String json) {
        return HEX.formatHex(ProtoWire.write(type, Json.parseObject(json), b -> b));
    }
}
