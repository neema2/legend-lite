// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.ts;

import com.legend.Compiler;
import com.legend.json.Json;
import com.legend.parser.SpecParser;
import com.legend.protocol.ProtocolEmitter;
import com.legend.protocol.ProtocolReader;
import com.legend.protocol.PureComposer;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.SqlIsland;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The {@code #TS{...}#} island on the wire (docs/TS_LANGUAGE_BUILD_2026_10_30.md P2): parsed at the product surface
 * (LEGEND_LITE), it is a {@code classInstance} of type {@code TS} whose value is {@code {"ts": ...}}; emitted, read back
 * and printed, it is the text it was, byte for byte. The LEGEND_ENGINE surface refuses it as any unknown island type
 * (upstream has none); and {@link TsParser} reads the clauses the language's v0 has.
 */
class TsIslandRoundTripTest {

    @ParameterizedTest
    @ValueSource(strings = {
            "|#TS{SELECT avg(close, 20) AS ma20 FROM prices ON date BY symbol}#",
            "|#TS{ WITH q AS (SELECT instrument.ticker AS t, quoteTime, closePrice AS c FROM MarketQuote)\n"
                    + "  SELECT sum(c, all) AS s FROM q ON quoteTime BY t\n  FILTER t IN ('AAPL') }#",
            "|#TS{SELECT returns(x) AS r FROM $quotes ON d}#->filter(r|$r.r > 0)"})
    void textWireAndTextAgain_byteForByte(String text) {
        LambdaFunction parsed = (LambdaFunction) Compiler.parseQuery(text);
        String json = ProtocolEmitter.emitLambda(parsed);
        LambdaFunction read = (LambdaFunction) ProtocolReader.valueSpec(Json.parse(json));
        assertEquals(json, ProtocolEmitter.emitLambda(read), "emit(read(J)) is J");
        assertEquals(text, PureComposer.lambda(json, PureComposer.Style.STANDARD), "printed as written");
    }

    @Test
    void theWireIsAClassInstanceOfTypeTsHoldingTheText() {
        String q = "|#TS{SELECT x AS y FROM t ON d}#";
        String json = ProtocolEmitter.emitLambda((LambdaFunction) Compiler.parseQuery(q));
        assertTrue(json.contains("\"type\":\"TS\",\"value\":{\"ts\":\"SELECT x AS y FROM t ON d\"}"), json);
        SqlIsland island = (SqlIsland) ((LambdaFunction) Compiler.parseQuery(q)).body().get(0);
        assertEquals(com.legend.protocol.spec.IslandLanguage.TS, island.language());
        assertEquals("SELECT x AS y FROM t ON d", island.sql());
        assertNull(island.profile());
        assertEquals("#TS{", island.opening());
    }

    @Test
    void theEngineSurfaceKeepsRefusingAnUnknownIsland() {
        RuntimeException e = assertThrows(RuntimeException.class, () -> SpecParser.parseLambda("|#TS{SELECT 1}#"));
        assertTrue(e.getMessage().contains("unknown DSL island type: '#TS{'"), e.getMessage());
    }

    @Test
    void theClausesV0Reads() {
        TsAst.Query q = TsParser.parse("""
                WITH q AS (SELECT instrument.ticker AS t, day, close AS c FROM pkg::Quote WHERE venue = 'X')
                SELECT avg(c, 20) AS ma, sum(c, all) AS s, c / ma - 1 AS dev
                FROM   q qq ON day BY t
                FILTER t = 'A'""");
        assertEquals(1, q.withs().size());
        TsAst.Flat flat = (TsAst.Flat) q.withs().get(0).body();
        assertEquals("pkg::Quote", ((TsAst.Named) flat.from()).name());
        assertEquals(3, flat.items().size());
        assertEquals(java.util.List.of("ma", "s", "dev"), q.items().stream().map(i -> i.alias().text()).toList());
        TsAst.From from = q.sources().get(0);
        assertEquals("qq", from.alias().text());
        assertEquals("day", from.on().parts().get(0).text());
        assertEquals("t", from.by().parts().get(0).text());
        assertEquals(1, q.alls().size(), "all as a window, known by its place");
        assertTrue(q.filter() != null);
        TsAst.Variable v = (TsAst.Variable) TsParser.parse("SELECT x AS y FROM $quotes ON d").sources().get(0)
                .source();
        assertEquals("quotes", v.name());
        TsAst.Filtered f = (TsAst.Filtered) TsParser.parse("SELECT x AS y FROM (db::Db.s.prices WHERE v > 0) ON d")
                .sources().get(0).source();
        assertEquals("db::Db.s.prices", ((TsAst.Named) f.of()).name());
    }
}
