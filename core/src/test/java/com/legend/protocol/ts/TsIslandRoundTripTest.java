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
import com.legend.protocol.sql.SqlAst;
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
    void aStringKeepsItsText() {
        // #81's final review, item 14k (2), 2026-11-10: the reader's marks ($x a variable, 3mo a span) are made outside
        // every string the lexer reads -- an escape string, whose \' is not its close, and a dollar-quoted one
        String filter = TsParser.parse("SELECT x AS y FROM t ON d FILTER a = E'it\\'s $x at 3mo' AND b = $$ $from 3mo"
                + " $$ AND c = $q$ 2w $q$").filter().toString();
        assertTrue(filter.contains("value=it's $x at 3mo,"), filter);
        assertTrue(filter.contains("value= $from 3mo ,"), filter);
        assertTrue(filter.contains("value= 2w ,"), filter);
        // and a variable outside them is still one
        assertTrue(TsParser.parse("SELECT x AS y FROM t ON d FILTER a = $from").filter().toString().contains("from"));
    }

    @Test
    void aNameShowTsWritesReadsBackAsThatName() {
        // #81's final review, item 14k (3), 2026-11-10: Show #TS writes a column's name bare only where the reader
        // reads it as that column -- the reader's reserved words (lateral, left, ...), the language's own (latest,
        // the value), a capital (Postgres folds it) are quoted
        for (String name : java.util.List.of("close", "x1_y", "year", "Close", "lateral", "left", "latest", "window",
                "whole", "my col", "über")) {
            SqlAst.Ident id = TsShow.ident(name);
            String written = TsPrinter.print(new TsAst.Query(java.util.List.of(), java.util.List.of(new TsAst.Item(
                    new SqlAst.ColumnRef(java.util.List.of(id), id.pos()), new SqlAst.Ident("y", false, id.pos()),
                    id.pos())), java.util.List.of(new TsAst.From(new TsAst.Named("t", id.pos()), null,
                    new SqlAst.ColumnRef(java.util.List.of(new SqlAst.Ident("d", false, id.pos())), id.pos()), null,
                    null, id.pos())), null, "LAST", null, null, java.util.Set.of(), java.util.Map.of(), null));
            TsAst.Query read = TsParser.parse(written);
            SqlAst.ColumnRef back = (SqlAst.ColumnRef) read.items().get(0).expr();
            assertEquals(name, back.parts().get(0).text(), written);
            assertEquals(!java.util.Set.of("close", "x1_y", "year").contains(name), back.parts().get(0).quoted(),
                    written);
        }
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
        // LATEST BY seq (the user's ruling of 2026-11-08 on duplicate prints): the source's sequence, read and printed
        String latest = "SELECT sum(volume) AS v FROM prices ON date BY symbol LATEST BY seq SAMPLE BY 1w";
        TsAst.From named = TsParser.parse(latest).sources().get(0);
        assertEquals("symbol", named.by().parts().get(0).text());
        assertEquals("seq", named.latest().parts().get(0).text());
        assertNull(TsParser.parse("SELECT x AS y FROM t ON d BY s").sources().get(0).latest(), "none unless said");
        assertTrue(TsPrinter.print(TsParser.parse(latest)).contains("FROM prices ON date BY symbol LATEST BY seq"),
                TsPrinter.print(TsParser.parse(latest)));
    }
}
