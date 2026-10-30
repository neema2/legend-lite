// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.sql.iq;

import com.legend.Compiler;
import com.legend.json.Json;
import com.legend.parser.SpecParser;
import com.legend.protocol.ProtocolEmitter;
import com.legend.protocol.ProtocolReader;
import com.legend.protocol.PureComposer;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.SqlIsland;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The {@code #IQ{...}#} island on the wire (docs/IQ_SQL_STUDY_2026_10_13.md §5.3): parsed at the product surface
 * (LEGEND_LITE), it is a {@code classInstance} of type {@code IQ} whose value is {@code {"sql": ...}}, with
 * {@code "profile"} when the island names one; emitted, read back and printed, it is the text it was, byte for byte.
 * A profile outside the closed set is refused by name, by the parser and by the wire reader; the LEGEND_ENGINE surface
 * refuses the island as any unknown island type (upstream has none).
 */
class IqIslandRoundTripTest {

    @ParameterizedTest
    @ValueSource(strings = {
            "|#IQ{SELECT id FROM tb('e2e::DB.t') WHERE vc = 'x' ORDER BY id}#",
            "|#IQ:iq16-tds{ SELECT \"vc\" AS v\n  FROM tb('e2e::DB.t') }#",
            "|#IQ:sqla17-default{SELECT [a b] FROM tb('e2e::DB.t')}#->filter(r|$r.id > 1)",
            "|#SQL{select 1}#"})
    void textWireAndTextAgain_byteForByte(String text) {
        LambdaFunction parsed = (LambdaFunction) Compiler.parseQuery(text);
        String json = ProtocolEmitter.emitLambda(parsed);
        LambdaFunction read = (LambdaFunction) ProtocolReader.valueSpec(Json.parse(json));
        assertEquals(json, ProtocolEmitter.emitLambda(read), "emit(read(J)) is J");
        assertEquals(text, PureComposer.lambda(json, PureComposer.Style.STANDARD), "printed as written");
    }

    @Test
    void theWireNamesTheLanguageAndAProfileOnlyWhenWritten() {
        String plain = ProtocolEmitter.emitLambda((LambdaFunction) Compiler.parseQuery("|#IQ{SELECT 1}#"));
        assertTrue(plain.contains("\"type\":\"IQ\",\"value\":{\"sql\":\"SELECT 1\"}"), plain);
        String named = ProtocolEmitter.emitLambda((LambdaFunction) Compiler.parseQuery("|#IQ:iq16-odbc{SELECT 1}#"));
        assertTrue(named.contains("\"type\":\"IQ\",\"value\":{\"sql\":\"SELECT 1\",\"profile\":\"iq16-odbc\"}"), named);
        SqlIsland island = (SqlIsland) ((LambdaFunction) Compiler.parseQuery("|#IQ{SELECT 1}#"))
                .body().get(0);
        assertEquals(com.legend.protocol.spec.IslandLanguage.IQ, island.language());
        assertNull(island.profile());
    }

    @Test
    void aProfileOutsideTheClosedSetIsRefusedByName() {
        RuntimeException parsed = assertThrows(RuntimeException.class,
                () -> Compiler.parseQuery("|#IQ:iq15{SELECT 1}#"));
        assertTrue(parsed.getMessage().contains("unknown #IQ{...}# profile 'iq15'"), parsed.getMessage());
        IllegalArgumentException wire = assertThrows(IllegalArgumentException.class, () -> ProtocolReader.valueSpec(
                Json.parse("{\"_type\":\"classInstance\",\"type\":\"IQ\",\"value\":{\"sql\":\"SELECT 1\","
                        + "\"profile\":\"iq15\"}}")));
        assertTrue(wire.getMessage().contains("unknown #IQ{...}# profile 'iq15'"), wire.getMessage());
    }

    /** An island in a model's function body, read at the product surface: the declared lite extension
     *  LITE-DESIGN-iq-island (docs/OWN_CORPUS_DECISIONS.md), which upstream's grammar refuses as an unknown island. */
    @Test
    void aModelsFunctionMayBeAnIsland() {
        com.legend.model.ParsedModel model = Compiler.parseModel("###Pure\n"
                + "function iq::ids(): Any[*]\n"
                + "{\n"
                + "    #IQ{SELECT TOP 2 id FROM tb('e2e::DB.t') ORDER BY id}#\n"
                + "}\n");
        com.legend.model.FunctionDefinition f = (com.legend.model.FunctionDefinition) model.elements().get(0);
        SqlIsland island = (SqlIsland) f.body().get(0);
        assertEquals(com.legend.protocol.spec.IslandLanguage.IQ, island.language());
        assertNull(island.profile());
    }

    @Test
    void theEngineSurfaceKeepsRefusingAnUnknownIsland() {
        // the engine drop-in surface's lambda door (upstream's grammarToJson/lambda)
        RuntimeException e = assertThrows(RuntimeException.class, () -> SpecParser.parseLambda("|#IQ{SELECT 1}#"));
        assertTrue(e.getMessage().contains("unknown DSL island type: '#IQ{'"), e.getMessage());
    }
}
