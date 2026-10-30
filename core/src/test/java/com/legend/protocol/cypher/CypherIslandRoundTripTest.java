// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.cypher;

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
 * The {@code #Cypher{...}#} island on the wire (docs/CYPHER_STUDY_2026_10_15.md §5.3): parsed at the product surface
 * (LEGEND_LITE), it is a {@code classInstance} of type {@code Cypher} whose value is {@code {"cypher": ...}};
 * emitted, read back and printed, it is the text it was, byte for byte. The LEGEND_ENGINE surface refuses the island
 * as any unknown island type (upstream has none).
 */
class CypherIslandRoundTripTest {

    @ParameterizedTest
    @ValueSource(strings = {
            "|#Cypher{MATCH (p:Person) WHERE p.age > 30 RETURN p.name}#",
            "|#Cypher{ CYPHER 5\n  MATCH (p:Person)-[:firm]->(f:Firm)\n  RETURN f.legalName, count(*) AS n }#",
            "|#Cypher{MATCH (p:Person) RETURN p.name AS name}#->filter(r|$r.name == 'x')"})
    void textWireAndTextAgain_byteForByte(String text) {
        LambdaFunction parsed = (LambdaFunction) Compiler.parseQuery(text);
        String json = ProtocolEmitter.emitLambda(parsed);
        LambdaFunction read = (LambdaFunction) ProtocolReader.valueSpec(Json.parse(json));
        assertEquals(json, ProtocolEmitter.emitLambda(read), "emit(read(J)) is J");
        assertEquals(text, PureComposer.lambda(json, PureComposer.Style.STANDARD), "printed as written");
    }

    @Test
    void theWireIsAClassInstanceOfTypeCypherHoldingTheText() {
        String json = ProtocolEmitter.emitLambda((LambdaFunction) Compiler.parseQuery("|#Cypher{RETURN 1}#"));
        assertTrue(json.contains("\"type\":\"Cypher\",\"value\":{\"cypher\":\"RETURN 1\"}"), json);
        SqlIsland island = (SqlIsland) ((LambdaFunction) Compiler.parseQuery("|#Cypher{RETURN 1}#")).body().get(0);
        assertEquals(com.legend.protocol.spec.IslandLanguage.CYPHER, island.language());
        assertEquals("RETURN 1", island.sql());
        assertNull(island.profile());
        assertEquals("#Cypher{", island.opening());
    }

    @Test
    void theEngineSurfaceKeepsRefusingAnUnknownIsland() {
        // the engine drop-in surface's lambda door (upstream's grammarToJson/lambda)
        RuntimeException e = assertThrows(RuntimeException.class, () -> SpecParser.parseLambda("|#Cypher{RETURN 1}#"));
        assertTrue(e.getMessage().contains("unknown DSL island type: '#Cypher{'"), e.getMessage());
    }
}
