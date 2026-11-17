package com.legend.integration;

import com.legend.testing.LaneFloor;
import com.legend.testing.Lean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * An M2M enum transformer ({@code side: EnumerationMapping Codes: $src.code}) executes as legend-engine's in-memory
 * M2M does (graphFetchInMemory.pure {@code generateTransformer}): an empty source is empty, a mapped code is its enum
 * value, any other code RAISES {@code Unmapped value: <code>}. Every expected answer here is legend-engine's own,
 * probed on the same model, data and query (2026-11-14). The roundtrip cases this closes:
 * DSL_Mapping-mapping-include-enum-mapping (and the enum half of DSL_Mapping-basic-M2M and CORE-legacy-service-tests).
 */
@DisplayName("M2M enum transformer")
@org.junit.jupiter.api.extension.ExtendWith(LaneFloor.class)
class M2MEnumTransformTest {

    private static final String QUERY = """
            t::Tgt.all()->graphFetch(#{t::Tgt{k,side,s1,load,inc}}#)->serialize(#{t::Tgt{k,side,s1,load,inc}}#)""";

    /** {@code data}: the JSON source's rows. A String [1] code, a String [0..1] code, an Integer code, and an included
     *  mapping's anonymous enumeration mapping named by its implicit id ({@code t_Inc}, engine
     *  {@code getEnumerationMappingId}). */
    private static String model(String data) {
        return """
                ###Pure
                Enum t::Side { BUY, SELL }
                Enum t::Load { IDLE, BUSY }
                Enum t::Inc { LLC, CORP }
                Class t::Src { k: String[1]; code: String[0..1]; c1: String[1]; n: Integer[0..1]; i: String[1]; }
                Class t::Tgt { k: String[1]; side: t::Side[0..1]; s1: t::Side[1]; load: t::Load[0..1]; inc: t::Inc[1]; }
                ###Mapping
                Mapping t::Enums
                (
                  t::Inc: EnumerationMapping { LLC: ['llc'], CORP: ['corp'] }
                )
                Mapping t::M
                (
                  include mapping t::Enums
                  t::Side: EnumerationMapping Codes { BUY: ['B', 'BOT'], SELL: ['S'] }
                  t::Load: EnumerationMapping Loads { IDLE: [0], BUSY: [1, 2] }
                  t::Tgt: Pure
                  {
                    ~src t::Src
                    k: $src.k,
                    side: EnumerationMapping Codes: $src.code,
                    s1: EnumerationMapping Codes: $src.c1,
                    load: EnumerationMapping Loads: $src.n,
                    inc: EnumerationMapping t_Inc: $src.i
                  }
                )
                ###Relational
                Database store::TestDb ( )
                ###Connection
                RelationalDatabaseConnection store::TestConnection { type: DuckDB; specification: DuckDB { }; auth: Test; }
                ###Runtime
                Runtime t::R
                {
                  mappings: [ t::M ];
                  connections: [ store::TestDb: [ env: store::TestConnection ], ModelStore: [ c: #{ JsonModelConnection { class: t::Src; url: 'data:application/json,%s'; } }# ] ];
                }
                """.formatted(data);
    }

    private Connection connection;

    @BeforeEach
    void setUp() throws SQLException {
        connection = LaneFloor.backend().open();
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    private String fetch(String data) {
        return Lean.run(model(data), QUERY + "->from(t::M, t::R)", null, connection).json();
    }

    @Test
    @DisplayName("mapped codes decode, an empty source is null: legend-engine's answer")
    void decodes() {
        assertEquals("[{\"k\":\"a\",\"side\":\"BUY\",\"s1\":\"SELL\",\"load\":\"BUSY\",\"inc\":\"CORP\"},"
                        + "{\"k\":\"b\",\"side\":null,\"s1\":\"BUY\",\"load\":\"IDLE\",\"inc\":\"LLC\"}]",
                fetch("[{\"k\":\"a\",\"code\":\"BOT\",\"c1\":\"S\",\"n\":2,\"i\":\"corp\"},"
                        + "{\"k\":\"b\",\"c1\":\"B\",\"n\":0,\"i\":\"llc\"}]"));
    }

    @Test
    @DisplayName("a code no entry maps raises legend-engine's 'Unmapped value: <code>', never a null")
    void anUnmappedCodeRaises() {
        Throwable t = assertThrows(Throwable.class, () -> fetch(
                "[{\"k\":\"a\",\"code\":\"ZZ\",\"c1\":\"S\",\"n\":2,\"i\":\"corp\"}]"));
        assertTrue(rootMessage(t).contains("Unmapped value: ZZ"), rootMessage(t));
        Throwable t2 = assertThrows(Throwable.class, () -> fetch(
                "[{\"k\":\"a\",\"c1\":\"S\",\"n\":7,\"i\":\"corp\"}]"));
        assertTrue(rootMessage(t2).contains("Unmapped value: 7"), rootMessage(t2));
    }

    @Test
    @DisplayName("a to-many source is refused by name at model build (its per-element decode is not composed yet)")
    void aToManySourceIsRefusedByName() {
        Throwable t = assertThrows(Throwable.class, () -> com.legend.Compiler.compileModel("""
                Enum t::Side { BUY, SELL }
                Class t::Src { codes: String[*]; }
                Class t::Tgt { sides: t::Side[*]; }
                ###Mapping
                Mapping t::M
                (
                  t::Side: EnumerationMapping Codes { BUY: ['B'], SELL: ['S'] }
                  t::Tgt: Pure { ~src t::Src sides: EnumerationMapping Codes: $src.codes }
                )
                """));
        assertTrue(rootMessage(t).contains("decodes a to-many source"), rootMessage(t));
    }

    private static String rootMessage(Throwable t) {
        StringBuilder all = new StringBuilder();
        for (Throwable c = t; c != null; c = c.getCause() == c ? null : c.getCause()) {
            all.append(c.getMessage()).append(" | ");
        }
        return all.toString();
    }
}
