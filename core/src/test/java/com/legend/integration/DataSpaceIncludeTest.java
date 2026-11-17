package com.legend.integration;

import com.legend.model.ConnectionDefinition.DatabaseType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * {@code include dataspace X} includes X's default execution context's mapping, as legend-engine's
 * {@code DataSpaceIncludedMappingHandler} does, and refuses a data space with no default execution context with the
 * engine's words. Until 2026-11-14 the model reader dropped the include (the Studio roundtrip's
 * DSL_DataSpace-mapping-include-dataspace then failed on an XStore end its included mapping maps).
 */
@DisplayName("include dataspace")
class DataSpaceIncludeTest {

    private static String model(String include, String dataSpaceDefault) {
        return model(include, dataSpaceDefault, DataDeliveryMatrixTest.DATABASES.get(0));
    }

    private static String model(String include, String dataSpaceDefault, DatabaseType db) {
        return """
                ###Pure
                Class x::Entity { id: String[1]; name: String[1]; }
                Class x::Trade { ticker: String[1]; }
                ###Relational
                Database x::DB
                (
                  Table ENTITY ( ID VARCHAR(10) PRIMARY KEY, NAME VARCHAR(20) )
                  Table TRADE ( TICKER VARCHAR(10) PRIMARY KEY )
                )
                ###DataSpace
                DataSpace x::EntitySpace
                {
                  executionContexts:
                  [
                    {
                      name: 'default';
                      mapping: x::EntityMapping;
                      defaultRuntime: x::RT;
                    }
                  ];
                  %s
                }
                ###Mapping
                Mapping x::EntityMapping
                (
                  *x::Entity: Relational { ~mainTable [x::DB]ENTITY id: [x::DB]ENTITY.ID, name: [x::DB]ENTITY.NAME }
                )
                Mapping x::TradeMapping
                (
                  %s
                  *x::Trade: Relational { ~mainTable [x::DB]TRADE ticker: [x::DB]TRADE.TICKER }
                )
                ###Connection
                """.formatted(dataSpaceDefault, include) + DataDeliveryMatrixTest.connection(db,
                "specification: LocalH2 { testDataSetupCSV: 'default\\nENTITY\\nID,NAME\\n1,Tesla\\n2,Acme\\n"
                        + "-----\\ndefault\\nTRADE\\nTICKER\\nAPPL\\n'; }; auth: DefaultH2;") + """
                ###Runtime
                Runtime x::RT { mappings: [ x::EntityMapping, x::TradeMapping ]; connections: [ x::DB: [ c: x::Conn ] ]; }
                """;
    }

    private static final String QUERY = "|x::Entity.all()->project(~[i:x|$x.id, n:x|$x.name])->sort(~i->ascending())"
            + "->from(x::TradeMapping, x::RT)";

    private static List<List<String>> read(String model, DatabaseType db) throws Exception {
        return DataDeliveryMatrixTest.read(new DataDeliveryMatrixTest.Data(model, QUERY, List.of(), "x::RT"), QUERY, db);
    }

    @Test
    @DisplayName("the data space's default context's mapping is included: its sets serve the including mapping")
    void includesTheDefaultContextsMapping() throws Exception {
        for (DatabaseType db : DataDeliveryMatrixTest.DATABASES) {
            List<List<String>> viaDataSpace = read(model("include dataspace x::EntitySpace",
                    "defaultExecutionContext: 'default';", db), db);
            assertEquals(List.of(List.of("1", "Tesla"), List.of("2", "Acme")), viaDataSpace, db + "");
            assertEquals(read(model("include mapping x::EntityMapping", "defaultExecutionContext: 'default';", db), db),
                    viaDataSpace, db + ": the same rows as including the mapping itself");
        }
    }

    @Test
    @DisplayName("a path that names no data space is refused by name")
    void anUnknownDataSpaceIsRefused() {
        assertRefused("Can't find the packageable element 'x::Nowhere'",
                model("include dataspace x::Nowhere", "defaultExecutionContext: 'default';"));
        assertRefused("names a", model("include dataspace x::EntityMapping",
                "defaultExecutionContext: 'default';").replace("include dataspace x::EntityMapping",
                "include dataspace x::Trade"));
    }

    @Test
    @DisplayName("a tolerant build walls the mapping with the reason; a mapping that includes it meets no marker")
    void aTolerantBuildLeavesNoMarker() {
        // the data space has no default execution context: x::TradeMapping's include cannot be rewritten. The
        // tolerant build walls it by name and drops the marker, so x::Outer's walk of its includes never reads
        // the data space's path as a mapping's
        String model = model("include dataspace x::EntitySpace", "") + """
                ###Mapping
                Mapping x::Outer ( include mapping x::TradeMapping )
                """;
        com.legend.Compiler.BuiltModule built = com.legend.Compiler.buildModule(com.legend.Compiler.parseSources(
                List.of(new com.legend.Compiler.ModelSource("m.pure", model))).model());
        assertTrue(String.valueOf(built.walls().get("x::TradeMapping")).contains("has no default execution context"),
                () -> "the including mapping is walled with the reason: " + built.walls());
        assertFalse(built.walls().containsKey("x::Outer"), () -> "x::Outer met the marker: " + built.walls());
    }

    private static void assertRefused(String fragment, String model) {
        Throwable t = assertThrows(Throwable.class, () -> com.legend.Compiler.compileModel(model));
        StringBuilder all = new StringBuilder();
        for (Throwable c = t; c != null; c = c.getCause() == c ? null : c.getCause()) {
            all.append(c.getMessage()).append(" | ");
        }
        assertTrue(all.toString().contains(fragment), all.toString());
    }
}
