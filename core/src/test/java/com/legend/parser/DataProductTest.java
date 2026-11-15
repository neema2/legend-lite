// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.parser;

import com.legend.Compiler;
import com.legend.json.Json;
import com.legend.protocol.ModelComposer;
import com.legend.protocol.ModelReader;
import com.legend.protocol.Protocol;
import com.legend.protocol.ProtocolEmitter;
import com.legend.protocol.SourceInformation;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code ###DataProduct}, the part lite carries (docs/DEPLOYMENT_DESIGN_2026_11_14.md §7, step 2; register S408): the
 * forms legend-studio shows written read as Studio's fixtures hold them in JSON; Legend's JSON read and written back as
 * Legend wrote it; text to the record and back; what lite does not carry refused by name; and a data space's
 * {@code mappingProvider} checked against the product it names.
 */
class DataProductTest {

    /** legend-studio's TEST_DATA__DataProductRoundtrip.ts, TEST_DATA__DATAPRODUCT_GROUPS's written form (its comment). */
    private static final String GROUPS = """
            ###DataProduct
            DataProduct x::A
            {
              accessPoints: [
                group <<meta::pure::profiles::temporal.bitemporal>> ('Group description') [
                  myId ('Id description'): LH(Snowflake, |1)classification1,
                  myOtherId ('Other Id description'): LH(Snowflake, |1)classification1
                ],
                group2 [
                  latest : LH(Snowflake, |1)classification1
                ]
              ]
            }
            """;

    private static List<Protocol.PDataProduct> parse(String text) {
        return PmcdParser.parse(text, Dialect.LEGEND_LITE).model().elements().stream()
                .filter(e -> e instanceof Protocol.PDataProduct).map(e -> (Protocol.PDataProduct) e).toList();
    }

    private static String resource(String name) throws IOException {
        try (InputStream in = DataProductTest.class.getResourceAsStream("/dataproduct/" + name)) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    /** A node with its empty lists left out (Studio's hand-written fixtures leave them out; the engine writes them). */
    private static Json.Node withoutEmptyLists(Json.Node n) {
        if (n instanceof Json.Obj o) {
            LinkedHashMap<String, Json.Node> out = new LinkedHashMap<>();
            o.fields().forEach((k, v) -> {
                if (!(v instanceof Json.Arr a && a.items().isEmpty())) {
                    out.put(k, withoutEmptyLists(v));
                }
            });
            return new Json.Obj(out);
        }
        if (n instanceof Json.Arr a) {
            return new Json.Arr(a.items().stream().map(DataProductTest::withoutEmptyLists).toList());
        }
        return n;
    }

    private static Json.Node written(Protocol.Element e) {
        return withoutEmptyLists(Json.parse(SourceInformation.stripAll(ProtocolEmitter.emitElement(e))));
    }

    @Test
    void legendsJsonReadsAndWritesBackAsLegendWroteIt() throws IOException {
        Json.Arr products = (Json.Arr) Json.parse(resource("studio-data-products.json"));
        assertEquals(2, products.items().size());
        for (Json.Node n : products.items()) {
            Protocol.Element read = ModelReader.readElement(n);
            assertEquals(withoutEmptyLists(n), written(read));
        }
        Protocol.PDataProduct model = (Protocol.PDataProduct) ModelReader.readElement(products.items().get(1));
        assertEquals("model::dummyMapping", model.accessPointGroups().get(0).mapping());
    }

    /** Studio's written form reads as Studio's JSON holds it (the comment above its fixture: the two are one product). */
    @Test
    void studiosWrittenFormIsItsJson() throws IOException {
        Json.Node legend = ((Json.Arr) Json.parse(resource("studio-data-products.json"))).items().get(0);
        assertEquals(withoutEmptyLists(legend), written(parse(GROUPS).get(0)));
        // access points outside a group are the engine's default group (TEST_DATA__DATAPRODUCT_DELIVERY)
        Protocol.PDataProduct loose = parse("""
                ###DataProduct
                DataProduct x::A
                {
                  title: 'Test title'
                  description: 'Test description'
                  accessPoints: [
                    ap ('Description'): LH(Snowflake, |1)classification1
                  ]
                }
                """).get(0);
        assertEquals("Test title", loose.title());
        assertEquals(List.of(new Protocol.PAccessPointGroup("default", "The default access group", List.of(), null,
                loose.accessPointGroups().get(0).accessPoints())), loose.accessPointGroups());
        assertEquals("Snowflake", loose.accessPointGroups().get(0).accessPoints().get(0).targetEnvironment());
    }

    /** text -> record -> text -> record: the same record, spans aside; lite's own spellings among them. */
    @Test
    void theElementGoesToTextAndBack() {
        String lite = """
                ###DataProduct
                DataProduct <<meta::pure::profiles::doc.deprecated>> desk::Desk
                {
                  title: 'Trading desk'
                  description: 'The desk\\'s classes, live'
                  accessPoints: [
                    model ('The desk classes') Model { mapping: desk::DeskMapping; } [
                      volume ('Traded volume'): LH(|#>{desk::DB.TRADES}#),
                      latest: LH(Deephaven, |1)
                    ],
                    other [
                      x: LH(|2)internal
                    ]
                  ]
                }
                """;
        for (String text : List.of(GROUPS, lite)) {
            for (Protocol.PDataProduct p : parse(text)) {
                String printed = ModelComposer.element(p);
                Protocol.PDataProduct again = parse("###DataProduct\n" + printed + "\n").get(0);
                assertEquals(SourceInformation.stripAll(ProtocolEmitter.emitElement(p)),
                        SourceInformation.stripAll(ProtocolEmitter.emitElement(again)), printed);
            }
        }
        Protocol.PAccessPointGroup model = parse(lite).get(0).accessPointGroups().get(0);
        assertEquals("desk::DeskMapping", model.mapping());
        assertEquals(null, model.accessPoints().get(0).targetEnvironment(), "LH(|...): served wherever deployed");
        assertTrue(ProtocolEmitter.emitElement(parse(lite).get(0)).contains("\"_type\":\"modelAccessPointGroup\""));
    }

    private static void refused(String body, String message) {
        ParseException e = assertThrows(ParseException.class, () -> PmcdParser.parse(
                "###DataProduct\nDataProduct x::A\n{\n" + body + "\n}\n", Dialect.LEGEND_LITE));
        assertTrue(String.valueOf(e.getMessage()).contains(message), body + " -> " + e.getMessage());
    }

    @Test
    void whatLiteDoesNotCarryIsRefusedByName() {
        refused("icon: LibraryIcon('react-icons', 'UpArrow') accessPoints: [ a: LH(|1) ]", "'icon' is not carried");
        refused("supportInfo: { } accessPoints: [ a: LH(|1) ]", "'supportInfo' is not carried");
        refused("title: 'a'", "writes no accessPoints");
        refused("accessPoints: [ a: LH(|1), g [ b: LH(|1) ] ]", "outside a group beside groups");
        refused("accessPoints: [ a: FN(|1) ]", "lite carries lakehouse access points");
        refused("accessPoints: [ g Model { } [ a: LH(|1) ] ]", "names no mapping");
        refused("accessPoints: [ g Model { mapping: m::M; diagrams: [ ]; } [ a: LH(|1) ] ]", "'diagrams' is not carried");
        refused("title: 'a' title: 'b' accessPoints: [ a: LH(|1) ]", "'title' is written twice");
        String base = "{\"_type\":\"dataProduct\",\"name\":\"A\",\"package\":\"x\",\"accessPointGroups\":[]";
        for (String extra : List.of("\"icon\":{}", "\"owner\":{}", "\"nativeModelAccess\":{}")) {
            assertThrows(IllegalArgumentException.class, () -> ModelReader.readElement(base + "," + extra + "}"), extra);
        }
        assertThrows(ParseException.class, () -> PmcdParser.parse(GROUPS, Dialect.LEGEND_ENGINE),
                "the engine's dialect knows no ###DataProduct");
    }

    private static final String SPACE = """
            ###Pure
            Class m::P { name: String[1]; }

            ###Relational
            Database m::DB ( Table P (NAME VARCHAR(20) PRIMARY KEY) )

            ###Mapping
            Mapping m::M
            (
              m::P: Relational { ~mainTable [m::DB] P name: [m::DB] P.NAME }
            )

            ###Runtime
            Runtime m::R
            {
              mappings: [ m::M ];
              connections: [ ];
            }

            ###DataProduct
            DataProduct m::Product
            {
              accessPoints: [
                model Model { mapping: m::M; } [
                  everyone: LH(|#>{m::DB.P}#)
                ]
              ]
            }

            ###DataSpace
            DataSpace m::Space
            {
              executionContexts:
              [
                {
                  name: 'one';
                  mappingProvider: m::Product.%s;
                  defaultRuntime: m::R;
                }
              ];
              defaultExecutionContext: 'one';
            }
            """;

    @Test
    void aDataSpacesMappingProviderNamesAProductsModelGroup() {
        Compiler.compileModel(SPACE.formatted("model"));
        RuntimeException e = assertThrows(RuntimeException.class, () -> Compiler.compileModel(SPACE.formatted("nope")));
        assertTrue(e.getMessage().contains("has no model access group 'nope'"), e.getMessage());
    }
}
