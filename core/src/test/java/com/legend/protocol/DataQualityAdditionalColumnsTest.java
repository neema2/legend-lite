package com.legend.protocol;

import com.legend.Compiler;
import com.legend.json.Json;
import com.legend.parser.PmcdParser;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A DataQualityRelationComparison's {@code additionalColumnsToPersist: [col, ...];}: legend-engine 4.148.0's optional
 * list (DataQualityParserGrammar, DataQualityTreeWalker, the protocol's {@code additionalColumnsToPersist},
 * DataQualityGrammarComposerExtension), built ahead of the 4.145.0 pin on the user's ruling of 2026-11-16 ("Build
 * ahead, ledgered"; docs/SEMANTICS_REGISTER.md S422). Until then lite refused it, "unknown DataQualityRelationComparison
 * key 'additionalColumnsToPersist'" (the Studio roundtrip's DSL_DataQualityRelationComparison).
 *
 * <p>The JSON writes the list only when it is not empty, so a comparison without one keeps the pin's bytes (4.148.0
 * writes {@code []}; lite reads that too). Lite carries a DataQuality element and checks none of its column lists
 * (4.148.0's compiler checks this one's columns against the relations and the other lists, as it checks keys and
 * columnsToCompare): S422.
 */
@DisplayName("a DataQualityRelationComparison's additionalColumnsToPersist")
class DataQualityAdditionalColumnsTest {

    /** The Studio case, as legend-engine 4.148.0 prints it (STANDARD). */
    private static final String PRINTED = """
            ###Relational
            Database store::db
            (
              Table personTable
              (
                ID VARCHAR(200),
                FIRSTNAME VARCHAR(200),
                LASTNAME VARCHAR(200),
                AGE VARCHAR(200),
                ADDRESSID VARCHAR(200),
                FIRMID VARCHAR(200)
              )
            )


            ###Runtime
            Runtime model::DummyRuntime
            {
              mappings:
              [
                model::DummyMapping
              ];
            }


            ###Pure
            Class model::Person
            {
              ID: String[1];
              FIRSTNAME: String[1];
              LASTNAME: String[1];
              AGE: String[1];
              ADDRESSID: String[1];
              FIRMID: String[1];
            }


            ###Mapping
            Mapping model::DummyMapping
            (
              *model::Person: Relational
              {
                ~mainTable [store::db]personTable
                ID: [store::db]personTable.ID,
                FIRSTNAME: [store::db]personTable.FIRSTNAME,
                LASTNAME: [store::db]personTable.LASTNAME,
                AGE: [store::db]personTable.AGE,
                ADDRESSID: [store::db]personTable.ADDRESSID,
                FIRMID: [store::db]personTable.FIRMID
              }
            )


            ###DataQualityValidation
            DataQualityRelationComparison model::relationComparison
            {
               source: |#>{store::db.personTable}#->select(~[ID, FIRSTNAME, LASTNAME, AGE, ADDRESSID])->from(model::DummyRuntime);
               target: |#>{store::db.personTable}#->select(~[ID, FIRSTNAME, LASTNAME, AGE, FIRMID])->from(model::DummyRuntime);
               keys: [ID];
               columnsToCompare: [LASTNAME];
               additionalColumnsToPersist: [AGE, ADDRESSID, FIRMID];
               strategy: MD5Hash;
            }
            """;

    private static final Json.Config DEEP = new Json.Config(4096);

    @Test
    @DisplayName("parsed, written as JSON, read back and printed: the text as written")
    void printsBackAsWritten() {
        assertEquals(PRINTED, ModelComposer.model(PmcdParser.parseDocument(PRINTED), PureComposer.Style.STANDARD));
    }

    @Test
    @DisplayName("the JSON names the list first, in its order; a comparison without one writes no key")
    void theWire() {
        Json.Obj comparison = comparison(PmcdParser.parseDocument(PRINTED));
        assertEquals("additionalColumnsToPersist", new ArrayList<>(comparison.fields().keySet()).get(1),
                "first after _type, by name, as the engine's mapper writes it");
        assertEquals(List.of("AGE", "ADDRESSID", "FIRMID"), strings(comparison.fields().get("additionalColumnsToPersist")));
        Json.Obj without = comparison(PmcdParser.parseDocument(
                PRINTED.replace("   additionalColumnsToPersist: [AGE, ADDRESSID, FIRMID];\n", "")));
        assertFalse(without.fields().containsKey("additionalColumnsToPersist"), "the pin's bytes when there is none");
    }

    @Test
    @DisplayName("4.148.0's empty list reads, and prints no line")
    void anEmptyListReads() {
        String json = PmcdParser.parseDocument(PRINTED.replace("   additionalColumnsToPersist: [AGE, ADDRESSID, FIRMID];\n", ""));
        Json.Obj pmcd = (Json.Obj) Json.parse(json, DEEP);
        List<Json.Node> elements = new ArrayList<>();
        for (Json.Node e : ((Json.Arr) pmcd.fields().get("elements")).items()) {
            Json.Obj o = (Json.Obj) e;
            if ("dataQualityRelationComparison".equals(o.getStringOr("_type", ""))) {
                LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>(o.fields());
                f.put("additionalColumnsToPersist", new Json.Arr(List.of()));
                o = new Json.Obj(f);
            }
            elements.add(o);
        }
        LinkedHashMap<String, Json.Node> top = new LinkedHashMap<>(pmcd.fields());
        top.put("elements", new Json.Arr(elements));
        String printed = ModelComposer.model(new Json.Obj(top));
        assertFalse(printed.contains("additionalColumnsToPersist"), printed);
        assertTrue(printed.contains("   columnsToCompare: [LASTNAME];\n   strategy: MD5Hash;\n"), printed);
    }

    @Test
    @DisplayName("the list is 4.148.0's (identifier (COMMA identifier)*)?: a trailing comma is refused, an empty list read")
    void theListsGrammar() {
        String trailing = PRINTED.replace("[AGE, ADDRESSID, FIRMID]", "[AGE,]");
        assertThrows(com.legend.error.LegendCompileException.class, () -> PmcdParser.parseDocument(trailing));
        String empty = PRINTED.replace("[AGE, ADDRESSID, FIRMID]", "[]");
        assertFalse(comparison(PmcdParser.parseDocument(empty)).fields().containsKey("additionalColumnsToPersist"));
    }

    @Test
    @DisplayName("the model compiles")
    void compiles() {
        assertEquals(List.of(), Compiler.compileErrors(PRINTED));
    }

    private static Json.Obj comparison(String json) {
        Json.Obj pmcd = (Json.Obj) Json.parse(json, DEEP);
        return (Json.Obj) ((Json.Arr) pmcd.fields().get("elements")).items().stream()
                .filter(e -> "dataQualityRelationComparison".equals(((Json.Obj) e).getStringOr("_type", "")))
                .findFirst().orElseThrow();
    }

    private static List<String> strings(Json.Node n) {
        List<String> out = new ArrayList<>();
        for (Json.Node x : ((Json.Arr) n).items()) {
            out.add(((Json.Str) x).value());
        }
        return out;
    }
}
