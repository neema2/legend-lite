package com.legend.integration;

import com.legend.json.Json;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.parser.PmcdParser;
import com.legend.protocol.ModelComposer;
import com.legend.protocol.PureComposer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * A Relation class mapping's embedded block reads upstream's {@code singlePropertyMapping} on every line
 * (RelationFunctionMappingParserGrammar, legend-engine 4.145.0, the pin): a local {@code +prop: Type[m]: rhs}, a
 * further embedded block, an inline-embedded set, as well as a plain binding. Until 2026-11-16 lite read plain lines
 * only there, so the Studio roundtrip's STO_Relational-relation-function-mapping-advanced ({@code +postalCity} inside
 * {@code address ( ... )}) failed to parse ("expected identifier, got PLUS").
 *
 * <p>The wire is upstream's walker's: a line inside an embedded block has no class and no source
 * ({@code visitPropertyMapping(c, null, null, ...)}), and the printer writes a nested block at the class mapping's own
 * tab level ({@code DEPRECATED_PureGrammarComposerCore.visit(RelationFunctionEmbeddedPropertyMapping)}).
 */
@DisplayName("a Relation mapping's embedded block holds every kind of line")
class RelationMappingEmbeddedLinesTest {

    /** Every kind of line inside one embedded block, printed as upstream prints it (STANDARD). */
    private static final String PRINTED = """
            ###Mapping
            Mapping x::M
            (
              *x::Person[person]: Relation
              {
                ~func x::people():Relation<Any>[1]
                name: NAME,
                address
                (
                  street: STREET,
                  +postalCity: String[0..1]: $src.CITY->map(c|$c->toUpper()),
                  geo
                (
                  city: CITY
                ),
                  owner () Inline [person]
                )
              }
            )
            """;

    private static String model(DatabaseType db) {
        return """
                ###Relational
                Database x::DB
                (
                  Table PERSON ( ID INTEGER PRIMARY KEY, NAME VARCHAR(100), STREET VARCHAR(100), CITY VARCHAR(100) )
                )
                ###Pure
                Class x::Geo { city: String[0..1]; }
                Class x::Address { street: String[0..1]; city: String[0..1]; geo: x::Geo[0..1]; }
                Class x::Person { name: String[1]; address: x::Address[0..1]; }
                function x::people(): meta::pure::metamodel::relation::Relation<meta::pure::metamodel::type::Any>[1]
                {
                  #>{x::DB.PERSON}#
                }
                ###Mapping
                Mapping x::M
                (
                  *x::Person: Relation
                  {
                    ~func x::people():Relation<Any>[1]
                    name: NAME,
                    address
                    (
                      street: STREET,
                      +postalCity: String[0..1]: $src.CITY->map(c|$c->toUpper()),
                      city: $src.CITY,
                      geo ( city: CITY )
                    )
                  }
                )
                ###Connection
                """ + DataDeliveryMatrixTest.connection(db, "specification: LocalH2 { "
                + "testDataSetupCSV: 'default\\nPERSON\\nID,NAME,STREET,CITY\\n"
                + "1,Ann,Main St,Boston\\n2,Bob,,\\n'; }; auth: DefaultH2;") + """
                ###Runtime
                Runtime x::RT { mappings: [ x::M ]; connections: [ x::DB: [ c: x::Conn ] ]; }
                """;
    }

    @Test
    @DisplayName("parsed, written as JSON, read back and printed: the text as written")
    void printsBackAsWritten() {
        String json = PmcdParser.parseDocument(PRINTED);
        assertEquals(PRINTED.stripTrailing(), ModelComposer.model(json, PureComposer.Style.STANDARD).stripTrailing());
    }

    @Test
    @DisplayName("a line inside the block is upstream's: no class, no source; the local line carries its type")
    void theNestedLinesWire() {
        Json.Obj pmcd = (Json.Obj) Json.parse(PmcdParser.parseDocument(PRINTED), new Json.Config(4096));
        Json.Obj mapping = (Json.Obj) ((Json.Arr) pmcd.fields().get("elements")).items().stream()
                .filter(e -> "mapping".equals(((Json.Obj) e).getStringOr("_type", ""))).findFirst().orElseThrow();
        Json.Obj cm = (Json.Obj) ((Json.Arr) mapping.fields().get("classMappings")).items().get(0);
        Json.Obj address = (Json.Obj) ((Json.Arr) cm.fields().get("propertyMappings")).items().get(1);
        assertEquals("relationFunctionEmbeddedPropertyMapping", address.getStringOr("_type", ""));
        assertEquals("person", address.getStringOr("source", ""), "the outer line carries the set's id");
        List<Json.Node> lines = ((Json.Arr) address.fields().get("propertyMappings")).items();
        assertEquals(4, lines.size());
        Json.Obj local = (Json.Obj) lines.get(1);
        assertEquals("relationFunctionPropertyMapping", local.getStringOr("_type", ""));
        Json.Obj localInfo = (Json.Obj) local.fields().get("localMappingProperty");
        assertEquals("String", localInfo.getStringOr("type", ""));
        Json.Obj localPointer = (Json.Obj) local.fields().get("property");
        assertEquals("postalCity", localPointer.getStringOr("property", ""));
        assertFalse(localPointer.fields().containsKey("class"), "a nested line names no class");
        assertFalse(local.fields().containsKey("source"), "a nested line names no source");
        Json.Obj geo = (Json.Obj) lines.get(2);
        assertEquals("relationFunctionEmbeddedPropertyMapping", geo.getStringOr("_type", ""));
        assertFalse(((Json.Obj) geo.fields().get("property")).fields().containsKey("class"));
        Json.Obj owner = (Json.Obj) lines.get(3);
        assertEquals("person", owner.getStringOr("id", ""), "the inline line names its set");
    }

    @Test
    @DisplayName("the mapping with a local line in its embedded block runs; the nested block reads its row too")
    void runs() throws Exception {
        String query = "|x::Person.all()->project(~[n:x|$x.name, s:x|$x.address.street, c:x|$x.address.city,"
                + " g:x|$x.address.geo.city])->sort(~n->ascending())->from(x::M, x::RT)";
        List<List<String>> rows = List.of(Arrays.asList("Ann", "Main St", "Boston", "Boston"),
                Arrays.asList("Bob", "null", "null", "null"));
        for (DatabaseType db : DataDeliveryMatrixTest.DATABASES) {
            List<List<String>> got = DataDeliveryMatrixTest.read(
                    new DataDeliveryMatrixTest.Data(model(db), query, List.of(), "x::RT"), query, db);
            assertEquals(rows, got, db + ": the embedded block's lines");
        }
    }
}
