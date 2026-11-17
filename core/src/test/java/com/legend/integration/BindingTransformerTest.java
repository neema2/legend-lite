package com.legend.integration;

import com.legend.model.ConnectionDefinition.DatabaseType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A relational property's {@code Binding} transformer ({@code firm: Binding B : [DB] T.FIRM}, and at a join's end,
 * {@code @J | T.FIRM}, {@code @J > @J | T.FIRM}): the column's semi-structured value decoded into the class-typed
 * property, navigated key by key as legend-engine's {@code SemiStructuredPropertyAccess} navigates it. Until
 * 2026-11-14 lite dropped the parsed binding ({@code MappingFromProtocol}) and refused the model as a Variant bound to
 * a class (the Studio roundtrip's STO_Relational-relational-semi-structured).
 *
 * <p>Every expected row is legend-engine's own, probed on the same model and data on H2 (2026-11-14). The engine
 * refuses a graph fetch of a bound property and {@code isEmpty} over one; lite refuses both too. On every in-process
 * database.
 */
@DisplayName("a relational Binding transformer")
class BindingTransformerTest {

    private static String model(DatabaseType db, String extraMapping) {
        return """
                ###Relational
                Database x::DB
                (
                  Table PERSON ( ID INTEGER PRIMARY KEY, NAME VARCHAR(100), FIRM SEMISTRUCTURED, MANAGERID INTEGER )
                  Join mgr(PERSON.MANAGERID = {target}.ID)
                )
                ###ExternalFormat
                Binding x::FirmBinding
                {
                  contentType: 'application/json';
                  modelIncludes: [ x::Firm, x::Address ];
                }
                ###Pure
                Class x::Address { city: String[1]; zip: String[0..1]; }
                Class x::Firm { legalName: String[1]; size: Integer[0..1]; address: x::Address[0..1]; }
                Class x::Person { name: String[1]; firm: x::Firm[0..1]; managerFirm: x::Firm[0..1]; mm: x::Firm[0..1]; }
                ###Mapping
                Mapping x::M
                (
                  x::Person: Relational
                  {
                    ~mainTable [x::DB]PERSON
                    name: [x::DB]PERSON.NAME,
                    firm: Binding x::FirmBinding : [x::DB]PERSON.FIRM,
                    managerFirm: Binding x::FirmBinding : [x::DB]@mgr | [x::DB]PERSON.FIRM,
                    mm: Binding x::FirmBinding : [x::DB]@mgr > [x::DB]@mgr | [x::DB]PERSON.FIRM%s
                  }
                )
                Mapping x::RM
                (
                  x::Person: Relation
                  {
                    ~func x::people():Relation<Any>[1]
                    name: NAME,
                    firm: Binding x::FirmBinding : FIRM
                  }
                )
                ###Pure
                function x::people(): meta::pure::metamodel::relation::Relation<meta::pure::metamodel::type::Any>[1]
                {
                  #>{x::DB.PERSON}#
                }
                ###Connection
                """.formatted(extraMapping) + DataDeliveryMatrixTest.connection(db, "specification: LocalH2 { "
                + "testDataSetupCSV: 'default\\nPERSON\\nID,NAME,FIRM,MANAGERID\\n"
                + "1,Ann,\"{\"\"legalName\"\":\"\"Acme\"\",\"\"size\"\":10,\"\"address\"\":{\"\"city\"\":\"\"NYC\"\"}}\",\\n"
                + "2,Bob,\"{\"\"legalName\"\":\"\"Beta\"\"}\",1\\n"
                + "3,Cy,,2\\n'; }; auth: DefaultH2;") + """
                ###Runtime
                Runtime x::RT { mappings: [ x::M, x::RM ]; connections: [ x::DB: [ c: x::Conn ] ]; }
                """;
    }

    /** One in-process database, for what does not depend on which (a refusal at model build). */
    private static final DatabaseType ANY = DataDeliveryMatrixTest.DATABASES.get(0);

    private static List<List<String>> read(DatabaseType db, String query) throws Exception {
        return DataDeliveryMatrixTest.read(new DataDeliveryMatrixTest.Data(model(db, ""), query, List.of(), "x::RT"),
                query, db);
    }

    @Test
    @DisplayName("navigation through the bound object, its nested object and a join's end: legend-engine's rows")
    void navigates() throws Exception {
        String query = "|x::Person.all()->project(~[n:x|$x.name, f:x|$x.firm.legalName, s:x|$x.firm.size,"
                + " m:x|$x.managerFirm.legalName, c:x|$x.firm.address.city, z:x|$x.firm.address.zip,"
                + " mm:x|$x.mm.legalName])->sort(~n->ascending())->from(x::M, x::RT)";
        List<List<String>> rows = List.of(
                Arrays.asList("Ann", "Acme", "10", "null", "NYC", "null", "null"),
                Arrays.asList("Bob", "Beta", "null", "Acme", "null", "null", "null"),
                Arrays.asList("Cy", "null", "null", "Beta", "null", "null", "Acme"));
        for (DatabaseType db : DataDeliveryMatrixTest.DATABASES) {
            assertEquals(rows, read(db, query), db + ": the bound column navigated");
        }
    }

    @Test
    @DisplayName("a [1] bound property and a [1] nested object (toOne over the emptiness guard): legend-engine's rows")
    void navigatesToOneProperties() throws Exception {
        String query = "|x::Person.all()->project(~[n:x|$x.name, f:x|$x.firm.legalName, c:x|$x.firm.address.city,"
                + " z:x|$x.firm.address.zip])->sort(~n->ascending())->from(x::M, x::RT)";
        List<List<String>> rows = List.of(Arrays.asList("Ann", "Acme", "NYC", "null"),
                Arrays.asList("Bob", "Beta", "null", "null"), Arrays.asList("Cy", "null", "null", "null"));
        for (DatabaseType db : DataDeliveryMatrixTest.DATABASES) {
            String model = model(db, "").replace("firm: x::Firm[0..1];", "firm: x::Firm[1];")
                    .replace("address: x::Address[0..1];", "address: x::Address[1];");
            assertEquals(rows, DataDeliveryMatrixTest.read(new DataDeliveryMatrixTest.Data(model, query, List.of(),
                    "x::RT"), query, db), db + ": the [1] properties navigated");
        }
    }

    @Test
    @DisplayName("a relation function mapping's bound column: the same decode")
    void aRelationFunctionsBoundColumn() throws Exception {
        String query = "|x::Person.all()->project(~[n:x|$x.name, f:x|$x.firm.legalName, c:x|$x.firm.address.city])"
                + "->sort(~n->ascending())->from(x::RM, x::RT)";
        List<List<String>> rows = List.of(Arrays.asList("Ann", "Acme", "NYC"), Arrays.asList("Bob", "Beta", "null"),
                Arrays.asList("Cy", "null", "null"));
        for (DatabaseType db : DataDeliveryMatrixTest.DATABASES) {
            assertEquals(rows, read(db, query), db + ": the relation function's bound column navigated");
        }
    }

    @Test
    @DisplayName("a relation function mapping's Binding, named through the section's import: the same decode")
    void aRelationFunctionsBindingResolvesThroughAnImport() throws Exception {
        String query = "|x::Person.all()->project(~[n:x|$x.name, f:x|$x.firm.legalName])"
                + "->sort(~n->ascending())->from(x::RM, x::RT)";
        String model = model(ANY, "").replace("###Mapping\nMapping x::M", "###Mapping\nimport x::*;\nMapping x::M")
                .replace("firm: Binding x::FirmBinding : FIRM", "firm: Binding FirmBinding : FIRM");
        assertTrue(model.contains("import x::*;") && model.contains("firm: Binding FirmBinding : FIRM"), model);
        assertEquals(List.of(Arrays.asList("Ann", "Acme"), Arrays.asList("Bob", "Beta"), Arrays.asList("Cy", "null")),
                DataDeliveryMatrixTest.read(new DataDeliveryMatrixTest.Data(model, query, List.of(), "x::RT"),
                        query, ANY), "the binding's short name resolved against the import");
    }

    @Test
    @DisplayName("a filter through the bound object: legend-engine's rows")
    void filters() throws Exception {
        String query = "|x::Person.all()->filter(x|$x.firm.legalName == 'Acme')->project(~[n:x|$x.name])"
                + "->from(x::M, x::RT)";
        for (DatabaseType db : DataDeliveryMatrixTest.DATABASES) {
            assertEquals(List.of(List.of("Ann")), read(db, query), db + ": the filter");
        }
    }

    @Test
    @DisplayName("a graph fetch of the bound property is refused, as legend-engine refuses it")
    void aGraphFetchOfTheBoundPropertyIsRefused() {
        String query = "|x::Person.all()->graphFetch(#{x::Person{name,firm{legalName}}}#)"
                + "->serialize(#{x::Person{name,firm{legalName}}}#)->from(x::M, x::RT)";
        assertThrows(Throwable.class, () -> read(ANY, query));
    }

    @Test
    @DisplayName("the engine's model checks: a class property, in the binding's model unit, and no array of objects")
    void theEnginesChecks() {
        assertRefused("Binding transformer can be used with complex properties only",
                model(ANY, ",\n    name: Binding x::FirmBinding : [x::DB]PERSON.NAME")
                        .replace("name: [x::DB]PERSON.NAME,", ""));
        assertRefused("should be included in modelUnit for binding",
                model(ANY, "").replace("modelIncludes: [ x::Firm, x::Address ];",
                        "modelIncludes: [ x::Address ];"));
    }

    @Test
    @DisplayName("a key lite cannot decode is deferred: the class and the object's other keys read, that key is refused by name")
    void anUndecodableKeyIsDeferred() throws Exception {
        // lite gaps, not engine refusals (MappingNormalizer's line between the two): an array of objects, a class
        // reachable from itself, a to-many bound property. Each defers its own key (VariantDecode.deferred), as
        // legend-engine's key-by-key navigation leaves the rest readable
        List<List<String>> names = List.of(List.of("Ann"), List.of("Bob"), List.of("Cy"));
        List<List<String>> firms = List.of(Arrays.asList("Ann", "Acme"), Arrays.asList("Bob", "Beta"),
                Arrays.asList("Cy", "null"));
        String name = "|x::Person.all()->project(~[n:x|$x.name])->sort(~n->ascending())->from(x::M, x::RT)";
        String firm = "|x::Person.all()->project(~[n:x|$x.name, f:x|$x.firm.legalName])->sort(~n->ascending())"
                + "->from(x::M, x::RT)";

        String arrays = model(ANY, "").replace("address: x::Address[0..1];", "address: x::Address[*];");
        com.legend.Compiler.compileModel(arrays);
        assertEquals(names, rows(arrays, name), "the owning class reads");
        assertEquals(firms, rows(arrays, firm), "the bound object's other keys read");
        assertRefusedAtUse("lite does not decode an array of objects", arrays,
                "|x::Person.all()->project(~[c:x|$x.firm.address.city])->from(x::M, x::RT)");

        String cycle = model(ANY, "").replace("size: Integer[0..1];", "size: Integer[0..1]; parent: x::Firm[0..1];");
        assertEquals(firms, rows(cycle, firm), "a class reachable from itself: its other keys read");
        assertRefusedAtUse("is reachable from itself", cycle,
                "|x::Person.all()->project(~[c:x|$x.firm.parent.legalName])->from(x::M, x::RT)");

        String many = model(ANY, "").replace("firm: x::Firm[0..1];", "firm: x::Firm[*];");
        assertEquals(names, rows(many, name), "a to-many bound property: the owning class reads");
        assertRefusedAtUse("is to-many and bound by Binding", many,
                "|x::Person.all()->project(~[c:x|$x.firm.legalName])->from(x::M, x::RT)");
    }

    private static List<List<String>> rows(String model, String query) throws Exception {
        return DataDeliveryMatrixTest.read(new DataDeliveryMatrixTest.Data(model, query, List.of(), "x::RT"), query, ANY);
    }

    private static void assertRefusedAtUse(String fragment, String model, String query) {
        Throwable t = assertThrows(Throwable.class, () -> rows(model, query));
        StringBuilder all = new StringBuilder();
        for (Throwable c = t; c != null; c = c.getCause() == c ? null : c.getCause()) {
            all.append(c.getMessage()).append(" | ");
        }
        assertTrue(all.toString().contains(fragment), all.toString());
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
