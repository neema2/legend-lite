package org.finos.legend.engine.test;

import org.antlr.v4.runtime.*;
import org.finos.legend.pure.dsl.antlr.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Nested;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the new ANTLR-based Mapping grammar.
 * Validates parsing of Relational, Pure M2M, Association, and Enumeration
 * mappings.
 */
class MappingParserTest {

    private PureParser createParser(String source) {
        PureLexer lexer = new PureLexer(CharStreams.fromString(source));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        return new PureParser(tokens);
    }

    @Nested
    @DisplayName("Relational Class Mapping")
    class RelationalMappingTests {

        @Test
        @DisplayName("Parse simple relational class mapping")
        void testSimpleRelationalMapping() {
            String source = """
                    Mapping model::PersonMapping
                    (
                        Person: Relational
                        {
                            ~mainTable [PersonDatabase] T_PERSON
                            firstName: [PersonDatabase] T_PERSON.FIRST_NAME,
                            lastName: [PersonDatabase] T_PERSON.LAST_NAME,
                            age: [PersonDatabase] T_PERSON.AGE_VAL
                        }
                    )
                    """;

            PureParser parser = createParser(source);
            PureParser.MappingContext ctx = parser.mapping();

            assertNotNull(ctx);
            assertEquals("model::PersonMapping", ctx.qualifiedName().getText());
            assertEquals(1, ctx.classMappingElement().size());

            var classMapping = ctx.classMappingElement(0);
            assertEquals("Person", classMapping.qualifiedName().getText());
            assertEquals("Relational", classMapping.classMappingType().getText());

            var body = classMapping.classMappingBody().relationalClassMappingBody();
            assertNotNull(body.mappingMainTable());
            assertEquals("PersonDatabase", body.mappingMainTable().databasePointer().qualifiedName().getText());
            assertEquals("T_PERSON", body.mappingMainTable().mappingTableRef().getText());

            // 3 property mappings
            assertEquals(3, body.relationalPropertyMapping().size());
        }

        @Test
        @DisplayName("Parse relational mapping with schema qualified table")
        void testMappingWithSchemaTable() {
            String source = """
                    Mapping model::PersonMapping
                    (
                        Person: Relational
                        {
                            ~mainTable [DB] SCHEMA.TABLE_NAME
                            name: [DB] SCHEMA.TABLE_NAME.NAME_COL
                        }
                    )
                    """;

            PureParser parser = createParser(source);
            PureParser.MappingContext ctx = parser.mapping();

            assertNotNull(ctx);
            var mainTable = ctx.classMappingElement(0)
                    .classMappingBody().relationalClassMappingBody().mappingMainTable();
            assertEquals("SCHEMA.TABLE_NAME", mainTable.mappingTableRef().getText());
        }

        @Test
        @DisplayName("Parse relational mapping with join")
        void testMappingWithJoin() {
            String source = """
                    Mapping model::PersonMapping
                    (
                        Person: Relational
                        {
                            ~mainTable [DB] PERSON
                            firstName: [DB] PERSON.FIRST_NAME,
                            address: [DB] @Person_Address | ADDRESS.STREET
                        }
                    )
                    """;

            PureParser parser = createParser(source);
            PureParser.MappingContext ctx = parser.mapping();

            assertNotNull(ctx);
            var body = ctx.classMappingElement(0).classMappingBody().relationalClassMappingBody();
            assertEquals(2, body.relationalPropertyMapping().size());

            // The second property mapping should have a join
            var addressMapping = body.relationalPropertyMapping(1);
            assertEquals("address", addressMapping.standardPropertyMapping().identifier().getText());
        }

        @Test
        @DisplayName("Parse relational mapping with root marker")
        void testMappingWithRootMarker() {
            String source = """
                    Mapping model::PersonMapping
                    (
                        *Person: Relational
                        {
                            ~mainTable [DB] T_PERSON
                            id: [DB] T_PERSON.ID
                        }
                    )
                    """;

            PureParser parser = createParser(source);
            PureParser.MappingContext ctx = parser.mapping();

            assertNotNull(ctx);
            var classMapping = ctx.classMappingElement(0);
            assertNotNull(classMapping.STAR()); // Root marker present
        }

        @Test
        @DisplayName("Parse relational mapping with mapping ID")
        void testMappingWithId() {
            String source = """
                    Mapping model::PersonMapping
                    (
                        Person[person_set1]: Relational
                        {
                            ~mainTable [DB] T_PERSON
                            id: [DB] T_PERSON.ID
                        }
                    )
                    """;

            PureParser parser = createParser(source);
            PureParser.MappingContext ctx = parser.mapping();

            assertNotNull(ctx);
            var classMapping = ctx.classMappingElement(0);
            assertEquals("person_set1", classMapping.mappingElementId().getText());
        }

        @Test
        @DisplayName("Parse relational mapping with extends")
        void testMappingWithExtends() {
            String source = """
                    Mapping model::PersonMapping
                    (
                        Employee[emp] extends [person_base]: Relational
                        {
                            ~mainTable [DB] T_EMPLOYEE
                            salary: [DB] T_EMPLOYEE.SALARY
                        }
                    )
                    """;

            PureParser parser = createParser(source);
            PureParser.MappingContext ctx = parser.mapping();

            assertNotNull(ctx);
            var classMapping = ctx.classMappingElement(0);
            assertEquals("emp", classMapping.mappingElementId().getText());
            assertEquals("person_base", classMapping.superClassMappingId().getText());
        }
    }

    @Nested
    @DisplayName("Pure M2M Class Mapping")
    class PureM2MMappingTests {

        @Test
        @DisplayName("Parse simple Pure M2M class mapping")
        void testSimplePureMapping() {
            String source = """
                    Mapping model::PersonMapping
                    (
                        TargetPerson: Pure
                        {
                            ~src SourcePerson
                            fullName: $src.firstName + ' ' + $src.lastName,
                            age: $src.age
                        }
                    )
                    """;

            PureParser parser = createParser(source);
            PureParser.MappingContext ctx = parser.mapping();

            assertNotNull(ctx);
            var classMapping = ctx.classMappingElement(0);
            assertEquals("TargetPerson", classMapping.qualifiedName().getText());
            assertEquals("Pure", classMapping.classMappingType().getText());

            var body = classMapping.classMappingBody().pureM2MClassMappingBody();
            assertNotNull(body.pureM2MSrcClause());
            assertEquals("SourcePerson", body.pureM2MSrcClause().qualifiedName().getText());

            assertEquals(2, body.pureM2MPropertyMapping().size());
        }

        @Test
        @DisplayName("Parse Pure M2M mapping with filter")
        void testPureMappingWithFilter() {
            String source = """
                    Mapping model::ActivePersonMapping
                    (
                        ActivePerson: Pure
                        {
                            ~src Person
                            ~filter $src.isActive == true
                            name: $src.name
                        }
                    )
                    """;

            PureParser parser = createParser(source);
            PureParser.MappingContext ctx = parser.mapping();

            assertNotNull(ctx);
            var body = ctx.classMappingElement(0).classMappingBody().pureM2MClassMappingBody();
            assertNotNull(body.pureM2MSrcClause());
            assertNotNull(body.pureM2MFilterClause());
        }
    }

    @Nested
    @DisplayName("Association Mapping")
    class AssociationMappingTests {

        @Test
        @DisplayName("Parse association mapping")
        void testAssociationMapping() {
            String source = """
                    Mapping model::PersonAddressMapping
                    (
                        PersonAddressAssociation: AssociationMapping
                        (
                            persons: [DB] @Address_Person,
                            addresses: [DB] @Person_Address
                        )
                    )
                    """;

            PureParser parser = createParser(source);
            PureParser.MappingContext ctx = parser.mapping();

            assertNotNull(ctx);
            assertEquals(1, ctx.associationMappingElement().size());

            var assocMapping = ctx.associationMappingElement(0);
            assertEquals("PersonAddressAssociation", assocMapping.qualifiedName().getText());
            assertEquals(2, assocMapping.associationPropertyMapping().size());

            assertEquals("persons", assocMapping.associationPropertyMapping(0).identifier().getText());
            assertEquals("addresses", assocMapping.associationPropertyMapping(1).identifier().getText());
        }
    }

    @Nested
    @DisplayName("Enumeration Mapping")
    class EnumerationMappingTests {

        @Test
        @DisplayName("Parse enumeration mapping with single values")
        void testEnumerationMappingSingleValues() {
            String source = """
                    Mapping model::StatusMapping
                    (
                        Status: EnumerationMapping
                        {
                            ACTIVE: 'A',
                            INACTIVE: 'I'
                        }
                    )
                    """;

            PureParser parser = createParser(source);
            PureParser.MappingContext ctx = parser.mapping();

            assertNotNull(ctx);
            assertEquals(1, ctx.enumerationMappingElement().size());

            var enumMapping = ctx.enumerationMappingElement(0);
            assertEquals("Status", enumMapping.qualifiedName().getText());
            assertEquals(2, enumMapping.enumValueMapping().size());

            assertEquals("ACTIVE", enumMapping.enumValueMapping(0).identifier().getText());
            assertEquals("INACTIVE", enumMapping.enumValueMapping(1).identifier().getText());
        }

        @Test
        @DisplayName("Parse enumeration mapping with array values")
        void testEnumerationMappingArrayValues() {
            String source = """
                    Mapping model::StatusMapping
                    (
                        Status: EnumerationMapping
                        {
                            ACTIVE: ['A', 'ACT', 'ACTIVE'],
                            INACTIVE: 'I'
                        }
                    )
                    """;

            PureParser parser = createParser(source);
            PureParser.MappingContext ctx = parser.mapping();

            assertNotNull(ctx);
            var enumMapping = ctx.enumerationMappingElement(0);

            // First value should be an array
            var activeMapping = enumMapping.enumValueMapping(0);
            assertNotNull(activeMapping.enumSourceValueArray());
            assertEquals(3, activeMapping.enumSourceValueArray().enumSourceValue().size());
        }
    }

    @Nested
    @DisplayName("Combined Mappings")
    class CombinedMappingTests {

        @Test
        @DisplayName("Parse mapping with multiple element types")
        void testCombinedMapping() {
            String source = """
                    Mapping model::FullMapping
                    (
                        Person: Relational
                        {
                            ~mainTable [DB] T_PERSON
                            name: [DB] T_PERSON.NAME
                        }

                        Address: Relational
                        {
                            ~mainTable [DB] T_ADDRESS
                            street: [DB] T_ADDRESS.STREET
                        }

                        PersonAddressAssociation: AssociationMapping
                        (
                            persons: [DB] @Address_Person,
                            addresses: [DB] @Person_Address
                        )

                        Status: EnumerationMapping
                        {
                            ACTIVE: 'A'
                        }
                    )
                    """;

            PureParser parser = createParser(source);
            PureParser.MappingContext ctx = parser.mapping();

            assertNotNull(ctx);
            assertEquals(2, ctx.classMappingElement().size());
            assertEquals(1, ctx.associationMappingElement().size());
            assertEquals(1, ctx.enumerationMappingElement().size());
        }

        @Test
        @DisplayName("Parse mapping with include")
        void testMappingWithInclude() {
            String source = """
                    Mapping model::ExtendedMapping
                    (
                        include model::BaseMapping

                        NewClass: Relational
                        {
                            ~mainTable [DB] T_NEW
                            id: [DB] T_NEW.ID
                        }
                    )
                    """;

            PureParser parser = createParser(source);
            PureParser.MappingContext ctx = parser.mapping();

            assertNotNull(ctx);
            assertEquals(1, ctx.includeMapping().size());
            assertEquals("model::BaseMapping", ctx.includeMapping(0).qualifiedName().getText());
            assertEquals(1, ctx.classMappingElement().size());
        }
    }
}
