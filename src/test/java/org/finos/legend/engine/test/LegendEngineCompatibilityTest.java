package org.finos.legend.engine.test;

import org.finos.legend.pure.dsl.definition.*;
import org.finos.legend.pure.dsl.antlr.PureDefinitionBuilder;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for legend-engine Pure file compatibility.
 * 
 * Tests the ability to parse Pure files with:
 * - Section headers (###Pure, ###Relational, ###Mapping)
 * - Import statements (import package::name::*)
 * - Comments (line and block)
 */
@DisplayName("Legend-Engine Compatibility Tests")
class LegendEngineCompatibilityTest {

    // ==================== Phase 1: Section Headers + Imports ====================

    @Nested
    @DisplayName("Section Header Stripping")
    class SectionHeaderTests {

        @Test
        @DisplayName("Strip ###Pure section header")
        void testStripPureSectionHeader() {
            String source = """
                    ###Pure
                    Class model::Person
                    {
                        name: String[1];
                    }
                    """;

            List<PureDefinition> defs = PureDefinitionBuilder.parse(source);

            assertEquals(1, defs.size());
            assertInstanceOf(ClassDefinition.class, defs.get(0));
            assertEquals("model::Person", ((ClassDefinition) defs.get(0)).qualifiedName());
        }

        @Test
        @DisplayName("Strip multiple section headers")
        void testStripMultipleSectionHeaders() {
            String source = """
                    ###Pure
                    Class model::Person
                    {
                        name: String[1];
                    }

                    ###Relational
                    Database store::DB
                    (
                        Table T_PERSON
                        (
                            ID INTEGER PRIMARY KEY,
                            NAME VARCHAR(100) NOT NULL
                        )
                    )

                    ###Mapping
                    Mapping model::Map
                    (
                        Person: Relational
                        {
                            ~mainTable [DB] T_PERSON
                            name: [DB] T_PERSON.NAME
                        }
                    )
                    """;

            List<PureDefinition> defs = PureDefinitionBuilder.parse(source);

            assertEquals(3, defs.size());
            assertInstanceOf(ClassDefinition.class, defs.get(0));
            assertInstanceOf(DatabaseDefinition.class, defs.get(1));
            assertInstanceOf(MappingDefinition.class, defs.get(2));
        }

        @Test
        @DisplayName("Handle various section header formats")
        void testVariousSectionHeaders() {
            String source = """
                    ###Pure
                    Class model::A { prop: String[1]; }
                    ###Relational
                    Database store::DB ( Table T ( ID INTEGER ) )
                    ###Mapping
                    Mapping model::M ( A: Relational { ~mainTable [DB] T } )
                    """;

            List<PureDefinition> defs = PureDefinitionBuilder.parse(source);
            assertEquals(3, defs.size());
        }
    }

    @Nested
    @DisplayName("Import Statement Parsing")
    class ImportTests {

        @Test
        @DisplayName("Strip import statements from source")
        void testStripImportStatements() {
            String source = """
                    ###Pure
                    import simple::model::*;

                    Class simple::model::Person
                    {
                        name: String[1];
                    }
                    """;

            List<PureDefinition> defs = PureDefinitionBuilder.parse(source);

            assertEquals(1, defs.size());
            assertInstanceOf(ClassDefinition.class, defs.get(0));
        }

        @Test
        @DisplayName("Strip multiple import statements")
        void testStripMultipleImports() {
            String source = """
                    ###Mapping
                    import simple::model::*;
                    import simple::store::*;

                    Mapping simple::mapping::Map
                    (
                        Person: Relational
                        {
                            ~mainTable [DB] T_PERSON
                            name: [DB] T_PERSON.NAME
                        }
                    )
                    """;

            // Should not throw - imports are stripped
            List<PureDefinition> defs = PureDefinitionBuilder.parse(source);
            assertEquals(1, defs.size());
            assertInstanceOf(MappingDefinition.class, defs.get(0));
        }
    }

    @Nested
    @DisplayName("ImportScope Resolution")
    class ImportScopeTests {

        @Test
        @DisplayName("Wildcard import resolution")
        void testWildcardImportResolution() {
            ImportScope scope = new ImportScope();
            scope.addImport("simple::model::*");

            Set<String> knownTypes = Set.of(
                    "simple::model::Person",
                    "simple::model::Address",
                    "simple::store::DB");

            // Should resolve to fully qualified name
            assertEquals("simple::model::Person", scope.resolve("Person", knownTypes));
            assertEquals("simple::model::Address", scope.resolve("Address", knownTypes));

            // Already qualified - return as-is
            assertEquals("other::pkg::Type", scope.resolve("other::pkg::Type", knownTypes));

            // Unknown type - return as-is
            assertEquals("Unknown", scope.resolve("Unknown", knownTypes));
        }

        @Test
        @DisplayName("Specific type import resolution")
        void testSpecificImportResolution() {
            ImportScope scope = new ImportScope();
            scope.addImport("some::other::pkg::SpecialType");

            Set<String> knownTypes = Set.of("some::other::pkg::SpecialType");

            // Specific import takes precedence
            assertEquals("some::other::pkg::SpecialType", scope.resolve("SpecialType", knownTypes));
        }

        @Test
        @DisplayName("Multiple wildcard imports - first match wins")
        void testMultipleWildcardImports() {
            ImportScope scope = new ImportScope();
            scope.addImport("pkg1::*");
            scope.addImport("pkg2::*");

            Set<String> knownTypes = Set.of("pkg1::Type", "pkg2::Type");

            // First matching wildcard wins
            assertEquals("pkg1::Type", scope.resolve("Type", knownTypes));
        }

        @Test
        @DisplayName("resolveSimple uses first wildcard without type checking")
        void testResolveSimple() {
            ImportScope scope = new ImportScope();
            scope.addImport("simple::model::*");

            // resolveSimple doesn't check against known types
            assertEquals("simple::model::UnknownType", scope.resolveSimple("UnknownType"));
        }
    }

    @Nested
    @DisplayName("Simple Name Lookups in PureModelBuilder")
    class SimpleNameLookupTests {

        @Test
        @DisplayName("Look up class by simple name")
        void testLookupClassBySimpleName() {
            String source = """
                    Class model::Person
                    {
                        name: String[1];
                    }
                    """;

            PureModelBuilder builder = new PureModelBuilder().addSource(source);

            // Both qualified and simple lookups should work
            assertNotNull(builder.getClass("model::Person"));
            assertNotNull(builder.getClass("Person"));
            assertEquals(builder.getClass("model::Person"), builder.getClass("Person"));
        }

        @Test
        @DisplayName("Look up association by simple name")
        void testLookupAssociationBySimpleName() {
            String source = """
                    Class model::Person { name: String[1]; }
                    Class model::Address { street: String[1]; }
                    Association model::Person_Address
                    {
                        person: Person[1];
                        addresses: Address[*];
                    }
                    """;

            PureModelBuilder builder = new PureModelBuilder().addSource(source);

            // Both qualified and simple lookups should work
            assertTrue(builder.getAssociation("model::Person_Address").isPresent());
            assertTrue(builder.getAssociation("Person_Address").isPresent());
        }
    }

    @Nested
    @DisplayName("Comments Handling")
    class CommentTests {

        @Test
        @DisplayName("Line comments are ignored")
        void testLineComments() {
            String source = """
                    // This is a comment
                    Class model::Person
                    {
                        // Comment inside class
                        name: String[1]; // End of line comment
                    }
                    """;

            List<PureDefinition> defs = PureDefinitionBuilder.parse(source);

            assertEquals(1, defs.size());
            ClassDefinition classDef = (ClassDefinition) defs.get(0);
            assertEquals(1, classDef.properties().size());
        }

        @Test
        @DisplayName("Block comments are ignored")
        void testBlockComments() {
            String source = """
                    /*
                     * Copyright 2022 Goldman Sachs
                     * Licensed under Apache 2.0
                     */
                    Class model::Person
                    {
                        /* multi-line
                           comment */
                        name: String[1];
                    }
                    """;

            List<PureDefinition> defs = PureDefinitionBuilder.parse(source);

            assertEquals(1, defs.size());
        }
    }

    @Nested
    @DisplayName("Full Legend-Engine Format")
    class FullFormatTests {

        @Test
        @DisplayName("Parse complete legend-engine style Pure file")
        void testCompleteLegendEngineFormat() {
            String source = """
                    // Copyright 2022 Goldman Sachs
                    // SPDX-License-Identifier: Apache-2.0

                    ###Pure
                    import simple::model::*;

                    Class simple::model::Firm
                    {
                        legalName: String[1];
                    }

                    Class simple::model::Person
                    {
                        firstName: String[1];
                        lastName: String[1];
                    }

                    Association simple::model::Firm_Employees
                    {
                        employees: Person[*];
                        employer: Firm[0..1];
                    }

                    ###Relational
                    Database simple::store::DB
                    (
                        Table FIRM_TABLE
                        (
                            ID INTEGER PRIMARY KEY,
                            LEGAL_NAME VARCHAR(100) NOT NULL
                        )
                        Table PERSON_TABLE
                        (
                            ID INTEGER PRIMARY KEY,
                            FIRST_NAME VARCHAR(100) NOT NULL,
                            LAST_NAME VARCHAR(100) NOT NULL,
                            FIRM_ID INTEGER
                        )
                        Join PERSON_FIRM(PERSON_TABLE.FIRM_ID = FIRM_TABLE.ID)
                    )

                    ###Mapping
                    import simple::model::*;
                    import simple::store::*;

                    Mapping simple::mapping::Map
                    (
                        Firm: Relational
                        {
                            ~mainTable [DB] FIRM_TABLE
                            legalName: [DB] FIRM_TABLE.LEGAL_NAME
                        }

                        Person: Relational
                        {
                            ~mainTable [DB] PERSON_TABLE
                            firstName: [DB] PERSON_TABLE.FIRST_NAME,
                            lastName: [DB] PERSON_TABLE.LAST_NAME
                        }
                    )
                    """;

            List<PureDefinition> defs = PureDefinitionBuilder.parse(source);

            // Should parse 5 definitions: 2 classes + 1 association + 1 database + 1
            // mapping
            assertEquals(5, defs.size());

            // Verify types
            long classCount = defs.stream().filter(d -> d instanceof ClassDefinition).count();
            long assocCount = defs.stream().filter(d -> d instanceof AssociationDefinition).count();
            long dbCount = defs.stream().filter(d -> d instanceof DatabaseDefinition).count();
            long mappingCount = defs.stream().filter(d -> d instanceof MappingDefinition).count();

            assertEquals(2, classCount, "Expected 2 classes");
            assertEquals(1, assocCount, "Expected 1 association");
            assertEquals(1, dbCount, "Expected 1 database");
            assertEquals(1, mappingCount, "Expected 1 mapping");
        }

        @Test
        @DisplayName("Parse legend-engine file with all features")
        void testAllFeaturesLegendEngineFormat() {
            String source = """
                    // Copyright 2022 Goldman Sachs
                    // SPDX-License-Identifier: Apache-2.0

                    /*
                     * Multi-line block comment
                     * Testing comment stripping
                     */

                    ###Pure
                    import simple::model::*;
                    import simple::store::*;

                    Class simple::model::Firm
                    {
                        legalName: String[1];
                        employees: Person[*];

                        // Derived property without parameters
                        employeeCount() {$this.employees->size()}: Integer[1];

                        // Parameterized derived property
                        employeeByName(name: String[1]) {$this.employees->filter(e|$e.firstName == $name)->first()}: Person[0..1];
                    }

                    Class simple::model::Person
                    {
                        firstName: String[1];
                        lastName: String[1];

                        // Derived property with expression
                        fullName() {$this.firstName + ' ' + $this.lastName}: String[1];
                    }

                    Association simple::model::Firm_Employees
                    {
                        firm: Firm[0..1];
                        employees: Person[*];
                    }

                    ###Relational
                    Database simple::store::DB
                    (
                        Table FIRM_TABLE
                        (
                            ID INTEGER PRIMARY KEY,
                            LEGAL_NAME VARCHAR(100) NOT NULL
                        )
                        Table PERSON_TABLE
                        (
                            ID INTEGER PRIMARY KEY,
                            FIRST_NAME VARCHAR(100) NOT NULL,
                            LAST_NAME VARCHAR(100) NOT NULL,
                            FIRM_ID INTEGER
                        )
                        Join PERSON_FIRM(PERSON_TABLE.FIRM_ID = FIRM_TABLE.ID)
                    )

                    ###Mapping
                    import simple::model::*;
                    import simple::store::*;

                    Mapping simple::mapping::FirmPersonMapping
                    (
                        Firm: Relational
                        {
                            ~mainTable [DB] FIRM_TABLE
                            legalName: [DB] FIRM_TABLE.LEGAL_NAME
                        }

                        Person: Relational
                        {
                            ~mainTable [DB] PERSON_TABLE
                            firstName: [DB] PERSON_TABLE.FIRST_NAME,
                            lastName: [DB] PERSON_TABLE.LAST_NAME
                        }
                    )
                    """;

            List<PureDefinition> defs = PureDefinitionBuilder.parse(source);

            // Verify all definitions parsed
            assertEquals(5, defs.size(), "Expected 5 definitions (2 classes + 1 assoc + 1 db + 1 mapping)");

            // Find and verify Firm class
            ClassDefinition firmClass = defs.stream()
                    .filter(d -> d instanceof ClassDefinition)
                    .map(d -> (ClassDefinition) d)
                    .filter(c -> c.qualifiedName().endsWith("Firm"))
                    .findFirst()
                    .orElseThrow();

            assertEquals(2, firmClass.derivedProperties().size(), "Firm should have 2 derived properties");

            // Verify parameterized derived property
            var employeeByName = firmClass.derivedProperties().stream()
                    .filter(dp -> dp.name().equals("employeeByName"))
                    .findFirst()
                    .orElseThrow();
            assertTrue(employeeByName.hasParameters(), "employeeByName should have parameters");
            assertEquals(1, employeeByName.parameters().size());
            assertEquals("name", employeeByName.parameters().get(0).name());

            // Find and verify Person class
            ClassDefinition personClass = defs.stream()
                    .filter(d -> d instanceof ClassDefinition)
                    .map(d -> (ClassDefinition) d)
                    .filter(c -> c.qualifiedName().endsWith("Person"))
                    .findFirst()
                    .orElseThrow();

            assertEquals(1, personClass.derivedProperties().size(), "Person should have 1 derived property");
            assertEquals("fullName", personClass.derivedProperties().get(0).name());
        }
    }

    // ==================== Phase 2: first() Function ====================

    @Nested
    @DisplayName("First Function")
    class FirstFunctionTests {

        @Test
        @DisplayName("Parse first() expression")
        void testParseFirst() {
            String query = "Person.all()->first()";

            org.finos.legend.pure.dsl.PureExpression expr = org.finos.legend.pure.dsl.PureParser.parse(query);

            assertInstanceOf(org.finos.legend.pure.dsl.FirstExpression.class, expr);
            org.finos.legend.pure.dsl.FirstExpression first = (org.finos.legend.pure.dsl.FirstExpression) expr;
            assertInstanceOf(org.finos.legend.pure.dsl.ClassAllExpression.class, first.source());
        }

        @Test
        @DisplayName("Parse first() with filter")
        void testParseFirstWithFilter() {
            String query = "Person.all()->filter(p|$p.name == 'John')->first()";

            org.finos.legend.pure.dsl.PureExpression expr = org.finos.legend.pure.dsl.PureParser.parse(query);

            assertInstanceOf(org.finos.legend.pure.dsl.FirstExpression.class, expr);
        }
    }

    // ==================== Phase 3: Parameterized Derived Properties
    // ====================

    @Nested
    @DisplayName("Parameterized Derived Properties")
    class ParameterizedDerivedPropertyTests {

        @Test
        @DisplayName("Parse derived property with parameter")
        void testParseParameterizedDerivedProperty() {
            String source = """
                    Class simple::model::Person
                    {
                        firstName: String[1];
                        lastName: String[1];
                        firmByName(name: String[1]) {$this.firms->filter(f|$f.name == $name)->first()}: simple::model::Firm[0..1];
                    }
                    """;

            List<PureDefinition> defs = PureDefinitionBuilder.parse(source);

            assertEquals(1, defs.size());
            ClassDefinition classDef = (ClassDefinition) defs.get(0);
            assertEquals(1, classDef.derivedProperties().size());

            var derivedProp = classDef.derivedProperties().get(0);
            assertEquals("firmByName", derivedProp.name());
            assertTrue(derivedProp.hasParameters());
            assertEquals(1, derivedProp.parameters().size());

            var param = derivedProp.parameters().get(0);
            assertEquals("name", param.name());
            assertEquals("String", param.type());
        }

        @Test
        @DisplayName("Parse derived property without parameters")
        void testParseDerivedPropertyWithoutParameters() {
            String source = """
                    Class simple::model::Person
                    {
                        firstName: String[1];
                        lastName: String[1];
                        fullName() {$this.firstName + ' ' + $this.lastName}: String[1];
                    }
                    """;

            List<PureDefinition> defs = PureDefinitionBuilder.parse(source);

            ClassDefinition classDef = (ClassDefinition) defs.get(0);
            assertEquals(1, classDef.derivedProperties().size());

            var derivedProp = classDef.derivedProperties().get(0);
            assertEquals("fullName", derivedProp.name());
            assertFalse(derivedProp.hasParameters());
        }
    }

    // ==================== Phase 4: Let Bindings ====================

    @Nested
    @DisplayName("Let Bindings")
    class LetBindingTests {

        @Test
        @DisplayName("Parse let expression")
        void testParseLetExpression() {
            // Pure syntax: let x = value; must end with semicolon
            String query = "{|let x = 'hello'; $x;}";

            org.finos.legend.pure.dsl.PureExpression expr = org.finos.legend.pure.dsl.PureParser.parse(query);

            // The lambda contains a code block with let + return
            assertInstanceOf(org.finos.legend.pure.dsl.LambdaExpression.class, expr);
        }

        @Test
        @DisplayName("Parse let with variable reference in Pure query")
        void testLetInPureQuery() {
            // Let bindings in derived properties - variable should be usable
            String source = """
                    Class model::Person
                    {
                        firstName: String[1];
                        lastName: String[1];
                        greeting() {let name = $this.firstName; 'Hello, ' + $name}: String[1];
                    }
                    """;

            List<PureDefinition> defs = PureDefinitionBuilder.parse(source);

            assertEquals(1, defs.size());
            ClassDefinition classDef = (ClassDefinition) defs.get(0);
            assertEquals(1, classDef.derivedProperties().size());

            var derivedProp = classDef.derivedProperties().get(0);
            assertEquals("greeting", derivedProp.name());
            assertTrue(derivedProp.expression().contains("let"));
        }
    }
}
