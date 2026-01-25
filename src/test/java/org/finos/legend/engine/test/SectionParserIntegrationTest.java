package org.finos.legend.engine.test;

import org.finos.legend.pure.dsl.SectionParser;
import org.finos.legend.pure.dsl.SectionParser.Section;
import org.finos.legend.pure.dsl.SectionParser.SectionType;
import org.finos.legend.pure.dsl.antlr.PureDefinitionBuilder;
import org.finos.legend.pure.dsl.definition.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Incremental test class to verify section parsing works step by step.
 * Tests each section type individually, then combinations.
 */
@DisplayName("Section Parser Integration Tests")
public class SectionParserIntegrationTest {

    // ==================== Simple Section Content ====================

    private static final String SIMPLE_CLASS = """
            Class model::Person
            {
                firstName: String[1];
                lastName: String[1];
            }
            """;

    private static final String SIMPLE_DATABASE = """
            Database store::TestDB
            (
                Table T_PERSON
                (
                    ID INTEGER PRIMARY KEY,
                    NAME VARCHAR(100)
                )
            )
            """;

    private static final String SIMPLE_MAPPING = """
            Mapping model::TestMapping
            (
            )
            """;

    private static final String SIMPLE_RUNTIME = """
            Runtime test::TestRuntime
            {
                mappings: [ model::TestMapping ];
                connections: [
                    store::TestDB: [
                        conn1: store::TestConnection
                    ]
                ];
            }
            """;

    private static final String SIMPLE_CONNECTION = """
            RelationalDatabaseConnection store::TestConnection
            {
                type: DuckDB;
                specification: InMemory { };
                auth: NoAuth { };
            }
            """;

    // ==================== Level 1: Section Parser Tests ====================

    @Nested
    @DisplayName("Level 1: Raw Section Parsing")
    class SectionParserTests {

        @Test
        @DisplayName("Parse ###Pure section only")
        void testParsePureSection() {
            String source = "###Pure\n" + SIMPLE_CLASS;
            List<Section> sections = SectionParser.parse(source);

            System.out.println("Sections found: " + sections.size());
            for (Section s : sections) {
                System.out.println("  - Type: " + s.type() + ", Content length: " + s.content().length());
            }

            assertEquals(1, sections.size(), "Should have exactly 1 section");
            assertEquals(SectionType.PURE, sections.get(0).type());
            assertTrue(sections.get(0).content().contains("Class model::Person"));
        }

        @Test
        @DisplayName("Parse ###Relational section only")
        void testParseRelationalSection() {
            String source = "###Relational\n" + SIMPLE_DATABASE;
            List<Section> sections = SectionParser.parse(source);

            System.out.println("Sections found: " + sections.size());
            for (Section s : sections) {
                System.out.println("  - Type: " + s.type() + ", Content length: " + s.content().length());
            }

            assertEquals(1, sections.size(), "Should have exactly 1 section");
            assertEquals(SectionType.RELATIONAL, sections.get(0).type());
            assertTrue(sections.get(0).content().contains("Database store::TestDB"));
        }

        @Test
        @DisplayName("Parse ###Mapping section only")
        void testParseMappingSection() {
            String source = "###Mapping\n" + SIMPLE_MAPPING;
            List<Section> sections = SectionParser.parse(source);

            System.out.println("Sections found: " + sections.size());
            for (Section s : sections) {
                System.out.println("  - Type: " + s.type() + ", Content length: " + s.content().length());
            }

            assertEquals(1, sections.size(), "Should have exactly 1 section");
            assertEquals(SectionType.MAPPING, sections.get(0).type());
            assertTrue(sections.get(0).content().contains("Mapping model::TestMapping"));
        }

        @Test
        @DisplayName("Parse ###Runtime section only")
        void testParseRuntimeSection() {
            String source = "###Runtime\n" + SIMPLE_RUNTIME;
            List<Section> sections = SectionParser.parse(source);

            System.out.println("Sections found: " + sections.size());
            for (Section s : sections) {
                System.out.println("  - Type: " + s.type() + ", Content length: " + s.content().length());
            }

            assertEquals(1, sections.size(), "Should have exactly 1 section");
            assertEquals(SectionType.RUNTIME, sections.get(0).type());
            assertTrue(sections.get(0).content().contains("Runtime test::TestRuntime"));
        }

        @Test
        @DisplayName("Parse ###Connection section only")
        void testParseConnectionSection() {
            String source = "###Connection\n" + SIMPLE_CONNECTION;
            List<Section> sections = SectionParser.parse(source);

            System.out.println("Sections found: " + sections.size());
            for (Section s : sections) {
                System.out.println("  - Type: " + s.type() + ", Content length: " + s.content().length());
            }

            assertEquals(1, sections.size(), "Should have exactly 1 section");
            assertEquals(SectionType.CONNECTION, sections.get(0).type());
            assertTrue(sections.get(0).content().contains("RelationalDatabaseConnection"));
        }

        @Test
        @DisplayName("Parse ###Pure + ###Relational combined")
        void testParsePureAndRelationalSections() {
            String source = "###Pure\n" + SIMPLE_CLASS + "\n###Relational\n" + SIMPLE_DATABASE;
            List<Section> sections = SectionParser.parse(source);

            System.out.println("Sections found: " + sections.size());
            for (Section s : sections) {
                System.out.println("  - Type: " + s.type() + ", Content preview: " +
                        s.content().substring(0, Math.min(50, s.content().length())).replace("\n", "\\n"));
            }

            assertEquals(2, sections.size(), "Should have exactly 2 sections");
            assertEquals(SectionType.PURE, sections.get(0).type());
            assertEquals(SectionType.RELATIONAL, sections.get(1).type());
        }
    }

    // ==================== Level 2: Definition Builder Tests ====================

    @Nested
    @DisplayName("Level 2: Definition Building")
    class DefinitionBuilderTests {

        @Test
        @DisplayName("Build ClassDefinition from ###Pure section")
        void testBuildClassDefinition() {
            String source = "###Pure\n" + SIMPLE_CLASS;
            List<PureDefinition> definitions = PureDefinitionBuilder.parse(source);

            System.out.println("Definitions built: " + definitions.size());
            definitions.forEach(d -> System.out.println("  - Type: " + d.getClass().getSimpleName()));

            assertFalse(definitions.isEmpty(), "Should have at least 1 definition");
            assertTrue(definitions.stream().anyMatch(d -> d instanceof ClassDefinition),
                    "Should contain a ClassDefinition");

            ClassDefinition classDef = definitions.stream()
                    .filter(d -> d instanceof ClassDefinition)
                    .map(d -> (ClassDefinition) d)
                    .findFirst()
                    .orElseThrow();
            assertEquals("model::Person", classDef.qualifiedName());
        }

        @Test
        @DisplayName("Build DatabaseDefinition from ###Relational section")
        void testBuildDatabaseDefinition() {
            String source = "###Relational\n" + SIMPLE_DATABASE;
            List<PureDefinition> definitions = PureDefinitionBuilder.parse(source);

            System.out.println("Definitions built: " + definitions.size());
            definitions.forEach(d -> System.out.println("  - Type: " + d.getClass().getSimpleName()));

            assertFalse(definitions.isEmpty(), "Should have at least 1 definition");
            assertTrue(definitions.stream().anyMatch(d -> d instanceof DatabaseDefinition),
                    "Should contain a DatabaseDefinition");
        }

        @Test
        @DisplayName("Build MappingDefinition from ###Mapping section")
        void testBuildMappingDefinition() {
            String source = "###Mapping\n" + SIMPLE_MAPPING;
            List<PureDefinition> definitions = PureDefinitionBuilder.parse(source);

            System.out.println("Definitions built: " + definitions.size());
            definitions.forEach(d -> System.out.println("  - Type: " + d.getClass().getSimpleName()));

            assertFalse(definitions.isEmpty(), "Should have at least 1 definition");
            assertTrue(definitions.stream().anyMatch(d -> d instanceof MappingDefinition),
                    "Should contain a MappingDefinition");
        }

        @Test
        @DisplayName("Build RuntimeDefinition from ###Runtime section")
        void testBuildRuntimeDefinition() {
            String source = "###Runtime\n" + SIMPLE_RUNTIME;
            List<PureDefinition> definitions = PureDefinitionBuilder.parse(source);

            System.out.println("Definitions built: " + definitions.size());
            definitions.forEach(d -> System.out.println("  - Type: " + d.getClass().getSimpleName()));

            assertFalse(definitions.isEmpty(), "Should have at least 1 definition");
            assertTrue(definitions.stream().anyMatch(d -> d instanceof RuntimeDefinition),
                    "Should contain a RuntimeDefinition");

            RuntimeDefinition runtimeDef = definitions.stream()
                    .filter(d -> d instanceof RuntimeDefinition)
                    .map(d -> (RuntimeDefinition) d)
                    .findFirst()
                    .orElseThrow();
            assertEquals("test::TestRuntime", runtimeDef.qualifiedName());
        }

        @Test
        @DisplayName("Build ConnectionDefinition from ###Connection section")
        void testBuildConnectionDefinition() {
            String source = "###Connection\n" + SIMPLE_CONNECTION;
            List<PureDefinition> definitions = PureDefinitionBuilder.parse(source);

            System.out.println("Definitions built: " + definitions.size());
            definitions.forEach(d -> System.out.println("  - Type: " + d.getClass().getSimpleName()));

            assertFalse(definitions.isEmpty(), "Should have at least 1 definition");
            assertTrue(definitions.stream().anyMatch(d -> d instanceof ConnectionDefinition),
                    "Should contain a ConnectionDefinition");
        }
    }

    // ==================== Level 3: Combined Definition Tests ====================

    @Nested
    @DisplayName("Level 3: Combined Definitions")
    class CombinedDefinitionTests {

        @Test
        @DisplayName("Build Class + Database from combined sections")
        void testBuildClassAndDatabase() {
            String source = "###Pure\n" + SIMPLE_CLASS + "\n###Relational\n" + SIMPLE_DATABASE;
            List<PureDefinition> definitions = PureDefinitionBuilder.parse(source);

            System.out.println("Definitions built: " + definitions.size());
            definitions.forEach(d -> System.out.println("  - Type: " + d.getClass().getSimpleName()));

            assertTrue(definitions.stream().anyMatch(d -> d instanceof ClassDefinition),
                    "Should contain a ClassDefinition");
            assertTrue(definitions.stream().anyMatch(d -> d instanceof DatabaseDefinition),
                    "Should contain a DatabaseDefinition");
        }

        @Test
        @DisplayName("Build Class + Database + Mapping from combined sections")
        void testBuildClassDatabaseAndMapping() {
            String source = "###Pure\n" + SIMPLE_CLASS +
                    "\n###Relational\n" + SIMPLE_DATABASE +
                    "\n###Mapping\n" + SIMPLE_MAPPING;
            List<PureDefinition> definitions = PureDefinitionBuilder.parse(source);

            System.out.println("Definitions built: " + definitions.size());
            definitions.forEach(d -> System.out.println("  - Type: " + d.getClass().getSimpleName()));

            assertTrue(definitions.stream().anyMatch(d -> d instanceof ClassDefinition),
                    "Should contain a ClassDefinition");
            assertTrue(definitions.stream().anyMatch(d -> d instanceof DatabaseDefinition),
                    "Should contain a DatabaseDefinition");
            assertTrue(definitions.stream().anyMatch(d -> d instanceof MappingDefinition),
                    "Should contain a MappingDefinition");
        }

        @Test
        @DisplayName("Build full model: Class + Database + Mapping + Runtime")
        void testBuildFullModel() {
            String source = "###Pure\n" + SIMPLE_CLASS +
                    "\n###Relational\n" + SIMPLE_DATABASE +
                    "\n###Mapping\n" + SIMPLE_MAPPING +
                    "\n###Runtime\n" + SIMPLE_RUNTIME;
            List<PureDefinition> definitions = PureDefinitionBuilder.parse(source);

            System.out.println("Definitions built: " + definitions.size());
            definitions.forEach(d -> System.out.println("  - Type: " + d.getClass().getSimpleName()));

            assertTrue(definitions.stream().anyMatch(d -> d instanceof ClassDefinition),
                    "Should contain a ClassDefinition");
            assertTrue(definitions.stream().anyMatch(d -> d instanceof DatabaseDefinition),
                    "Should contain a DatabaseDefinition");
            assertTrue(definitions.stream().anyMatch(d -> d instanceof MappingDefinition),
                    "Should contain a MappingDefinition");
            assertTrue(definitions.stream().anyMatch(d -> d instanceof RuntimeDefinition),
                    "Should contain a RuntimeDefinition");

            // Verify runtime can be found by name
            RuntimeDefinition runtime = definitions.stream()
                    .filter(d -> d instanceof RuntimeDefinition)
                    .map(d -> (RuntimeDefinition) d)
                    .filter(r -> "test::TestRuntime".equals(r.qualifiedName()))
                    .findFirst()
                    .orElse(null);
            assertNotNull(runtime, "Should find runtime 'test::TestRuntime'");
        }
    }
}
