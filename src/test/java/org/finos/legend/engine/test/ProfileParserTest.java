package org.finos.legend.engine.test;

import org.finos.legend.pure.dsl.definition.*;
import org.finos.legend.pure.dsl.antlr.PureDefinitionBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Pure Profile/Stereotype/Tag parsing.
 * All tests use Pure language syntax as the starting point.
 */
public class ProfileParserTest {

    @Test
    @DisplayName("Parse Profile with stereotypes and tags")
    void testParseProfile() {
        String pure = """
                Profile doc::Documentation {
                    stereotypes: [legacy, deprecated, experimental];
                    tags: [author, since, description];
                }
                """;

        ProfileDefinition profile = PureDefinitionBuilder.parseProfileDefinition(pure);

        assertEquals("doc::Documentation", profile.qualifiedName());
        assertEquals("Documentation", profile.simpleName());
        assertEquals("doc", profile.packagePath());
        assertEquals(List.of("legacy", "deprecated", "experimental"), profile.stereotypes());
        assertEquals(List.of("author", "since", "description"), profile.tags());
    }

    @Test
    @DisplayName("Parse Profile with only stereotypes")
    void testParseProfileStereotypesOnly() {
        String pure = """
                Profile meta::Validation {
                    stereotypes: [required, unique];
                }
                """;

        ProfileDefinition profile = PureDefinitionBuilder.parseProfileDefinition(pure);

        assertEquals("meta::Validation", profile.qualifiedName());
        assertEquals(List.of("required", "unique"), profile.stereotypes());
        assertTrue(profile.tags().isEmpty());
    }

    @Test
    @DisplayName("Parse Class with stereotype annotation")
    void testParseClassWithStereotype() {
        String pure = """
                <<doc::Documentation.legacy>>
                Class model::Person {
                    name: String[1];
                }
                """;

        ClassDefinition clazz = PureDefinitionBuilder.parseClassDefinition(pure);

        assertEquals("model::Person", clazz.qualifiedName());
        assertEquals(1, clazz.stereotypes().size());
        assertEquals("doc::Documentation", clazz.stereotypes().get(0).profileName());
        assertEquals("legacy", clazz.stereotypes().get(0).stereotypeName());
        assertEquals(1, clazz.properties().size());
    }

    @Test
    @DisplayName("Parse Class with multiple stereotypes")
    void testParseClassWithMultipleStereotypes() {
        String pure = """
                <<doc::Documentation.deprecated>>
                <<meta::Validation.required>>
                Class model::Order {
                    id: Integer[1];
                }
                """;

        ClassDefinition clazz = PureDefinitionBuilder.parseClassDefinition(pure);

        assertEquals("model::Order", clazz.qualifiedName());
        assertEquals(2, clazz.stereotypes().size());
        assertEquals("deprecated", clazz.stereotypes().get(0).stereotypeName());
        assertEquals("required", clazz.stereotypes().get(1).stereotypeName());
    }

    @Test
    @DisplayName("Parse Class with tagged value")
    void testParseClassWithTaggedValue() {
        String pure = """
                <<doc::Documentation.author: 'John Smith'>>
                Class model::Customer {
                    email: String[1];
                }
                """;

        ClassDefinition clazz = PureDefinitionBuilder.parseClassDefinition(pure);

        assertEquals("model::Customer", clazz.qualifiedName());
        assertEquals(1, clazz.taggedValues().size());
        assertEquals("doc::Documentation", clazz.taggedValues().get(0).profileName());
        assertEquals("author", clazz.taggedValues().get(0).tagName());
        assertEquals("John Smith", clazz.taggedValues().get(0).value());
    }

    @Test
    @DisplayName("Parse Class with both stereotypes and tagged values")
    void testParseClassWithStereotypesAndTaggedValues() {
        String pure = """
                <<doc::Documentation.deprecated>>
                <<doc::Documentation.author: 'Jane Doe'>>
                <<doc::Documentation.since: '2024-01-01'>>
                Class model::LegacyEntity {
                    code: String[1];
                }
                """;

        ClassDefinition clazz = PureDefinitionBuilder.parseClassDefinition(pure);

        assertEquals("model::LegacyEntity", clazz.qualifiedName());
        assertEquals(1, clazz.stereotypes().size());
        assertEquals("deprecated", clazz.stereotypes().get(0).stereotypeName());
        assertEquals(2, clazz.taggedValues().size());
        assertEquals("author", clazz.taggedValues().get(0).tagName());
        assertEquals("Jane Doe", clazz.taggedValues().get(0).value());
        assertEquals("since", clazz.taggedValues().get(1).tagName());
        assertEquals("2024-01-01", clazz.taggedValues().get(1).value());
    }

    @Test
    @DisplayName("Parse Class without any annotations (backwards compatibility)")
    void testParseClassWithoutAnnotations() {
        String pure = """
                Class model::Simple {
                    value: Integer[1];
                }
                """;

        ClassDefinition clazz = PureDefinitionBuilder.parseClassDefinition(pure);

        assertEquals("model::Simple", clazz.qualifiedName());
        assertTrue(clazz.stereotypes().isEmpty());
        assertTrue(clazz.taggedValues().isEmpty());
        assertEquals(1, clazz.properties().size());
    }

    @Test
    @DisplayName("Parse multiple definitions including Profile and annotated Class")
    void testParseMultipleDefinitions() {
        String pure = """
                Profile doc::Documentation {
                    stereotypes: [legacy];
                    tags: [author];
                }

                <<doc::Documentation.legacy>>
                <<doc::Documentation.author: 'Team A'>>
                Class model::Entity {
                    id: Integer[1];
                }
                """;

        List<PureDefinition> definitions = PureDefinitionBuilder.parse(pure);

        assertEquals(2, definitions.size());
        assertInstanceOf(ProfileDefinition.class, definitions.get(0));
        assertInstanceOf(ClassDefinition.class, definitions.get(1));

        ProfileDefinition profile = (ProfileDefinition) definitions.get(0);
        assertEquals("doc::Documentation", profile.qualifiedName());

        ClassDefinition clazz = (ClassDefinition) definitions.get(1);
        assertEquals(1, clazz.stereotypes().size());
        assertEquals(1, clazz.taggedValues().size());
    }
}
