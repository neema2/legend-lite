package org.finos.legend.pure.dsl.antlr;

import org.finos.legend.pure.dsl.definition.ClassDefinition;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for PureDefinitionBuilder - specifically testing class inheritance
 * parsing.
 */
class PureDefinitionBuilderTest {

    @Test
    void testParseClassWithNoSuperclass() {
        String source = "Class model::Person { firstName: String[1]; }";

        org.finos.legend.pure.dsl.antlr.PureParser.DefinitionContext ctx = org.finos.legend.pure.dsl.PureParser
                .parseDefinition(source);
        Optional<ClassDefinition> result = PureDefinitionBuilder.extractFirstClassDefinition(ctx);

        assertTrue(result.isPresent());
        ClassDefinition classDef = result.get();
        assertEquals("model::Person", classDef.qualifiedName());
        assertTrue(classDef.superClasses().isEmpty());
        assertEquals(1, classDef.properties().size());
        assertEquals("firstName", classDef.properties().get(0).name());
    }

    @Test
    void testParseClassWithSingleSuperclass() {
        String source = "Class model::Employee extends model::Person { employeeId: String[1]; }";

        org.finos.legend.pure.dsl.antlr.PureParser.DefinitionContext ctx = org.finos.legend.pure.dsl.PureParser
                .parseDefinition(source);
        Optional<ClassDefinition> result = PureDefinitionBuilder.extractFirstClassDefinition(ctx);

        assertTrue(result.isPresent());
        ClassDefinition classDef = result.get();
        assertEquals("model::Employee", classDef.qualifiedName());
        assertEquals(List.of("model::Person"), classDef.superClasses());
        assertEquals(1, classDef.properties().size());
        assertEquals("employeeId", classDef.properties().get(0).name());
    }

    @Test
    void testParseClassWithMultipleSuperclasses() {
        String source = "Class model::Manager extends model::Employee, model::Leader { teamSize: Integer[1]; }";

        org.finos.legend.pure.dsl.antlr.PureParser.DefinitionContext ctx = org.finos.legend.pure.dsl.PureParser
                .parseDefinition(source);
        Optional<ClassDefinition> result = PureDefinitionBuilder.extractFirstClassDefinition(ctx);

        assertTrue(result.isPresent());
        ClassDefinition classDef = result.get();
        assertEquals("model::Manager", classDef.qualifiedName());
        assertEquals(List.of("model::Employee", "model::Leader"), classDef.superClasses());
        assertEquals(1, classDef.properties().size());
        assertEquals("teamSize", classDef.properties().get(0).name());
    }

    @Test
    void testExtractMultipleClassDefinitions() {
        String source = """
                Class model::Person { firstName: String[1]; lastName: String[1]; }
                Class model::Employee extends model::Person { employeeId: String[1]; }
                """;

        org.finos.legend.pure.dsl.antlr.PureParser.DefinitionContext ctx = org.finos.legend.pure.dsl.PureParser
                .parseDefinition(source);
        List<ClassDefinition> result = PureDefinitionBuilder.extractClassDefinitions(ctx);

        assertEquals(2, result.size());

        ClassDefinition person = result.get(0);
        assertEquals("model::Person", person.qualifiedName());
        assertTrue(person.superClasses().isEmpty());
        assertEquals(2, person.properties().size());

        ClassDefinition employee = result.get(1);
        assertEquals("model::Employee", employee.qualifiedName());
        assertEquals(List.of("model::Person"), employee.superClasses());
        assertEquals(1, employee.properties().size());
    }

    @Test
    void testParseClassWithMultipleProperties() {
        String source = """
                Class model::Person {
                    firstName: String[1];
                    lastName: String[1];
                    age: Integer[0..1];
                    emails: String[*];
                }
                """;

        org.finos.legend.pure.dsl.antlr.PureParser.DefinitionContext ctx = org.finos.legend.pure.dsl.PureParser
                .parseDefinition(source);
        Optional<ClassDefinition> result = PureDefinitionBuilder.extractFirstClassDefinition(ctx);

        assertTrue(result.isPresent());
        ClassDefinition classDef = result.get();
        assertEquals(4, classDef.properties().size());

        // Check property types and multiplicities
        var firstName = classDef.properties().get(0);
        assertEquals("firstName", firstName.name());
        assertEquals("String", firstName.type());
        assertEquals(1, firstName.lowerBound());
        assertEquals(1, (int) firstName.upperBound());

        var age = classDef.properties().get(2);
        assertEquals("age", age.name());
        assertEquals("Integer", age.type());
        assertEquals(0, age.lowerBound());
        assertEquals(1, (int) age.upperBound());

        var emails = classDef.properties().get(3);
        assertEquals("emails", emails.name());
        assertEquals("String", emails.type());
        assertEquals(0, emails.lowerBound());
        assertNull(emails.upperBound()); // * = unbounded
    }

    // ==================== Inherited Property Lookup Tests ====================

    @Test
    void testInheritedPropertyLookupWithPureModelBuilder() {
        var builder = new org.finos.legend.pure.dsl.definition.PureModelBuilder();
        builder.addSource("""
                Class model::Person { firstName: String[1]; lastName: String[1]; }
                Class model::Employee extends model::Person { employeeId: String[1]; }
                """);

        var employee = builder.getClass("model::Employee");
        assertNotNull(employee);

        // Own property should be found
        assertTrue(employee.findProperty("employeeId").isPresent());
        assertEquals("employeeId", employee.findProperty("employeeId").get().name());

        // Inherited property should be found
        assertTrue(employee.findProperty("firstName").isPresent());
        assertEquals("firstName", employee.findProperty("firstName").get().name());

        assertTrue(employee.findProperty("lastName").isPresent());
        assertEquals("lastName", employee.findProperty("lastName").get().name());

        // Non-existent property should not be found
        assertFalse(employee.findProperty("nonExistent").isPresent());
    }

    @Test
    void testMultipleLevelInheritance() {
        var builder = new org.finos.legend.pure.dsl.definition.PureModelBuilder();
        builder.addSource("""
                Class model::Entity { id: String[1]; }
                Class model::Person extends model::Entity { firstName: String[1]; lastName: String[1]; }
                Class model::Employee extends model::Person { employeeId: String[1]; }
                """);

        var employee = builder.getClass("model::Employee");
        assertNotNull(employee);

        // Own property
        assertTrue(employee.findProperty("employeeId").isPresent());
        // Parent property (Person)
        assertTrue(employee.findProperty("firstName").isPresent());
        assertTrue(employee.findProperty("lastName").isPresent());
        // Grandparent property (Entity)
        assertTrue(employee.findProperty("id").isPresent());
    }

    @Test
    void testAllPropertiesIncludesInherited() {
        var builder = new org.finos.legend.pure.dsl.definition.PureModelBuilder();
        builder.addSource("""
                Class model::Person { firstName: String[1]; lastName: String[1]; }
                Class model::Employee extends model::Person { employeeId: String[1]; }
                """);

        var employee = builder.getClass("model::Employee");
        assertNotNull(employee);

        var allProps = employee.allProperties();
        assertEquals(3, allProps.size());

        var propNames = allProps.stream().map(p -> p.name()).toList();
        assertTrue(propNames.contains("employeeId"));
        assertTrue(propNames.contains("firstName"));
        assertTrue(propNames.contains("lastName"));
    }

    @Test
    void testSuperClassesResolved() {
        var builder = new org.finos.legend.pure.dsl.definition.PureModelBuilder();
        builder.addSource("""
                Class model::Person { firstName: String[1]; }
                Class model::Employee extends model::Person { employeeId: String[1]; }
                """);

        var employee = builder.getClass("model::Employee");
        assertNotNull(employee);

        // Verify superclass is resolved (not just a string)
        assertEquals(1, employee.superClasses().size());
        var superClass = employee.superClasses().get(0);
        assertEquals("model::Person", superClass.qualifiedName());
        assertEquals("Person", superClass.name());
    }
}
