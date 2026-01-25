package org.finos.legend.engine.test;

import org.finos.legend.engine.codegen.JavaCodeGenerator;
import org.finos.legend.pure.dsl.definition.*;
import org.finos.legend.pure.dsl.antlr.PureDefinitionBuilder;
import org.junit.jupiter.api.*;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Java code generation from Pure definitions.
 */
@DisplayName("Java Code Generator Tests")
class JavaCodeGeneratorTest {

    private JavaCodeGenerator generator;

    @BeforeEach
    void setUp() {
        generator = new JavaCodeGenerator();
    }

    // ==================== Enum Generation ====================

    @Test
    @DisplayName("Generate Java enum from Pure Enum")
    void testGenerateEnum() {
        // GIVEN: A Pure Enum definition
        String pureEnum = """
                Enum model::Status
                {
                    ACTIVE,
                    INACTIVE,
                    PENDING
                }
                """;
        EnumDefinition enumDef = PureDefinitionBuilder.parseEnumDefinition(pureEnum);

        // WHEN: We generate Java code
        String javaCode = generator.generateEnum(enumDef);

        // THEN: We get valid Java enum source
        System.out.println("Generated Enum:\n" + javaCode);

        assertTrue(javaCode.contains("package model;"));
        assertTrue(javaCode.contains("public enum Status {"));
        assertTrue(javaCode.contains("ACTIVE,"));
        assertTrue(javaCode.contains("INACTIVE,"));
        assertTrue(javaCode.contains("PENDING"));
    }

    // ==================== Record Generation ====================

    @Test
    @DisplayName("Generate Java record with primitives for [1] properties")
    void testGenerateRecordWithPrimitives() {
        // GIVEN: A Pure Class definition with required properties
        String pureClass = """
                Class model::Person
                {
                    firstName: String[1];
                    lastName: String[1];
                    age: Integer[1];
                    active: Boolean[1];
                }
                """;
        ClassDefinition classDef = PureDefinitionBuilder.parseClassDefinition(pureClass);

        // WHEN: We generate Java code
        String javaCode = generator.generateRecord(classDef);

        // THEN: Required properties use primitives
        System.out.println("Generated Record with Primitives:\n" + javaCode);

        assertTrue(javaCode.contains("String firstName")); // String stays String
        assertTrue(javaCode.contains("String lastName"));
        assertTrue(javaCode.contains("int age")); // Integer[1] → int
        assertTrue(javaCode.contains("boolean active")); // Boolean[1] → boolean
    }

    @Test
    @DisplayName("Generate Java record with Optional for [0..1] properties")
    void testGenerateRecordWithOptional() {
        // GIVEN: A Pure Class with optional properties
        String pureClass = """
                Class model::Customer
                {
                    name: String[1];
                    email: String[0..1];
                    age: Integer[0..1];
                }
                """;
        ClassDefinition classDef = PureDefinitionBuilder.parseClassDefinition(pureClass);

        // WHEN: We generate Java code
        String javaCode = generator.generateRecord(classDef);

        // THEN: Optional properties use Optional<T>
        System.out.println("Generated Record with Optional:\n" + javaCode);

        assertTrue(javaCode.contains("import java.util.Optional;"));
        assertTrue(javaCode.contains("import java.util.OptionalInt;"));
        assertTrue(javaCode.contains("String name")); // Required
        assertTrue(javaCode.contains("Optional<String> email")); // Optional String
        assertTrue(javaCode.contains("OptionalInt age")); // Optional Integer → OptionalInt
    }

    @Test
    @DisplayName("Generate Java record with derived property")
    void testGenerateRecordWithDerivedProperty() {
        // GIVEN: A Pure Class with a derived property
        String pureClass = """
                Class model::Person
                {
                    firstName: String[1];
                    lastName: String[1];
                    fullName() {$this.firstName + ' ' + $this.lastName}: String[1];
                }
                """;
        ClassDefinition classDef = PureDefinitionBuilder.parseClassDefinition(pureClass);

        // WHEN: We generate Java code
        String javaCode = generator.generateRecord(classDef);

        // THEN: The record has a computed method
        System.out.println("Generated Record with Derived:\n" + javaCode);

        assertTrue(javaCode.contains("public record Person("));
        assertTrue(javaCode.contains("public String fullName() {"));
        assertTrue(javaCode.contains("return firstName + \" \" + lastName;"));
    }

    @Test
    @DisplayName("Generate Java record with List for [*] properties")
    void testGenerateRecordWithListProperty() {
        // GIVEN: A Pure Class with a to-many property
        String pureClass = """
                Class model::Team
                {
                    name: String[1];
                    members: String[*];
                }
                """;
        ClassDefinition classDef = PureDefinitionBuilder.parseClassDefinition(pureClass);

        // WHEN: We generate Java code
        String javaCode = generator.generateRecord(classDef);

        // THEN: The List property is generated correctly
        System.out.println("Generated Record with List:\n" + javaCode);

        assertTrue(javaCode.contains("import java.util.List;"));
        assertTrue(javaCode.contains("List<String> members"));
    }

    // ==================== Association Relationships ====================

    @Test
    @DisplayName("Generate records for classes with Association")
    void testGenerateRecordsForAssociation() {
        // GIVEN: Two Pure Classes connected via Association
        String pureSource = """
                Class model::Address
                {
                    street: String[1];
                    city: String[1];
                    zipCode: String[0..1];
                }

                Class model::Person
                {
                    firstName: String[1];
                    lastName: String[1];
                }

                Association model::Person_Address
                {
                    addresses: Address[*];
                    person: Person[1];
                }
                """;

        // WHEN: We generate all files
        List<JavaCodeGenerator.GeneratedFile> files = generator.generate(pureSource);

        // THEN: We get 2 generated files (Association doesn't generate a class)
        assertEquals(2, files.size());

        // Check Address
        var addressFile = files.stream()
                .filter(f -> f.fileName().equals("Address.java"))
                .findFirst()
                .orElseThrow();
        System.out.println("Address.java:\n" + addressFile.content());
        assertTrue(addressFile.content().contains("public record Address("));
        assertTrue(addressFile.content().contains("String street"));
        assertTrue(addressFile.content().contains("Optional<String> zipCode"));

        // Check Person (Association properties should be added to both classes)
        var personFile = files.stream()
                .filter(f -> f.fileName().equals("Person.java"))
                .findFirst()
                .orElseThrow();
        System.out.println("Person.java:\n" + personFile.content());
        assertTrue(personFile.content().contains("public record Person("));
        // Note: Full Association support would add these properties to generated
        // records
        // For now, we just verify the base classes are generated correctly
    }

    // ==================== Full Pipeline ====================

    @Test
    @DisplayName("Generate multiple files from Pure source")
    void testGenerateMultipleFiles() {
        // GIVEN: A Pure source with Enum and Class
        String pureSource = """
                Enum model::OrderStatus
                {
                    PENDING,
                    SHIPPED,
                    DELIVERED
                }

                Class model::Order
                {
                    orderId: String[1];
                    status: OrderStatus[1];
                }
                """;

        // WHEN: We generate all files
        List<JavaCodeGenerator.GeneratedFile> files = generator.generate(pureSource);

        // THEN: We get 2 generated files
        assertEquals(2, files.size());

        // Check enum file
        var enumFile = files.stream()
                .filter(f -> f.fileName().equals("OrderStatus.java"))
                .findFirst()
                .orElseThrow();
        System.out.println("OrderStatus.java:\n" + enumFile.content());
        assertTrue(enumFile.content().contains("public enum OrderStatus"));

        // Check class file
        var classFile = files.stream()
                .filter(f -> f.fileName().equals("Order.java"))
                .findFirst()
                .orElseThrow();
        System.out.println("Order.java:\n" + classFile.content());
        assertTrue(classFile.content().contains("public record Order"));
        assertTrue(classFile.content().contains("OrderStatus status"));
    }

    @Test
    @DisplayName("Complete type mapping verification")
    void testCompleteTypeMapping() {
        // GIVEN: A class with all type variations
        String pureClass = """
                Class app::Customer
                {
                    id: Integer[1];
                    name: String[1];
                    email: String[0..1];
                    tags: String[*];
                    displayName() {$this.name + ' <' + $this.email + '>'}: String[1];
                }
                """;
        ClassDefinition classDef = PureDefinitionBuilder.parseClassDefinition(pureClass);

        // WHEN: We generate Java code
        String javaCode = generator.generateRecord(classDef);
        System.out.println("Complete Type Mapping:\n" + javaCode);

        // THEN: Type mapping is correct
        assertTrue(javaCode.contains("package app;"));
        assertTrue(javaCode.contains("public record Customer("));
        assertTrue(javaCode.contains("int id")); // Integer[1] → int
        assertTrue(javaCode.contains("String name")); // String[1] → String
        assertTrue(javaCode.contains("Optional<String> email")); // String[0..1] → Optional<String>
        assertTrue(javaCode.contains("List<String> tags")); // String[*] → List<String>
        assertTrue(javaCode.contains("public String displayName()"));
    }

    // ==================== Constraint Validation ====================

    @Test
    @DisplayName("Generate Java record with validate() method for constraints")
    void testGenerateRecordWithConstraints() {
        // GIVEN: A Pure Class with constraints
        String pureClass = """
                Class model::Person
                {
                    firstName: String[1];
                    age: Integer[1];
                }
                [
                    validAge: $this.age >= 0,
                    nameNotEmpty: $this.firstName->length() > 0
                ]
                """;
        ClassDefinition classDef = PureDefinitionBuilder.parseClassDefinition(pureClass);

        // WHEN: We generate Java code
        String javaCode = generator.generateRecord(classDef);

        // THEN: The record has a validate() method
        System.out.println("Generated Record with Constraints:\n" + javaCode);

        assertTrue(javaCode.contains("public java.util.List<String> validate()"));
        assertTrue(javaCode.contains("if (!(age >= 0)) errors.add(\"validAge\")"));
        assertTrue(javaCode.contains("if (!(firstName.length() > 0)) errors.add(\"nameNotEmpty\")"));
        assertTrue(javaCode.contains("return errors;"));
    }
}
