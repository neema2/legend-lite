package org.finos.legend.engine.test;

import org.finos.legend.pure.dsl.definition.*;
import org.junit.jupiter.api.*;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Pure parsing tests - no database interaction required.
 * 
 * Tests the Pure syntax parser for:
 * - Class definitions (properties, derived, constraints)
 * - Database definitions (tables, columns, joins)
 * - Mapping definitions
 * - Association definitions
 * - Enum definitions
 */
@DisplayName("Pure Parser Tests")
class PureParserTest {

    // ==================== Class Parsing Tests ====================

    @Test
    @DisplayName("Parse Pure Class definition")
    void testParseClassDefinition() {
        // GIVEN: A Pure Class definition
        String pureClass = """
                Class model::Person
                {
                    firstName: String[1];
                    lastName: String[1];
                    age: Integer[1];
                }
                """;

        // WHEN: We parse it
        ClassDefinition classDef = PureDefinitionParser.parseClassDefinition(pureClass);

        // THEN: We get a valid ClassDefinition
        assertEquals("model::Person", classDef.qualifiedName());
        assertEquals("Person", classDef.simpleName());
        assertEquals("model", classDef.packagePath());
        assertEquals(3, classDef.properties().size());

        // Verify properties
        var firstName = classDef.properties().get(0);
        assertEquals("firstName", firstName.name());
        assertEquals("String", firstName.type());
        assertEquals(1, firstName.lowerBound());
        assertEquals(Integer.valueOf(1), firstName.upperBound());

        var age = classDef.properties().get(2);
        assertEquals("age", age.name());
        assertEquals("Integer", age.type());
    }

    @Test
    @DisplayName("Parse Pure Class with derived property")
    void testParseClassWithDerivedProperty() {
        // GIVEN: A Pure Class definition with a derived property
        String pureClass = """
                Class model::Person
                {
                    firstName: String[1];
                    lastName: String[1];
                    fullName() {$this.firstName + ' ' + $this.lastName}: String[1];
                }
                """;

        // WHEN: We parse it
        ClassDefinition classDef = PureDefinitionParser.parseClassDefinition(pureClass);

        // THEN: We get a valid ClassDefinition with both regular and derived properties
        assertEquals("model::Person", classDef.qualifiedName());
        assertEquals(2, classDef.properties().size());
        assertEquals(1, classDef.derivedProperties().size());

        // Verify regular properties
        var firstName = classDef.properties().get(0);
        assertEquals("firstName", firstName.name());

        var lastName = classDef.properties().get(1);
        assertEquals("lastName", lastName.name());

        // Verify derived property
        var fullName = classDef.derivedProperties().get(0);
        assertEquals("fullName", fullName.name());
        assertEquals("$this.firstName + ' ' + $this.lastName", fullName.expression());
        assertEquals("String", fullName.type());
        assertEquals(1, fullName.lowerBound());
        assertEquals(Integer.valueOf(1), fullName.upperBound());
    }

    @Test
    @DisplayName("Parse Pure Class with constraints")
    void testParseClassWithConstraints() {
        // GIVEN: A Pure Class definition with constraints
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

        // WHEN: We parse it
        ClassDefinition classDef = PureDefinitionParser.parseClassDefinition(pureClass);

        // THEN: We get a valid ClassDefinition with constraints
        assertEquals("model::Person", classDef.qualifiedName());
        assertEquals(2, classDef.properties().size());
        assertEquals(2, classDef.constraints().size());

        // Verify constraints
        var validAge = classDef.constraints().get(0);
        assertEquals("validAge", validAge.name());
        assertEquals("$this.age >= 0", validAge.expression());

        var nameNotEmpty = classDef.constraints().get(1);
        assertEquals("nameNotEmpty", nameNotEmpty.name());
        assertEquals("$this.firstName->length() > 0", nameNotEmpty.expression());
    }

    // ==================== Database Parsing Tests ====================

    @Test
    @DisplayName("Parse Pure Database definition")
    void testParseDatabaseDefinition() {
        // GIVEN: A Pure Database definition
        String pureDatabase = """
                Database store::MyDatabase
                (
                    Table T_PERSON
                    (
                        ID INTEGER PRIMARY KEY,
                        FIRST_NAME VARCHAR(100) NOT NULL,
                        LAST_NAME VARCHAR(100) NOT NULL,
                        AGE_VAL INTEGER
                    )
                )
                """;

        // WHEN: We parse it
        DatabaseDefinition dbDef = PureDefinitionParser.parseDatabaseDefinition(pureDatabase);

        // THEN: We get a valid DatabaseDefinition
        assertEquals("store::MyDatabase", dbDef.qualifiedName());
        assertEquals("MyDatabase", dbDef.simpleName());
        assertEquals(1, dbDef.tables().size());

        // Verify table
        var table = dbDef.tables().getFirst();
        assertEquals("T_PERSON", table.name());
        assertEquals(4, table.columns().size());

        // Verify columns
        var idCol = table.columns().get(0);
        assertEquals("ID", idCol.name());
        assertEquals("INTEGER", idCol.dataType());
        assertTrue(idCol.primaryKey());

        var firstNameCol = table.columns().get(1);
        assertEquals("FIRST_NAME", firstNameCol.name());
        assertTrue(firstNameCol.notNull());
        assertFalse(firstNameCol.primaryKey());
    }

    @Test
    @DisplayName("Parse Database with Join definition")
    void testParseDatabaseWithJoin() {
        // GIVEN: A Pure Database with tables and a join
        String pureDatabase = """
                Database store::TestDB
                (
                    Table T_PERSON
                    (
                        ID INTEGER PRIMARY KEY,
                        NAME VARCHAR(100) NOT NULL
                    )
                    Table T_ADDRESS
                    (
                        ID INTEGER PRIMARY KEY,
                        PERSON_ID INTEGER NOT NULL,
                        STREET VARCHAR(200) NOT NULL
                    )
                    Join Person_Address(T_PERSON.ID = T_ADDRESS.PERSON_ID)
                )
                """;

        // WHEN: We parse it
        DatabaseDefinition dbDef = PureDefinitionParser.parseDatabaseDefinition(pureDatabase);

        // THEN: We get tables and joins
        assertEquals("store::TestDB", dbDef.qualifiedName());
        assertEquals(2, dbDef.tables().size());
        assertEquals(1, dbDef.joins().size());

        // Verify join
        var join = dbDef.findJoin("Person_Address").orElseThrow();
        assertEquals("Person_Address", join.name());
        assertEquals("T_PERSON", join.leftTable());
        assertEquals("ID", join.leftColumn());
        assertEquals("T_ADDRESS", join.rightTable());
        assertEquals("PERSON_ID", join.rightColumn());
    }

    // ==================== Enum Parsing Tests ====================

    @Test
    @DisplayName("Parse Pure Enum definition")
    void testParseEnumDefinition() {
        // GIVEN: A Pure Enum definition
        String pureEnum = """
                Enum model::Status
                {
                    ACTIVE,
                    INACTIVE,
                    PENDING
                }
                """;

        // WHEN: We parse it
        EnumDefinition enumDef = PureDefinitionParser.parseEnumDefinition(pureEnum);

        // THEN: We get a valid EnumDefinition
        assertEquals("model::Status", enumDef.qualifiedName());
        assertEquals("Status", enumDef.simpleName());
        assertEquals("model", enumDef.packagePath());
        assertEquals(3, enumDef.values().size());

        // Verify values
        assertTrue(enumDef.hasValue("ACTIVE"));
        assertTrue(enumDef.hasValue("INACTIVE"));
        assertTrue(enumDef.hasValue("PENDING"));
        assertFalse(enumDef.hasValue("UNKNOWN"));
    }

    @Test
    @DisplayName("Parse Enum as part of complete model")
    void testParseEnumInModel() {
        // GIVEN: A model with Class and Enum
        String pureModel = """
                Enum model::OrderStatus
                {
                    PENDING,
                    SHIPPED,
                    DELIVERED,
                    CANCELLED
                }

                Class model::Order
                {
                    orderId: String[1];
                    status: OrderStatus[1];
                }
                """;

        // WHEN: We parse it
        List<PureDefinition> definitions = PureDefinitionParser.parse(pureModel);

        // THEN: We get both definitions
        assertEquals(2, definitions.size());

        // First should be the Enum
        assertTrue(definitions.get(0) instanceof EnumDefinition);
        EnumDefinition enumDef = (EnumDefinition) definitions.get(0);
        assertEquals("model::OrderStatus", enumDef.qualifiedName());
        assertEquals(4, enumDef.values().size());

        // Second should be the Class
        assertTrue(definitions.get(1) instanceof ClassDefinition);
        ClassDefinition classDef = (ClassDefinition) definitions.get(1);
        assertEquals("model::Order", classDef.qualifiedName());

        // The status property should have type OrderStatus
        var statusProp = classDef.properties().get(1);
        assertEquals("status", statusProp.name());
        assertEquals("OrderStatus", statusProp.type());
    }

    // ==================== Mapping Parsing Tests ====================

    @Test
    @DisplayName("Parse Pure Mapping definition")
    void testParseMappingDefinition() {
        // GIVEN: A Pure Mapping definition
        String pureMapping = """
                Mapping model::PersonMapping
                (
                    Person: Relational
                    {
                        ~mainTable [MyDatabase] T_PERSON
                        firstName: [MyDatabase] T_PERSON.FIRST_NAME,
                        lastName: [MyDatabase] T_PERSON.LAST_NAME,
                        age: [MyDatabase] T_PERSON.AGE_VAL
                    }
                )
                """;

        // WHEN: We parse it
        MappingDefinition mappingDef = PureDefinitionParser.parseMappingDefinition(pureMapping);

        // THEN: We get a valid MappingDefinition
        assertEquals("model::PersonMapping", mappingDef.qualifiedName());
        assertEquals("PersonMapping", mappingDef.simpleName());
        assertEquals(1, mappingDef.classMappings().size());

        // Verify class mapping
        var classMapping = mappingDef.classMappings().getFirst();
        assertEquals("Person", classMapping.className());
        assertEquals("Relational", classMapping.mappingType());
        assertNotNull(classMapping.mainTable());
        assertEquals("MyDatabase", classMapping.mainTable().databaseName());
        assertEquals("T_PERSON", classMapping.mainTable().tableName());

        // Verify property mappings
        assertEquals(3, classMapping.propertyMappings().size());

        var firstNameMapping = classMapping.propertyMappings().get(0);
        assertEquals("firstName", firstNameMapping.propertyName());
        assertEquals("FIRST_NAME", firstNameMapping.columnReference().columnName());
    }

    // ==================== Association Parsing Tests ====================

    @Test
    @DisplayName("Parse Pure Association definition")
    void testParseAssociationDefinition() {
        // GIVEN: A Pure Association definition
        String pureAssociation = """
                Association model::Person_Address
                {
                    person: Person[1];
                    addresses: Address[*];
                }
                """;

        // WHEN: We parse it
        AssociationDefinition assocDef = PureDefinitionParser.parseAssociationDefinition(pureAssociation);

        // THEN: We get a valid AssociationDefinition
        assertEquals("model::Person_Address", assocDef.qualifiedName());
        assertEquals("Person_Address", assocDef.simpleName());

        // Verify property1 (person -> Person)
        var prop1 = assocDef.property1();
        assertEquals("person", prop1.propertyName());
        assertEquals("Person", prop1.targetClass());
        assertEquals("1", prop1.multiplicityString());

        // Verify property2 (addresses -> Address)
        var prop2 = assocDef.property2();
        assertEquals("addresses", prop2.propertyName());
        assertEquals("Address", prop2.targetClass());
        assertEquals("*", prop2.multiplicityString());
    }

    // ==================== Complete Model Parsing Tests ====================

    @Test
    @DisplayName("Parse complete Pure model")
    void testParseCompletePureModel() {
        // GIVEN: A complete Pure model
        String completePureModel = """
                Class model::Person
                {
                    firstName: String[1];
                    lastName: String[1];
                    age: Integer[1];
                }

                Class model::Address
                {
                    street: String[1];
                    city: String[1];
                }

                Association model::Person_Address
                {
                    person: Person[1];
                    addresses: Address[*];
                }

                Database store::PersonDatabase
                (
                    Table T_PERSON
                    (
                        ID INTEGER PRIMARY KEY,
                        FIRST_NAME VARCHAR(100) NOT NULL,
                        LAST_NAME VARCHAR(100) NOT NULL,
                        AGE_VAL INTEGER NOT NULL
                    )
                    Table T_ADDRESS
                    (
                        ID INTEGER PRIMARY KEY,
                        PERSON_ID INTEGER NOT NULL,
                        STREET VARCHAR(200) NOT NULL,
                        CITY VARCHAR(100) NOT NULL
                    )
                    Join Person_Address(T_PERSON.ID = T_ADDRESS.PERSON_ID)
                )

                Mapping model::PersonMapping
                (
                    Person: Relational
                    {
                        ~mainTable [PersonDatabase] T_PERSON
                        firstName: [PersonDatabase] T_PERSON.FIRST_NAME,
                        lastName: [PersonDatabase] T_PERSON.LAST_NAME,
                        age: [PersonDatabase] T_PERSON.AGE_VAL
                    }

                    Address: Relational
                    {
                        ~mainTable [PersonDatabase] T_ADDRESS
                        street: [PersonDatabase] T_ADDRESS.STREET,
                        city: [PersonDatabase] T_ADDRESS.CITY
                    }
                )
                """;

        // WHEN: We parse it
        List<PureDefinition> definitions = PureDefinitionParser.parse(completePureModel);

        // THEN: We get 5 definitions (Person, Address, Association, Database, Mapping)
        assertEquals(5, definitions.size());
        assertInstanceOf(ClassDefinition.class, definitions.get(0)); // Person
        assertInstanceOf(ClassDefinition.class, definitions.get(1)); // Address
        assertInstanceOf(AssociationDefinition.class, definitions.get(2)); // Person_Address
        assertInstanceOf(DatabaseDefinition.class, definitions.get(3)); // PersonDatabase
        assertInstanceOf(MappingDefinition.class, definitions.get(4)); // PersonMapping
    }
}
