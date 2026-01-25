package org.finos.legend.engine.test;

import org.finos.legend.pure.dsl.definition.*;
import org.finos.legend.pure.dsl.antlr.PureDefinitionBuilder;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for parsing mapping testSuites.
 */
@DisplayName("Mapping Test Suite Parser Tests")
class MappingTestSuiteParserTest {

    @Test
    @DisplayName("Parse mapping with test suite - relational")
    void testParseMappingWithTestSuite() {
        String pureMapping = """
                Mapping model::PersonMapping
                (
                    Person: Relational
                    {
                        ~mainTable [MyDatabase] T_PERSON
                        firstName: [MyDatabase] T_PERSON.FIRST_NAME,
                        lastName: [MyDatabase] T_PERSON.LAST_NAME
                    }

                    testSuites:
                    [
                        PersonSuite:
                        {
                            function: |Person.all()->graphFetch(#{Person{firstName}}#)->serialize(#{Person{firstName}}#);
                            tests:
                            [
                                JohnTest:
                                {
                                    doc: 'Test John data';
                                    data:
                                    [
                                        ModelStore:
                                            ExternalFormat
                                            #{
                                                contentType: 'application/json';
                                                data: '{"firstName":"John","lastName":"Doe"}';
                                            }#
                                    ];
                                    asserts:
                                    [
                                        expectedResult:
                                            EqualToJson
                                            #{
                                                expected:
                                                    ExternalFormat
                                                    #{
                                                        contentType: 'application/json';
                                                        data: '{"firstName":"John"}';
                                                    }#;
                                            }#
                                    ];
                                }
                            ];
                        }
                    ]
                )
                """;

        MappingDefinition mapping = PureDefinitionBuilder.parseMappingDefinition(pureMapping);

        // Verify basic mapping parsed
        assertEquals("model::PersonMapping", mapping.qualifiedName());
        assertEquals(1, mapping.classMappings().size());

        // Verify test suites parsed
        assertEquals(1, mapping.testSuites().size());

        var suite = mapping.testSuites().get(0);
        assertEquals("PersonSuite", suite.name());
        assertNotNull(suite.functionBody());
        assertTrue(suite.functionBody().contains("graphFetch"));

        // Verify tests
        assertEquals(1, suite.tests().size());
        var test = suite.tests().get(0);
        assertEquals("JohnTest", test.name());
        assertEquals("Test John data", test.documentation());

        // Verify input data
        assertEquals(1, test.inputData().size());
        var data = test.inputData().get(0);
        assertEquals("ModelStore", data.store());
        assertEquals("application/json", data.contentType());
        assertTrue(data.data().contains("John"));
        assertFalse(data.isReference());

        // Verify assertions
        assertEquals(1, test.asserts().size());
        var assertion = test.asserts().get(0);
        assertEquals("expectedResult", assertion.name());
        assertEquals("EqualToJson", assertion.type());
        assertTrue(assertion.expectedData().contains("firstName"));
    }

    @Test
    @DisplayName("Parse mapping without test suite")
    void testParseMappingWithoutTestSuite() {
        String pureMapping = """
                Mapping model::SimpleMapping
                (
                    Person: Relational
                    {
                        ~mainTable [MyDatabase] T_PERSON
                        name: [MyDatabase] T_PERSON.NAME
                    }
                )
                """;

        MappingDefinition mapping = PureDefinitionBuilder.parseMappingDefinition(pureMapping);

        assertEquals("model::SimpleMapping", mapping.qualifiedName());
        assertEquals(1, mapping.classMappings().size());
        assertTrue(mapping.testSuites().isEmpty());
    }

    @Test
    @DisplayName("Parse M2M mapping with test suite")
    void testParseM2MMappingWithTestSuite() {
        String pureMapping = """
                Mapping model::M2MMapping
                (
                    TargetPerson: Pure
                    {
                        ~src SourcePerson
                        fullName: $src.firstName + ' ' + $src.lastName
                    }

                    testSuites:
                    [
                        M2MSuite:
                        {
                            function: |TargetPerson.all()->graphFetch(#{TargetPerson{fullName}}#)->serialize(#{TargetPerson{fullName}}#);
                            tests:
                            [
                                TransformTest:
                                {
                                    data:
                                    [
                                        ModelStore:
                                            ExternalFormat
                                            #{
                                                contentType: 'application/json';
                                                data: '{"firstName":"John","lastName":"Smith"}';
                                            }#
                                    ];
                                    asserts:
                                    [
                                        result:
                                            EqualToJson
                                            #{
                                                expected:
                                                    ExternalFormat
                                                    #{
                                                        contentType: 'application/json';
                                                        data: '{"fullName":"John Smith"}';
                                                    }#;
                                            }#
                                    ];
                                }
                            ];
                        }
                    ]
                )
                """;

        MappingDefinition mapping = PureDefinitionBuilder.parseMappingDefinition(pureMapping);

        // Verify M2M mapping parsed
        assertEquals("model::M2MMapping", mapping.qualifiedName());
        assertEquals(1, mapping.classMappings().size());
        assertTrue(mapping.classMappings().get(0).isM2M());

        // Verify test suites parsed
        assertEquals(1, mapping.testSuites().size());
        var suite = mapping.testSuites().get(0);
        assertEquals("M2MSuite", suite.name());

        // Verify test
        assertEquals(1, suite.tests().size());
        assertEquals("TransformTest", suite.tests().get(0).name());
    }

    @Test
    @DisplayName("Parse M2M mapping with DataElement reference")
    void testParseM2MMappingWithDataElementRef() {
        String pureMapping = """
                Mapping model::M2MMapping
                (
                    TargetPerson: Pure
                    {
                        ~src SourcePerson
                        fullName: $src.firstName + ' ' + $src.lastName
                    }

                    testSuites:
                    [
                        M2MSuite:
                        {
                            function: |TargetPerson.all()->graphFetch(#{TargetPerson{fullName}}#);
                            tests:
                            [
                                RefTest:
                                {
                                    data:
                                    [
                                        ModelStore:
                                            Reference
                                            #{
                                                data::PersonTestData
                                            }#
                                    ];
                                    asserts:
                                    [
                                        result:
                                            EqualToJson
                                            #{
                                                expected:
                                                    ExternalFormat
                                                    #{
                                                        contentType: 'application/json';
                                                        data: '{"fullName":"Test Person"}';
                                                    }#;
                                            }#
                                    ];
                                }
                            ];
                        }
                    ]
                )
                """;

        MappingDefinition mapping = PureDefinitionBuilder.parseMappingDefinition(pureMapping);

        // Verify reference is parsed
        var test = mapping.testSuites().get(0).tests().get(0);
        assertEquals(1, test.inputData().size());

        var inputData = test.inputData().get(0);
        assertTrue(inputData.isReference());
        assertEquals("data::PersonTestData", inputData.data());
    }
}
