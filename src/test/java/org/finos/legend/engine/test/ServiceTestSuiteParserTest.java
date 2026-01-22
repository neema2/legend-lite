package org.finos.legend.engine.test;

import org.finos.legend.pure.dsl.definition.*;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for parsing service testSuites.
 */
@DisplayName("Service Test Suite Parser Tests")
class ServiceTestSuiteParserTest {

    @Test
    @DisplayName("Parse service with test suite")
    void testParseServiceWithTestSuite() {
        String pureService = """
                Service api::PersonService
                {
                    pattern: '/api/persons/{lastName}';
                    function: |Person.all()->filter({p | $p.lastName == $lastName});
                    documentation: 'Get persons by last name';

                    testSuites:
                    [
                        PersonServiceSuite:
                        {
                            function: |Person.all()->filter({p | $p.lastName == 'Smith'});
                            tests:
                            [
                                GetSmithFamily:
                                {
                                    doc: 'Test getting Smith family';
                                    data:
                                    [
                                        ModelStore:
                                            ExternalFormat
                                            #{
                                                contentType: 'application/json';
                                                data: '[{"firstName":"John","lastName":"Smith"}]';
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
                                                        data: '[{"firstName":"John","lastName":"Smith"}]';
                                                    }#;
                                            }#
                                    ];
                                }
                            ];
                        }
                    ]
                }
                """;

        ServiceDefinition service = PureDefinitionParser.parseServiceDefinition(pureService);

        // Verify basic service parsed
        assertEquals("api::PersonService", service.qualifiedName());
        assertEquals("/api/persons/{lastName}", service.pattern());
        assertEquals(1, service.pathParams().size());
        assertEquals("lastName", service.pathParams().get(0));

        // Verify test suites parsed
        assertEquals(1, service.testSuites().size());

        var suite = service.testSuites().get(0);
        assertEquals("PersonServiceSuite", suite.name());
        assertNotNull(suite.functionBody());

        // Verify tests
        assertEquals(1, suite.tests().size());
        var test = suite.tests().get(0);
        assertEquals("GetSmithFamily", test.name());
        assertEquals("Test getting Smith family", test.documentation());

        // Verify assertions
        assertEquals(1, test.asserts().size());
        assertEquals("EqualToJson", test.asserts().get(0).type());
    }

    @Test
    @DisplayName("Parse service without test suite")
    void testParseServiceWithoutTestSuite() {
        String pureService = """
                Service api::SimpleService
                {
                    pattern: '/api/simple';
                    function: |Person.all();
                }
                """;

        ServiceDefinition service = PureDefinitionParser.parseServiceDefinition(pureService);

        assertEquals("api::SimpleService", service.qualifiedName());
        assertTrue(service.testSuites().isEmpty());
    }

    @Test
    @DisplayName("Parse service with multiple path params and test suite")
    void testParseServiceWithMultipleParams() {
        String pureService = """
                Service api::AddressService
                {
                    pattern: '/api/persons/{personId}/addresses/{addressId}';
                    function: |Address.all()->filter({a | $a.personId == $personId && $a.id == $addressId});

                    testSuites:
                    [
                        AddressSuite:
                        {
                            function: |Address.all();
                            tests:
                            [
                                GetAddress:
                                {
                                    data:
                                    [
                                        ModelStore:
                                            ExternalFormat
                                            #{
                                                contentType: 'application/json';
                                                data: '{"id":1,"street":"123 Main St"}';
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
                                                        data: '{"street":"123 Main St"}';
                                                    }#;
                                            }#
                                    ];
                                }
                            ];
                        }
                    ]
                }
                """;

        ServiceDefinition service = PureDefinitionParser.parseServiceDefinition(pureService);

        // Verify path params
        assertEquals(2, service.pathParams().size());
        assertEquals("personId", service.pathParams().get(0));
        assertEquals("addressId", service.pathParams().get(1));

        // Verify test suite
        assertEquals(1, service.testSuites().size());
        assertEquals("AddressSuite", service.testSuites().get(0).name());
    }
}
