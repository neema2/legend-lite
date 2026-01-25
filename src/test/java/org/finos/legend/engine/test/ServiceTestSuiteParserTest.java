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
    @DisplayName("Parse service with complex pattern")
    void testParseServiceWithTestSuite() {
        String pureService = """
                Service api::PersonService
                {
                    pattern: '/api/persons/{lastName}';
                    documentation: 'Get persons by last name';
                    execution: Single
                    {
                        query: |Person.all()->filter({p | $p.lastName == $lastName});
                    }
                }
                """;

        ServiceDefinition service = PureDefinitionParser.parseServiceDefinition(pureService);

        // Verify basic service parsed
        assertEquals("api::PersonService", service.qualifiedName());
        assertEquals("/api/persons/{lastName}", service.pattern());
        assertEquals(1, service.pathParams().size());
        assertEquals("lastName", service.pathParams().get(0));

        // Verify function body is extracted
        assertTrue(service.functionBody().contains("Person.all()"));
    }

    @Test
    @DisplayName("Parse service without test suite")
    void testParseServiceWithoutTestSuite() {
        String pureService = """
                Service api::SimpleService
                {
                    pattern: '/api/simple';
                    execution: Single
                    {
                        query: |Person.all();
                    }
                }
                """;

        ServiceDefinition service = PureDefinitionParser.parseServiceDefinition(pureService);

        assertEquals("api::SimpleService", service.qualifiedName());
        assertTrue(service.testSuites().isEmpty());
    }

    @Test
    @DisplayName("Parse service with multiple path params")
    void testParseServiceWithMultipleParams() {
        String pureService = """
                Service api::AddressService
                {
                    pattern: '/api/persons/{personId}/addresses/{addressId}';
                    execution: Single
                    {
                        query: |Address.all()->filter({a | $a.personId == $personId && $a.id == $addressId});
                    }
                }
                """;

        ServiceDefinition service = PureDefinitionParser.parseServiceDefinition(pureService);

        // Verify path params
        assertEquals(2, service.pathParams().size());
        assertEquals("personId", service.pathParams().get(0));
        assertEquals("addressId", service.pathParams().get(1));
    }
}
