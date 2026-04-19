package com.gs.legend.test;

import com.gs.legend.model.def.ServiceDefinition;
import com.gs.legend.parser.PureParser;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for parsing service testSuites.
 */
@DisplayName("Service Test Suite Parser Tests")
class ServiceTestSuiteParserTest {

    @Test
    @DisplayName("Parse service with complex pattern")
    void testParseServiceWithTestSuite() {
        String pureService = """
                import api::*;

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

        ServiceDefinition service = PureParser.parseSingle(pureService, ServiceDefinition.class);

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
                import api::*;

                Service api::SimpleService
                {
                    pattern: '/api/simple';
                    execution: Single
                    {
                        query: |Person.all();
                    }
                }
                """;

        ServiceDefinition service = PureParser.parseSingle(pureService, ServiceDefinition.class);

        assertEquals("api::SimpleService", service.qualifiedName());
        assertTrue(service.testSuites().isEmpty());
    }

    @Test
    @DisplayName("Parse service with multiple path params")
    void testParseServiceWithMultipleParams() {
        String pureService = """
                import api::*;

                Service api::AddressService
                {
                    pattern: '/api/persons/{personId}/addresses/{addressId}';
                    execution: Single
                    {
                        query: |Address.all()->filter({a | $a.personId == $personId && $a.id == $addressId});
                    }
                }
                """;

        ServiceDefinition service = PureParser.parseSingle(pureService, ServiceDefinition.class);

        // Verify path params
        assertEquals(2, service.pathParams().size());
        assertEquals("personId", service.pathParams().get(0));
        assertEquals("addressId", service.pathParams().get(1));
    }
}
