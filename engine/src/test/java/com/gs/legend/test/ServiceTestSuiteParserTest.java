package com.gs.legend.test;
import com.gs.legend.ast.*;
import com.gs.legend.antlr.*;
import com.gs.legend.parser.*;
import com.gs.legend.compiler.*;
import com.gs.legend.model.*;
import com.gs.legend.model.def.*;
import com.gs.legend.model.m3.*;
import com.gs.legend.model.store.*;
import com.gs.legend.model.mapping.*;
import com.gs.legend.plan.*;
import com.gs.legend.exec.*;
import com.gs.legend.serial.*;
import com.gs.legend.sqlgen.*;
import com.gs.legend.server.*;
import com.gs.legend.service.*;import com.gs.legend.parser.PureParser;

import com.gs.legend.model.def.*;
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

        ServiceDefinition service = PureParser.parseServiceDefinition(pureService);

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

        ServiceDefinition service = PureParser.parseServiceDefinition(pureService);

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

        ServiceDefinition service = PureParser.parseServiceDefinition(pureService);

        // Verify path params
        assertEquals(2, service.pathParams().size());
        assertEquals("personId", service.pathParams().get(0));
        assertEquals("addressId", service.pathParams().get(1));
    }
}
