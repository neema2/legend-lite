package org.finos.legend.engine.test;

import org.finos.legend.pure.dsl.definition.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Pure Function definition parsing.
 * All tests use Pure language syntax as the starting point.
 */
public class FunctionParserTest {

    @Test
    @DisplayName("Parse function with single parameter")
    void testParseFunctionWithParameters() {
        String pure = """
                function my::utils::greet(name: String[1]): String[1]
                {
                    'Hello, ' + $name
                }
                """;

        FunctionDefinition func = PureDefinitionParser.parseFunctionDefinition(pure);

        assertEquals("my::utils::greet", func.qualifiedName());
        assertEquals("greet", func.simpleName());
        assertEquals("my::utils", func.packagePath());
        assertEquals(1, func.parameters().size());
        assertEquals("name", func.parameters().get(0).name());
        assertEquals("String", func.parameters().get(0).type());
        assertEquals(1, func.parameters().get(0).lowerBound());
        assertEquals(Integer.valueOf(1), func.parameters().get(0).upperBound());
        assertEquals("String", func.returnType());
        assertEquals(1, func.returnLowerBound());
        assertEquals(Integer.valueOf(1), func.returnUpperBound());
        assertEquals("'Hello, ' + $name", func.body());
    }

    @Test
    @DisplayName("Parse function with multiple parameters")
    void testParseFunctionMultipleParameters() {
        String pure = """
                function math::add(a: Integer[1], b: Integer[1]): Integer[1]
                {
                    $a + $b
                }
                """;

        FunctionDefinition func = PureDefinitionParser.parseFunctionDefinition(pure);

        assertEquals("math::add", func.qualifiedName());
        assertEquals(2, func.parameters().size());
        assertEquals("a", func.parameters().get(0).name());
        assertEquals("Integer", func.parameters().get(0).type());
        assertEquals("b", func.parameters().get(1).name());
        assertEquals("$a + $b", func.body());
    }

    @Test
    @DisplayName("Parse function with optional parameter")
    void testParseFunctionWithOptionalParameter() {
        String pure = """
                function utils::format(value: String[1], prefix: String[0..1]): String[1]
                {
                    if($prefix->isEmpty(), |$value, |$prefix->toOne() + $value)
                }
                """;

        FunctionDefinition func = PureDefinitionParser.parseFunctionDefinition(pure);

        assertEquals(2, func.parameters().size());
        assertEquals("prefix", func.parameters().get(1).name());
        assertEquals(0, func.parameters().get(1).lowerBound());
        assertEquals(Integer.valueOf(1), func.parameters().get(1).upperBound());
    }

    @Test
    @DisplayName("Parse function with many return type")
    void testParseFunctionWithManyReturn() {
        String pure = """
                function query::getAllNames(): String[*]
                {
                    Person.all()->map({p | $p.name})
                }
                """;

        FunctionDefinition func = PureDefinitionParser.parseFunctionDefinition(pure);

        assertEquals("getAllNames", func.simpleName());
        assertEquals("String", func.returnType());
        assertEquals(0, func.returnLowerBound());
        assertNull(func.returnUpperBound()); // null means unbounded (*)
    }

    @Test
    @DisplayName("Parse function with no parameters")
    void testParseFunctionNoParameters() {
        String pure = """
                function app::getVersion(): String[1]
                {
                    '1.0.0'
                }
                """;

        FunctionDefinition func = PureDefinitionParser.parseFunctionDefinition(pure);

        assertEquals("app::getVersion", func.qualifiedName());
        assertTrue(func.parameters().isEmpty());
        assertEquals("'1.0.0'", func.body());
    }

    @Test
    @DisplayName("Parse function with stereotype")
    void testParseFunctionWithStereotypes() {
        // In legend-engine, stereotypes come AFTER the function keyword
        String pure = """
                function <<doc::Documentation.deprecated>> old::legacy::method(): Boolean[1]
                {
                    true
                }
                """;

        FunctionDefinition func = PureDefinitionParser.parseFunctionDefinition(pure);

        assertEquals("old::legacy::method", func.qualifiedName());
        assertEquals(1, func.stereotypes().size());
        assertEquals("doc::Documentation", func.stereotypes().get(0).profileName());
        assertEquals("deprecated", func.stereotypes().get(0).stereotypeName());
        assertEquals("true", func.body());
    }

    @Test
    @DisplayName("Parse function with tagged value")
    void testParseFunctionWithTaggedValue() {
        // In legend-engine, tagged values come AFTER the function keyword
        String pure = """
                function {doc::Documentation.author = 'John Smith'} utils::helper(): String[1]
                {
                    'help'
                }
                """;

        FunctionDefinition func = PureDefinitionParser.parseFunctionDefinition(pure);

        assertEquals(1, func.taggedValues().size());
        assertEquals("doc::Documentation", func.taggedValues().get(0).profileName());
        assertEquals("author", func.taggedValues().get(0).tagName());
        assertEquals("John Smith", func.taggedValues().get(0).value());
    }

    @Test
    @DisplayName("Parse multiple definitions including function")
    void testParseMultipleDefinitionsWithFunction() {
        String pure = """
                Class model::Person {
                    name: String[1];
                }

                function query::getPersons(): model::Person[*]
                {
                    Person.all()
                }
                """;

        List<PureDefinition> definitions = PureDefinitionParser.parse(pure);

        assertEquals(2, definitions.size());
        assertInstanceOf(ClassDefinition.class, definitions.get(0));
        assertInstanceOf(FunctionDefinition.class, definitions.get(1));

        FunctionDefinition func = (FunctionDefinition) definitions.get(1);
        assertEquals("query::getPersons", func.qualifiedName());
        assertEquals("model::Person", func.returnType());
    }
}
