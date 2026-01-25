package org.finos.legend.engine.test;

import org.finos.legend.pure.dsl.PureParseException;
import org.finos.legend.pure.dsl.definition.PureDefinitionParser;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that parse errors include accurate line number information.
 * 
 * These tests verify that when Pure parsing fails, the exception contains
 * the correct source location so IDEs can show red squiggles on the right line.
 */
@DisplayName("Parse Error Location Tests")
class ParseErrorLocationTest {

    @Nested
    @DisplayName("Class parsing errors")
    class ClassParsingErrors {

        @Test
        @DisplayName("Missing colon error reports correct line number")
        void missingColonReportsCorrectLine() {
            // GIVEN: A class with a missing colon on line 5
            String pureClass = """
                    Class model::Person
                    {
                        firstName: String[1];
                        lastName: String[1];
                        age Integer[1];
                        email: String[1];
                    }
                    """;
            // The error is on line 5 (age Integer[1] missing colon)
            // Line 1: Class model::Person
            // Line 2: {
            // Line 3: firstName: String[1];
            // Line 4: lastName: String[1];
            // Line 5: age Integer[1]; <-- ERROR HERE
            // Line 6: email: String[1];
            // Line 7: }

            // WHEN: We try to parse it
            PureParseException exception = assertThrows(
                    PureParseException.class,
                    () -> PureDefinitionParser.parseClassDefinition(pureClass));

            // THEN: The exception has correct line number
            assertTrue(exception.hasLocation(),
                    "Exception should have location info, but message was: " + exception.getMessage());
            assertEquals(5, exception.getLine(),
                    "Error should be on line 5 (age Integer[1]), but was line " + exception.getLine());
        }

        @Test
        @DisplayName("Missing colon at different line reports that line")
        void missingColonAtDifferentLineReportsCorrectLine() {
            // GIVEN: A class with a missing colon on line 3
            String pureClass = """
                    Class Person
                    {
                        firstName String[1];
                        lastName: String[1];
                    }
                    """;
            // Error on line 3

            // WHEN: We try to parse it
            PureParseException exception = assertThrows(
                    PureParseException.class,
                    () -> PureDefinitionParser.parseClassDefinition(pureClass));

            // THEN: The exception has correct line number
            assertTrue(exception.hasLocation(),
                    "Exception should have location info");
            assertEquals(3, exception.getLine(),
                    "Error should be on line 3 (firstName String[1])");
        }

        @Test
        @DisplayName("Error after comment lines reports correct line number")
        void errorAfterCommentsReportsCorrectLine() {
            // GIVEN: Source with comments before the error
            String pureSource = """
                    // This is a comment
                    // Another comment line

                    Class model::Person
                    {
                        firstName: String[1];
                        age Integer[1];
                    }
                    """;
            // Line 1: // This is a comment
            // Line 2: // Another comment line
            // Line 3: (empty)
            // Line 4: Class model::Person
            // Line 5: {
            // Line 6: firstName: String[1];
            // Line 7: age Integer[1]; <-- ERROR HERE

            // WHEN: We try to parse it
            PureParseException exception = assertThrows(
                    PureParseException.class,
                    () -> PureDefinitionParser.parse(pureSource));

            // THEN: The exception reports line 7, NOT a lower line
            assertTrue(exception.hasLocation(),
                    "Exception should have location info");
            assertEquals(7, exception.getLine(),
                    "Error should be on line 7 (after comments)");
        }
    }

    @Nested
    @DisplayName("Error message format")
    class ErrorMessageFormat {

        @Test
        @DisplayName("Error message includes 'line X:Y' format for LSP compatibility")
        void errorMessageIncludesLineFormat() {
            // GIVEN: Invalid Pure code
            String pureClass = """
                    Class Person
                    {
                        name String[1];
                    }
                    """;

            // WHEN: We try to parse it
            PureParseException exception = assertThrows(
                    PureParseException.class,
                    () -> PureDefinitionParser.parseClassDefinition(pureClass));

            // THEN: The message includes "line X:Y" format
            String message = exception.getMessage();
            assertTrue(message.matches("line \\d+:\\d+ .*"),
                    "Message should start with 'line X:Y', but was: " + message);
        }
    }
}
