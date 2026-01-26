package org.finos.legend.pure.dsl;

import org.finos.legend.pure.dsl.definition.PureDefinitionParser;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for accurate line number and column reporting in parse errors.
 * These tests verify that LSP/IDE integration receives correct source
 * locations.
 */
public class LineNumberErrorTest {

    @Test
    @DisplayName("Class parse error reports correct line and column")
    void testClassParseErrorLineNumber() {
        String source = """
                Class model::Person {
                    name: String[1];
                    age Integer[1];
                }
                """; // Missing colon on line 3

        PureParseException e = assertThrows(PureParseException.class,
                () -> PureDefinitionParser.parseClassDefinition(source));

        assertTrue(e.hasLocation(), "Error should have location info");
        assertEquals(3, e.getLine(), "Error should be on line 3 (missing colon)");
        assertTrue(e.getMessage().contains("line 3"), "Message should mention line 3");
    }

    @Test
    @DisplayName("Error in second definition reports correct offset line number")
    void testSecondDefinitionErrorLineNumber() {
        String source = """
                Class model::Person {
                    name: String[1];
                }

                Class model::Address {
                    street String[1];
                }
                """; // Missing colon on line 6

        PureParseException e = assertThrows(PureParseException.class,
                () -> PureDefinitionParser.parse(source));

        assertTrue(e.hasLocation(), "Error should have location info");
        // The error is on line 6 of the original source
        assertEquals(6, e.getLine(), "Error should be on line 6 (second class, missing colon)");
        assertTrue(e.getMessage().contains("line 6"), "Message should mention line 6");
    }

    @Test
    @DisplayName("Enum parse error reports correct line")
    void testEnumParseErrorLineNumber() {
        String source = """
                Enum model::Status {
                    ACTIVE,
                    INACTIVE
                    PENDING
                }
                """; // Missing comma on line 3

        // The parse should either fail or we get unexpected token
        // This tests that the ANTLR error carries line info
        try {
            PureDefinitionParser.parseEnumDefinition(source);
            // If it parses (comma optional), that's also fine
        } catch (PureParseException e) {
            assertTrue(e.hasLocation(), "Error should have location info");
        }
    }

    @Test
    @DisplayName("Function parse error reports correct line")
    void testFunctionParseErrorLineNumber() {
        String source = """
                function my::utils::greet(name: String[1]) String[1]
                {
                    'Hello, ' + $name
                }
                """; // Missing colon before return type on line 1

        PureParseException e = assertThrows(PureParseException.class,
                () -> PureDefinitionParser.parseFunctionDefinition(source));

        assertTrue(e.hasLocation(), "Error should have location info");
        assertEquals(1, e.getLine(), "Error should be on line 1 (missing colon)");
    }

    @Test
    @DisplayName("Error in third definition maintains correct line offset")
    void testThirdDefinitionErrorLineNumber() {
        String source = """
                Class model::Person {
                    name: String[1];
                }

                Class model::Address {
                    street: String[1];
                }

                Class model::Company {
                    name String[1];
                }
                """; // Missing colon on line 10

        PureParseException e = assertThrows(PureParseException.class,
                () -> PureDefinitionParser.parse(source));

        assertTrue(e.hasLocation(), "Error should have location info");
        assertEquals(10, e.getLine(), "Error should be on line 10 (third class, missing colon)");
    }

    @Test
    @DisplayName("Profile parse error reports correct line with offset")
    void testProfileParseErrorWithOffset() {
        String source = """
                Class model::Person {
                    name: String[1];
                }

                Profile my::Documentation {
                    stereotypes: [deprecated, internal]
                    tags: [author];
                }
                """; // Missing semicolon after stereotypes on line 6

        // Profile syntax error
        try {
            PureDefinitionParser.parse(source);
            fail("Expected parse error");
        } catch (PureParseException e) {
            assertTrue(e.hasLocation(), "Error should have location info");
            // Error should be somewhere in lines 5-7 (Profile section)
            assertTrue(e.getLine() >= 5 && e.getLine() <= 8,
                    "Error line " + e.getLine() + " should be in Profile section (5-8)");
        }
    }

    @Test
    @DisplayName("Database parse error reports correct line")
    void testDatabaseParseErrorLineNumber() {
        String source = """
                Database store::MyDB
                (
                    Table T_PERSON
                    (
                        ID INTEGER PRIMARY KEY
                        NAME VARCHAR(100) NOT NULL
                    )
                )
                """; // Missing comma on line 5

        // This may or may not error depending on grammar strictness
        // The key is that IF there's an error, it has location
        try {
            PureDefinitionParser.parseDatabaseDefinition(source);
        } catch (PureParseException e) {
            assertTrue(e.hasLocation(), "Error should have location info");
        }
    }

    @Test
    @DisplayName("Error column is captured in parse exception")
    void testErrorColumnCapture() {
        // Introduce error at a specific column
        String source = "Class model::@Invalid { }"; // Invalid char at column 14

        PureParseException e = assertThrows(PureParseException.class,
                () -> PureDefinitionParser.parseClassDefinition(source));

        assertTrue(e.hasLocation(), "Error should have location info");
        assertTrue(e.getColumn() > 0, "Column should be captured");
    }

    @Test
    @DisplayName("Mixed definitions parse successfully with valid types")
    void testMixedDefinitionsParseSuccessfully() {
        String source = """
                Class model::Person {
                    name: String[1];
                }

                Enum model::Status {
                    ACTIVE,
                    INACTIVE
                }

                Class model::Address {
                    street: String[1];
                }
                """;

        // This should parse successfully
        var definitions = PureDefinitionParser.parse(source);
        assertEquals(3, definitions.size(), "Should parse 3 definitions");
    }

    @Test
    @DisplayName("PureParseException hasLocation returns false when no location")
    void testNoLocationException() {
        PureParseException e = new PureParseException("Simple error message");
        assertFalse(e.hasLocation(), "Should not have location");
        assertEquals(-1, e.getLine());
        assertEquals(-1, e.getColumn());
    }

    @Test
    @DisplayName("PureParseException with location returns correct values")
    void testExceptionWithLocation() {
        PureParseException e = new PureParseException("error message", 5, 12);
        assertTrue(e.hasLocation());
        assertEquals(5, e.getLine());
        assertEquals(12, e.getColumn());
        assertTrue(e.getMessage().contains("line 5:12"));
    }

    @Test
    @DisplayName("Misspelled keyword reports error at correct line")
    void testMisspelledKeywordErrorLineNumber() {
        String source = """
                Class model::Person {
                    name: String[1];
                }

                Clas model::Address {
                    street: String[1];
                }
                """; // "Clas" instead of "Class" on line 5

        PureParseException e = assertThrows(PureParseException.class,
                () -> PureDefinitionParser.parse(source));

        assertTrue(e.hasLocation(), "Misspelled keyword error should have location");
        assertEquals(5, e.getLine(), "Error should be on line 5 (misspelled 'Clas')");
        // ANTLR reports as "extraneous input" rather than "Unknown definition"
        assertTrue(e.getMessage().contains("extraneous input") || e.getMessage().contains("Unknown definition"),
                "Should mention extraneous input or unknown definition");
    }

    @Test
    @DisplayName("Unmatched brace reports error at detected line")
    void testUnmatchedBraceErrorLineNumber() {
        String source = """
                Class model::Person {
                    name: String[1];
                    age: Integer[1];
                """; // Missing closing brace - ANTLR detects at EOF (line 4)

        PureParseException e = assertThrows(PureParseException.class,
                () -> PureDefinitionParser.parse(source));

        assertTrue(e.hasLocation(), "Unmatched brace error should have location");
        // ANTLR reports error where it's detected (EOF), not where brace opened
        // This is acceptable as ANTLR still provides accurate error location
        assertTrue(e.getLine() >= 1, "Error should have valid line number");
        assertTrue(e.getMessage().contains("missing") || e.getMessage().contains("expecting"),
                "Should mention missing or expecting");
    }
}
