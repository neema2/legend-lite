package com.legend.lexer;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link Lexer}.
 *
 * <p>Each test asserts the {@link TokenType} sequence produced by
 * tokenizing a small source fragment. Text content is spot-checked
 * where relevant. Source-offset correctness is verified by reconstructing
 * the original input from each token's {@code start}/{@code end} pair.
 */
final class LexerTest {

    // ----- helpers --------------------------------------------------------

    private static List<TokenType> types(String source) {
        TokenStream s = Lexer.tokenize(source);
        return s.asList().stream().map(Token::type).toList();
    }

    private static void assertTypes(String source, TokenType... expected) {
        assertEquals(List.of(expected), types(source),
                "Token type sequence for: " + source);
    }

    // ----- empty / whitespace / comments ---------------------------------

    @Test
    void emptySourceProducesNoTokens() {
        assertEquals(0, Lexer.tokenize("").count());
    }

    @Test
    void whitespaceOnlyProducesNoTokens() {
        assertEquals(0, Lexer.tokenize("   \n\t\r  ").count());
    }

    @Test
    void lineCommentIsSkipped() {
        assertTypes("// a comment\n42", TokenType.INTEGER);
    }

    @Test
    void blockCommentIsSkipped() {
        assertTypes("/* a /* nested-ish */ 42", TokenType.INTEGER);
    }

    @Test
    void sectionHeaderIsSkipped() {
        assertTypes("###Pure\n42", TokenType.INTEGER);
    }

    // ----- identifiers / keywords ----------------------------------------

    @Test
    void plainIdentifierIsValidString() {
        TokenStream s = Lexer.tokenize("foo");
        assertEquals(1, s.count());
        assertSame(TokenType.VALID_STRING, s.type(0));
        assertEquals("foo", s.text(0));
    }

    @Test
    void classKeywordIsRecognized() {
        assertTypes("Class", TokenType.CLASS);
    }

    @Test
    void identifierContainingKeywordIsNotKeyword() {
        TokenStream s = Lexer.tokenize("Classify");
        assertEquals(1, s.count());
        assertSame(TokenType.VALID_STRING, s.type(0));
    }

    // ----- path separator ------------------------------------------------

    @Test
    void pathSeparator() {
        // my::Pkg::Foo
        assertTypes("my::Pkg::Foo",
                TokenType.VALID_STRING, TokenType.PATH_SEPARATOR,
                TokenType.VALID_STRING, TokenType.PATH_SEPARATOR,
                TokenType.VALID_STRING);
    }

    // ----- numeric literals ----------------------------------------------

    @Test
    void integerLiteral() {
        assertTypes("42", TokenType.INTEGER);
    }

    @Test
    void floatLiteralWithDecimal() {
        assertTypes("3.14", TokenType.FLOAT);
    }

    @Test
    void floatLiteralWithExponent() {
        assertTypes("1.5e10", TokenType.FLOAT);
    }

    @Test
    void decimalLiteralSuffix() {
        assertTypes("42d", TokenType.DECIMAL);
    }

    // ----- string literals -----------------------------------------------

    @Test
    void singleQuotedString() {
        TokenStream s = Lexer.tokenize("'hello world'");
        assertEquals(1, s.count());
        assertSame(TokenType.STRING, s.type(0));
        assertEquals("'hello world'", s.text(0));
    }

    @Test
    void doubleQuotedString() {
        assertTypes("\"abc\"", TokenType.QUOTED_STRING);
    }

    // ----- punctuation ---------------------------------------------------

    @Test
    void braces() {
        assertTypes("{}", TokenType.BRACE_OPEN, TokenType.BRACE_CLOSE);
    }

    @Test
    void arrowVsMinus() {
        assertTypes("a -> b - c",
                TokenType.VALID_STRING, TokenType.ARROW,
                TokenType.VALID_STRING, TokenType.MINUS,
                TokenType.VALID_STRING);
    }

    @Test
    void multiCharPunctuation() {
        assertTypes("a >= b <= c == d != e",
                TokenType.VALID_STRING, TokenType.GREATER_OR_EQUAL,
                TokenType.VALID_STRING, TokenType.LESS_OR_EQUAL,
                TokenType.VALID_STRING, TokenType.TEST_EQUAL,
                TokenType.VALID_STRING, TokenType.TEST_NOT_EQUAL,
                TokenType.VALID_STRING);
    }

    // ----- date / time literals ------------------------------------------

    @Test
    void dateLiteral() {
        assertTypes("%2024-01-15", TokenType.DATE);
    }

    @Test
    void strictTimeLiteral() {
        assertTypes("%10:30:45", TokenType.STRICTTIME);
    }

    @Test
    void latestDateLiteral() {
        assertTypes("%latest", TokenType.LATEST_DATE);
    }

    // ----- end-to-end smoke ----------------------------------------------

    @Test
    void simpleEmptyClassDeclaration() {
        // Class my::Pkg::Foo {}
        assertTypes("Class my::Pkg::Foo {}",
                TokenType.CLASS,
                TokenType.VALID_STRING, TokenType.PATH_SEPARATOR,
                TokenType.VALID_STRING, TokenType.PATH_SEPARATOR,
                TokenType.VALID_STRING,
                TokenType.BRACE_OPEN, TokenType.BRACE_CLOSE);
    }

    @Test
    void functionDeclarationWithArrowAndBody() {
        assertTypes("function foo(): Integer[1] { 1 + 2 }",
                TokenType.FUNCTION, TokenType.VALID_STRING,
                TokenType.PAREN_OPEN, TokenType.PAREN_CLOSE,
                TokenType.COLON, TokenType.VALID_STRING,
                TokenType.BRACKET_OPEN, TokenType.INTEGER, TokenType.BRACKET_CLOSE,
                TokenType.BRACE_OPEN,
                TokenType.INTEGER, TokenType.PLUS, TokenType.INTEGER,
                TokenType.BRACE_CLOSE);
    }

    // ----- source-offset round-trip --------------------------------------

    @Test
    void tokenOffsetsReproduceOriginalSource() {
        String src = "Class my::Foo { name: String[1]; }";
        TokenStream s = Lexer.tokenize(src);
        // For every token, source.substring(start, end) must equal text(i).
        for (int i = 0; i < s.count(); i++) {
            assertEquals(s.text(i), src.substring(s.start(i), s.end(i)),
                    "Token " + i + " (type=" + s.type(i) + ") offsets mismatch");
        }
    }

    // ----- zero-allocation comparison -------------------------------------

    // ----- slice ----------------------------------------------------------

    @Test
    void sliceProducesEquivalentSubStream() {
        String src = "Class my::Foo { x: String[1]; } Class my::Bar { y: Integer[1]; }";
        TokenStream full = Lexer.tokenize(src);

        // Find the boundary: token after the first BRACE_CLOSE.
        int boundary = -1;
        for (int i = 0; i < full.count(); i++) {
            if (full.type(i) == TokenType.BRACE_CLOSE) { boundary = i + 1; break; }
        }
        assertTrue(boundary > 0 && boundary < full.count(), "boundary token must exist");

        TokenStream barSlice = full.slice(boundary, full.count());

        // Slice's first token is what was at the boundary in the original.
        assertEquals(full.type(boundary), barSlice.type(0));
        assertEquals(full.start(boundary), barSlice.start(0),
                "start offsets are preserved (point into the SAME source)");
        assertEquals(full.end(boundary), barSlice.end(0));
        assertEquals(full.count() - boundary, barSlice.count());
        // Source string identity preserved \u2014 text(i) still works.
        assertEquals(full.source(), barSlice.source());
        assertEquals("Class", barSlice.text(0));
    }

    @Test
    void unsupportedConstructsLexAsInvalidAndTerminate() {
        // Re-audit H1/H2/M1: the dead-token purge's first cut left branches
        // that consumed input while emitting NOTHING — one was an INFINITE
        // LOOP (->subType(@ never advanced pos). Unsupported constructs must
        // lex as INVALID (the cursor's loud trap) and the lexer must always
        // TERMINATE.
        // ->subType(@T) LEXES now (ARROW + ident + AT type ref) — the
        // termination property this test pins is unaffected
        assertTrue(Lexer.tokenize("$x->subType(@Foo)").asList().stream()
                .noneMatch(t -> t.type() == TokenType.INVALID), "->subType(@ lexes cleanly");
        // navigation paths LEX now (PATH_LITERAL; the parser desugars the
        // plain-property subset) — the termination property still pins
        assertTrue(Lexer.tokenize("#/Person/name#").asList().stream()
                .anyMatch(t -> t.type() == TokenType.PATH_LITERAL), "#/...# lexes as PATH_LITERAL");
        assertTrue(Lexer.tokenize("?[data.csv").asList().stream()
                .anyMatch(t -> t.type() == TokenType.INVALID), "?[file is INVALID");
    }

    @Test
    void sliceEmptyRangeProducesEmptyStream() {
        TokenStream full = Lexer.tokenize("Class my::Foo {}");
        TokenStream empty = full.slice(2, 2);
        assertEquals(0, empty.count());
    }

    @Test
    void sliceOutOfBoundsThrows() {
        TokenStream full = Lexer.tokenize("Class my::Foo {}");
        assertThrows(IndexOutOfBoundsException.class, () -> full.slice(-1, 1));
        assertThrows(IndexOutOfBoundsException.class, () -> full.slice(0, full.count() + 1));
        assertThrows(IndexOutOfBoundsException.class, () -> full.slice(3, 2));
    }
}
