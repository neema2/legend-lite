package com.legend.parser;

import com.legend.lexer.Lexer;
import com.legend.lexer.TokenStream;
import com.legend.parser.spec.CBoolean;
import com.legend.parser.spec.CDateTime;
import com.legend.parser.spec.CDecimal;
import com.legend.parser.spec.CFloat;
import com.legend.parser.spec.CInteger;
import com.legend.parser.spec.CLatestDate;
import com.legend.parser.spec.CStrictDate;
import com.legend.parser.spec.CStrictTime;
import com.legend.parser.spec.CString;
import com.legend.parser.spec.PureCollection;
import com.legend.parser.spec.ValueSpecification;
import com.legend.parser.spec.Variable;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link SpecParser}, Phase C.1 grammar &mdash; leaf
 * expressions: literals, variables, and collection literals.
 *
 * <p>Tests are grouped by production. Each group has at least one
 * positive case pinning the produced record shape and at least one
 * negative case proving we reject the natural mis-spelling rather than
 * accepting it with a fallback value.
 */
final class SpecParserTest {

    // ----- numeric literals --------------------------------------------

    @Test
    void integerLiteralFits64Bits() {
        // Bytes that fit in a signed long stay a Long; the record holds
        // a Number to leave room for BigInteger overflow.
        ValueSpecification spec = SpecParser.parse("42");
        assertEquals(new CInteger(42L), spec);
        assertInstanceOf(Long.class, ((CInteger) spec).value(),
                "small integers should be narrowed to Long for cheap equality");
    }

    @Test
    void integerLiteralOverflowsToBigInteger() {
        // A digit string Larger than Long.MAX_VALUE forces BigInteger so
        // we don't silently lose precision.
        String huge = "92233720368547758980000";
        ValueSpecification spec = SpecParser.parse(huge);
        assertEquals(new CInteger(new BigInteger(huge)), spec);
        assertInstanceOf(BigInteger.class, ((CInteger) spec).value(),
                "overflowing literal should be carried as BigInteger");
    }

    @Test
    void floatLiteralParsesAsDouble() {
        assertEquals(new CFloat(3.14), SpecParser.parse("3.14"));
    }

    @Test
    void floatLiteralWithExponent() {
        assertEquals(new CFloat(1.5e-3), SpecParser.parse("1.5e-3"));
    }

    @Test
    void floatLiteralWithFSuffix() {
        // Pure permits a trailing 'f' on float literals; Java's
        // Double.parseDouble does not, so the parser must strip it.
        assertEquals(new CFloat(2.5), SpecParser.parse("2.5f"));
    }

    @Test
    void decimalLiteralFromIntegerShape() {
        // The lexer emits DECIMAL for 42d; the parser must round-trip
        // through BigDecimal without losing the integer shape.
        assertEquals(new CDecimal(new BigDecimal("42")), SpecParser.parse("42d"));
    }

    @Test
    void decimalLiteralFromFloatShape() {
        assertEquals(new CDecimal(new BigDecimal("3.14")), SpecParser.parse("3.14d"));
    }

    // ----- string literals ---------------------------------------------

    @Test
    void stringLiteralStripsQuotes() {
        assertEquals(new CString("hello"), SpecParser.parse("'hello'"));
    }

    @Test
    void stringLiteralResolvesEscapes() {
        // \\ \' \n \t \r are the documented supported escape set; we
        // assert each round-trips to the expected character so a future
        // refactor can't silently change the table.
        assertEquals(new CString("a\\b'c\nd\te\rf"),
                SpecParser.parse("'a\\\\b\\'c\\nd\\te\\rf'"));
    }

    @Test
    void stringLiteralEmptyIsLegal() {
        assertEquals(new CString(""), SpecParser.parse("''"));
    }

    @Test
    void stringLiteralUnknownEscapeRejected() {
        // \z is not in the supported escape set; we must fail loudly
        // rather than emit it verbatim (which would silently diverge
        // from engine semantics).
        ParseException ex = assertThrows(ParseException.class,
                () -> SpecParser.parse("'a\\zb'"));
        assertTrue(ex.getMessage().contains("unsupported escape"),
                () -> "want unsupported-escape error, got: " + ex.getMessage());
    }

    // ----- booleans ----------------------------------------------------

    @Test
    void booleanTrue() {
        assertEquals(new CBoolean(true), SpecParser.parse("true"));
    }

    @Test
    void booleanFalse() {
        assertEquals(new CBoolean(false), SpecParser.parse("false"));
    }

    // ----- temporal literals -------------------------------------------

    @Test
    void strictDateLiteralYearMonthDay() {
        // %2024-01-15 has no 'T' -> StrictDate, not DateTime.
        assertEquals(new CStrictDate("2024-01-15"), SpecParser.parse("%2024-01-15"));
    }

    @Test
    void strictDateLiteralYearOnly() {
        // Pure permits %2024 as a year-precision strict date.
        assertEquals(new CStrictDate("2024"), SpecParser.parse("%2024"));
    }

    @Test
    void dateTimeLiteralWithTimeComponent() {
        // Presence of 'T' is the sole discriminator between StrictDate
        // and DateTime; the test pins this so a future "smarter" parser
        // doesn't drift the rule.
        assertEquals(new CDateTime("2024-01-15T10:30:00"),
                SpecParser.parse("%2024-01-15T10:30:00"));
    }

    @Test
    void dateTimeLiteralWithFractionalSecondsAndTimezone() {
        assertEquals(new CDateTime("2024-01-15T10:30:00.123+0000"),
                SpecParser.parse("%2024-01-15T10:30:00.123+0000"));
    }

    @Test
    void strictTimeLiteral() {
        // %hh:mm:ss with no dashes -> StrictTime.
        assertEquals(new CStrictTime("10:30:45"),
                SpecParser.parse("%10:30:45"));
    }

    @Test
    void latestDateSentinel() {
        // CLatestDate is fieldless; equality follows record contract.
        assertEquals(new CLatestDate(), SpecParser.parse("%latest"));
    }

    // ----- variables ---------------------------------------------------

    @Test
    void variableReference() {
        // $name -> Variable("name"); the $ is NOT in the carried name.
        assertEquals(new Variable("this"), SpecParser.parse("$this"));
    }

    @Test
    void variableWithUnderscoresAndDigits() {
        assertEquals(new Variable("my_var2"), SpecParser.parse("$my_var2"));
    }

    @Test
    void dollarWithoutIdentifierRejected() {
        ParseException ex = assertThrows(ParseException.class,
                () -> SpecParser.parse("$"));
        assertTrue(ex.getMessage().toLowerCase().contains("expected identifier"),
                () -> "want identifier-after-dollar error, got: " + ex.getMessage());
    }

    // ----- collection literals -----------------------------------------

    @Test
    void emptyCollection() {
        assertEquals(new PureCollection(List.of()), SpecParser.parse("[]"));
    }

    @Test
    void singletonCollection() {
        assertEquals(new PureCollection(List.of(new CInteger(1L))),
                SpecParser.parse("[1]"));
    }

    @Test
    void collectionOfMixedLiterals() {
        // Element kinds are independent: a single collection may carry
        // any ValueSpecification per element.
        assertEquals(
                new PureCollection(List.of(
                        new CInteger(1L),
                        new CString("two"),
                        new CBoolean(true))),
                SpecParser.parse("[1, 'two', true]"));
    }

    @Test
    void nestedCollection() {
        assertEquals(
                new PureCollection(List.of(
                        new PureCollection(List.of(new CInteger(1L), new CInteger(2L))),
                        new PureCollection(List.of(new CInteger(3L))))),
                SpecParser.parse("[[1, 2], [3]]"));
    }

    @Test
    void collectionTrailingCommaRejected() {
        // Trailing comma is not legal in Pure; matching engine here
        // keeps test corpora byte-comparable.
        ParseException ex = assertThrows(ParseException.class,
                () -> SpecParser.parse("[1, 2,]"));
        assertTrue(ex.getMessage().toLowerCase().contains("trailing comma"),
                () -> "want trailing-comma error, got: " + ex.getMessage());
    }

    @Test
    void collectionUnclosedRejected() {
        // Tighter than contains("]"): a regression throwing the wrong-
        // direction error ("unexpected ]") would falsely pass that.
        // Pin the exact wording so it must be a missing-close error.
        ParseException ex = assertThrows(ParseException.class,
                () -> SpecParser.parse("[1, 2"));
        assertTrue(ex.getMessage().contains("expected ']'"),
                () -> "want missing-close error, got: " + ex.getMessage());
    }

    // ----- top-level error surfaces ------------------------------------

    @Test
    void unsupportedTokenSurfacesAsError() {
        // C.2+ tokens like '->' (method chain) must be rejected loudly
        // in C.1 grammar; silently accepting them as something else
        // would mask the not-yet-implemented gap. Pin BOTH the error
        // word and the actual token type so a regression that throws a
        // different unsupported-token error fails this test.
        ParseException ex = assertThrows(ParseException.class,
                () -> SpecParser.parse("->"));
        assertTrue(ex.getMessage().contains("unsupported expression token"),
                () -> "want unsupported-token error, got: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("ARROW"),
                () -> "error should name the ARROW token type, got: " + ex.getMessage());
    }

    @Test
    void emptyInputRejected() {
        // Empty input is a distinct error from "unsupported token" —
        // pin the exact wording so a regression that conflates the two
        // surfaces here.
        ParseException ex = assertThrows(ParseException.class,
                () -> SpecParser.parse(""));
        assertTrue(ex.getMessage().contains("end of input"),
                () -> "want end-of-input error, got: " + ex.getMessage());
    }

    @Test
    void trailingTokensRejected() {
        // A single expression must consume the entire input; trailing
        // tokens are a strong signal that the input is malformed (or
        // that the caller meant to parse a multi-statement body, which
        // is a C.4 feature).
        ParseException ex = assertThrows(ParseException.class,
                () -> SpecParser.parse("1 2"));
        assertTrue(ex.getMessage().toLowerCase().contains("trailing"),
                () -> "want trailing-tokens error, got: " + ex.getMessage());
    }

    // ----- coverage gap markers (C.2+ features) -----------------------

    @Test
    void negativeIntegerLiteralNotYetSupported() {
        // Pure '-42' parses as unary minus applied to 42, not as a
        // negative literal. Unary operators land in C.3, so this MUST
        // throw in C.1. When C.3 ships, this test flips to a positive
        // assertion on the AppliedFunction('-', [CInteger(42)]) shape
        // — the failure is the signal that the gap closed.
        ParseException ex = assertThrows(ParseException.class,
                () -> SpecParser.parse("-42"));
        assertTrue(ex.getMessage().contains("unsupported expression token")
                        && ex.getMessage().contains("MINUS"),
                () -> "want MINUS-unsupported error, got: " + ex.getMessage());
    }

    @Test
    void variableNameMayBeAKeyword() {
        // Pure permits identifiers that lex as keyword tokens (let,
        // class, all, etc.) in any position where an identifier is
        // expected. SpecParser borrows ElementParser.IDENTIFIER_TOKENS
        // for this. Pin the bridge so a future tightening of the
        // keyword list doesn't silently break variable parsing.
        assertEquals(new Variable("let"), SpecParser.parse("$let"));
        assertEquals(new Variable("class"), SpecParser.parse("$class"));
        assertEquals(new Variable("all"), SpecParser.parse("$all"));
    }

    // ----- slice entry point -------------------------------------------

    @Test
    void parseFromTokenStreamSlicePreservesSourceOffsets() {
        // The slice entry point is what ModelOrchestrator will use to
        // hand the parser a body's token range. Errors raised over a
        // slice must still point at the ORIGINAL source line/column,
        // not at offset 0 within the slice. This invariant comes from
        // TokenStream.slice (C.0); pinning it here documents the
        // cross-phase contract.
        String src = "Class my::X { x: String[1]; }\n" + "INVALID";
        TokenStream all = Lexer.tokenize(src);
        // Slice covering only "INVALID" (last token).
        TokenStream slice = all.slice(all.count() - 1, all.count());
        ParseException ex = assertThrows(ParseException.class,
                () -> SpecParser.parse(slice));
        assertEquals(2, ex.line(),
                () -> "error in slice should report ORIGINAL line 2, got: " + ex.getMessage());
    }
}
