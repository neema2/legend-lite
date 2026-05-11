package com.legend.parser;

import com.legend.lexer.Lexer;
import com.legend.lexer.TokenStream;
import com.legend.parser.spec.AppliedFunction;
import com.legend.parser.spec.AppliedProperty;
import com.legend.parser.spec.CBoolean;
import com.legend.parser.spec.CDateTime;
import com.legend.parser.spec.CDecimal;
import com.legend.parser.spec.CFloat;
import com.legend.parser.spec.CInteger;
import com.legend.parser.spec.CLatestDate;
import com.legend.parser.spec.CStrictDate;
import com.legend.parser.spec.CStrictTime;
import com.legend.parser.spec.CString;
import com.legend.parser.spec.PackageableElementPtr;
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

    // ----- parenthesised grouping (C.2) -------------------------------

    @Test
    void parenthesisedExpressionUnwrapsToInner() {
        // The AST carries no Grouping node \u2014 parens are pure source
        // sugar at parse time. Pinning identity-equality with the
        // unwrapped form would be over-specified (records don't share
        // identity), so we pin RECORD equality to the same literal
        // the bare form produces.
        assertEquals(SpecParser.parse("42"), SpecParser.parse("(42)"));
        assertEquals(SpecParser.parse("$x"), SpecParser.parse(("($x)")));
    }

    @Test
    void parenthesisedExpressionNests() {
        // Multiple layers of parens must all unwrap to the same AST,
        // since none of them carry semantic info.
        assertEquals(new CInteger(7L), SpecParser.parse("(((7)))"));
    }

    // ----- packageable element references (C.2) -----------------------

    @Test
    void bareIdentifierBecomesPackageableElementPtr() {
        // A bare identifier at expression position is a class / element
        // reference; PackageableElementPtr carries the source name
        // verbatim (no FQN resolution until Phase D).
        assertEquals(new PackageableElementPtr("Person"),
                SpecParser.parse("Person"));
    }

    @Test
    void qualifiedIdentifierBecomesPackageableElementPtr() {
        assertEquals(new PackageableElementPtr("my::app::Person"),
                SpecParser.parse("my::app::Person"));
    }

    // ----- prefix function application (C.2) --------------------------

    @Test
    void zeroArgPrefixCall() {
        assertEquals(new AppliedFunction("now", List.of()),
                SpecParser.parse("now()"));
    }

    @Test
    void singleArgPrefixCall() {
        assertEquals(new AppliedFunction("abs", List.of(new CInteger(5L))),
                SpecParser.parse("abs(5)"));
    }

    @Test
    void multiArgPrefixCallWithMixedKinds() {
        assertEquals(
                new AppliedFunction("plus", List.of(
                        new CInteger(1L),
                        new CFloat(2.5),
                        new Variable("x"))),
                SpecParser.parse("plus(1, 2.5, $x)"));
    }

    @Test
    void qualifiedPrefixCall() {
        // FQN preserved verbatim in AppliedFunction.function field;
        // resolution is the next pipeline stage's job.
        assertEquals(
                new AppliedFunction("my::pkg::add",
                        List.of(new CInteger(1L), new CInteger(2L))),
                SpecParser.parse("my::pkg::add(1, 2)"));
    }

    @Test
    void prefixCallWithNestedCall() {
        // Args may themselves be any expression \u2014 nested calls included.
        assertEquals(
                new AppliedFunction("outer", List.of(
                        new AppliedFunction("inner", List.of(new CInteger(1L))))),
                SpecParser.parse("outer(inner(1))"));
    }

    // ----- property access (C.2) --------------------------------------

    @Test
    void singlePropertyAccess() {
        // AppliedProperty(receiver, property) reads left-to-right like
        // source: $x.name -> AppliedProperty($x, "name").
        assertEquals(new AppliedProperty(new Variable("x"), "name"),
                SpecParser.parse("$x.name"));
    }

    @Test
    void chainedPropertyAccessIsLeftAssociative() {
        // $x.foo.bar -> AppliedProperty(AppliedProperty($x, "foo"), "bar").
        // Pin the nesting shape so a refactor cannot silently flip the
        // associativity (which would be a SILENT semantic change).
        assertEquals(
                new AppliedProperty(
                        new AppliedProperty(new Variable("x"), "foo"),
                        "bar"),
                SpecParser.parse("$x.foo.bar"));
    }

    @Test
    void quotedPropertyNameStripsQuotesAndResolvesEscapes() {
        // Property names with whitespace / special chars are written
        // with single quotes; same lexical rules as a CString literal.
        assertEquals(
                new AppliedProperty(new Variable("x"), "My Name"),
                SpecParser.parse("$x.'My Name'"));
    }

    // ----- method-on-receiver (C.2) -----------------------------------

    @Test
    void methodOnVariableReceiver() {
        // $x.method(y) is sugar for method($x, y) per Pure's arrow
        // convention; receiver lives at parameter index 0.
        assertEquals(
                new AppliedFunction("filter", List.of(
                        new Variable("x"),
                        new Variable("y"))),
                SpecParser.parse("$x.filter($y)"));
    }

    @Test
    void methodOnPackageableElementReceiver() {
        // The canonical Pure entry point: Person.all() ->
        // AppliedFunction("all", [PackageableElementPtr("Person")]).
        assertEquals(
                new AppliedFunction("all", List.of(
                        new PackageableElementPtr("Person"))),
                SpecParser.parse("Person.all()"));
    }

    // ----- arrow form (C.2) -------------------------------------------

    @Test
    void arrowFormDesugaringIsIdenticalToMethodForm() {
        // The whole point of the AppliedFunction shape: after parsing,
        // $x->foo(y) and $x.foo(y) MUST produce identical ASTs. This
        // test pins the desugaring invariant so any future divergence
        // (e.g. accidentally re-introducing a hasReceiver flag) fails
        // here loud and clear.
        ValueSpecification arrow = SpecParser.parse("$x->foo($y)");
        ValueSpecification method = SpecParser.parse("$x.foo($y)");
        assertEquals(method, arrow,
                "$x->foo($y) and $x.foo($y) must produce equal AST");
    }

    @Test
    void arrowChainIsLeftAssociative() {
        // Person.all()->filter($p)->limit(10):
        //   filter applies to all(Person), limit applies to filter(...).
        // Chain associativity decides which call wraps which; pin it.
        assertEquals(
                new AppliedFunction("limit", List.of(
                        new AppliedFunction("filter", List.of(
                                new AppliedFunction("all", List.of(
                                        new PackageableElementPtr("Person"))),
                                new Variable("p"))),
                        new CInteger(10L))),
                SpecParser.parse("Person.all()->filter($p)->limit(10)"));
    }

    @Test
    void arrowCallToQualifiedFunctionName() {
        // Receiver-arrow with an FQN'd function: $x->my::pkg::fn().
        // FQN preserved verbatim on AppliedFunction.function.
        assertEquals(
                new AppliedFunction("my::pkg::fn", List.of(new Variable("x"))),
                SpecParser.parse("$x->my::pkg::fn()"));
    }

    // ----- C.2 error surfaces -----------------------------------------

    @Test
    void dotWithoutPropertyNameRejected() {
        ParseException ex = assertThrows(ParseException.class,
                () -> SpecParser.parse("$x."));
        assertTrue(ex.getMessage().contains("expected property name"),
                () -> "want missing-property-name error, got: " + ex.getMessage());
    }

    @Test
    void arrowWithoutFunctionNameRejected() {
        // Disjunction would let weaker messages slip through; my parser
        // emits BOTH substrings, so pin the exact (more specific) phrase.
        ParseException ex = assertThrows(ParseException.class,
                () -> SpecParser.parse("$x->"));
        assertTrue(ex.getMessage().contains("qualified-name start"),
                () -> "want missing-identifier error, got: " + ex.getMessage());
    }

    @Test
    void arrowWithoutParensRejected() {
        // '->' must be followed by 'fn(...)'. Bare '->foo' with no
        // call-parens is malformed.
        ParseException ex = assertThrows(ParseException.class,
                () -> SpecParser.parse("$x->foo"));
        assertTrue(ex.getMessage().contains("expected '(' after arrow-call"),
                () -> "want missing-paren error, got: " + ex.getMessage());
    }

    @Test
    void unclosedArgListRejected() {
        // Pin the production-specific phrasing ("argument list") so this
        // test cannot also pass for the parenthesised-expression error
        // — the two productions emit different messages by design and
        // we want refactor-time crosswire bugs to fail here.
        ParseException ex = assertThrows(ParseException.class,
                () -> SpecParser.parse("fn(1, 2"));
        assertTrue(ex.getMessage().contains("close argument list"),
                () -> "want missing-close error, got: " + ex.getMessage());
    }

    @Test
    void trailingCommaInArgListRejected() {
        ParseException ex = assertThrows(ParseException.class,
                () -> SpecParser.parse("fn(1, 2,)"));
        assertTrue(ex.getMessage().contains("trailing comma"),
                () -> "want trailing-comma error, got: " + ex.getMessage());
    }

    @Test
    void unclosedParenthesisedExprRejected() {
        // Sister to unclosedArgListRejected: pin the OTHER specific
        // phrasing ("parenthesised expression") so these two negative
        // tests cover disjoint code paths.
        ParseException ex = assertThrows(ParseException.class,
                () -> SpecParser.parse("(42"));
        assertTrue(ex.getMessage().contains("close parenthesised expression"),
                () -> "want missing-close error, got: " + ex.getMessage());
    }

    // ----- mixed postfix composition (C.2) ----------------------------

    @Test
    void dotThenArrowComposes() {
        // $x.foo->fn() exercises the postfix loop's ability to bridge
        // from a property access into an arrow call. Pin the resulting
        // AST so a refactor that mishandled the receiver hand-off would
        // fail loud (e.g. wrapping the AppliedProperty inside the
        // function name instead of as parameter 0).
        assertEquals(
                new AppliedFunction("fn", List.of(
                        new AppliedProperty(new Variable("x"), "foo"))),
                SpecParser.parse("$x.foo->fn()"));
    }

    @Test
    void arrowThenDotComposes() {
        // $x->fn().bar exercises the other direction: a property access
        // applied to the RESULT of an arrow call. The dot postfix must
        // accept any expression as its receiver, not just primaries.
        assertEquals(
                new AppliedProperty(
                        new AppliedFunction("fn", List.of(new Variable("x"))),
                        "bar"),
                SpecParser.parse("$x->fn().bar"));
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
        // Use a token that's still unsupported in C.2 (operators land
        // in C.3). '&&' lexes to TokenType.AND, which has no expression
        // production yet, so the slice MUST error \u2014 and the error must
        // report the ORIGINAL line 2 of the source, not slice offset 0.
        String src = "Class my::X { x: String[1]; }\n" + "&&";
        TokenStream all = Lexer.tokenize(src);
        TokenStream slice = all.slice(all.count() - 1, all.count());
        ParseException ex = assertThrows(ParseException.class,
                () -> SpecParser.parse(slice));
        assertEquals(2, ex.line(),
                () -> "error in slice should report ORIGINAL line 2, got: " + ex.getMessage());
    }
}
