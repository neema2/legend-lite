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
import com.legend.parser.spec.KeyExpression;
import com.legend.parser.spec.NewInstance;
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

    // ----- binary operators: arithmetic (C.3) -------------------------

    @Test
    void additionDesugarsToAppliedFunctionPlus() {
        // Binary '+' desugars to AppliedFunction("plus", [lhs, rhs]).
        // Pin both the function name and the parameter shape; engine
        // uses the same name and order.
        assertEquals(
                new AppliedFunction("plus", List.of(
                        new CInteger(1L), new CInteger(2L))),
                SpecParser.parse("1 + 2"));
    }

    @Test
    void subtractionMultiplicationDivisionUseCanonicalNames() {
        // The operator-to-function mapping must match engine for
        // corpus-byte-comparability: minus/times/divide (NOT
        // sub/mul/div). One assertion per op so a typo on any single
        // mapping fails its own test, not all of them.
        assertEquals(
                new AppliedFunction("minus", List.of(
                        new CInteger(5L), new CInteger(3L))),
                SpecParser.parse("5 - 3"));
        assertEquals(
                new AppliedFunction("times", List.of(
                        new CInteger(4L), new CInteger(2L))),
                SpecParser.parse("4 * 2"));
        assertEquals(
                new AppliedFunction("divide", List.of(
                        new CInteger(10L), new CInteger(2L))),
                SpecParser.parse("10 / 2"));
    }

    @Test
    void sameOperatorChainsFlattenLeftAssociatively() {
        // '1 + 2 + 3' parses as plus(plus(1, 2), 3) \u2014 left-associative
        // same-op chaining inside a single parseArithmeticPart call.
        // The shape would be plus(1, plus(2, 3)) if associativity were
        // wrong; pin the nesting to catch that regression.
        assertEquals(
                new AppliedFunction("plus", List.of(
                        new AppliedFunction("plus", List.of(
                                new CInteger(1L), new CInteger(2L))),
                        new CInteger(3L))),
                SpecParser.parse("1 + 2 + 3"));
    }

    @Test
    void mixedArithmeticOpsAreFlatLeftAssociative() {
        // PURE'S GRAMMAR INVARIANT: '1 + 2 * 3' is (1+2)*3, not 1+(2*3).
        // There is NO precedence between + and *; all arithmetic ops
        // share one flat level. This is the most counter-intuitive
        // grammar choice in C.3 \u2014 if a future refactor "fixed" it to
        // standard precedence, this test fails immediately, forcing a
        // conscious decision.
        assertEquals(
                new AppliedFunction("times", List.of(
                        new AppliedFunction("plus", List.of(
                                new CInteger(1L), new CInteger(2L))),
                        new CInteger(3L))),
                SpecParser.parse("1 + 2 * 3"));
    }

    @Test
    void parensOverridePrecedence() {
        // The user's only escape hatch from flat-left-to-right
        // arithmetic is parentheses. Without parens, '1 + 2 * 3' is
        // (1+2)*3; WITH '(2 * 3)' parens, it's 1 + (2*3) = plus(1,
        // times(2,3)). Pin the override pathway.
        assertEquals(
                new AppliedFunction("plus", List.of(
                        new CInteger(1L),
                        new AppliedFunction("times", List.of(
                                new CInteger(2L), new CInteger(3L))))),
                SpecParser.parse("1 + (2 * 3)"));
    }

    // ----- binary operators: comparison & equality (C.3) --------------

    @Test
    void comparisonOpsUseCanonicalNames() {
        // <, <=, >, >= \u2192 lessThan, lessThanEqual, greaterThan,
        // greaterThanEqual. Engine names; pin one of each so the
        // switch's arm wiring is verified.
        assertEquals(
                new AppliedFunction("lessThan", List.of(
                        new CInteger(1L), new CInteger(2L))),
                SpecParser.parse("1 < 2"));
        assertEquals(
                new AppliedFunction("lessThanEqual", List.of(
                        new CInteger(1L), new CInteger(2L))),
                SpecParser.parse("1 <= 2"));
        assertEquals(
                new AppliedFunction("greaterThan", List.of(
                        new CInteger(3L), new CInteger(2L))),
                SpecParser.parse("3 > 2"));
        assertEquals(
                new AppliedFunction("greaterThanEqual", List.of(
                        new CInteger(3L), new CInteger(2L))),
                SpecParser.parse("3 >= 2"));
    }

    @Test
    void equalityOpsUseCanonicalNames() {
        // == and != desugar to equal/notEqual. These are handled at the
        // parseExpression level (between postfix and arithmetic) in
        // Pure's grammar, so they're tested as their own case.
        assertEquals(
                new AppliedFunction("equal", List.of(
                        new Variable("x"), new CInteger(1L))),
                SpecParser.parse("$x == 1"));
        assertEquals(
                new AppliedFunction("notEqual", List.of(
                        new Variable("x"), new CInteger(1L))),
                SpecParser.parse("$x != 1"));
    }

    @Test
    void alternateNotEqualSpellingDesugarsIdentically() {
        // Pure spells inequality two ways: '!=' and '<>'. Both must
        // produce the same AppliedFunction("notEqual", ...) so the
        // model layer never has to care which form was written.
        // A regression that handled only one form would fail here.
        ValueSpecification bang = SpecParser.parse("$x != 1");
        ValueSpecification angle = SpecParser.parse("$x <> 1");
        assertEquals(bang, angle,
                "'$x != 1' and '$x <> 1' must produce equal AST");
        assertEquals(
                new AppliedFunction("notEqual", List.of(
                        new Variable("x"), new CInteger(1L))),
                angle);
    }

    @Test
    void equalityRightSideAcceptsArithmeticChain() {
        // The right side of '==' is parseCombinedArithmeticOnly, so
        // 'a == b + c' parses as equal(a, plus(b, c)), NOT as
        // plus(equal(a, b), c). The asymmetry is intentional (Pure
        // grammar) \u2014 left side is bare, right side allows arith.
        assertEquals(
                new AppliedFunction("equal", List.of(
                        new Variable("a"),
                        new AppliedFunction("plus", List.of(
                                new Variable("b"), new Variable("c"))))),
                SpecParser.parse("$a == $b + $c"));
    }

    @Test
    void equalityBindsTighterThanArithmetic() {
        // PURE-GRAMMAR SURPRISE: '$a + $b == $c' parses as
        // plus($a, equal($b, $c)), NOT (plus($a, $b)) == $c.
        // In Pure (and in engine), '==' lives at parseExpression
        // level, which is BELOW the combined-expression arithmetic
        // loop. When parseArithmeticPart calls parseExpression for
        // the right operand of '+', that inner parseExpression
        // greedily consumes the trailing '== $c'. Net effect: '=='
        // binds TIGHTER than '+', opposite of C/Java/Python intuition.
        //
        // Users who want the C-like reading must write
        // '($a + $b) == $c' explicitly. Pin the actual behaviour so
        // a future reader cannot "fix" it without consciously breaking
        // engine parity.
        assertEquals(
                new AppliedFunction("plus", List.of(
                        new Variable("a"),
                        new AppliedFunction("equal", List.of(
                                new Variable("b"), new Variable("c"))))),
                SpecParser.parse("$a + $b == $c"));
    }

    // ----- binary operators: boolean (C.3) ----------------------------

    @Test
    void booleanOpsUseCanonicalNames() {
        // && and || desugar to 'and' and 'or' \u2014 engine names.
        assertEquals(
                new AppliedFunction("and", List.of(
                        new CBoolean(true), new CBoolean(false))),
                SpecParser.parse("true && false"));
        assertEquals(
                new AppliedFunction("or", List.of(
                        new CBoolean(true), new CBoolean(false))),
                SpecParser.parse("true || false"));
    }

    @Test
    void booleanOpChainsLeftAssociatively() {
        // 'a && b && c' chains via the OUTER parseCombinedExpression
        // loop (boolean ops are at a different level than same-op
        // arith chains, which loop inside parseArithmeticPart). Pin
        // left-associative nesting so a regression that broke either
        // loop's left-fold fails here.
        assertEquals(
                new AppliedFunction("and", List.of(
                        new AppliedFunction("and", List.of(
                                new Variable("a"), new Variable("b"))),
                        new Variable("c"))),
                SpecParser.parse("$a && $b && $c"));
    }

    @Test
    void booleanOpsComposeWithComparisons() {
        // Realistic Pure: '$x > 10 && $x < 20'. The arithmetic level
        // resolves both comparisons first, then && joins them.
        assertEquals(
                new AppliedFunction("and", List.of(
                        new AppliedFunction("greaterThan", List.of(
                                new Variable("x"), new CInteger(10L))),
                        new AppliedFunction("lessThan", List.of(
                                new Variable("x"), new CInteger(20L))))),
                SpecParser.parse("$x > 10 && $x < 20"));
    }

    // ----- unary operators (C.3) --------------------------------------

    @Test
    void unaryNotDesugarsToAppliedFunctionNot() {
        assertEquals(
                new AppliedFunction("not", List.of(new Variable("x"))),
                SpecParser.parse("!$x"));
    }

    @Test
    void unaryMinusBindsEntirePostfixChain() {
        // CRITICAL: '-$x.foo' must be minus($x.foo), NOT minus($x).foo.
        // Tests that unary's operand is parseExpression (postfix-aware)
        // rather than parsePrimary alone. A buggy parser that bound
        // unary too tightly would produce the wrong shape.
        assertEquals(
                new AppliedFunction("minus", List.of(
                        new AppliedProperty(new Variable("x"), "foo"))),
                SpecParser.parse("-$x.foo"));
    }

    @Test
    void unaryPlusIsRetainedAsAppliedFunction() {
        // Engine emits AppliedFunction("plus", [inner]) for unary '+';
        // we match (it's a useless operator in source, but preserving
        // it keeps round-trip parity with the engine corpus).
        assertEquals(
                new AppliedFunction("plus", List.of(new CInteger(42L))),
                SpecParser.parse("+42"));
    }

    // ----- new-instance (C.3) -----------------------------------------

    @Test
    void newInstanceEmptyBindings() {
        // ^Foo() is legal: zero property bindings, default-constructed
        // class. Type-arguments list is empty.
        assertEquals(
                new NewInstance("Foo", List.of(), List.of()),
                SpecParser.parse("^Foo()"));
    }

    @Test
    void newInstanceWithBindings() {
        // Source order preserved by List<KeyExpression> (this is the
        // whole point of diverging from engine's Map-based shape).
        assertEquals(
                new NewInstance("Person", List.of(), List.of(
                        new KeyExpression("name", new CString("Alice")),
                        new KeyExpression("age", new CInteger(30L)))),
                SpecParser.parse("^Person(name='Alice', age=30)"));
    }

    @Test
    void newInstanceWithQualifiedClassName() {
        assertEquals(
                new NewInstance("my::app::Person", List.of(), List.of(
                        new KeyExpression("name", new CString("Bob")))),
                SpecParser.parse("^my::app::Person(name='Bob')"));
    }

    @Test
    void newInstanceWithTypeArguments() {
        // ^Pair<Integer, String>(...) \u2014 typeArguments captured as
        // source-level FQN strings.
        assertEquals(
                new NewInstance("Pair",
                        List.of("Integer", "String"),
                        List.of(
                                new KeyExpression("first", new CInteger(1L)),
                                new KeyExpression("second", new CString("a")))),
                SpecParser.parse("^Pair<Integer, String>(first=1, second='a')"));
    }

    @Test
    void newInstanceBindingValueCanBeAnyExpression() {
        // Bindings parse with parseCombinedExpression, so values can be
        // anything: arithmetic, calls, nested new-instances, etc.
        assertEquals(
                new NewInstance("Box", List.of(), List.of(
                        new KeyExpression("value",
                                new AppliedFunction("plus", List.of(
                                        new CInteger(1L), new CInteger(2L)))))),
                SpecParser.parse("^Box(value=1+2)"));
    }

    @Test
    void newInstanceMissingEqualsRejected() {
        ParseException ex = assertThrows(ParseException.class,
                () -> SpecParser.parse("^Foo(x 5)"));
        assertTrue(ex.getMessage().contains("expected '='"),
                () -> "want missing-equals error, got: " + ex.getMessage());
    }

    @Test
    void newInstanceTrailingCommaRejected() {
        // Pin the production-specific phrase ("binding list") so this
        // test cannot also pass against the collection or argument-list
        // trailing-comma errors, all of which contain "trailing comma".
        ParseException ex = assertThrows(ParseException.class,
                () -> SpecParser.parse("^Foo(x=1,)"));
        assertTrue(ex.getMessage().contains("^NewInstance binding list"),
                () -> "want NewInstance-specific trailing-comma error, got: "
                        + ex.getMessage());
    }

    // ----- realistic combined forms (C.3) -----------------------------

    @Test
    void filterPredicateWithComparisonAsArrowArgument() {
        // This is the canonical Pure query pattern from C.2 + C.3:
        // Person.all()->filter($p.age > 21). The filter arg is a full
        // comparison expression \u2014 verifies operators work inside
        // function arguments via parseCombinedExpression.
        assertEquals(
                new AppliedFunction("filter", List.of(
                        new AppliedFunction("all", List.of(
                                new PackageableElementPtr("Person"))),
                        new AppliedFunction("greaterThan", List.of(
                                new AppliedProperty(new Variable("p"), "age"),
                                new CInteger(21L))))),
                SpecParser.parse("Person.all()->filter($p.age > 21)"));
    }

    @Test
    void operatorsInsideCollectionLiteral() {
        // [1+2, 3*4] \u2014 collection element parsing uses
        // parseCombinedExpression so operators are admitted.
        assertEquals(
                new PureCollection(List.of(
                        new AppliedFunction("plus", List.of(
                                new CInteger(1L), new CInteger(2L))),
                        new AppliedFunction("times", List.of(
                                new CInteger(3L), new CInteger(4L))))),
                SpecParser.parse("[1+2, 3*4]"));
    }

    // ----- coverage gap markers (C.3+ features) -----------------------

    @Test
    void negativeIntegerLiteralIsUnaryMinusOnLiteral() {
        // Phase-flip from C.1/C.2: -42 was previously a parser error
        // (unary ops deferred to C.3). With C.3 it parses as unary
        // minus applied to the literal 42 \u2014 the standard Pure
        // desugaring. Pinned here so a future regression that
        // accidentally folded the sign into the literal (producing
        // CInteger(-42L) directly) would fail loud.
        assertEquals(
                new AppliedFunction("minus", List.of(new CInteger(42L))),
                SpecParser.parse("-42"));
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
