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
import com.legend.parser.spec.ColSpec;
import com.legend.parser.spec.ColSpecArray;
import com.legend.parser.spec.KeyExpression;
import com.legend.parser.spec.LambdaFunction;
import com.legend.parser.spec.Multiplicity;
import com.legend.parser.spec.NewInstance;
import com.legend.parser.spec.PackageableElementPtr;
import com.legend.parser.spec.PureCollection;
import com.legend.parser.spec.ValueSpecification;
import com.legend.parser.spec.Variable;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;

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

    // The C.3 shape was bare NewInstance; the C.4-followup analysis
    // (engine-lite parity + 'everything is a function' uniformity)
    // moved this to AppliedFunction("new", [PE, NewInstance]) and
    // switched bindings from List<KeyExpression> to
    // Map<String, KeyExpression> with the isAdd flag preserved.

    @Test
    void newInstanceDesugarsToNewFunctionCall() {
        // ^Foo() is legal: zero property bindings, default-constructed
        // class. The outer AppliedFunction("new", ...) is the desugar
        // wrapper; the inner NewInstance carries the structured
        // payload. Engine-lite's parseExpressionInstance produces the
        // identical shape \u2014 verified by inspection.
        assertEquals(
                new AppliedFunction("new", List.of(
                        new PackageableElementPtr("Foo"),
                        new NewInstance("Foo", List.of(), Map.of()))),
                SpecParser.parse("^Foo()"));
    }

    @Test
    void newInstanceWithBindings() {
        // Properties as Map<String, KeyExpression>. Each binding is
        // wrapped in a KeyExpression carrying the value plus an
        // isAdd=false flag (this is the '=' form, not '+=').
        assertEquals(
                new AppliedFunction("new", List.of(
                        new PackageableElementPtr("Person"),
                        new NewInstance("Person", List.of(), Map.of(
                                "name", new KeyExpression(new CString("Alice")),
                                "age", new KeyExpression(new CInteger(30L)))))),
                SpecParser.parse("^Person(name='Alice', age=30)"));
    }

    @Test
    void newInstanceWithQualifiedClassName() {
        // Both occurrences of the className \u2014 in the PE wrapper and
        // in NewInstance.className \u2014 carry the same source-level
        // FQN. Engine-lite duplicates them likewise; the PE is the
        // type-system entry point, the NewInstance.className is the
        // class to instantiate.
        assertEquals(
                new AppliedFunction("new", List.of(
                        new PackageableElementPtr("my::app::Person"),
                        new NewInstance("my::app::Person", List.of(), Map.of(
                                "name", new KeyExpression(new CString("Bob")))))),
                SpecParser.parse("^my::app::Person(name='Bob')"));
    }

    @Test
    void newInstanceWithTypeArguments() {
        // ^Pair<Integer, String>(...) \u2014 type arguments captured as
        // source-level FQN strings, stored on the inner NewInstance.
        assertEquals(
                new AppliedFunction("new", List.of(
                        new PackageableElementPtr("Pair"),
                        new NewInstance("Pair",
                                List.of("Integer", "String"),
                                Map.of(
                                        "first", new KeyExpression(new CInteger(1L)),
                                        "second", new KeyExpression(new CString("a")))))),
                SpecParser.parse("^Pair<Integer, String>(first=1, second='a')"));
    }

    @Test
    void newInstanceBindingValueCanBeAnyExpression() {
        // Bindings parse with parseCombinedExpression, so values can be
        // anything: arithmetic, calls, nested new-instances, etc.
        assertEquals(
                new AppliedFunction("new", List.of(
                        new PackageableElementPtr("Box"),
                        new NewInstance("Box", List.of(), Map.of(
                                "value", new KeyExpression(
                                        new AppliedFunction("plus", List.of(
                                                new CInteger(1L), new CInteger(2L))))))
                )),
                SpecParser.parse("^Box(value=1+2)"));
    }

    @Test
    void newInstancePlusEqualsPreservesIsAddFlag() {
        // 'prop += val' is the append-to-collection form, semantically
        // distinct from 'prop = val' (assign). Engine-pure carries
        // this distinction via KeyExpression._add(boolean); engine-lite
        // silently discards the leading '+' (a latent bug in lite that
        // we deliberately do NOT inherit). The parser preserves the
        // distinction so the typechecker / downstream codegen can
        // emit the correct semantics.
        //
        // Mixed-form binding pinned by record equality: 'name=' has
        // isAdd=false, 'tags+=' has isAdd=true.
        assertEquals(
                new AppliedFunction("new", List.of(
                        new PackageableElementPtr("Person"),
                        new NewInstance("Person", List.of(), Map.of(
                                "name", new KeyExpression(
                                        new CString("Alice"), false),
                                "tags", new KeyExpression(
                                        new CString("admin"), true))))),
                SpecParser.parse("^Person(name='Alice', tags+='admin')"));
    }

    @Test
    void newInstanceDuplicateKeySilentlyLastWins() {
        // Pin engine-cross-consistent behaviour: ^Foo(x=1, x=2)
        // produces ONE binding {x -> KeyExpression(2, false)}; the
        // first is silently dropped via Map.put. Engine-lite does the
        // same (also Map-based); engine-pure keeps both in a list but
        // its validator iterates without tracking duplicates, so the
        // observable behaviour is also last-wins. Documented here so
        // a future change to a stricter parse-time validator would
        // flip this test loudly.
        assertEquals(
                new AppliedFunction("new", List.of(
                        new PackageableElementPtr("Foo"),
                        new NewInstance("Foo", List.of(), Map.of(
                                "x", new KeyExpression(new CInteger(2L)))))),
                SpecParser.parse("^Foo(x=1, x=2)"));
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

    // ----- let-binding (C.4) ------------------------------------------

    @Test
    void letBindingDesugarsToLetFunctionCall() {
        // 'let x = 5' parses as AppliedFunction("letFunction",
        // [CString("x"), CInteger(5)]) \u2014 matching engine's
        // parse-time representation. The 'everything is a function'
        // pillar: 'let' is sugar for calling the stdlib's
        // letFunction_String_1__T_m__T_m_, just like '+' is sugar
        // for 'plus'. See parseLetExpression Javadoc for the full
        // rationale (and the C.4 commit message for the analysis
        // that landed on this shape over a dedicated record).
        assertEquals(
                new AppliedFunction("letFunction", List.of(
                        new CString("x"), new CInteger(5L))),
                SpecParser.parse("let x = 5"));
    }

    @Test
    void letBindingValueIsFullCombinedExpression() {
        // The RHS of '=' parses as parseCombinedExpression, so
        // operators, calls, and arrow chains all work. The value
        // becomes the second argument of the letFunction call.
        assertEquals(
                new AppliedFunction("letFunction", List.of(
                        new CString("result"),
                        new AppliedFunction("plus", List.of(
                                new CInteger(1L), new CInteger(2L))))),
                SpecParser.parse("let result = 1 + 2"));
    }

    @Test
    void letVarNameIsCarriedInCString() {
        // The variable name lives in a CString \u2014 NOT a Variable
        // record. This matters: a Variable would mean 'reference to
        // an existing binding', but here we are INTRODUCING a new
        // binding. The CString is the same shape engine uses
        // (InstanceValue containing a String literal). Downstream
        // scope-registration code reads it via
        //   ((CString) appliedFunction.parameters().get(0)).value()
        // matching engine's letFunction-name guard pattern.
        AppliedFunction parsed = (AppliedFunction) SpecParser.parse("let myVar = 42");
        assertEquals("letFunction", parsed.function());
        assertInstanceOf(CString.class, parsed.parameters().get(0));
        assertEquals("myVar", ((CString) parsed.parameters().get(0)).value());
    }

    @Test
    void letInsideExpressionIsRejected() {
        // 'let' is statement-level only \u2014 parseProgramLine dispatches
        // it, but parseCombinedExpression / parsePrimary do not. So
        // 'let' nested in an arithmetic expression must error.
        //
        // SUBTLETY: LET is in ElementParser.IDENTIFIER_TOKENS (so that
        // '$let' parses as a variable with that name). As a result,
        // mid-expression 'let' is silently absorbed as
        // PackageableElementPtr("let"), and the subsequent
        // identifier (e.g. 'x') is what surfaces as a trailing
        // token. The exact wording matters because a future change
        // that DID add an explicit LET dispatch in parsePrimary would
        // emit a different (clearer) message; flipping this test
        // would be the signal that the change happened.
        ParseException ex = assertThrows(ParseException.class,
                () -> SpecParser.parse("1 + let x = 2"));
        assertTrue(ex.getMessage().toLowerCase().contains("trailing"),
                () -> "want trailing-tokens error (LET absorbed as identifier, "
                        + "subsequent 'x' is trailing), got: " + ex.getMessage());
    }

    @Test
    void letMissingVariableNameRejected() {
        ParseException ex = assertThrows(ParseException.class,
                () -> SpecParser.parse("let = 5"));
        assertTrue(ex.getMessage().contains("variable name after 'let'"),
                () -> "want missing-var-name error, got: " + ex.getMessage());
    }

    @Test
    void letMissingEqualsRejected() {
        ParseException ex = assertThrows(ParseException.class,
                () -> SpecParser.parse("let x 5"));
        assertTrue(ex.getMessage().contains("expected '=' after 'let x'"),
                () -> "want missing-equals error, got: " + ex.getMessage());
    }

    // ----- lambdas: braced form (C.4) ---------------------------------

    @Test
    void bracedSingleParamLambda() {
        // '{p | $p}' \u2014 one parameter, single-statement body.
        // Body is a List with exactly one element.
        assertEquals(
                new LambdaFunction(
                        List.of(new Variable("p")),
                        List.of(new Variable("p"))),
                SpecParser.parse("{p | $p}"));
    }

    @Test
    void bracedMultiParamLambda() {
        // '{p, q | $p + $q}' \u2014 multi-param.
        assertEquals(
                new LambdaFunction(
                        List.of(new Variable("p"), new Variable("q")),
                        List.of(new AppliedFunction("plus", List.of(
                                new Variable("p"), new Variable("q"))))),
                SpecParser.parse("{p, q | $p + $q}"));
    }

    @Test
    void bracedZeroParamLambda() {
        // '{| 42}' \u2014 zero parameters. params list is empty;
        // body is a single statement.
        assertEquals(
                new LambdaFunction(List.of(), List.of(new CInteger(42L))),
                SpecParser.parse("{| 42}"));
    }

    @Test
    void bracedLambdaWithMultiStatementBody() {
        // '{p | let x = $p; $x + 1}' \u2014 body is two statements:
        // a letFunction call (the desugared 'let') followed by an
        // arithmetic expression. The lambda's value would be the
        // LAST statement (1 + $x); the parser preserves the sequence
        // verbatim and leaves evaluation semantics to the compiler.
        assertEquals(
                new LambdaFunction(
                        List.of(new Variable("p")),
                        List.of(
                                new AppliedFunction("letFunction", List.of(
                                        new CString("x"), new Variable("p"))),
                                new AppliedFunction("plus", List.of(
                                        new Variable("x"), new CInteger(1L))))),
                SpecParser.parse("{p | let x = $p; $x + 1}"));
    }

    // ----- lambdas: shorthand forms (C.4) ------------------------------

    @Test
    void singleParamShorthandLambda() {
        // 'p | $p.age > 21' \u2014 no braces, single param.
        // The IDENT+PIPE lookahead in parsePrimary's default arm
        // dispatches to parseSingleParamLambda BEFORE falling
        // through to parseQualifiedNameStart (which would have
        // produced PackageableElementPtr("p")).
        assertEquals(
                new LambdaFunction(
                        List.of(new Variable("p")),
                        List.of(new AppliedFunction("greaterThan", List.of(
                                new AppliedProperty(new Variable("p"), "age"),
                                new CInteger(21L))))),
                SpecParser.parse("p | $p.age > 21"));
    }

    @Test
    void zeroParamPipeLambda() {
        // '| 42' \u2014 PIPE at start, zero params.
        assertEquals(
                new LambdaFunction(List.of(), List.of(new CInteger(42L))),
                SpecParser.parse("| 42"));
    }

    @Test
    void nestedLambdaInLambdaBody() {
        // '{p | {q | $p + $q}}' \u2014 outer lambda's body is a single
        // statement which is itself a lambda. Exercises the recursive
        // case: parseLambdaFunction is called from parseProgramLine
        // (statement in outer body) which is called by
        // parseLambdaFunction (outer body parsing). The inner
        // lambda's body references the outer's parameter, but the
        // parser doesn't resolve scopes \u2014 it just produces the
        // structural shape. Closure semantics belong to the
        // typechecker.
        assertEquals(
                new LambdaFunction(
                        List.of(new Variable("p")),
                        List.of(new LambdaFunction(
                                List.of(new Variable("q")),
                                List.of(new AppliedFunction("plus", List.of(
                                        new Variable("p"),
                                        new Variable("q"))))))),
                SpecParser.parse("{p | {q | $p + $q}}"));
    }

    @Test
    void lambdaAsLetRhsIsCommonRealWorldPattern() {
        // 'let f = {p | $p + 1}' \u2014 binding a lambda to a name is
        // the single most common real-world use of let in Pure. It
        // composes the two main C.4 constructs: the let desugars to
        // a letFunction call whose second argument is the lambda.
        // Pin the full nested shape so a regression in either
        // direction (let-desugaring OR lambda parsing) fails loud.
        assertEquals(
                new AppliedFunction("letFunction", List.of(
                        new CString("f"),
                        new LambdaFunction(
                                List.of(new Variable("p")),
                                List.of(new AppliedFunction("plus", List.of(
                                        new Variable("p"),
                                        new CInteger(1L))))))),
                SpecParser.parse("let f = {p | $p + 1}"));
    }

    @Test
    void lambdaInsideCollectionLiteral() {
        // '[{p | $p}, {q | $q + 1}]' \u2014 lambdas as collection
        // elements. This works because parseCollection (C.1) calls
        // parseCombinedExpression for each element, which descends
        // through parseExpression \u2192 parseUnaryAndPrimary \u2192
        // parsePrimary, where BRACE_OPEN dispatches to
        // parseLambdaFunction. A regression that hardened collection
        // parsing to literals-only would fail this test.
        assertEquals(
                new PureCollection(List.of(
                        new LambdaFunction(
                                List.of(new Variable("p")),
                                List.of(new Variable("p"))),
                        new LambdaFunction(
                                List.of(new Variable("q")),
                                List.of(new AppliedFunction("plus", List.of(
                                        new Variable("q"),
                                        new CInteger(1L))))))),
                SpecParser.parse("[{p | $p}, {q | $q + 1}]"));
    }

    @Test
    void lambdaWithCascadingLetsInBody() {
        // '{p | let x = $p + 1; let y = $x * 2; $y}' \u2014 the
        // canonical "compute intermediate values, return the last"
        // pattern. Body is THREE statements: two cascading lets
        // (each referencing the previous binding), then a final
        // expression whose value is the lambda's result.
        //
        // bracedLambdaWithMultiStatementBody covers ONE let in a
        // body; this covers the realistic multi-let case where each
        // let references the previous one. A regression that
        // confused which statement was the return-value, or that
        // stopped admitting more than one let in a body, would fail
        // here.
        assertEquals(
                new LambdaFunction(
                        List.of(new Variable("p")),
                        List.of(
                                new AppliedFunction("letFunction", List.of(
                                        new CString("x"),
                                        new AppliedFunction("plus", List.of(
                                                new Variable("p"),
                                                new CInteger(1L))))),
                                new AppliedFunction("letFunction", List.of(
                                        new CString("y"),
                                        new AppliedFunction("times", List.of(
                                                new Variable("x"),
                                                new CInteger(2L))))),
                                new Variable("y"))),
                SpecParser.parse(
                        "{p | let x = $p + 1; let y = $x * 2; $y}"));
    }

    @Test
    void codeBlockBuildingMultipleNamedLambdas() {
        // 'let f = {p | $p + 1}; let g = {q | $q * 2}; $f' \u2014
        // a code block that builds two named lambdas (Pure's
        // closest analogue to a module body or local function
        // library) and produces one of them as the result.
        //
        // Exercises three things at once that no single existing
        // test combines: parseCodeBlock with multiple statements,
        // let-binding a lambda value, and the parse-time
        // independence of each let (each desugars to its own
        // letFunction call without any cross-statement state).
        List<ValueSpecification> stmts = SpecParser.parseCodeBlock(
                "let f = {p | $p + 1}; let g = {q | $q * 2}; $f");
        assertEquals(
                List.of(
                        new AppliedFunction("letFunction", List.of(
                                new CString("f"),
                                new LambdaFunction(
                                        List.of(new Variable("p")),
                                        List.of(new AppliedFunction("plus", List.of(
                                                new Variable("p"),
                                                new CInteger(1L))))))),
                        new AppliedFunction("letFunction", List.of(
                                new CString("g"),
                                new LambdaFunction(
                                        List.of(new Variable("q")),
                                        List.of(new AppliedFunction("times", List.of(
                                                new Variable("q"),
                                                new CInteger(2L))))))),
                        new Variable("f")),
                stmts);
    }

    @Test
    void pipeFormLambdaAsArrowArgument() {
        // '$opt->orElse(| 42)' \u2014 zero-param pipe lambda inside an
        // arrow-call argument list. Exercises a DIFFERENT path from
        // bracedZeroParamLambda: here the lambda is parsed from
        // inside parseArgList (via parseCombinedExpression), and
        // parsePrimary's PIPE case dispatches to parseLambdaPipe.
        // The argument list must terminate the body at PAREN_CLOSE,
        // which works because parseCombinedExpression breaks on any
        // non-operator token (including ')').
        assertEquals(
                new AppliedFunction("orElse", List.of(
                        new Variable("opt"),
                        new LambdaFunction(
                                List.of(),
                                List.of(new CInteger(42L))))),
                SpecParser.parse("$opt->orElse(| 42)"));
    }

    @Test
    void lambdaAsArrowArgumentIsRealisticPureCode() {
        // The canonical Pure query pattern:
        // Person.all()->filter(p | $p.age > 21)
        // This combines C.2 (arrow + property access), C.3
        // (comparison operator), and C.4 (shorthand lambda inside
        // an arrow argument). Pin the full nesting.
        assertEquals(
                new AppliedFunction("filter", List.of(
                        new AppliedFunction("all", List.of(
                                new PackageableElementPtr("Person"))),
                        new LambdaFunction(
                                List.of(new Variable("p")),
                                List.of(new AppliedFunction("greaterThan", List.of(
                                        new AppliedProperty(new Variable("p"), "age"),
                                        new CInteger(21L))))))),
                SpecParser.parse("Person.all()->filter(p | $p.age > 21)"));
    }

    // ----- lambda error cases (C.4) ------------------------------------

    // ----- typed lambda parameters (C.5) ------------------------------

    @Test
    void bracedTypedLambdaParam() {
        // C.5 phase-flip: a test pinned in C.4 expected this input
        // to fail with a 'typed-params-deferred-to-C.5' error. C.5
        // delivers the feature, so the test now pins the produced
        // AST shape: Variable carries declared typeName and a
        // structured Multiplicity.Concrete.PURE_ONE.
        assertEquals(
                new LambdaFunction(
                        List.of(new Variable(
                                "p", "Integer", Multiplicity.Concrete.PURE_ONE)),
                        List.of(new Variable("p"))),
                SpecParser.parse("{p: Integer[1] | $p}"));
    }

    @Test
    void bracedTypedMultiParamLambda() {
        // Multi-param typed: each parameter independently carries
        // its declared type and multiplicity. Mixing forms (PURE_ONE
        // and ZERO_MANY) pins that each Variable is built
        // independently rather than sharing a single template.
        assertEquals(
                new LambdaFunction(
                        List.of(
                                new Variable("p", "Integer",
                                        Multiplicity.Concrete.PURE_ONE),
                                new Variable("q", "String",
                                        Multiplicity.Concrete.ZERO_MANY)),
                        List.of(new AppliedFunction("plus", List.of(
                                new Variable("p"), new Variable("q"))))),
                SpecParser.parse("{p: Integer[1], q: String[*] | $p + $q}"));
    }

    @Test
    void typedShorthandSingleParamLambda() {
        // 'p: Integer[1] | body' \u2014 typed shorthand outside
        // braces. The default-arm dispatch in parsePrimary uses
        // looksLikeTypedLambdaParam() to commit to this form only
        // when IDENT+COLON is followed by the full type+mult+PIPE
        // sequence; otherwise it falls through to other parses.
        assertEquals(
                new LambdaFunction(
                        List.of(new Variable(
                                "p", "Integer", Multiplicity.Concrete.PURE_ONE)),
                        List.of(new AppliedFunction("plus", List.of(
                                new Variable("p"), new CInteger(1L))))),
                SpecParser.parse("p: Integer[1] | $p + 1"));
    }

    @Test
    void typedLambdaWithQualifiedTypeName() {
        // Type can be a qualified name. Stored verbatim in
        // Variable.typeName for downstream resolution.
        assertEquals(
                new LambdaFunction(
                        List.of(new Variable(
                                "p", "my::pkg::Person",
                                Multiplicity.Concrete.PURE_ONE)),
                        List.of(new AppliedProperty(
                                new Variable("p"), "age"))),
                SpecParser.parse("{p: my::pkg::Person[1] | $p.age}"));
    }

    @Test
    void typedLambdaWithTypeArguments() {
        // Generic type \u2014 the type-argument list is collected
        // verbatim into the typeName string by token concatenation.
        // Note: inter-token whitespace is NOT preserved (we splice
        // tokens, not source ranges), so source 'Pair<Integer, String>'
        // becomes 'Pair<Integer,String>' in the typeName. Downstream
        // consumers re-parse if they need to inspect the args; the
        // parser doesn't try to model nested generics structurally
        // at this layer. The lossy whitespace is acceptable because
        // type names are semantic identifiers, not source-faithful
        // text.
        assertEquals(
                new LambdaFunction(
                        List.of(new Variable(
                                "p", "Pair<Integer,String>",
                                Multiplicity.Concrete.PURE_ONE)),
                        List.of(new AppliedProperty(
                                new Variable("p"), "first"))),
                SpecParser.parse("{p: Pair<Integer, String>[1] | $p.first}"));
    }

    @Test
    void multiplicityFormsProduceCorrectConcreteValues() {
        // Pin each surface form's mapping to a Concrete record. The
        // four well-known constants (PURE_ONE, ZERO_ONE, PURE_MANY,
        // ZERO_MANY) plus the explicit bounded form.
        assertEquals(
                Multiplicity.Concrete.PURE_ONE,
                ((Variable) ((LambdaFunction) SpecParser.parse(
                        "{p: T[1] | $p}")).parameters().get(0)).multiplicity());
        assertEquals(
                Multiplicity.Concrete.ZERO_ONE,
                ((Variable) ((LambdaFunction) SpecParser.parse(
                        "{p: T[0..1] | $p}")).parameters().get(0)).multiplicity());
        assertEquals(
                Multiplicity.Concrete.PURE_MANY,
                ((Variable) ((LambdaFunction) SpecParser.parse(
                        "{p: T[1..*] | $p}")).parameters().get(0)).multiplicity());
        assertEquals(
                Multiplicity.Concrete.ZERO_MANY,
                ((Variable) ((LambdaFunction) SpecParser.parse(
                        "{p: T[*] | $p}")).parameters().get(0)).multiplicity());
        // Explicit bounded form not covered by a constant.
        assertEquals(
                new Multiplicity.Concrete(3, 7),
                ((Variable) ((LambdaFunction) SpecParser.parse(
                        "{p: T[3..7] | $p}")).parameters().get(0)).multiplicity());
    }

    @Test
    void multiplicityParameterFormFromStdlib() {
        // Multiplicity variables ('[m]') appear in stdlib native
        // function signatures: letFunction<T|m>, if<T|m>,
        // cast<T|m>, match<T|m,n>, reverse<T|m>, sort<T|m>, map<T|m>.
        // Parsing them must produce Multiplicity.Parameter, not
        // an error or a Concrete fallback.
        //
        // We don't typically encounter this form INSIDE a lambda
        // body in user code, but the grammar is shared with the
        // function-signature path, and the parser must admit the
        // variable form everywhere multiplicity is parsed.
        assertEquals(
                new LambdaFunction(
                        List.of(new Variable(
                                "p", "T", new Multiplicity.Parameter("m"))),
                        List.of(new Variable("p"))),
                SpecParser.parse("{p: T[m] | $p}"));
    }

    @Test
    void typedLambdaInArrowArgumentPosition() {
        // The realistic case: typed predicate as a filter argument.
        // Combines C.2 (arrow + filter call), C.5 (typed shorthand
        // lambda inside the arg list).
        assertEquals(
                new AppliedFunction("filter", List.of(
                        new AppliedFunction("all", List.of(
                                new PackageableElementPtr("Person"))),
                        new LambdaFunction(
                                List.of(new Variable(
                                        "p", "Person",
                                        Multiplicity.Concrete.PURE_ONE)),
                                List.of(new AppliedFunction("greaterThan", List.of(
                                        new AppliedProperty(new Variable("p"), "age"),
                                        new CInteger(21L))))))),
                SpecParser.parse(
                        "Person.all()->filter(p: Person[1] | $p.age > 21)"));
    }

    @Test
    void typedLambdaParamMissingMultiplicityRejected() {
        // 'p: Integer | body' \u2014 type without multiplicity is
        // not legal Pure. Engine grammar requires '[mult]' after
        // the type. Pin the error so a future grammar change can't
        // silently accept missing multiplicity (which would lose
        // type information that the type-checker needs).
        ParseException ex = assertThrows(ParseException.class,
                () -> SpecParser.parse("{p: Integer | $p}"));
        assertTrue(ex.getMessage().contains("'['")
                        && ex.getMessage().contains("multiplicity"),
                () -> "want missing-multiplicity error, got: " + ex.getMessage());
    }

    @Test
    void multiplicityWithBadSyntaxRejected() {
        // 'p: T[1..]' \u2014 missing upper bound after '..'.
        ParseException ex = assertThrows(ParseException.class,
                () -> SpecParser.parse("{p: T[1..] | $p}"));
        assertTrue(ex.getMessage().contains("upper bound"),
                () -> "want missing-upper-bound error, got: " + ex.getMessage());
    }

    @Test
    void mixedTypedAndUntypedLambdaParams() {
        // Pure code occasionally annotates only the params whose type
        // can't be inferred from context. The parser must accept this
        // mixed form because parseLambdaParam is called independently
        // per parameter \u2014 there's no shared state forcing all
        // params into the same form. Pin the AST so a future
        // refactor (e.g. batch-parsing of params) doesn't accidentally
        // require a homogeneous typed-ness.
        assertEquals(
                new LambdaFunction(
                        List.of(
                                new Variable("p"),
                                new Variable("q", "String",
                                        Multiplicity.Concrete.PURE_ONE)),
                        List.of(new AppliedFunction("plus", List.of(
                                new Variable("p"), new Variable("q"))))),
                SpecParser.parse("{p, q: String[1] | $p + $q}"));
    }

    @Test
    void unterminatedTypeArgumentListRejected() {
        // '{p: Pair<Integer | $p}' \u2014 missing closing '>' on the
        // type-argument list. The depth-tracker in parseTypeText
        // never reaches depth==0 and consumes to EOF, then throws.
        // Without this test a future regression that returned
        // 'Pair<Integer' as the typeName (a silent truncation) would
        // pass; the explicit error path is the right behaviour.
        ParseException ex = assertThrows(ParseException.class,
                () -> SpecParser.parse("{p: Pair<Integer | $p}"));
        assertTrue(ex.getMessage().contains("unterminated")
                        && ex.getMessage().contains("type-argument"),
                () -> "want unterminated-type-args error, got: " + ex.getMessage());
    }

    @Test
    void emptyMultiplicityRejected() {
        // '[]' is not a legal multiplicity \u2014 falls through all
        // three branches of parseMultiplicityBody (STAR, INTEGER,
        // identifier) and lands on the unexpected-token error. Pin
        // so a future change that accidentally treats empty brackets
        // as ZERO_MANY or PURE_ONE would fail this test.
        ParseException ex = assertThrows(ParseException.class,
                () -> SpecParser.parse("{p: T[] | $p}"));
        assertTrue(ex.getMessage().contains("unexpected token in multiplicity"),
                () -> "want empty-multiplicity error, got: " + ex.getMessage());
    }

    @Test
    void multiplicityUpperLessThanLowerRejected() {
        // '[5..3]' \u2014 inverted bounds. The Multiplicity.Concrete
        // constructor enforces upper >= lower as a defensive invariant
        // (for programmatic constructors), but the parser pre-checks
        // and throws a ParseException with source line/col rather
        // than letting an IllegalArgumentException leak out. Pin both
        // the error class (ParseException, not Runtime/IAE) and the
        // diagnostic content so the user gets a useful error.
        ParseException ex = assertThrows(ParseException.class,
                () -> SpecParser.parse("{p: T[5..3] | $p}"));
        assertTrue(ex.getMessage().contains("upper bound")
                        && ex.getMessage().contains(">= lower bound"),
                () -> "want bound-order error, got: " + ex.getMessage());
    }

    @Test
    void lambdaMissingPipeRejected() {
        ParseException ex = assertThrows(ParseException.class,
                () -> SpecParser.parse("{p $p}"));
        assertTrue(ex.getMessage().contains("expected '|'"),
                () -> "want missing-pipe error, got: " + ex.getMessage());
    }

    @Test
    void lambdaMissingCloseBraceRejected() {
        ParseException ex = assertThrows(ParseException.class,
                () -> SpecParser.parse("{p | $p"));
        assertTrue(ex.getMessage().contains("expected '}'"),
                () -> "want missing-close-brace error, got: " + ex.getMessage());
    }

    @Test
    void lambdaEmptyBodyRejected() {
        // '{| }' is not legal \u2014 lambda body must have at least
        // one statement.
        ParseException ex = assertThrows(ParseException.class,
                () -> SpecParser.parse("{| }"));
        assertTrue(ex.getMessage().contains("at least one statement"),
                () -> "want empty-body error, got: " + ex.getMessage());
    }

    // ----- code block entry point (C.4) --------------------------------

    @Test
    void parseCodeBlockSingleStatement() {
        // A code block with one statement is a list of one element;
        // not collapsed to the bare value. Consumers always get a
        // List<ValueSpecification>.
        List<ValueSpecification> stmts = SpecParser.parseCodeBlock("42");
        assertEquals(List.of(new CInteger(42L)), stmts);
    }

    @Test
    void parseCodeBlockMultipleStatements() {
        // Statements separated by ';'. Each statement is a full
        // program line (so 'let' is admitted on each, each desugaring
        // independently to its own letFunction call).
        List<ValueSpecification> stmts = SpecParser.parseCodeBlock(
                "let x = 1; let y = 2; $x + $y");
        assertEquals(
                List.of(
                        new AppliedFunction("letFunction", List.of(
                                new CString("x"), new CInteger(1L))),
                        new AppliedFunction("letFunction", List.of(
                                new CString("y"), new CInteger(2L))),
                        new AppliedFunction("plus", List.of(
                                new Variable("x"), new Variable("y")))),
                stmts);
    }

    @Test
    void parseCodeBlockTrailingSemicolonAllowed() {
        // Engine permits a trailing ';' after the last statement
        // ('1; 2;' is equivalent to '1; 2'). We match.
        List<ValueSpecification> stmts = SpecParser.parseCodeBlock("1; 2;");
        assertEquals(
                List.of(new CInteger(1L), new CInteger(2L)),
                stmts);
    }

    @Test
    void parseCodeBlockEmptyInputIsEmptyList() {
        // No statements at all is a degenerate but well-defined
        // case: empty list, no error.
        assertEquals(List.of(), SpecParser.parseCodeBlock(""));
    }

    // ----- coverage gap markers (C.4+ features) -----------------------

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

    // ----- column builders (C.6): ~col / ~[a, b] / ~name:lambda --------

    @Test
    void bareColumnReference() {
        // '~name' \u2014 the simplest column spec: name only, both
        // lambda slots null. Used in project/select positions where
        // the column passes through unchanged.
        assertEquals(
                new ColSpec("name", null, null),
                SpecParser.parse("~name"));
    }

    @Test
    void columnWithMapLambda() {
        // '~total:x|$x.amount' \u2014 column with a map function.
        // function1 is the shorthand lambda; function2 stays null.
        // Used in extend/rename positions.
        assertEquals(
                new ColSpec("total",
                        new LambdaFunction(
                                List.of(new Variable("x")),
                                List.of(new AppliedProperty(
                                        new Variable("x"), "amount"))),
                        null),
                SpecParser.parse("~total:x|$x.amount"));
    }

    @Test
    void columnWithMapAndAggregateLambdas() {
        // '~total:x|$x.amount:y|$y->sum()' \u2014 the canonical
        // group-by column spec. function1 is the per-row map;
        // function2 is the reduction. Engine grammar pins this
        // exact shape (FuncColSpec vs AggColSpec in engine-pure;
        // collapsed to a single ColSpec with two lambdas here).
        assertEquals(
                new ColSpec("total",
                        new LambdaFunction(
                                List.of(new Variable("x")),
                                List.of(new AppliedProperty(
                                        new Variable("x"), "amount"))),
                        new LambdaFunction(
                                List.of(new Variable("y")),
                                List.of(new AppliedFunction("sum", List.of(
                                        new Variable("y")))))),
                SpecParser.parse("~total:x|$x.amount:y|$y->sum()"));
    }

    @Test
    void colSpecArrayOfBareColumns() {
        // '~[name, age, salary]' \u2014 array of bare references.
        // Engine groups these for uniform project / groupBy calls.
        assertEquals(
                new ColSpecArray(List.of(
                        new ColSpec("name"),
                        new ColSpec("age"),
                        new ColSpec("salary"))),
                SpecParser.parse("~[name, age, salary]"));
    }

    @Test
    void colSpecArrayMixesBareAndMapped() {
        // '~[name, total:x|$x.amount]' \u2014 mixed forms in one
        // array. Pin that each element parses independently
        // (consistent with our parseLambdaParam discipline from C.5).
        assertEquals(
                new ColSpecArray(List.of(
                        new ColSpec("name"),
                        new ColSpec("total",
                                new LambdaFunction(
                                        List.of(new Variable("x")),
                                        List.of(new AppliedProperty(
                                                new Variable("x"), "amount"))),
                                null))),
                SpecParser.parse("~[name, total:x|$x.amount]"));
    }

    @Test
    void emptyColSpecArrayIsLegalStructurally() {
        // '~[]' \u2014 the parser admits an empty array; rejection
        // (if any) is the type-checker's job. Pin so a future
        // 'require >= 1 element' guard in the parser would fail
        // this test loudly.
        assertEquals(
                new ColSpecArray(List.of()),
                SpecParser.parse("~[]"));
    }

    @Test
    void colSpecArrayTrailingCommaRejected() {
        // Trailing comma is rejected, matching parseCollection and
        // parseArgList conventions. The error phrase is
        // ColSpec-specific so this test cannot crosswire to other
        // trailing-comma cases.
        ParseException ex = assertThrows(ParseException.class,
                () -> SpecParser.parse("~[a, b,]"));
        assertTrue(ex.getMessage().contains("ColSpec array"),
                () -> "want ColSpec-array trailing-comma error, got: "
                        + ex.getMessage());
    }

    @Test
    void colSpecArrayUnterminatedRejected() {
        // '~[a, b' \u2014 missing closing ']'. Pin the close-bracket
        // expectation so a future regression couldn't silently
        // consume to EOF.
        ParseException ex = assertThrows(ParseException.class,
                () -> SpecParser.parse("~[a, b"));
        assertTrue(ex.getMessage().contains("']'")
                        && ex.getMessage().contains("ColSpec array"),
                () -> "want ColSpec-array close-bracket error, got: "
                        + ex.getMessage());
    }

    @Test
    void columnWithBracedLambda() {
        // '~total:{x | $x.amount}' \u2014 braced lambda form (rather
        // than shorthand). Both forms must be accepted in
        // post-colon position.
        assertEquals(
                new ColSpec("total",
                        new LambdaFunction(
                                List.of(new Variable("x")),
                                List.of(new AppliedProperty(
                                        new Variable("x"), "amount"))),
                        null),
                SpecParser.parse("~total:{x | $x.amount}"));
    }

    @Test
    void columnWithTypedLambda() {
        // '~total:x: Integer[1] | $x' \u2014 typed shorthand lambda
        // in column-spec position. Confirms the C.5 typed-lambda
        // dispatch is reachable from parseColumnLambda.
        assertEquals(
                new ColSpec("total",
                        new LambdaFunction(
                                List.of(new Variable(
                                        "x", "Integer",
                                        Multiplicity.Concrete.PURE_ONE)),
                                List.of(new Variable("x"))),
                        null),
                SpecParser.parse("~total:x: Integer[1] | $x"));
    }

    @Test
    void columnSpecAsArrowArgument() {
        // The realistic case: passing a ColSpec to a relation-API
        // function. Combines C.2 (arrow + call), C.6 (column spec).
        // Pin the full nesting so a regression in either layer's
        // emission shape would fail loudly.
        assertEquals(
                new AppliedFunction("project", List.of(
                        new AppliedFunction("all", List.of(
                                new PackageableElementPtr("Person"))),
                        new ColSpecArray(List.of(
                                new ColSpec("name"),
                                new ColSpec("age"))))),
                SpecParser.parse("Person.all()->project(~[name, age])"));
    }

    @Test
    void columnSpecMissingNameAfterTildeRejected() {
        // '~' followed by something that isn't an identifier or '['
        // \u2014 e.g. '~123' or '~+'. Pin the error phrase.
        ParseException ex = assertThrows(ParseException.class,
                () -> SpecParser.parse("~123"));
        assertTrue(ex.getMessage().contains("column name after '~'"),
                () -> "want missing-column-name error, got: " + ex.getMessage());
    }

    @Test
    void quotedColumnName() {
        // '~'My Column'' \u2014 quoted column name. Real Pure idiom
        // for CSV/JDBC columns whose names contain whitespace or
        // punctuation. Engine grammar admits both bare and quoted
        // forms pervasively. Quotes are stripped and escapes
        // resolved via the same unescapeString pipeline as quoted
        // property names (.'My Name') and string literals.
        //
        // This case was discovered as a gap during a parser-vs-tests
        // audit \u2014 the original parseOneColSpec only accepted bare
        // identifiers, which would silently reject realistic input.
        // The fix mirrors readPropertyName's STRING-first dispatch.
        assertEquals(
                new ColSpec("My Column", null, null),
                SpecParser.parse("~'My Column'"));
    }

    @Test
    void quotedColumnNameInArrayMixedWithBare() {
        // Bare and quoted names mix freely in an array. Pin the
        // parser handles them per-element, matching the
        // 'parseOneColSpec is independent per call' discipline.
        assertEquals(
                new ColSpecArray(List.of(
                        new ColSpec("name"),
                        new ColSpec("Full Name"),
                        new ColSpec("age"))),
                SpecParser.parse("~[name, 'Full Name', age]"));
    }

    @Test
    void columnLambdaBodyAllowsFullExpression() {
        // '~total:x|$x.amount * 2' \u2014 the post-pipe body parses
        // as a full combinedExpression, so arithmetic, calls, and
        // arrow chains all work inside a column lambda. Pin the
        // composition.
        assertEquals(
                new ColSpec("total",
                        new LambdaFunction(
                                List.of(new Variable("x")),
                                List.of(new AppliedFunction("times", List.of(
                                        new AppliedProperty(new Variable("x"), "amount"),
                                        new CInteger(2L))))),
                        null),
                SpecParser.parse("~total:x|$x.amount * 2"));
    }

    @Test
    void columnMapAndAggregateMixesLambdaForms() {
        // '~total:{x|$x.amount}:y|$y->sum()' \u2014 the map slot
        // uses a braced lambda; the aggregate slot uses shorthand.
        // Pins that parseColumnLambda is called fresh for each slot
        // and that the slot dispatcher doesn't lock to one form per
        // ColSpec. A regression here would still pass the
        // map-and-aggregate test (which uses two shorthand lambdas).
        assertEquals(
                new ColSpec("total",
                        new LambdaFunction(
                                List.of(new Variable("x")),
                                List.of(new AppliedProperty(
                                        new Variable("x"), "amount"))),
                        new LambdaFunction(
                                List.of(new Variable("y")),
                                List.of(new AppliedFunction("sum", List.of(
                                        new Variable("y")))))),
                SpecParser.parse("~total:{x | $x.amount}:y|$y->sum()"));
    }

    @Test
    void columnLambdaSupportsMultiStatementBracedBody() {
        // '~total:{x | let y = 1; $y + 2}' \u2014 multi-statement
        // body inside a braced lambda in column position.
        // Cross-phase wiring (C.4 code blocks + C.6 column lambda):
        // parseColumnLambda dispatches BRACE_OPEN to
        // parseLambdaFunction, which uses parseCodeBlockStatements
        // for the body. Pinning ensures a future refactor that
        // bypasses parseLambdaFunction would not silently truncate
        // multi-statement bodies at the first semicolon.
        assertEquals(
                new ColSpec("total",
                        new LambdaFunction(
                                List.of(new Variable("x")),
                                List.of(
                                        new AppliedFunction("letFunction",
                                                List.of(new CString("y"),
                                                        new CInteger(1L))),
                                        new AppliedFunction("plus", List.of(
                                                new Variable("y"),
                                                new CInteger(2L))))),
                        null),
                SpecParser.parse("~total:{x | let y = 1; $y + 2}"));
    }

    @Test
    void columnSpecColonAtEndOfInputRejected() {
        // '~name:' \u2014 colon at end-of-input. parseColumnLambda
        // sees EOF and throws explicitly, rather than silently
        // returning a null lambda or some default. Small gap caught
        // by the audit; the existing 'non-lambda after colon' test
        // covers '~name:42' but not '~name:<EOF>'.
        ParseException ex = assertThrows(ParseException.class,
                () -> SpecParser.parse("~name:"));
        assertTrue(ex.getMessage().contains("expected lambda")
                        && ex.getMessage().contains("column spec"),
                () -> "want EOF-after-colon error, got: " + ex.getMessage());
    }

    @Test
    void columnSpecColonFollowedByNonLambdaRejected() {
        // '~name:42' \u2014 something other than a lambda after the
        // colon. parseColumnLambda explicitly rejects rather than
        // silently calling parseCombinedExpression (which would
        // accept '42' and produce a malformed AST).
        ParseException ex = assertThrows(ParseException.class,
                () -> SpecParser.parse("~name:42"));
        assertTrue(ex.getMessage().contains("expected lambda")
                        && ex.getMessage().contains("column spec"),
                () -> "want non-lambda-after-colon error, got: "
                        + ex.getMessage());
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
