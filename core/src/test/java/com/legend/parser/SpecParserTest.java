package com.legend.parser;

import com.legend.protocol.Multiplicity;
import com.legend.protocol.TypeExpression;

import com.legend.lexer.Lexer;
import com.legend.lexer.TokenStream;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.AppliedProperty;
import com.legend.protocol.spec.CBoolean;
import com.legend.protocol.spec.CDate;
import com.legend.protocol.spec.CDecimal;
import com.legend.protocol.spec.CFloat;
import com.legend.protocol.spec.CInteger;
import com.legend.protocol.spec.CLatestDate;
import com.legend.protocol.spec.CTime;
import com.legend.values.PureDateLiteral;
import com.legend.values.PureTimeLiteral;
import com.legend.protocol.spec.CString;
import com.legend.protocol.spec.ColSpec;
import com.legend.protocol.spec.ColSpecArray;
import com.legend.protocol.spec.EnumValue;
import com.legend.protocol.spec.KeyExpression;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.NewInstance;
import com.legend.protocol.spec.NewInstanceCast;
import com.legend.protocol.spec.PackageableElementPtr;
import com.legend.protocol.spec.PureCollection;
import com.legend.protocol.spec.TypeAnnotation;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.spec.Variable;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;

import static com.legend.model.TypeExpressionFixtures.nr;
import static com.legend.model.TypeExpressionFixtures.tg;
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

    /** Engine-shaped n-ary arithmetic node: {@code fn[Collection[operands...]]}
     *  — the wire spelling of operator-built plus/minus/times
     *  (DomainParseTreeWalker.buildArithmeticOp). */
    private static AppliedFunction nary(String fn, ValueSpecification... operands) {
        return new AppliedFunction(fn,
                List.of(new PureCollection(List.of(operands))));
    }

    // ----- numeric literals --------------------------------------------

    @Test
    void integerLiteralFits64Bits() {
        // Bytes that fit in a signed long stay a Long; the record holds
        // a Number to leave room for BigInteger overflow.
        ValueSpecification spec = com.legend.testing.Platform.spec("42");
        assertEquals(new CInteger(42L), spec);
        assertInstanceOf(Long.class, ((CInteger) spec).value(),
                "small integers should be narrowed to Long for cheap equality");
    }

    @Test
    void integerLiteralOverflowsToBigInteger() {
        // A digit string Larger than Long.MAX_VALUE forces BigInteger so
        // we don't silently lose precision.
        String huge = "92233720368547758980000";
        ValueSpecification spec = com.legend.testing.Platform.spec(huge);
        assertEquals(new CInteger(new BigInteger(huge)), spec);
        assertInstanceOf(BigInteger.class, ((CInteger) spec).value(),
                "overflowing literal should be carried as BigInteger");
    }

    @Test
    void floatLiteralParsesAsDouble() {
        assertEquals(new CFloat(3.14), com.legend.testing.Platform.spec("3.14"));
    }

    @Test
    void floatLiteralWithExponent() {
        assertEquals(new CFloat(1.5e-3), com.legend.testing.Platform.spec("1.5e-3"));
    }

    @Test
    void floatLiteralWithFSuffix() {
        // Pure permits a trailing 'f' on float literals; Java's
        // Double.parseDouble does not, so the parser must strip it.
        assertEquals(new CFloat(2.5), com.legend.testing.Platform.spec("2.5f"));
    }

    @Test
    void decimalLiteralFromIntegerShape() {
        // The lexer emits DECIMAL for 42d; the parser must round-trip
        // through BigDecimal without losing the integer shape.
        assertEquals(new CDecimal(new BigDecimal("42")), com.legend.testing.Platform.spec("42d"));
    }

    @Test
    void decimalLiteralFromFloatShape() {
        assertEquals(new CDecimal(new BigDecimal("3.14")), com.legend.testing.Platform.spec("3.14d"));
    }

    // ----- string literals ---------------------------------------------

    @Test
    void stringLiteralStripsQuotes() {
        assertEquals(new CString("hello"), com.legend.testing.Platform.spec("'hello'"));
    }

    @Test
    void stringLiteralResolvesEscapes() {
        // \\ \' \n \t \r are the documented supported escape set; we
        // assert each round-trips to the expected character so a future
        // refactor can't silently change the table.
        assertEquals(new CString("a\\b'c\nd\te\rf"),
                com.legend.testing.Platform.spec("'a\\\\b\\'c\\nd\\te\\rf'"));
    }

    @Test
    void stringLiteralEmptyIsLegal() {
        assertEquals(new CString(""), com.legend.testing.Platform.spec("''"));
    }

    @Test
    void stringLiteralUnknownEscapeDropsBackslash() {
        // real pure's StringEscape.UNESCAPE_PURE ends with {"\\", ""} —
        // an UNRECOGNIZED escape drops the backslash and keeps the char
        // (the corpus's '\ ' seed literal depends on it). Pin the decoded
        // engine semantics, not a stricter invention.
        ValueSpecification v = com.legend.testing.Platform.spec("'a\\zb'");
        assertEquals("azb", ((CString) v).value());
        // octal and unicode escapes DECODE — the oracle's decoder is
        // commons-text unescapeJava (verified live 2026-08-12; refusing
        // them was an invented divergence the adversarial fuzz caught)
        assertEquals("aAb", ((CString) com.legend.testing.Platform
                .spec("'a\\u0041b'")).value());
        assertEquals("aAb", ((CString) com.legend.testing.Platform
                .spec("'a\\101b'")).value());
    }

    // ----- booleans ----------------------------------------------------

    @Test
    void booleanTrue() {
        assertEquals(new CBoolean(true), com.legend.testing.Platform.spec("true"));
    }

    @Test
    void booleanFalse() {
        assertEquals(new CBoolean(false), com.legend.testing.Platform.spec("false"));
    }

    // ----- temporal literals -------------------------------------------

    @Test
    void strictDateLiteralYearMonthDay() {
        // %2024-01-15 -> structured StrictDate variant.
        assertEquals(new CDate(new PureDateLiteral.StrictDate(2024, 1, 15)),
                com.legend.testing.Platform.spec("%2024-01-15"));
    }

    @Test
    void yearOnlyLiteral() {
        // %2024 -> Year variant. Previously this masqueraded as a
        // "strict date" (type lie); the structured hierarchy gives it
        // its own variant.
        assertEquals(new CDate(new PureDateLiteral.Year(2024)),
                com.legend.testing.Platform.spec("%2024"));
    }

    @Test
    void yearMonthLiteral() {
        assertEquals(new CDate(new PureDateLiteral.YearMonth(2024, 1)),
                com.legend.testing.Platform.spec("%2024-01"));
    }

    @Test
    void dateWithHourLiteral() {
        // %2024-01-15T10 -> DateWithHour (legal per engine grammar).
        assertEquals(new CDate(new PureDateLiteral.DateWithHour(2024, 1, 15, 10)),
                com.legend.testing.Platform.spec("%2024-01-15T10"));
    }

    @Test
    void dateTimeLiteralWithSeconds() {
        assertEquals(new CDate(new PureDateLiteral.DateWithSecond(2024, 1, 15, 10, 30, 0)),
                com.legend.testing.Platform.spec("%2024-01-15T10:30:00"));
    }

    @Test
    void dateTimeLiteralWithSubsecondAndGmtTimezone() {
        // GMT (+0000) -> no shift, just dropped.
        assertEquals(new CDate(new PureDateLiteral.DateWithSubsecond(2024, 1, 15, 10, 30, 0, "123")),
                com.legend.testing.Platform.spec("%2024-01-15T10:30:00.123+0000"));
    }

    @Test
    void dateTimeLiteralTimezoneShifts() {
        // %2024-01-15T10:00+0500 -> shift -5h to GMT -> %2024-01-15T05:00.
        assertEquals(new CDate(new PureDateLiteral.DateWithMinute(2024, 1, 15, 5, 0)),
                com.legend.testing.Platform.spec("%2024-01-15T10:00+0500"));
    }

    @Test
    void strictTimeLiteral() {
        // %hh:mm:ss -> structured TimeWithSecond.
        assertEquals(new CTime(new PureTimeLiteral.TimeWithSecond(10, 30, 45)),
                com.legend.testing.Platform.spec("%10:30:45"));
    }

    @Test
    void latestDateSentinel() {
        // CLatestDate is fieldless; equality follows record contract.
        assertEquals(new CLatestDate(), com.legend.testing.Platform.spec("%latest"));
    }

    // ----- variables ---------------------------------------------------

    @Test
    void variableReference() {
        // $name -> Variable("name"); the $ is NOT in the carried name.
        assertEquals(new Variable("this"), com.legend.testing.Platform.spec("$this"));
    }

    @Test
    void variableWithUnderscoresAndDigits() {
        assertEquals(new Variable("my_var2"), com.legend.testing.Platform.spec("$my_var2"));
    }

    @Test
    void dollarWithoutIdentifierRejected() {
        ParseException ex = assertThrows(ParseException.class,
                () -> com.legend.testing.Platform.spec("$"));
        assertTrue(String.valueOf(ex.getMessage()).toLowerCase(java.util.Locale.ROOT).contains("expected identifier"),
                () -> "want identifier-after-dollar error, got: " + ex.getMessage());
    }

    // ----- collection literals -----------------------------------------

    @Test
    void emptyCollection() {
        assertEquals(new PureCollection(List.of()), com.legend.testing.Platform.spec("[]"));
    }

    @Test
    void singletonCollection() {
        assertEquals(new PureCollection(List.of(new CInteger(1L))),
                com.legend.testing.Platform.spec("[1]"));
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
                com.legend.testing.Platform.spec("[1, 'two', true]"));
    }

    @Test
    void nestedCollection() {
        assertEquals(
                new PureCollection(List.of(
                        new PureCollection(List.of(new CInteger(1L), new CInteger(2L))),
                        new PureCollection(List.of(new CInteger(3L))))),
                com.legend.testing.Platform.spec("[[1, 2], [3]]"));
    }

    @Test
    void collectionTrailingCommaRejected() {
        // Trailing comma is not legal in Pure; matching engine here
        // keeps test corpora byte-comparable.
        ParseException ex = assertThrows(ParseException.class,
                () -> com.legend.testing.Platform.spec("[1, 2,]"));
        assertTrue(String.valueOf(ex.getMessage()).toLowerCase(java.util.Locale.ROOT).contains("trailing comma"),
                () -> "want trailing-comma error, got: " + ex.getMessage());
    }

    @Test
    void collectionUnclosedRejected() {
        // Tighter than contains("]"): a regression throwing the wrong-
        // direction error ("unexpected ]") would falsely pass that.
        // Pin the exact wording so it must be a missing-close error.
        ParseException ex = assertThrows(ParseException.class,
                () -> com.legend.testing.Platform.spec("[1, 2"));
        assertTrue(String.valueOf(ex.getMessage()).contains("expected ']'"),
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
                () -> com.legend.testing.Platform.spec("->"));
        assertTrue(String.valueOf(ex.getMessage()).contains("unsupported expression token"),
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
                () -> com.legend.testing.Platform.spec(""));
        assertTrue(String.valueOf(ex.getMessage()).contains("end of input"),
                () -> "want end-of-input error, got: " + ex.getMessage());
    }

    @Test
    void trailingTokensRejected() {
        // A single expression must consume the entire input; trailing
        // tokens are a strong signal that the input is malformed (or
        // that the caller meant to parse a multi-statement body, which
        // is a C.4 feature).
        ParseException ex = assertThrows(ParseException.class,
                () -> com.legend.testing.Platform.spec("1 2"));
        assertTrue(String.valueOf(ex.getMessage()).toLowerCase(java.util.Locale.ROOT).contains("trailing"),
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
        assertEquals(com.legend.testing.Platform.spec("42"), com.legend.testing.Platform.spec("(42)"));
        assertEquals(com.legend.testing.Platform.spec("$x"), com.legend.testing.Platform.spec(("($x)")));
    }

    @Test
    void parenthesisedExpressionNests() {
        // Multiple layers of parens must all unwrap to the same AST,
        // since none of them carry semantic info.
        assertEquals(new CInteger(7L), com.legend.testing.Platform.spec("(((7)))"));
    }

    // ----- packageable element references (C.2) -----------------------

    @Test
    void bareIdentifierBecomesPackageableElementPtr() {
        // A bare identifier at expression position is a class / element
        // reference; PackageableElementPtr carries the source name
        // verbatim (no FQN resolution until Phase D).
        assertEquals(new PackageableElementPtr("Person"),
                com.legend.testing.Platform.spec("Person"));
    }

    @Test
    void qualifiedIdentifierBecomesPackageableElementPtr() {
        assertEquals(new PackageableElementPtr("my::app::Person"),
                com.legend.testing.Platform.spec("my::app::Person"));
    }

    // ----- prefix function application (C.2) --------------------------

    @Test
    void zeroArgPrefixCall() {
        assertEquals(new AppliedFunction("now", List.of()),
                com.legend.testing.Platform.spec("now()"));
    }

    @Test
    void singleArgPrefixCall() {
        assertEquals(new AppliedFunction("abs", List.of(new CInteger(5L))),
                com.legend.testing.Platform.spec("abs(5)"));
    }

    @Test
    void multiArgPrefixCallWithMixedKinds() {
        assertEquals(
                new AppliedFunction("plus", List.of(
                        new CInteger(1L),
                        new CFloat(2.5),
                        new Variable("x"))),
                com.legend.testing.Platform.spec("plus(1, 2.5, $x)"));
    }

    @Test
    void qualifiedPrefixCall() {
        // FQN preserved verbatim in AppliedFunction.function field;
        // resolution is the next pipeline stage's job.
        assertEquals(
                new AppliedFunction("my::pkg::add",
                        List.of(new CInteger(1L), new CInteger(2L))),
                com.legend.testing.Platform.spec("my::pkg::add(1, 2)"));
    }

    @Test
    void prefixCallWithNestedCall() {
        // Args may themselves be any expression \u2014 nested calls included.
        assertEquals(
                new AppliedFunction("outer", List.of(
                        new AppliedFunction("inner", List.of(new CInteger(1L))))),
                com.legend.testing.Platform.spec("outer(inner(1))"));
    }

    // ----- property access (C.2) --------------------------------------

    @Test
    void singlePropertyAccess() {
        // AppliedProperty(receiver, property) reads left-to-right like
        // source: $x.name -> AppliedProperty($x, "name").
        assertEquals(new AppliedProperty(new Variable("x"), "name"),
                com.legend.testing.Platform.spec("$x.name"));
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
                com.legend.testing.Platform.spec("$x.foo.bar"));
    }

    @Test
    void quotedPropertyNameStripsQuotesAndResolvesEscapes() {
        // Property names with whitespace / special chars are written
        // with single quotes; same lexical rules as a CString literal.
        assertEquals(
                new AppliedProperty(new Variable("x"), "My Name"),
                com.legend.testing.Platform.spec("$x.'My Name'"));
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
                com.legend.testing.Platform.spec("$x.filter($y)"));
    }

    @Test
    void methodOnPackageableElementReceiver() {
        // The canonical Pure entry point: Person.all() ->
        // AppliedFunction("all", [PackageableElementPtr("Person")]).
        assertEquals(
                new AppliedFunction("getAll", List.of(
                        new PackageableElementPtr("Person"))),
                com.legend.testing.Platform.spec("Person.all()"));
    }

    // ----- arrow form (C.2) -------------------------------------------

    @Test
    void arrowFormDesugaringIsIdenticalToMethodForm() {
        // The whole point of the AppliedFunction shape: after parsing,
        // $x->foo(y) and $x.foo(y) MUST produce identical ASTs. This
        // test pins the desugaring invariant so any future divergence
        // (e.g. accidentally re-introducing a hasReceiver flag) fails
        // here loud and clear.
        ValueSpecification arrow = com.legend.testing.Platform.spec("$x->foo($y)");
        ValueSpecification method = com.legend.testing.Platform.spec("$x.foo($y)");
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
                                new AppliedFunction("getAll", List.of(
                                        new PackageableElementPtr("Person"))),
                                new Variable("p"))),
                        new CInteger(10L))),
                com.legend.testing.Platform.spec("Person.all()->filter($p)->limit(10)"));
    }

    @Test
    void arrowCallToQualifiedFunctionName() {
        // Receiver-arrow with an FQN'd function: $x->my::pkg::fn().
        // FQN preserved verbatim on AppliedFunction.function.
        assertEquals(
                new AppliedFunction("my::pkg::fn", List.of(new Variable("x"))),
                com.legend.testing.Platform.spec("$x->my::pkg::fn()"));
    }

    // ----- C.2 error surfaces -----------------------------------------

    @Test
    void dotWithoutPropertyNameRejected() {
        ParseException ex = assertThrows(ParseException.class,
                () -> com.legend.testing.Platform.spec("$x."));
        assertTrue(String.valueOf(ex.getMessage()).contains("expected property name"),
                () -> "want missing-property-name error, got: " + ex.getMessage());
    }

    @Test
    void arrowWithoutFunctionNameRejected() {
        // After the FQN parser was consolidated onto TokenStreamCursor,
        // the missing-identifier message comes from the shared default:
        // 'expected type name, got EOF'. Pin the new (still descriptive,
        // still source-located) phrase.
        ParseException ex = assertThrows(ParseException.class,
                () -> com.legend.testing.Platform.spec("$x->"));
        assertTrue(String.valueOf(ex.getMessage()).contains("expected type name"),
                () -> "want missing-identifier error, got: " + ex.getMessage());
    }

    @Test
    void arrowWithoutParensRejected() {
        // '->' must be followed by 'fn(...)'. Bare '->foo' with no
        // call-parens is malformed.
        ParseException ex = assertThrows(ParseException.class,
                () -> com.legend.testing.Platform.spec("$x->foo"));
        assertTrue(String.valueOf(ex.getMessage()).contains("expected '(' after arrow-call"),
                () -> "want missing-paren error, got: " + ex.getMessage());
    }

    @Test
    void unclosedArgListRejected() {
        // Pin the production-specific phrasing ("argument list") so this
        // test cannot also pass for the parenthesised-expression error
        // — the two productions emit different messages by design and
        // we want refactor-time crosswire bugs to fail here.
        ParseException ex = assertThrows(ParseException.class,
                () -> com.legend.testing.Platform.spec("fn(1, 2"));
        assertTrue(String.valueOf(ex.getMessage()).contains("close argument list"),
                () -> "want missing-close error, got: " + ex.getMessage());
    }

    @Test
    void trailingCommaInArgListRejected() {
        ParseException ex = assertThrows(ParseException.class,
                () -> com.legend.testing.Platform.spec("fn(1, 2,)"));
        assertTrue(String.valueOf(ex.getMessage()).contains("trailing comma"),
                () -> "want trailing-comma error, got: " + ex.getMessage());
    }

    @Test
    void unclosedParenthesisedExprRejected() {
        // Sister to unclosedArgListRejected: pin the OTHER specific
        // phrasing ("parenthesised expression") so these two negative
        // tests cover disjoint code paths.
        ParseException ex = assertThrows(ParseException.class,
                () -> com.legend.testing.Platform.spec("(42"));
        assertTrue(String.valueOf(ex.getMessage()).contains("close parenthesised expression"),
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
                com.legend.testing.Platform.spec("$x.foo->fn()"));
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
                com.legend.testing.Platform.spec("$x->fn().bar"));
    }

    // ----- binary operators: arithmetic (C.3) -------------------------

    @Test
    void additionDesugarsToAppliedFunctionPlus() {
        // Operator '+' desugars to the ENGINE's n-ary spelling:
        // AppliedFunction("plus", [Collection[lhs, rhs]])
        // (DomainParseTreeWalker.buildArithmeticOp wraps ALL operands of a
        // same-op run in ONE collection parameter).
        assertEquals(
                nary("plus", new CInteger(1L), new CInteger(2L)),
                com.legend.testing.Platform.spec("1 + 2"));
    }

    @Test
    void subtractionMultiplicationDivisionUseCanonicalNames() {
        // The operator-to-function mapping must match engine for
        // corpus-byte-comparability: minus/times/divide (NOT
        // sub/mul/div). minus/times carry the n-ary collection;
        // divide stays PAIRWISE (engine buildDivide).
        assertEquals(
                nary("minus", new CInteger(5L), new CInteger(3L)),
                com.legend.testing.Platform.spec("5 - 3"));
        assertEquals(
                nary("times", new CInteger(4L), new CInteger(2L)),
                com.legend.testing.Platform.spec("4 * 2"));
        assertEquals(
                new AppliedFunction("divide", List.of(
                        new CInteger(10L), new CInteger(2L))),
                com.legend.testing.Platform.spec("10 / 2"));
    }

    @Test
    void sameOperatorChainsFormOneNaryRun() {
        // '1 + 2 + 3' is ONE grammar context \u2014 (PLUS expression)+ \u2014 so the
        // engine builds ONE plus over a single 3-element collection:
        // plus[Collection[1, 2, 3]] (audit \u00a71.3: runs flatten per CONTEXT,
        // not per spine).
        assertEquals(
                nary("plus", new CInteger(1L), new CInteger(2L), new CInteger(3L)),
                com.legend.testing.Platform.spec("1 + 2 + 3"));
    }

    @Test
    void quotedNamesDecodeIdenticallyAtBindAndUse() {
        // audit M10: let 'my var' = ... and $'my var' previously decoded
        // DIFFERENTLY (one kept its quotes) so the reference could never
        // resolve. Both route through THE shared decoder now.
        LambdaFunction lf = (LambdaFunction) com.legend.testing.Platform.spec("|let 'my var' = 5; $'my var';");
        AppliedFunction let = (AppliedFunction) lf.body().get(0);
        String bound = ((CString) let.parameters().get(0)).value();
        String used = ((Variable) lf.body().get(1)).name();
        assertEquals("my var", bound);
        assertEquals(bound, used, "bind-site and use-site must decode identically");
    }

    @Test
    void utcSuffixDateLiteralLexes() {
        // audit M5: %2024-01-15T10:30Z was unlexable ('Z' fell out of the
        // date scan) though the literal parser fully supports it.
        ValueSpecification vs = com.legend.testing.Platform.spec("%2024-01-15T10:30Z");
        assertInstanceOf(CDate.class, vs);
    }

    @Test
    void schemaQualifiedTableReferenceSplitsAtTheFqnBoundary() {
        // audit M7: #>{db::DB.schema.T}# mis-split at the LAST dot, yielding
        // the structurally invalid FQN "db::DB.schema".
        AppliedFunction tr = (AppliedFunction) com.legend.testing.Platform.spec("#>{db::DB.schema.T}#");
        assertEquals("tableReference", tr.function());
        assertEquals("db::DB",
                ((PackageableElementPtr) tr.parameters().get(0)).fullPath());
        assertEquals("schema.T", ((CString) tr.parameters().get(1)).value());
    }

    @Test
    void pipeLambdaStatementSequenceMatchesRealGrammar() {
        // M3ParserGrammar.g4: codeBlock: programLine (';' (programLine ';')*)?
        // First statement's ';' optional; SUBSEQUENT statements REQUIRE it.
        assertEquals(1, ((LambdaFunction) com.legend.testing.Platform.spec("|1 + 1")).body().size());
        assertEquals(1, ((LambdaFunction) com.legend.testing.Platform.spec("|1 + 1;")).body().size());
        assertEquals(3, ((LambdaFunction) com.legend.testing.Platform.spec(
                "|let a = 1; let b = 2; $a + $b;")).body().size());
        // Missing the required trailing ';' on a subsequent statement: invalid.
        assertThrows(ParseException.class, () -> com.legend.testing.Platform.spec("|let a = 1; $a"));
    }

    @Test
    void booleanPrecedenceMatchesRealPure() {
        // && binds tighter than || (engine DomainParseTreeWalker
        // isLowerPrecedenceBoolean): 'a || b && c' is or(a, and(b, c)).
        AppliedFunction or = (AppliedFunction) com.legend.testing.Platform.spec("true || true && false");
        assertEquals("or", or.function());
        assertEquals("and", ((AppliedFunction) or.parameters().get(1)).function());
    }

    @Test
    void arithmeticPrecedenceMatchesRealPure() {
        // REAL Pure HAS precedence: legend-pure's AbstractTestPrecedence
        // asserts `2+3*4/2 == 8`-style facts, so '1 + 2 * 3' is 1+(2*3)=7.
        // Engine-lite's flat left-associative grammar was a DIVERGENCE from
        // real Pure (the previous version of this very test pinned it and
        // demanded a conscious decision — this is that decision, made with
        // legend-pure's own test corpus as the authority; found by an
        // EXECUTED lowering test returning 9 where Pure returns 7).
        assertEquals(
                nary("plus",
                        new CInteger(1L),
                        nary("times", new CInteger(2L), new CInteger(3L))),
                com.legend.testing.Platform.spec("1 + 2 * 3"));
        // Comparisons bind loosest: 1 + 2 * 3 > 4 * 5 + 6 == (7 > 26).
        assertEquals("greaterThan",
                ((AppliedFunction) com.legend.testing.Platform.spec("1 + 2 * 3 > 4 * 5 + 6")).function());
    }

    @Test
    void parensOverridePrecedence() {
        // Parens restart combinedExpression, so '(2 * 3)' is an ordinary
        // operand of the plus run: plus[Collection[1, times[Collection[2,3]]]].
        assertEquals(
                nary("plus",
                        new CInteger(1L),
                        nary("times", new CInteger(2L), new CInteger(3L))),
                com.legend.testing.Platform.spec("1 + (2 * 3)"));
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
                com.legend.testing.Platform.spec("1 < 2"));
        assertEquals(
                new AppliedFunction("lessThanEqual", List.of(
                        new CInteger(1L), new CInteger(2L))),
                com.legend.testing.Platform.spec("1 <= 2"));
        assertEquals(
                new AppliedFunction("greaterThan", List.of(
                        new CInteger(3L), new CInteger(2L))),
                com.legend.testing.Platform.spec("3 > 2"));
        assertEquals(
                new AppliedFunction("greaterThanEqual", List.of(
                        new CInteger(3L), new CInteger(2L))),
                com.legend.testing.Platform.spec("3 >= 2"));
    }

    @Test
    void equalityOpsUseCanonicalNames() {
        // == desugars to equal; != to not(equal(...)) — REAL pure's
        // spelling (there is NO notEqual native; FQN_MIGRATION finding).
        assertEquals(
                new AppliedFunction("equal", List.of(
                        new Variable("x"), new CInteger(1L))),
                com.legend.testing.Platform.spec("$x == 1"));
        assertEquals(
                new AppliedFunction("not", List.of(
                        new AppliedFunction("equal", List.of(
                                new Variable("x"), new CInteger(1L))))),
                com.legend.testing.Platform.spec("$x != 1"));
    }

    @Test
    void alternateNotEqualSpellingDesugarsIdentically() {
        // Pure spells inequality two ways: '!=' and '<>'. Both desugar to
        // not(equal(...)) — real pure's spelling — identically.
        ValueSpecification bang = com.legend.testing.Platform.spec("$x != 1");
        ValueSpecification angle = com.legend.testing.Platform.spec("$x <> 1");
        assertEquals(bang, angle,
                "'$x != 1' and '$x <> 1' must produce equal AST");
        assertEquals(
                new AppliedFunction("not", List.of(new AppliedFunction("equal", List.of(
                        new Variable("x"), new CInteger(1L))))),
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
                        nary("plus", new Variable("b"), new Variable("c")))),
                com.legend.testing.Platform.spec("$a == $b + $c"));
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
                nary("plus",
                        new Variable("a"),
                        new AppliedFunction("equal", List.of(
                                new Variable("b"), new Variable("c")))),
                com.legend.testing.Platform.spec("$a + $b == $c"));
    }

    // ----- binary operators: boolean (C.3) ----------------------------

    @Test
    void booleanOpsUseCanonicalNames() {
        // && and || desugar to 'and' and 'or' \u2014 engine names.
        assertEquals(
                new AppliedFunction("and", List.of(
                        new CBoolean(true), new CBoolean(false))),
                com.legend.testing.Platform.spec("true && false"));
        assertEquals(
                new AppliedFunction("or", List.of(
                        new CBoolean(true), new CBoolean(false))),
                com.legend.testing.Platform.spec("true || false"));
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
                com.legend.testing.Platform.spec("$a && $b && $c"));
    }

    @Test
    void booleanOpsComposeWithComparisons() {
        // '$x > 10 && $x < 20' parses to the ENGINE's flat left-associative
        // accumulator shape — lessThan(and(greaterThan($x,10), $x), 20) —
        // AT PARSE TIME (ProbeWireShapes "mixed bool arith"; audit §1.1: the
        // old emission-only rotation left the compiler a different tree).
        // The boolean group folds over the arithmetic accumulator and the
        // trailing comparison folds over the boolean result.
        assertEquals(
                new AppliedFunction("lessThan", List.of(
                        new AppliedFunction("and", List.of(
                                new AppliedFunction("greaterThan", List.of(
                                        new Variable("x"), new CInteger(10L))),
                                new Variable("x"))),
                        new CInteger(20L))),
                com.legend.testing.Platform.spec("$x > 10 && $x < 20"));
    }

    // ----- unary operators (C.3) --------------------------------------

    @Test
    void unaryNotDesugarsToAppliedFunctionNot() {
        assertEquals(
                new AppliedFunction("not", List.of(new Variable("x"))),
                com.legend.testing.Platform.spec("!$x"));
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
                com.legend.testing.Platform.spec("-$x.foo"));
    }

    @Test
    void unaryPlusIsRetainedAsAppliedFunction() {
        // Engine emits AppliedFunction("plus", [inner]) for unary '+';
        // we match (it's a useless operator in source, but preserving
        // it keeps round-trip parity with the engine corpus).
        assertEquals(
                new AppliedFunction("plus", List.of(new CInteger(42L))),
                com.legend.testing.Platform.spec("+42"));
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
                        new NewInstance("Foo", List.of(), List.of()))),
                com.legend.testing.Platform.spec("^Foo()"));
    }

    @Test
    void newInstanceWithBindings() {
        // Properties as Map<String, KeyExpression>. Each binding is
        // wrapped in a KeyExpression carrying the value plus an
        // isAdd=false flag (this is the '=' form, not '+=').
        assertEquals(
                new AppliedFunction("new", List.of(
                        new PackageableElementPtr("Person"),
                        new NewInstance("Person", List.of(), List.of(
                                new NewInstance.KeyBinding("name", new KeyExpression(new CString("Alice"), false, false)),
                                new NewInstance.KeyBinding("age", new KeyExpression(new CInteger(30L), false, false)))))),
                com.legend.testing.Platform.spec("^Person(name='Alice', age=30)"));
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
                        new NewInstance("my::app::Person", List.of(), List.of(
                                new NewInstance.KeyBinding("name", new KeyExpression(new CString("Bob"), false, false)))))),
                com.legend.testing.Platform.spec("^my::app::Person(name='Bob')"));
    }

    @Test
    void newInstanceWithTypeArguments() {
        // ^Pair<Integer, String>(...) \u2014 type arguments captured as
        // source-level FQN strings, stored on the inner NewInstance.
        assertEquals(
                new AppliedFunction("new", List.of(
                        new PackageableElementPtr("Pair"),
                        new NewInstance("Pair",
                                List.of(nr("Integer"), nr("String")),
                                List.of(
                                new NewInstance.KeyBinding("first", new KeyExpression(new CInteger(1L), false, false)),
                                new NewInstance.KeyBinding("second", new KeyExpression(new CString("a"), false, false)))))),
                com.legend.testing.Platform.spec("^Pair<Integer, String>(first=1, second='a')"));
    }

    @Test
    void newInstanceBindingValueCanBeAnyExpression() {
        // Bindings parse with parseCombinedExpression, so values can be
        // anything: arithmetic, calls, nested new-instances, etc.
        assertEquals(
                new AppliedFunction("new", List.of(
                        new PackageableElementPtr("Box"),
                        new NewInstance("Box", List.of(), List.of(
                                new NewInstance.KeyBinding("value", new KeyExpression(
                                        nary("plus", new CInteger(1L), new CInteger(2L)),
                                        false, false))))
                )),
                com.legend.testing.Platform.spec("^Box(value=1+2)"));
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
                        new NewInstance("Person", List.of(), List.of(
                                new NewInstance.KeyBinding("name", new KeyExpression(
                                        new CString("Alice"), false, false)),
                                new NewInstance.KeyBinding("tags", new KeyExpression(
                                        new CString("admin"), true, false)))))),
                com.legend.testing.Platform.spec("^Person(name='Alice', tags+='admin')"));
    }

    @Test
    void newInstanceDuplicateKeysAllKeptInSourceOrder() {
        // Engine-true behaviour (deep audit #2 finding 1b): ^Foo(x=1, x=2)
        // keeps BOTH bindings in source order — the engine's wire carries
        // two keyExpressions (multiplicity 2). The old Map carrier
        // silently collapsed to last-wins; the List carrier pins the fix.
        // Lookup consumers see the FIRST binding via first().
        var parsed = (AppliedFunction)
                com.legend.testing.Platform.spec("^Foo(x=1, x=2)");
        var ni = (NewInstance) parsed.parameters().get(1);
        assertEquals(2, ni.properties().size(),
                "both duplicate bindings survive the parse");
        assertEquals(List.of("x", "x"),
                ni.properties().stream()
                        .map(NewInstance.KeyBinding::key).toList());
        assertEquals(new CInteger(1L),
                java.util.Objects.requireNonNull(ni.first("x")).value(),
                "first() sees the FIRST binding");
    }

    @Test
    void newInstanceMissingEqualsRejected() {
        ParseException ex = assertThrows(ParseException.class,
                () -> com.legend.testing.Platform.spec("^Foo(x 5)"));
        assertTrue(String.valueOf(ex.getMessage()).contains("expected '='"),
                () -> "want missing-equals error, got: " + ex.getMessage());
    }

    @Test
    void newInstanceTrailingCommaRejected() {
        // Pin the production-specific phrase ("binding list") so this
        // test cannot also pass against the collection or argument-list
        // trailing-comma errors, all of which contain "trailing comma".
        ParseException ex = assertThrows(ParseException.class,
                () -> com.legend.testing.Platform.spec("^Foo(x=1,)"));
        assertTrue(String.valueOf(ex.getMessage()).contains("^NewInstance binding list"),
                () -> "want NewInstance-specific trailing-comma error, got: "
                        + ex.getMessage());
    }

    // ----- ^Class($src) positional cast (R1) -----------------------------

    // The positional-cast form is the second of the ^Class(...) shapes
    // documented in MAPPING_NORMALIZER_DESIGN.md ("Constructor forms"):
    // it feeds $src through Class's active mapping rather than building
    // an instance from explicit field bindings. The parser emits a
    // distinct NewInstanceCast carrier so downstream lowering can pattern-
    // match it (the cast lowers by invoking Class's synthesized M_Class
    // function on $src; the named-args form beta-reduces field accesses).

    @Test
    void newInstanceCastSingleVariableSrc() {
        // ^Firm($x) — minimal cast form. The inner carrier is
        // NewInstanceCast, not NewInstance: distinct shape, distinct
        // downstream behavior.
        assertEquals(
                new AppliedFunction("new", List.of(
                        new PackageableElementPtr("Firm"),
                        new NewInstanceCast("Firm", List.of(),
                                new Variable("x")))),
                com.legend.testing.Platform.spec("^Firm($x)"));
    }

    @Test
    void newInstanceCastWithPropertyAccessSrc() {
        // ^DeptInfo($emp.department) — the Layer 6 (M2M class-typed
        // slot) example from the design doc.
        assertEquals(
                new AppliedFunction("new", List.of(
                        new PackageableElementPtr("DeptInfo"),
                        new NewInstanceCast("DeptInfo", List.of(),
                                new AppliedProperty(
                                        new Variable("emp"), "department")))),
                com.legend.testing.Platform.spec("^DeptInfo($emp.department)"));
    }

    @Test
    void newInstanceCastWithQualifiedClassName() {
        // FQN preserved verbatim in both the PackageableElementPtr
        // wrapper and the NewInstanceCast carrier; mirrors the
        // named-args form's duplication.
        assertEquals(
                new AppliedFunction("new", List.of(
                        new PackageableElementPtr("my::pkg::Firm"),
                        new NewInstanceCast("my::pkg::Firm", List.of(),
                                new Variable("src")))),
                com.legend.testing.Platform.spec("^my::pkg::Firm($src)"));
    }

    @Test
    void newInstanceCastWithTypeArguments() {
        // ^Pair<Integer, String>($p) — type arguments live on the
        // cast carrier just as they do on NewInstance.
        assertEquals(
                new AppliedFunction("new", List.of(
                        new PackageableElementPtr("Pair"),
                        new NewInstanceCast("Pair",
                                List.of(nr("Integer"), nr("String")),
                                new Variable("p")))),
                com.legend.testing.Platform.spec("^Pair<Integer, String>($p)"));
    }

    @Test
    void newInstanceCastDoesNotShadowNamedArgs() {
        // Regression: adding the cast detection must not break the
        // named-args path. Same input parses to NewInstance, not
        // NewInstanceCast.
        assertEquals(
                new AppliedFunction("new", List.of(
                        new PackageableElementPtr("Person"),
                        new NewInstance("Person", List.of(), List.of(
                                new NewInstance.KeyBinding("name", new KeyExpression(new CString("Alice"), false, false)))))),
                com.legend.testing.Platform.spec("^Person(name='Alice')"));
    }

    @Test
    void newInstanceCastDoesNotShadowEmptyConstructor() {
        // ^Foo() with no body is an empty NewInstance (default
        // construction), never a cast. The disambiguator only fires
        // when there IS body content that doesn't look like a binding.
        assertEquals(
                new AppliedFunction("new", List.of(
                        new PackageableElementPtr("Foo"),
                        new NewInstance("Foo", List.of(), List.of()))),
                com.legend.testing.Platform.spec("^Foo()"));
    }

    @Test
    void newInstanceCastWithCallExpressionSrc() {
        // The cast body parses with parseCombinedExpression, so
        // arbitrary value expressions are admitted (not just bare
        // variables).
        assertEquals(
                new AppliedFunction("new", List.of(
                        new PackageableElementPtr("Result"),
                        new NewInstanceCast("Result", List.of(),
                                nary("plus", new CInteger(1L), new CInteger(2L))))),
                com.legend.testing.Platform.spec("^Result(1 + 2)"));
    }

    @Test
    void copyWithUpdateRejectsPositionalForm() {
        // Cast form is class-literal-only. The copy-with-update
        // ^$var(...) form always takes named bindings; a bare
        // expression inside must surface as a binding-shape parse
        // error so callers can't accidentally write a cast there.
        ParseException ex = assertThrows(ParseException.class,
                () -> com.legend.testing.Platform.spec("^$existing($src)"));
        // We don't pin the exact error text; the key contract is that
        // this does NOT parse to a NewInstanceCast.
        assertTrue(String.valueOf(ex.getMessage()).contains("^NewInstance")
                        || ex.getMessage().contains("property name"),
                () -> "want a NewInstance binding-shape error, got: "
                        + ex.getMessage());
    }

    // ----- map(@Class) sugar (already parses; pinning the shape) ---------

    @Test
    void mapAtClassSugarParsesAsTypeAnnotationArg() {
        // map(@Person) is the "construct Person from each row by
        // column-name match" sugar described in
        // MAPPING_NORMALIZER_DESIGN.md ("map(@Class) sugar"). No new
        // AST node is needed: the @Class form already parses as a
        // TypeAnnotation.Named at expression position. The synth
        // emits this as the second arg of map; lowering recognizes
        // the TypeAnnotation.Named arg and desugars to the explicit
        // map(r | ^Class(p1=$r.p1, ...)) form.
        //
        // Pin: map(rel, @Person) parses to an AppliedFunction("map",
        // ...) whose 2nd arg is a TypeAnnotation.Named carrying the
        // class NameRef.
        ValueSpecification parsed = com.legend.testing.Platform.spec("$rel->map(@Person)");
        AppliedFunction af = assertInstanceOf(AppliedFunction.class, parsed);
        assertEquals("map", af.function());
        assertEquals(2, af.parameters().size());
        TypeAnnotation.Named ann =
                assertInstanceOf(TypeAnnotation.Named.class, af.parameters().get(1));
        assertEquals(nr("Person"), ann.type());
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
                        new AppliedFunction("getAll", List.of(
                                new PackageableElementPtr("Person"))),
                        new AppliedFunction("greaterThan", List.of(
                                new AppliedProperty(new Variable("p"), "age"),
                                new CInteger(21L))))),
                com.legend.testing.Platform.spec("Person.all()->filter($p.age > 21)"));
    }

    @Test
    void operatorsInsideCollectionLiteral() {
        // [1+2, 3*4] \u2014 collection element parsing uses
        // parseCombinedExpression so operators are admitted.
        assertEquals(
                new PureCollection(List.of(
                        nary("plus", new CInteger(1L), new CInteger(2L)),
                        nary("times", new CInteger(3L), new CInteger(4L)))),
                com.legend.testing.Platform.spec("[1+2, 3*4]"));
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
                com.legend.testing.Platform.spec("let x = 5"));
    }

    @Test
    void letBindingValueIsFullCombinedExpression() {
        // The RHS of '=' parses as parseCombinedExpression, so
        // operators, calls, and arrow chains all work. The value
        // becomes the second argument of the letFunction call.
        assertEquals(
                new AppliedFunction("letFunction", List.of(
                        new CString("result"),
                        nary("plus", new CInteger(1L), new CInteger(2L)))),
                com.legend.testing.Platform.spec("let result = 1 + 2"));
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
        AppliedFunction parsed = (AppliedFunction) com.legend.testing.Platform.spec("let myVar = 42");
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
        // SUBTLETY: LET is in TokenStreamCursor.IDENTIFIER_TOKENS (so that
        // '$let' parses as a variable with that name). As a result,
        // mid-expression 'let' is silently absorbed as
        // PackageableElementPtr("let"), and the subsequent
        // identifier (e.g. 'x') is what surfaces as a trailing
        // token. The exact wording matters because a future change
        // that DID add an explicit LET dispatch in parsePrimary would
        // emit a different (clearer) message; flipping this test
        // would be the signal that the change happened.
        ParseException ex = assertThrows(ParseException.class,
                () -> com.legend.testing.Platform.spec("1 + let x = 2"));
        assertTrue(String.valueOf(ex.getMessage()).toLowerCase(java.util.Locale.ROOT).contains("trailing"),
                () -> "want trailing-tokens error (LET absorbed as identifier, "
                        + "subsequent 'x' is trailing), got: " + ex.getMessage());
    }

    @Test
    void letMissingVariableNameRejected() {
        ParseException ex = assertThrows(ParseException.class,
                () -> com.legend.testing.Platform.spec("let = 5"));
        assertTrue(String.valueOf(ex.getMessage()).contains("variable name after 'let'"),
                () -> "want missing-var-name error, got: " + ex.getMessage());
    }

    @Test
    void letMissingEqualsRejected() {
        ParseException ex = assertThrows(ParseException.class,
                () -> com.legend.testing.Platform.spec("let x 5"));
        assertTrue(String.valueOf(ex.getMessage()).contains("expected '=' after 'let x'"),
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
                com.legend.testing.Platform.spec("{p | $p}"));
    }

    @Test
    void bracedMultiParamLambda() {
        // '{p, q | $p + $q}' \u2014 multi-param.
        assertEquals(
                new LambdaFunction(
                        List.of(new Variable("p"), new Variable("q")),
                        List.of(nary("plus",
                                new Variable("p"), new Variable("q")))),
                com.legend.testing.Platform.spec("{p, q | $p + $q}"));
    }

    @Test
    void bracedZeroParamLambda() {
        // '{| 42}' \u2014 zero parameters. params list is empty;
        // body is a single statement.
        assertEquals(
                new LambdaFunction(List.of(), List.of(new CInteger(42L))),
                com.legend.testing.Platform.spec("{| 42}"));
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
                                nary("plus",
                                        new Variable("x"), new CInteger(1L)))),
                com.legend.testing.Platform.spec("{p | let x = $p; $x + 1;}"));
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
                com.legend.testing.Platform.spec("p | $p.age > 21"));
    }

    @Test
    void zeroParamPipeLambda() {
        // '| 42' \u2014 PIPE at start, zero params.
        assertEquals(
                new LambdaFunction(List.of(), List.of(new CInteger(42L))),
                com.legend.testing.Platform.spec("| 42"));
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
                                List.of(nary("plus",
                                        new Variable("p"),
                                        new Variable("q")))))),
                com.legend.testing.Platform.spec("{p | {q | $p + $q}}"));
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
                                List.of(nary("plus",
                                        new Variable("p"),
                                        new CInteger(1L)))))),
                com.legend.testing.Platform.spec("let f = {p | $p + 1}"));
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
                                List.of(nary("plus",
                                        new Variable("q"),
                                        new CInteger(1L)))))),
                com.legend.testing.Platform.spec("[{p | $p}, {q | $q + 1}]"));
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
                                        nary("plus",
                                                new Variable("p"),
                                                new CInteger(1L)))),
                                new AppliedFunction("letFunction", List.of(
                                        new CString("y"),
                                        nary("times",
                                                new Variable("x"),
                                                new CInteger(2L)))),
                                new Variable("y"))),
                com.legend.testing.Platform.spec(
                        "{p | let x = $p + 1; let y = $x * 2; $y;}"));
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
        List<ValueSpecification> stmts = com.legend.testing.Platform.block(
                "let f = {p | $p + 1}; let g = {q | $q * 2}; $f;");
        assertEquals(
                List.of(
                        new AppliedFunction("letFunction", List.of(
                                new CString("f"),
                                new LambdaFunction(
                                        List.of(new Variable("p")),
                                        List.of(nary("plus",
                                                new Variable("p"),
                                                new CInteger(1L)))))),
                        new AppliedFunction("letFunction", List.of(
                                new CString("g"),
                                new LambdaFunction(
                                        List.of(new Variable("q")),
                                        List.of(nary("times",
                                                new Variable("q"),
                                                new CInteger(2L)))))),
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
                com.legend.testing.Platform.spec("$opt->orElse(| 42)"));
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
                        new AppliedFunction("getAll", List.of(
                                new PackageableElementPtr("Person"))),
                        new LambdaFunction(
                                List.of(new Variable("p")),
                                List.of(new AppliedFunction("greaterThan", List.of(
                                        new AppliedProperty(new Variable("p"), "age"),
                                        new CInteger(21L))))))),
                com.legend.testing.Platform.spec("Person.all()->filter(p | $p.age > 21)"));
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
                                "p", nr("Integer"), Multiplicity.Concrete.PURE_ONE)),
                        List.of(new Variable("p"))),
                com.legend.testing.Platform.spec("{p: Integer[1] | $p}"));
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
                                new Variable("p", nr("Integer"),
                                        Multiplicity.Concrete.PURE_ONE),
                                new Variable("q", nr("String"),
                                        Multiplicity.Concrete.ZERO_MANY)),
                        List.of(nary("plus",
                                new Variable("p"), new Variable("q")))),
                com.legend.testing.Platform.spec("{p: Integer[1], q: String[*] | $p + $q}"));
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
                                "p", nr("Integer"), Multiplicity.Concrete.PURE_ONE)),
                        List.of(nary("plus",
                                new Variable("p"), new CInteger(1L)))),
                com.legend.testing.Platform.spec("p: Integer[1] | $p + 1"));
    }

    @Test
    void typedLambdaWithQualifiedTypeName() {
        // Type can be a qualified name. Stored verbatim in
        // Variable.typeName for downstream resolution.
        assertEquals(
                new LambdaFunction(
                        List.of(new Variable(
                                "p", nr("my::pkg::Person"),
                                Multiplicity.Concrete.PURE_ONE)),
                        List.of(new AppliedProperty(
                                new Variable("p"), "age"))),
                com.legend.testing.Platform.spec("{p: my::pkg::Person[1] | $p.age}"));
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
                                "p", tg("Pair", nr("Integer"), nr("String")),
                                Multiplicity.Concrete.PURE_ONE)),
                        List.of(new AppliedProperty(
                                new Variable("p"), "first"))),
                com.legend.testing.Platform.spec("{p: Pair<Integer, String>[1] | $p.first}"));
    }

    @Test
    void multiplicityFormsProduceCorrectConcreteValues() {
        // Pin each surface form's mapping to a Concrete record. The
        // four well-known constants (PURE_ONE, ZERO_ONE, PURE_MANY,
        // ZERO_MANY) plus the explicit bounded form.
        assertEquals(
                Multiplicity.Concrete.PURE_ONE,
                ((Variable) ((LambdaFunction) com.legend.testing.Platform.spec(
                        "{p: T[1] | $p}")).parameters().get(0)).multiplicity());
        assertEquals(
                Multiplicity.Concrete.ZERO_ONE,
                ((Variable) ((LambdaFunction) com.legend.testing.Platform.spec(
                        "{p: T[0..1] | $p}")).parameters().get(0)).multiplicity());
        assertEquals(
                Multiplicity.Concrete.PURE_MANY,
                ((Variable) ((LambdaFunction) com.legend.testing.Platform.spec(
                        "{p: T[1..*] | $p}")).parameters().get(0)).multiplicity());
        assertEquals(
                Multiplicity.Concrete.ZERO_MANY,
                ((Variable) ((LambdaFunction) com.legend.testing.Platform.spec(
                        "{p: T[*] | $p}")).parameters().get(0)).multiplicity());
        // Explicit bounded form not covered by a constant.
        assertEquals(
                new Multiplicity.Concrete(3, 7),
                ((Variable) ((LambdaFunction) com.legend.testing.Platform.spec(
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
                                "p", nr("T"), new Multiplicity.Parameter("m"))),
                        List.of(new Variable("p"))),
                com.legend.testing.Platform.spec("{p: T[m] | $p}"));
    }

    @Test
    void typedLambdaInArrowArgumentPosition() {
        // The realistic case: typed predicate as a filter argument.
        // Combines C.2 (arrow + filter call), C.5 (typed shorthand
        // lambda inside the arg list).
        assertEquals(
                new AppliedFunction("filter", List.of(
                        new AppliedFunction("getAll", List.of(
                                new PackageableElementPtr("Person"))),
                        new LambdaFunction(
                                List.of(new Variable(
                                        "p", nr("Person"),
                                        Multiplicity.Concrete.PURE_ONE)),
                                List.of(new AppliedFunction("greaterThan", List.of(
                                        new AppliedProperty(new Variable("p"), "age"),
                                        new CInteger(21L))))))),
                com.legend.testing.Platform.spec(
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
                () -> com.legend.testing.Platform.spec("{p: Integer | $p}"));
        // Error comes from the shared TypeExpressionParser when its
        // parseMultiplicity sees a non-'[' token after the type.
        assertTrue(String.valueOf(ex.getMessage()).contains("BRACKET_OPEN"),
                () -> "want missing-multiplicity error, got: " + ex.getMessage());
    }

    @Test
    void multiplicityWithBadSyntaxRejected() {
        // 'p: T[1..]' \u2014 missing upper bound after '..'.
        ParseException ex = assertThrows(ParseException.class,
                () -> com.legend.testing.Platform.spec("{p: T[1..] | $p}"));
        assertTrue(String.valueOf(ex.getMessage()).contains("after '..' in multiplicity"),
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
                                new Variable("q", nr("String"),
                                        Multiplicity.Concrete.PURE_ONE)),
                        List.of(nary("plus",
                                new Variable("p"), new Variable("q")))),
                com.legend.testing.Platform.spec("{p, q: String[1] | $p + $q}"));
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
                () -> com.legend.testing.Platform.spec("{p: Pair<Integer | $p}"));
        // With multiplicity type parameters supported (Pair<T|m>), the
        // '|' legally OPENS the multiplicity-argument section and '$p'
        // rejects there — still a loud error, never a silent truncation
        // of the type name.
        assertTrue(String.valueOf(ex.getMessage()).contains("multiplicity argument"),
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
                () -> com.legend.testing.Platform.spec("{p: T[] | $p}"));
        assertTrue(String.valueOf(ex.getMessage()).contains("multiplicity bound or parameter"),
                () -> "want empty-multiplicity error, got: " + ex.getMessage());
    }

    @Test
    void multiplicityUpperLessThanLowerAcceptedLikeEngine() {
        // [2..1] parses in the engine — bound sanity is the compiler's
        // (deep-audit 1f; the old parse-time check was an invention)
        ValueSpecification v = com.legend.testing.Platform.spec("~c:String[2..1]");
        org.junit.jupiter.api.Assertions.assertNotNull(v);
    }

    @Test
    void lambdaMissingPipeRejected() {
        ParseException ex = assertThrows(ParseException.class,
                () -> com.legend.testing.Platform.spec("{p $p}"));
        assertTrue(String.valueOf(ex.getMessage()).contains("expected '|'"),
                () -> "want missing-pipe error, got: " + ex.getMessage());
    }

    @Test
    void lambdaMissingCloseBraceRejected() {
        ParseException ex = assertThrows(ParseException.class,
                () -> com.legend.testing.Platform.spec("{p | $p"));
        assertTrue(String.valueOf(ex.getMessage()).contains("expected '}'"),
                () -> "want missing-close-brace error, got: " + ex.getMessage());
    }

    @Test
    void lambdaEmptyBodyRejected() {
        // '{| }' is not legal \u2014 lambda body must have at least
        // one statement.
        ParseException ex = assertThrows(ParseException.class,
                () -> com.legend.testing.Platform.spec("{| }"));
        assertTrue(String.valueOf(ex.getMessage()).contains("at least one statement"),
                () -> "want empty-body error, got: " + ex.getMessage());
    }

    // ----- code block entry point (C.4) --------------------------------

    @Test
    void parseCodeBlockSingleStatement() {
        // A code block with one statement is a list of one element;
        // not collapsed to the bare value. Consumers always get a
        // List<ValueSpecification>.
        List<ValueSpecification> stmts = com.legend.testing.Platform.block("42");
        assertEquals(List.of(new CInteger(42L)), stmts);
    }

    @Test
    void parseCodeBlockMultipleStatements() {
        // Statements separated by ';'. Each statement is a full
        // program line (so 'let' is admitted on each, each desugaring
        // independently to its own letFunction call).
        List<ValueSpecification> stmts = com.legend.testing.Platform.block(
                "let x = 1; let y = 2; $x + $y;");
        assertEquals(
                List.of(
                        new AppliedFunction("letFunction", List.of(
                                new CString("x"), new CInteger(1L))),
                        new AppliedFunction("letFunction", List.of(
                                new CString("y"), new CInteger(2L))),
                        nary("plus",
                                new Variable("x"), new Variable("y"))),
                stmts);
    }

    @Test
    void parseCodeBlockTrailingSemicolonAllowed() {
        // Engine permits a trailing ';' after the last statement
        // ('1; 2;' is equivalent to '1; 2'). We match.
        List<ValueSpecification> stmts = com.legend.testing.Platform.block("1; 2;");
        assertEquals(
                List.of(new CInteger(1L), new CInteger(2L)),
                stmts);
    }

    @Test
    void parseCodeBlockEmptyInputIsEmptyList() {
        // No statements at all is a degenerate but well-defined
        // case: empty list, no error.
        assertEquals(List.of(), com.legend.testing.Platform.block(""));
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
                com.legend.testing.Platform.spec("-42"));
    }

    @Test
    void variableNameMayBeAKeyword() {
        // Pure permits identifiers that lex as keyword tokens (let,
        // class, all, etc.) in any position where an identifier is
        // expected. SpecParser borrows TokenStreamCursor.IDENTIFIER_TOKENS
        // for this. Pin the bridge so a future tightening of the
        // keyword list doesn't silently break variable parsing.
        assertEquals(new Variable("let"), com.legend.testing.Platform.spec("$let"));
        assertEquals(new Variable("class"), com.legend.testing.Platform.spec("$class"));
        assertEquals(new Variable("all"), com.legend.testing.Platform.spec("$all"));
    }

    // ----- column builders (C.6): ~col / ~[a, b] / ~name:lambda --------

    @Test
    void bareColumnReference() {
        // '~name' \u2014 the simplest column spec: name only, both
        // lambda slots null. Used in project/select positions where
        // the column passes through unchanged.
        assertEquals(
                new ColSpec("name", null, null),
                com.legend.testing.Platform.spec("~name"));
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
                com.legend.testing.Platform.spec("~total:x|$x.amount"));
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
                com.legend.testing.Platform.spec("~total:x|$x.amount:y|$y->sum()"));
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
                com.legend.testing.Platform.spec("~[name, age, salary]"));
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
                com.legend.testing.Platform.spec("~[name, total:x|$x.amount]"));
    }

    @Test
    void emptyColSpecArrayIsLegalStructurally() {
        // '~[]' \u2014 the parser admits an empty array; rejection
        // (if any) is the type-checker's job. Pin so a future
        // 'require >= 1 element' guard in the parser would fail
        // this test loudly.
        assertEquals(
                new ColSpecArray(List.of()),
                com.legend.testing.Platform.spec("~[]"));
    }

    @Test
    void colSpecArrayTrailingCommaRejected() {
        // Trailing comma is rejected, matching parseCollection and
        // parseArgList conventions. The error phrase is
        // ColSpec-specific so this test cannot crosswire to other
        // trailing-comma cases.
        ParseException ex = assertThrows(ParseException.class,
                () -> com.legend.testing.Platform.spec("~[a, b,]"));
        assertTrue(String.valueOf(ex.getMessage()).contains("ColSpec array"),
                () -> "want ColSpec-array trailing-comma error, got: "
                        + ex.getMessage());
    }

    @Test
    void colSpecArrayUnterminatedRejected() {
        // '~[a, b' \u2014 missing closing ']'. Pin the close-bracket
        // expectation so a future regression couldn't silently
        // consume to EOF.
        ParseException ex = assertThrows(ParseException.class,
                () -> com.legend.testing.Platform.spec("~[a, b"));
        assertTrue(String.valueOf(ex.getMessage()).contains("']'")
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
                com.legend.testing.Platform.spec("~total:{x | $x.amount}"));
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
                                        "x", nr("Integer"),
                                        Multiplicity.Concrete.PURE_ONE)),
                                List.of(new Variable("x"))),
                        null),
                com.legend.testing.Platform.spec("~total:x: Integer[1] | $x"));
    }

    @Test
    void columnSpecAsArrowArgument() {
        // The realistic case: passing a ColSpec to a relation-API
        // function. Combines C.2 (arrow + call), C.6 (column spec).
        // Pin the full nesting so a regression in either layer's
        // emission shape would fail loudly.
        assertEquals(
                new AppliedFunction("project", List.of(
                        new AppliedFunction("getAll", List.of(
                                new PackageableElementPtr("Person"))),
                        new ColSpecArray(List.of(
                                new ColSpec("name"),
                                new ColSpec("age"))))),
                com.legend.testing.Platform.spec("Person.all()->project(~[name, age])"));
    }

    @Test
    void columnSpecMissingNameAfterTildeRejected() {
        // '~' followed by something that isn't an identifier or '['
        // \u2014 e.g. '~123' or '~+'. Pin the error phrase.
        ParseException ex = assertThrows(ParseException.class,
                () -> com.legend.testing.Platform.spec("~123"));
        assertTrue(String.valueOf(ex.getMessage()).contains("column name after '~'"),
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
                com.legend.testing.Platform.spec("~'My Column'"));
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
                com.legend.testing.Platform.spec("~[name, 'Full Name', age]"));
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
                                List.of(nary("times",
                                        new AppliedProperty(new Variable("x"), "amount"),
                                        new CInteger(2L)))),
                        null),
                com.legend.testing.Platform.spec("~total:x|$x.amount * 2"));
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
                com.legend.testing.Platform.spec("~total:{x | $x.amount}:y|$y->sum()"));
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
                                        nary("plus",
                                                new Variable("y"),
                                                new CInteger(2L)))),
                        null),
                com.legend.testing.Platform.spec("~total:{x | let y = 1; $y + 2;}"));
    }

    @Test
    void columnSpecColonAtEndOfInputRejected() {
        // '~name:' \u2014 colon at end-of-input. parseColumnLambda
        // sees EOF and throws explicitly, rather than silently
        // returning a null lambda or some default. Small gap caught
        // by the audit; the existing 'non-lambda after colon' test
        // covers '~name:42' but not '~name:<EOF>'.
        ParseException ex = assertThrows(ParseException.class,
                () -> com.legend.testing.Platform.spec("~name:"));
        assertTrue(String.valueOf(ex.getMessage()).contains("expected lambda")
                        && ex.getMessage().contains("column spec"),
                () -> "want EOF-after-colon error, got: " + ex.getMessage());
    }

    @Test
    void columnSpecColonFollowedByExpressionWrapsAsThunk() {
        // '~name: <expr>' — a non-lambda body wraps into a ZERO-PARAM thunk
        // (the clean-sheet navigate form `~firm: acme::Firm.all()`,
        // MAPPING_CLEAN_SHEET.md §3.1). The checker decides which enclosing
        // calls admit expression bodies; the parser stays uniform.
        ColSpec cs = assertInstanceOf(ColSpec.class, com.legend.testing.Platform.spec("~name:42"));
        assertEquals("name", cs.name());
        assertTrue(cs.function1().parameters().isEmpty(), "expression body = zero-param thunk");
        assertInstanceOf(CInteger.class, cs.function1().body().get(0));
    }

    // ----- type annotations (C.7): @Type / @Relation<(...)> ----------

    @Test
    void simpleNamedTypeAnnotation() {
        // '@Integer' \u2014 the most common form, used as the target
        // type in '$x->cast(@Integer)'. Pin the Named variant with
        // record equality.
        assertEquals(
                new TypeAnnotation.Named(nr("Integer")),
                com.legend.testing.Platform.spec("@Integer"));
    }

    @Test
    void qualifiedNamedTypeAnnotation() {
        // '@my::pkg::Foo' \u2014 fully-qualified type. The
        // PATH_SEPARATOR ('::') flows through parseQualifiedName,
        // and the result is one Named with the full path preserved.
        assertEquals(
                new TypeAnnotation.Named(nr("my::pkg::Foo")),
                com.legend.testing.Platform.spec("@my::pkg::Foo"));
    }

    @Test
    void typeAnnotationWithGenerics() {
        // '@List<Integer>' \u2014 generic-type-args carried verbatim
        // into the typeName by token concatenation. Inter-token
        // whitespace is lost (the documented C.5 quirk). 'List' is
        // not 'Relation' so the structural-shape branch does not
        // fire; we fall through to the depth-tracked angle-bracket
        // collection.
        assertEquals(
                new TypeAnnotation.Named(tg("List", nr("Integer"))),
                com.legend.testing.Platform.spec("@List<Integer>"));
    }

    @Test
    void typeAnnotationWithNestedGenerics() {
        // '@Map<String, List<Integer>>' \u2014 the depth tracker
        // handles nested '<...>' correctly. Pin to catch a regression
        // where the inner '>' would close the outer angle pair
        // prematurely.
        assertEquals(
                new TypeAnnotation.Named(tg("Map", nr("String"), tg("List", nr("Integer")))),
                com.legend.testing.Platform.spec("@Map<String, List<Integer>>"));
    }

    @Test
    void relationShapeAnnotationWithBareColumns() {
        // '@Relation<(name:String, age:Integer)>' \u2014 the
        // structural form: two columns, both bare names, both Named
        // types, no multiplicity. Pin the RelationShape with full
        // record equality.
        assertEquals(
                new TypeAnnotation.RelationShape(List.of(
                        new TypeAnnotation.RelationShape.Column(
                                "name",
                                new TypeAnnotation.Named(nr("String")),
                                null),
                        new TypeAnnotation.RelationShape.Column(
                                "age",
                                new TypeAnnotation.Named(nr("Integer")),
                                null))),
                com.legend.testing.Platform.spec("@Relation<(name:String, age:Integer)>"));
    }

    @Test
    void relationShapeAnnotationWithMultiplicities() {
        // '@Relation<(id:Integer[1], optional:String[0..1])>' \u2014
        // per-column multiplicity. Pin that we PRESERVE multiplicity
        // (engine-lite parses-and-discards; we keep it because
        // engine-pure's M3 metamodel preserves per-column
        // multiplicities on the structural type, and a future
        // relation-type-checker will need them).
        assertEquals(
                new TypeAnnotation.RelationShape(List.of(
                        new TypeAnnotation.RelationShape.Column(
                                "id",
                                new TypeAnnotation.Named(nr("Integer")),
                                Multiplicity.Concrete.PURE_ONE),
                        new TypeAnnotation.RelationShape.Column(
                                "optional",
                                new TypeAnnotation.Named(nr("String")),
                                Multiplicity.Concrete.ZERO_ONE))),
                com.legend.testing.Platform.spec(
                        "@Relation<(id:Integer[1], optional:String[0..1])>"));
    }

    @Test
    void relationShapeAnnotationWithQuotedColumnNames() {
        // Pulled directly from a real engine test fixture:
        // '@Relation<(city:String, '2011__|__newCol':Integer)>'.
        // Pivot-result column names embed punctuation that requires
        // quoting. Pin the quoted-name path inside @Relation.
        assertEquals(
                new TypeAnnotation.RelationShape(List.of(
                        new TypeAnnotation.RelationShape.Column(
                                "city",
                                new TypeAnnotation.Named(nr("String")),
                                null),
                        new TypeAnnotation.RelationShape.Column(
                                "2011__|__newCol",
                                new TypeAnnotation.Named(nr("Integer")),
                                null))),
                com.legend.testing.Platform.spec(
                        "@Relation<(city:String, '2011__|__newCol':Integer)>"));
    }

    @Test
    void relationShapeAnnotationWithWildcardColumn() {
        // '@Relation<(?:?, name:String)>' \u2014 mixed wildcards. The
        // first column has wildcard name AND wildcard type; the
        // second is concrete. Pin both wildcard slots (name=null,
        // type=Wildcard) so the wildcard support survives a future
        // refactor.
        assertEquals(
                new TypeAnnotation.RelationShape(List.of(
                        new TypeAnnotation.RelationShape.Column(
                                null,
                                new TypeAnnotation.Wildcard(),
                                null),
                        new TypeAnnotation.RelationShape.Column(
                                "name",
                                new TypeAnnotation.Named(nr("String")),
                                null))),
                com.legend.testing.Platform.spec("@Relation<(?:?, name:String)>"));
    }

    @Test
    void relationShapeWildcardNameAndTypeAreIndependent() {
        // '@Relation<(?:String, name:?)>' \u2014 the two wildcard
        // slots (name, type) must be independently settable. The
        // existing wildcardColumn test happens to pair both
        // wildcards in the same column ('?:?'), which would pass
        // even if the parser accidentally coupled them. Split here
        // so a future bug where 'name is wildcard \u21D2 type is also
        // wildcard' (or vice versa) fails loudly.
        assertEquals(
                new TypeAnnotation.RelationShape(List.of(
                        new TypeAnnotation.RelationShape.Column(
                                null,
                                new TypeAnnotation.Named(nr("String")),
                                null),
                        new TypeAnnotation.RelationShape.Column(
                                "name",
                                new TypeAnnotation.Wildcard(),
                                null))),
                com.legend.testing.Platform.spec("@Relation<(?:String, name:?)>"));
    }

    @Test
    void relationShapeColumnsAcceptQualifiedAndGenericTypes() {
        // '@Relation<(owner:my::pkg::Firm, tags:List<String>)>'
        // \u2014 column types use the full parseTypeText machinery
        // from C.5. Pin that parseTypeText's qualified-name and
        // generic-type-args paths are both reachable inside a
        // RelationShape column. Catches a regression where
        // parseRelationColumn accidentally used a simpler
        // identifier read (which would reject '::' and '<').
        assertEquals(
                new TypeAnnotation.RelationShape(List.of(
                        new TypeAnnotation.RelationShape.Column(
                                "owner",
                                new TypeAnnotation.Named(nr("my::pkg::Firm")),
                                null),
                        new TypeAnnotation.RelationShape.Column(
                                "tags",
                                new TypeAnnotation.Named(tg("List", nr("String"))),
                                null))),
                com.legend.testing.Platform.spec(
                        "@Relation<(owner:my::pkg::Firm, tags:List<String>)>"));
    }

    @Test
    void emptyRelationShapeIsLegalStructurally() {
        // '@Relation<()>' \u2014 admitted by the parser; rejection
        // (if any) is the type-checker's job. Pin so a future guard
        // (e.g. require >= 1 column) doesn't silently break this.
        assertEquals(
                new TypeAnnotation.RelationShape(List.of()),
                com.legend.testing.Platform.spec("@Relation<()>"));
    }

    @Test
    void typeAnnotationAsCastArgument() {
        // The canonical use: '$x->cast(@Integer)'. Combines C.2
        // (arrow + call), C.7 (type annotation as arg). Pin the
        // full nested AST so a regression in either emission shape
        // would fail loudly.
        assertEquals(
                new AppliedFunction("cast", List.of(
                        new Variable("x"),
                        new TypeAnnotation.Named(nr("Integer")))),
                com.legend.testing.Platform.spec("$x->cast(@Integer)"));
    }

    @Test
    void relationShapeAsCastArgument() {
        // Realistic relation-cast usage modelled on the pivot test
        // fixture: cast the result of a pivot to a known relation
        // shape so downstream stages have static column info.
        assertEquals(
                new AppliedFunction("cast", List.of(
                        new Variable("rel"),
                        new TypeAnnotation.RelationShape(List.of(
                                new TypeAnnotation.RelationShape.Column(
                                        "city",
                                        new TypeAnnotation.Named(nr("String")),
                                        null),
                                new TypeAnnotation.RelationShape.Column(
                                        "country",
                                        new TypeAnnotation.Named(nr("String")),
                                        null))))),
                com.legend.testing.Platform.spec(
                        "$rel->cast(@Relation<(city:String, country:String)>)"));
    }

    @Test
    void typeAnnotationMissingNameRejected() {
        // '@' followed by something that isn't a type name \u2014
        // e.g. '@123' or '@*'. Pin the error phrase.
        ParseException ex = assertThrows(ParseException.class,
                () -> com.legend.testing.Platform.spec("@123"));
        assertTrue(String.valueOf(ex.getMessage()).contains("type name after '@'"),
                () -> "want missing-type-name error, got: " + ex.getMessage());
    }

    @Test
    void relationShapeUnterminatedRejected() {
        // '@Relation<(a:Integer' \u2014 missing ')>' close. Pin the
        // explicit error path so a future regression couldn't
        // silently consume to EOF.
        ParseException ex = assertThrows(ParseException.class,
                () -> com.legend.testing.Platform.spec("@Relation<(a:Integer"));
        assertTrue(String.valueOf(ex.getMessage()).contains("')'")
                        && ex.getMessage().contains("@Relation"),
                () -> "want unterminated-relation error, got: "
                        + ex.getMessage());
    }

    @Test
    void relationShapeTrailingCommaRejected() {
        // '@Relation<(a:Integer,)>' \u2014 trailing comma. Specific
        // ColSpec-style error phrase so this cannot crosswire with
        // ColSpec-array trailing-comma errors.
        ParseException ex = assertThrows(ParseException.class,
                () -> com.legend.testing.Platform.spec("@Relation<(a:Integer,)>"));
        assertTrue(String.valueOf(ex.getMessage()).contains("trailing comma")
                        && ex.getMessage().contains("@Relation"),
                () -> "want trailing-comma error, got: " + ex.getMessage());
    }

    @Test
    void relationShapeMissingColonRejected() {
        // '@Relation<(a Integer)>' \u2014 missing ':' between column
        // name and type. Pin the explicit error vs silent
        // consumption.
        ParseException ex = assertThrows(ParseException.class,
                () -> com.legend.testing.Platform.spec("@Relation<(a Integer)>"));
        assertTrue(String.valueOf(ex.getMessage()).contains("':'")
                        && ex.getMessage().contains("@Relation"),
                () -> "want missing-colon error, got: " + ex.getMessage());
    }

    @Test
    void unterminatedTypeArgumentInAnnotationRejected() {
        // '@List<Integer' \u2014 missing closing '>'. Pin that the
        // depth-tracker reaches the EOF guard and throws.
        ParseException ex = assertThrows(ParseException.class,
                () -> com.legend.testing.Platform.spec("@List<Integer"));
        // parseTypeArguments delegates the closing '>' check directly;
        // the missing token surfaces as a 'close type arguments' error.
        assertTrue(String.valueOf(ex.getMessage()).contains("close type arguments"),
                () -> "want unterminated-generics error, got: "
                        + ex.getMessage());
    }

    // ----- C.7a: EnumValue (MyEnum.VALUE) ------------------------------

    @Test
    void enumValueFromPackageableElementReceiver() {
        // 'JoinKind.INNER' \u2014 the receiver is a bare qualified
        // name (parsed as PackageableElementPtr), and the dot
        // postfix with no trailing '(' is structurally an enum
        // value reference. Engine-lite emits EnumValue; we must
        // match so downstream dispatch on AST shape stays uniform.
        assertEquals(
                new EnumValue("JoinKind", "INNER"),
                com.legend.testing.Platform.spec("JoinKind.INNER"));
    }

    @Test
    void enumValueWithQualifiedType() {
        // 'my::pkg::Status.ACTIVE' \u2014 fully qualified enum type
        // preserved verbatim in EnumValue.fullPath.
        assertEquals(
                new EnumValue("my::pkg::Status", "ACTIVE"),
                com.legend.testing.Platform.spec("my::pkg::Status.ACTIVE"));
    }

    @Test
    void enumValueDoesNotFireOnMethodCall() {
        // 'Person.all()' must NOT be an EnumValue \u2014 the trailing
        // '(' commits to a method-call path, which is the getAll
        // special dispatch. Pin the discriminator so a regression
        // that collapses '.name()' into EnumValue couldn't slip.
        assertEquals(
                new AppliedFunction("getAll",
                        List.of(new PackageableElementPtr("Person"))),
                com.legend.testing.Platform.spec("Person.all()"));
    }

    // ----- C.7a: bracket postfix ($x[0], $x['key']) --------------------

    @Test
    void bracketIndexIntegerDesugarsToAt() {
        // '$x[0]' \u2014 integer index desugars to the stdlib 'at'
        // function. Engine-lite shape: AppliedFunction("at",
        // [receiver, CInteger]). Use the stdlib path rather than a
        // dedicated IndexAccess AST node so overload resolution
        // reuses the existing AppliedFunction machinery.
        assertEquals(
                new AppliedFunction("at", List.of(
                        new Variable("x"),
                        new CInteger(0L))),
                com.legend.testing.Platform.spec("$x[0]"));
    }

    @Test
    void bracketIndexStringDesugarsToAppliedProperty() {
        // '$x[\\'key\\']' \u2014 string key is sugar for a property
        // access whose name may contain punctuation. Engine-lite
        // emits AppliedProperty(receiver, key) \u2014 same as the
        // quoted-property form '$x.\\'key\\''.
        assertEquals(
                new AppliedProperty(new Variable("x"), "key"),
                com.legend.testing.Platform.spec("$x['key']"));
    }

    @Test
    void bracketIndexChainsOntoPostfix() {
        // '$x.items[0]' \u2014 bracket postfix applies after a dot
        // postfix. Pin the left-associative composition: first the
        // property access, then the 'at' call on the resulting
        // collection.
        assertEquals(
                new AppliedFunction("at", List.of(
                        new AppliedProperty(new Variable("x"), "items"),
                        new CInteger(0L))),
                com.legend.testing.Platform.spec("$x.items[0]"));
    }

    @Test
    void bracketIndexRejectsNonLiteral() {
        // '$x[$i]' \u2014 a variable inside brackets is rejected;
        // the grammar only admits INTEGER or STRING literals here.
        ParseException ex = assertThrows(ParseException.class,
                () -> com.legend.testing.Platform.spec("$x[$i]"));
        assertTrue(String.valueOf(ex.getMessage()).contains("integer or string"),
                () -> "want bracket-index error, got: " + ex.getMessage());
    }

    @Test
    void bracketIndexRejectsUnterminated() {
        // '$x[0' \u2014 missing ']'.
        ParseException ex = assertThrows(ParseException.class,
                () -> com.legend.testing.Platform.spec("$x[0"));
        assertTrue(String.valueOf(ex.getMessage()).contains("']'"),
                () -> "want unterminated-bracket error, got: " + ex.getMessage());
    }

    // ----- C.7a: .all()/.allVersions()/milestoning ---------------------

    @Test
    void classAllWithMilestoningDate() {
        // 'Person.all(%2024-01-01)' \u2014 business-milestoned
        // snapshot. Second argument is the milestoning date.
        assertEquals(
                new AppliedFunction("getAll", List.of(
                        new PackageableElementPtr("Person"),
                        new CDate(new PureDateLiteral.StrictDate(2024, 1, 1)))),
                com.legend.testing.Platform.spec("Person.all(%2024-01-01)"));
    }

    @Test
    void classAllWithBiTemporalMilestoning() {
        // 'Person.all(%2024-01-01, %latest)' \u2014 bi-temporal:
        // business date + processing date (%latest). Pin both
        // milestoning args and the ordered pair shape.
        assertEquals(
                new AppliedFunction("getAll", List.of(
                        new PackageableElementPtr("Person"),
                        new CDate(new PureDateLiteral.StrictDate(2024, 1, 1)),
                        new CLatestDate())),
                com.legend.testing.Platform.spec("Person.all(%2024-01-01, %latest)"));
    }

    @Test
    void classAllWithVariableMilestoning() {
        // 'Person.all($asOfDate)' \u2014 milestoning can be a
        // variable (bound from enclosing let / parameter).
        assertEquals(
                new AppliedFunction("getAll", List.of(
                        new PackageableElementPtr("Person"),
                        new Variable("asOfDate"))),
                com.legend.testing.Platform.spec("Person.all($asOfDate)"));
    }

    @Test
    void classAllVersions() {
        // '.allVersions()' \u2014 all milestoned snapshots.
        assertEquals(
                new AppliedFunction("getAllVersions",
                        List.of(new PackageableElementPtr("Person"))),
                com.legend.testing.Platform.spec("Person.allVersions()"));
    }

    @Test
    void classAllVersionsInRange() {
        // '.allVersionsInRange(start, end)'.
        assertEquals(
                new AppliedFunction("getAllVersionsInRange", List.of(
                        new PackageableElementPtr("Person"),
                        new CDate(new PureDateLiteral.StrictDate(2024, 1, 1)),
                        new CDate(new PureDateLiteral.StrictDate(2024, 12, 31)))),
                com.legend.testing.Platform.spec("Person.allVersionsInRange(%2024-01-01, %2024-12-31)"));
    }

    @Test
    void classAllWithExpressionMilestoningArgAccepted() {
        // engine allFunction args are plain expressions — Person.all(now())
        // is engine-legal (adversarial audit divergence 4, oracle-verified)
        ValueSpecification v = com.legend.testing.Platform.spec("my::Person.all(now())");
        org.junit.jupiter.api.Assertions.assertNotNull(v);
    }

    // ----- C.7a: unit names (Mass~kilogram) ----------------------------

    @Test
    void unitNameAfterQualifiedName() {
        // 'Mass~kilogram' \u2014 unit reference. PackageableElementPtr
        // whose fullPath embeds the unit marker verbatim.
        assertEquals(
                new PackageableElementPtr("Mass~kilogram"),
                com.legend.testing.Platform.spec("Mass~kilogram"));
    }

    @Test
    void qualifiedUnitName() {
        // 'my::pkg::Mass~kilogram'.
        assertEquals(
                new PackageableElementPtr("my::pkg::Mass~kilogram"),
                com.legend.testing.Platform.spec("my::pkg::Mass~kilogram"));
    }

    // ----- C.7a: CFloat -> CDecimal precision promotion ----------------

    @Test
    void floatWithinDoublePrecisionStaysCFloat() {
        // '1.5' round-trips exactly through double. Pin CFloat.
        assertEquals(new CFloat(1.5), com.legend.testing.Platform.spec("1.5"));
    }

    @Test
    void floatExceedingDoublePrecisionIsDialectSplit() {
        // '1.0000000000000001' — double rounds this to 1.0. DIALECT SPLIT
        // (both sides oracle-verified 2026-08-12; RE-GROUNDED B8
        // 2026-08-27 from the reference SOURCE): legend-pure's
        // interpreted Float primitive IS BigDecimal-backed
        // (FloatCoreInstance extends PrimitiveCoreInstance<BigDecimal>;
        // newFloatCoreInstance(String) parses the source text), so the
        // execution surfaces keep the exact digits ON THE FLOAT — the
        // old promotion to CDecimal made the TYPE lie to keep the
        // VALUE. The ENGINE surface builds the bare double like
        // DomainParseTreeWalker (float 1.0 on the wire, probed).
        assertEquals(
                new CFloat(1.0, new BigDecimal("1.0000000000000001"), null),
                com.legend.testing.Platform.spec("1.0000000000000001"));
        assertEquals(new CFloat(1.0), SpecParser.parse(
                "1.0000000000000001", com.legend.parser.Dialect.LEGEND_ENGINE));
    }

    // ----- C.7a (RETIRED): the inline 'comparator' expression ----------

    @Test
    void comparatorInlineExpressionIsDeleted() {
        // 'comparator(a:T[1], b:T[1]): R[1] { body }' was an engine-lite
        // INVENTION — a PRINT-ONLY form of legend-pure's
        // printFunctionDefinition that no real parser (engine or M3)
        // accepts. Deleted per the invention census (D2, 2026-08-14);
        // the PCT adapter now prints typed LAMBDAS for function-valued
        // arguments. 'comparator' is an ordinary identifier again.
        assertThrows(ParseException.class, () ->
                com.legend.testing.Platform.spec(
                        "comparator(a: Integer[1], b: Integer[1]):"
                                + " Bool[1] { $a - $b }"));
        // and as a plain identifier it still works
        assertEquals(new Variable("comparator"),
                com.legend.testing.Platform.spec("$comparator"));
    }

    // ----- C.7a: TDS literal -------------------------------------------

    @Test
    void tdsLiteralDesugarsToTdsCall() {
        // '#TDS name, age\\n alice, 30 #' \u2014 the lexer aggregates
        // this whole block into a single TDS_LITERAL token. The
        // parser emits 'tds("TDS", rawText)' so the stdlib tds
        // function resolves the overload.
        String src = "#TDS\n  name, age\n  alice, 30\n#";
        ValueSpecification result = com.legend.testing.Platform.spec(src);
        // Assertion on the structural shape rather than the full
        // raw text (which contains the entire source chunk); the
        // first argument 'TDS' is the discriminator.
        // since the wire claim (2026-08-10), the node is a TdsLiteral
        // carrying BOTH the wire's tdsString and the desugared tds() call
        // the compiler consumes (the resolver dissolves it, like PathLiteral)
        assertTrue(result instanceof com.legend.protocol.spec.TdsLiteral tl
                        && tl.desugared().function().equals("meta::legend::lite::tds")
                        && tl.desugared().parameters().size() == 2
                        && tl.desugared().parameters().get(0)
                                .equals(new CString("TDS")),
                () -> "want TdsLiteral over tds() call, got: " + result);
    }

    // ----- C.7b: DSL islands (graph-fetch + table reference) -----------

    @Test
    void graphFetchTreeSimpleFlatProperties() {
        // '#{Person {name, age}}#' — a flat graph-fetch tree: the root
        // class and one node per property, in order.
        assertEquals(tree("Person", gfNode("name"), gfNode("age")),
                com.legend.testing.Platform.spec("#{Person {name, age}}#"));
    }

    @Test
    void graphFetchTreeNested() {
        // '#{Person {name, firm {legalName}}}#' — nested tree: 'firm'
        // carries its own nodes.
        assertEquals(tree("Person", gfNode("name"), gfNode("firm", gfNode("legalName"))),
                com.legend.testing.Platform.spec("#{Person {name, firm {legalName}}}#"));
    }

    /** A tree of property nodes (equality ignores source positions). */
    private static com.legend.protocol.spec.GraphFetchLiteral tree(String cls,
            com.legend.protocol.spec.GraphFetchLiteral.Node... nodes) {
        return new com.legend.protocol.spec.GraphFetchLiteral(cls, List.of(nodes), List.of(), null);
    }

    private static com.legend.protocol.spec.GraphFetchLiteral.Node gfNode(String property,
            com.legend.protocol.spec.GraphFetchLiteral.Node... children) {
        return new com.legend.protocol.spec.GraphFetchLiteral.Node(property, null, List.of(), false,
                null, null, List.of(children));
    }

    @Test
    void graphFetchTreeWithAlias() {
        // '#{Person {\'alias\': name}}# — the leading quoted-string +
        // colon is a graph alias; the engine serializes the node under
        // it, so the node CARRIES it (task #78).
        assertEquals(tree("Person", new com.legend.protocol.spec.GraphFetchLiteral.Node(
                        "name", null, List.of(), false, "alias", null, List.of())),
                com.legend.testing.Platform.spec("#{Person {'alias': name}}#"));
    }

    @Test
    void graphFetchTreeWithPropertyParameters() {
        // '#{Person {name(%2024-01-01)}}# — property call args parse as
        // REAL expressions and ride the node (qualifier args inline
        // derived bodies, task #78); the CHECKER still drops them for
        // non-derived properties (milestoning threading is its own
        // feature). The node records its parentheses.
        var gf = assertInstanceOf(com.legend.protocol.spec.GraphFetchLiteral.class,
                com.legend.testing.Platform.spec("#{Person {name(%2024-01-01)}}#"));
        var n = gf.subTrees().get(0);
        assertEquals("name", n.property());
        assertTrue(n.qualified());
        assertEquals(new PureDateLiteral.StrictDate(2024, 1, 1),
                assertInstanceOf(CDate.class, n.parameters().get(0)).value());
    }

    @Test
    void graphFetchTreeTrailingCommaTolerated() {
        // '#{Person {name, age,}}#' — trailing comma in a
        // graph-fetch definition is tolerated per engine-lite.
        // Pins the lenient-termination behaviour so a future
        // tightening doesn't silently break compatibility.
        var gf = assertInstanceOf(com.legend.protocol.spec.GraphFetchLiteral.class,
                com.legend.testing.Platform.spec("#{Person {name, age,}}#"));
        assertEquals(2, gf.subTrees().size());
    }

    @Test
    void graphFetchTreeSubtypes() {
        // root ->subType(@X) {…} entries and a property's prop->subType(@X) {…} view;
        // a subType entry below the root refuses, as the engine's grammar does
        var gf = assertInstanceOf(com.legend.protocol.spec.GraphFetchLiteral.class,
                com.legend.testing.Platform.spec(
                        "#{Firm {legalName, owner->subType(@Person) {name}, ->subType(@Bank) {swift}}}#"));
        assertEquals("Person", gf.subTrees().get(1).subType());
        assertEquals(List.of(gfNode("name")), gf.subTrees().get(1).subTrees());
        assertEquals("Bank", gf.subTypeTrees().get(0).subTypeClass());
        assertEquals(List.of(gfNode("swift")), gf.subTypeTrees().get(0).subTrees());
        ParseException below = assertThrows(ParseException.class,
                () -> com.legend.testing.Platform.spec("#{Firm {owner {->subType(@Person) {name}}}}#"));
        assertTrue(String.valueOf(below.getMessage()).contains("supported only at root level"),
                () -> "got: " + below.getMessage());
    }

    @Test
    void graphFetchTreeSpansAreAbsolute() {
        // graph-fetch spans are the document's, not the island's: the tree re-lexes
        // its slice laid at its real line and column (the wire's sourceInformation)
        var gf = assertInstanceOf(com.legend.protocol.spec.GraphFetchLiteral.class,
                com.legend.testing.Platform.spec("\n  #{Person {\n    name($x)}}#"));
        assertEquals(new com.legend.protocol.SourceInfo("", 2, 5, 2, 10), gf.pos());
        var n = gf.subTrees().get(0);
        assertEquals(new com.legend.protocol.SourceInfo("", 3, 5, 3, 8), n.pos());
        assertEquals(new com.legend.protocol.SourceInfo("", 3, 10, 3, 11),
                assertInstanceOf(Variable.class, n.parameters().get(0)).pos());
    }

    @Test
    void tableReferenceDsl() {
        // '#>{my::db.CUSTOMER}#' \u2014 table reference DSL. The
        // content is split on the LAST '.': db = 'my::db', table
        // = 'CUSTOMER'. The db arg is emitted as a typed
        // PackageableElementPtr (an FQN reference) so the resolver
        // and downstream layers treat it uniformly with every other
        // element reference; the table name stays a CString because
        // it is a physical-DB identifier, not a Pure FQN.
        assertEquals(
                new AppliedFunction("tableReference", List.of(
                        new PackageableElementPtr("my::db"),
                        new CString("CUSTOMER"))),
                com.legend.testing.Platform.spec("#>{my::db.CUSTOMER}#"));
    }

    @Test
    void tableReferenceWithoutDotRejected() {
        // '#>{no_table}#' \u2014 no '.' means we can't split into
        // db and table. Engine-lite throws; we match.
        // REFUTED by the inline-snippet corpus: a store-only reference
        // (#>{my::Store}#) is legal — ONE path element, no table split.
        AppliedFunction af = assertInstanceOf(AppliedFunction.class,
                com.legend.testing.Platform.spec("#>{no_table}#"));
        assertEquals("tableReference", af.function());
        assertEquals(1, af.parameters().size());
    }

    @Test
    void graphFetchFollowedByArrowChain() {
        // '#{Person {name}}#->serialize()' — the ENGINE island closer
        // ('}#') followed by an ordinary arrow chain. The engine-lite
        // '}->' fused exit was an INVENTION and is DELETED (2026-08-14);
        // the invented spelling now refuses like the real engine.
        ValueSpecification result = com.legend.testing.Platform.spec(
                "#{Person {name}}#->serialize()");
        assertTrue(result instanceof AppliedFunction af
                        && af.function().equals("serialize")
                        && af.parameters().size() == 1
                        && af.parameters().get(0)
                                instanceof com.legend.protocol.spec.GraphFetchLiteral gf
                        && gf.subTrees().size() == 1,
                () -> "want serialize(graphFetchTree), got: " + result);
    }

    @Test
    void graphFetchRejectsUnknownDslType() {
        // PLATFORM: an identifier kind is m3's LEGACY graph-fetch
        // spelling (#x{content}# == #{x{content}}#) \u2014 it parses (A5
        // burn 2026-08-15); a NON-identifier kind still refuses.
        ParseException ex = assertThrows(ParseException.class,
                () -> com.legend.testing.Platform.spec("#9x{content}#"));
        assertTrue(String.valueOf(ex.getMessage()).contains("DSL")
                        || String.valueOf(ex.getMessage()).contains("unlexable"),
                () -> "want unknown-DSL error, got: " + ex.getMessage());
        // (engine/lite refusal of the legacy spelling is pinned by the
        // corpus sweep's both-reject rows and RejectionParity — an
        // inline negative snippet here would be harvested by the
        // own-dialect census and misread as misplaced machinery)
    }

    // ----- C.7b: engine-lite parity (copy-with-update + dotted keys) ---

    @Test
    void copyWithUpdateSingleField() {
        // '^$existing(name=\\'new\\')' \u2014 copy-with-update form.
        // Receiver is Variable(existing) (not PackageableElementPtr);
        // NewInstance.className is empty because the class is
        // recovered from the variable's static type at type-check
        // time. Function name stays 'new' for engine-lite binding
        // parity.
        List<NewInstance.KeyBinding> props = List.of(
                new NewInstance.KeyBinding("name",
                        new KeyExpression(new CString("new"), false, false)));
        assertEquals(
                new AppliedFunction("new", List.of(
                        new Variable("existing"),
                        new NewInstance("", List.of(), props))),
                com.legend.testing.Platform.spec("^$existing(name='new')"));
    }

    @Test
    void copyWithUpdateWithPlusEquals() {
        // '^$list(items+=$x)' \u2014 copy-with-update with '+='
        // append-form binding. KeyExpression.isAdd must be preserved
        // so the type-checker can distinguish the append form.
        List<NewInstance.KeyBinding> props = new java.util.ArrayList<>();
        props.add(new NewInstance.KeyBinding("items", new KeyExpression(new Variable("x"), true, false)));
        assertEquals(
                new AppliedFunction("new", List.of(
                        new Variable("list"),
                        new NewInstance("", List.of(), props))),
                com.legend.testing.Platform.spec("^$list(items+=$x)"));
    }

    @Test
    void copyWithUpdateMissingVarNameRejected() {
        // '^$(' \u2014 missing variable name after '^$'. Must error
        // at parse time with a specific message pinning the
        // copy-with-update context.
        ParseException ex = assertThrows(ParseException.class,
                () -> com.legend.testing.Platform.spec("^$(foo=1)"));
        assertTrue(String.valueOf(ex.getMessage()).contains("copy-with-update"),
                () -> "want copy-with-update error, got: " + ex.getMessage());
    }

    @Test
    void newInstanceDottedPropertyKey() {
        // '^Foo(addr.city = \\'NYC\\')' \u2014 dotted property path
        // for atomic nested-field update. Key is joined with '.'
        // into a single map key; TypeChecker walks the chain
        // against the class's declared properties.
        List<NewInstance.KeyBinding> props = new java.util.ArrayList<>();
        props.add(new NewInstance.KeyBinding("addr.city", new KeyExpression(new CString("NYC"), false, false)));
        assertEquals(
                new AppliedFunction("new", List.of(
                        new PackageableElementPtr("Foo"),
                        new NewInstance("Foo", List.of(), props))),
                com.legend.testing.Platform.spec("^Foo(addr.city = 'NYC')"));
    }

    @Test
    void newInstanceDeepDottedPropertyKey() {
        // '^Foo(a.b.c.d = 1)' \u2014 arbitrary-depth dotted path.
        List<NewInstance.KeyBinding> props = new java.util.ArrayList<>();
        props.add(new NewInstance.KeyBinding("a.b.c.d", new KeyExpression(new CInteger(1L), false, false)));
        assertEquals(
                new AppliedFunction("new", List.of(
                        new PackageableElementPtr("Foo"),
                        new NewInstance("Foo", List.of(), props))),
                com.legend.testing.Platform.spec("^Foo(a.b.c.d = 1)"));
    }

    @Test
    void letWithQuotedVariableName() {
        // 'let \\'my var\\' = 42' \u2014 quoted let-var name lets
        // users bind variables whose surface spelling isn't a
        // legal Pure identifier (whitespace, punctuation). The
        // AppliedFunction("letFunction", ...) carries the
        // UNQUOTED name as a CString, matching engine-lite's
        // parseIdentifierText behaviour.
        assertEquals(
                new AppliedFunction("letFunction", List.of(
                        new CString("my var"),
                        new CInteger(42L))),
                com.legend.testing.Platform.spec("let 'my var' = 42"));
    }

    @Test
    void copyWithUpdateDottedKey() {
        // '^$x(addr.city = \\'NYC\\')' \u2014 the two features compose.
        // Copy-with-update receiver + dotted property key.
        List<NewInstance.KeyBinding> props = new java.util.ArrayList<>();
        props.add(new NewInstance.KeyBinding("addr.city", new KeyExpression(new CString("NYC"), false, false)));
        assertEquals(
                new AppliedFunction("new", List.of(
                        new Variable("x"),
                        new NewInstance("", List.of(), props))),
                com.legend.testing.Platform.spec("^$x(addr.city = 'NYC')"));
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
                () -> com.legend.testing.Platform.spec(slice));
        assertEquals(2, ex.line(),
                () -> "error in slice should report ORIGINAL line 2, got: " + ex.getMessage());
    }
    @Test
    void rowStructTypedLambdaParam() {
        // The PCT fully-annotated form: a shorthand lambda whose parameter
        // carries a BARE row-struct type — filter(x: (id:Integer,
        // v:String)[1]|...). The typed-lambda LOOKAHEAD must accept a
        // paren-opened type (parseType already owns the grammar); before,
        // '(id:' was misread as a parenthesised expression.
        var vs = com.legend.testing.Platform.spec(
                "|#TDS\n id, v\n 1, 2\n#->filter(x: (id:Integer, v:Integer)[1]|$x.v > 1)");
        org.junit.jupiter.api.Assertions.assertNotNull(vs);
    }

    @Test
    void functionTypeTypedLambdaParam() {
        var vs = com.legend.testing.Platform.spec("|apply(f: {Integer[1]->Integer[1]}[1]|$f->eval(1))");
        org.junit.jupiter.api.Assertions.assertNotNull(vs);
    }

}
