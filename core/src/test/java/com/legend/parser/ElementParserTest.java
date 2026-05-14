package com.legend.parser;

import com.legend.parser.element.AssociationDefinition;
import com.legend.parser.element.AssociationDefinition.AssociationEndDefinition;
import com.legend.parser.element.AuthenticationSpec;
import com.legend.parser.element.ClassDefinition;
import com.legend.parser.element.ClassDefinition.ConstraintDefinition;
import com.legend.parser.element.ClassDefinition.DerivedPropertyDefinition;
import com.legend.parser.element.ClassDefinition.ParameterDefinition;
import com.legend.parser.element.ClassDefinition.PropertyDefinition;
import com.legend.parser.element.ConnectionDefinition;
import com.legend.parser.element.ConnectionSpecification;
import com.legend.parser.element.DatabaseDefinition;
import com.legend.parser.element.DatabaseDefinition.ColumnDefinition;
import com.legend.parser.element.DatabaseDefinition.FilterDefinition;
import com.legend.parser.element.DatabaseDefinition.JoinDefinition;
import com.legend.parser.element.DatabaseDefinition.SchemaDefinition;
import com.legend.parser.element.DatabaseDefinition.TableDefinition;
import com.legend.parser.element.DatabaseDefinition.ViewDefinition;
import com.legend.parser.element.EnumDefinition;
import com.legend.parser.element.EnumerationMapping;
import com.legend.parser.element.AssociationMapping;
import com.legend.parser.element.AssociationPropertyMapping;
import com.legend.parser.element.ClassMapping;
import com.legend.parser.element.FilterMapping;
import com.legend.parser.element.FilterPointer;
import com.legend.parser.element.FunctionDefinition;
import com.legend.parser.element.NativeFunctionDefinition;
import com.legend.parser.element.JoinChainElement;
import com.legend.parser.element.MappingDefinition;
import com.legend.parser.element.PropertyMapping;
import com.legend.parser.element.JsonModelConnection;
import com.legend.parser.element.PackageableElement;
import com.legend.parser.element.RelationalOperation;
import com.legend.parser.element.ComparisonOp;
import com.legend.parser.element.RelationalDataType;
import com.legend.parser.element.LogicalOp;
import com.legend.parser.element.RelationalOperation.BooleanOp;
import com.legend.parser.element.RelationalOperation.ColumnRef;
import com.legend.parser.element.RelationalOperation.Comparison;
import com.legend.parser.element.RelationalOperation.FunctionCall;
import com.legend.parser.element.RelationalOperation.Literal;
import com.legend.parser.element.ProfileDefinition;
import com.legend.parser.element.RuntimeDefinition;
import com.legend.parser.element.ServiceDefinition;
import com.legend.parser.element.StereotypeApplication;
import com.legend.parser.element.TaggedValue;
import com.legend.parser.TypeExpression.FunctionType;
import com.legend.parser.TypeExpression.NameRef;
import com.legend.parser.TypeExpression.Op;
import com.legend.parser.spec.AppliedFunction;
import com.legend.parser.spec.AppliedProperty;
import com.legend.parser.spec.CInteger;
import com.legend.parser.spec.CString;
import com.legend.parser.spec.PackageableElementPtr;
import com.legend.parser.spec.Variable;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static com.legend.parser.TypeExpressionFixtures.col;
import static com.legend.parser.TypeExpressionFixtures.nr;
import static com.legend.parser.TypeExpressionFixtures.rel;
import static com.legend.parser.TypeExpressionFixtures.sa;
import static com.legend.parser.TypeExpressionFixtures.tg;
import static com.legend.parser.TypeExpressionFixtures.tp;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link ElementParser} &mdash; sub-slice B.1 covers
 * imports and {@code Class} declarations.
 *
 * <p>Each test compares full records via record-{@code equals} where
 * practical, not piecewise field probes. This catches accidental
 * population of unrelated fields (e.g. parser leaking type params into
 * superclasses) that piecewise checks miss.
 */
final class ElementParserTest {

    // ---------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------

    /** Build the canonical empty/all-defaults shape for a {@code Class FQN {}}. */
    private static ClassDefinition emptyClass(String qn, boolean isNative) {
        return new ClassDefinition(
                qn, List.of(), List.of(), List.of(),
                List.of(), List.of(), List.of(), List.of(), isNative);
    }

    /** Property with no annotations &mdash; the common shape. The
     *  {@code type} argument is a bare type-name string wrapped in a
     *  {@link NameRef}. For structured types (generic / function /
     *  relation / schema-algebra), use
     *  {@link #propT(String, TypeExpression, int, Integer)}. */
    private static PropertyDefinition prop(String name, String type, int lower, Integer upper) {
        return new PropertyDefinition(
                name, new NameRef(type),
                Multiplicity.range(lower, upper),
                List.of(), List.of());
    }

    /** Property carrying a structured (non-NameRef) type. */
    private static PropertyDefinition propT(String name, TypeExpression type, int lower, Integer upper) {
        return new PropertyDefinition(name, type, Multiplicity.range(lower, upper), List.of(), List.of());
    }

    private static ClassDefinition parseOneClass(String source) {
        ParsedModel m = ElementParser.parse(source);
        assertEquals(1, m.elements().size(), () -> "expected exactly one element, got " + m.elements());
        PackageableElement el = m.elements().get(0);
        return assertInstanceOf(ClassDefinition.class, el);
    }

    // ---------------------------------------------------------------
    // Empty / whitespace / comments
    // ---------------------------------------------------------------

    @Test
    void emptySourceProducesEmptyModel() {
        ParsedModel m = ElementParser.parse("");
        assertEquals(new ParsedModel(List.of(), ImportScope.empty()), m,
                "empty source should produce an empty ParsedModel record");
    }

    @Test
    void whitespaceAndCommentsOnlyProduceEmptyModel() {
        ParsedModel m = ElementParser.parse("  // hi\n /* x */\n\n");
        assertEquals(new ParsedModel(List.of(), ImportScope.empty()), m);
    }

    @Test
    void emptyImportScopeFactoryEqualsExplicitEmpty() {
        assertEquals(new ImportScope(List.of(), Map.of()), ImportScope.empty());
        assertTrue(ImportScope.empty().isEmpty());
    }

    // ---------------------------------------------------------------
    // Imports — full ImportScope record comparisons
    // ---------------------------------------------------------------

    @Test
    void wildcardImport() {
        ParsedModel m = ElementParser.parse("import simple::model::*;");
        assertEquals(new ImportScope(List.of("simple::model"), Map.of()),
                m.imports());
    }

    @Test
    void specificImport() {
        ParsedModel m = ElementParser.parse("import simple::model::Firm;");
        assertEquals(new ImportScope(List.of(), Map.of("Firm", "simple::model::Firm")),
                m.imports());
    }

    @Test
    void multipleImportsPreserveOrderAndDeduplicate() {
        ParsedModel m = ElementParser.parse(
                "import a::b::*; import a::b::*; import c::d::Foo; import e::f::*;");
        assertEquals(
                new ImportScope(
                        List.of("a::b", "e::f"),                // dedup, preserve order
                        Map.of("Foo", "c::d::Foo")),
                m.imports());
    }

    // ---------------------------------------------------------------
    // Empty Class — full record comparison
    // ---------------------------------------------------------------

    @Test
    void emptyClassMatchesAllDefaults() {
        ClassDefinition c = parseOneClass("Class my::Pkg::Foo {}");
        assertEquals(emptyClass("my::Pkg::Foo", false), c,
                "empty class body must produce all-empty fields and isNative=false");
    }

    @Test
    void unpackagedClassQualifiedNameHasNoColons() {
        ClassDefinition c = parseOneClass("Class Foo {}");
        assertEquals(emptyClass("Foo", false), c);
        assertFalse(c.qualifiedName().contains("::"));
    }

    @Test
    void nativeClassFlagSetAndOnlyDifference() {
        ClassDefinition n = parseOneClass("native Class my::Foo {}");
        ClassDefinition normal = parseOneClass("Class my::Foo {}");
        assertTrue(n.isNative());
        assertFalse(normal.isNative());
        // The two should be identical except for isNative.
        assertEquals(emptyClass("my::Foo", true), n);
        assertEquals(emptyClass("my::Foo", false), normal);
    }

    // ---------------------------------------------------------------
    // Properties — full record comparison
    // ---------------------------------------------------------------

    @Test
    void singleRequiredProperty() {
        ClassDefinition c = parseOneClass("Class Person { name: String[1]; }");
        assertEquals(List.of(prop("name", "String", 1, 1)), c.properties());
    }

    @Test
    void multiplicityVariantsSpanAllFourShapes() {
        ClassDefinition c = parseOneClass(
                "Class P { a: String[1]; b: Integer[0..1]; c: Date[*]; d: Float[1..*]; }");
        assertEquals(
                List.of(
                        prop("a", "String", 1, 1),
                        prop("b", "Integer", 0, 1),
                        prop("c", "Date", 0, null),       // [*]
                        prop("d", "Float", 1, null)),     // [1..*]
                c.properties());
        // Defensive: confirm '*' is encoded as Concrete(lower, null), not Parameter.
        assertEquals(new Multiplicity.Concrete(0, null),
                c.properties().get(2).multiplicity());
        assertEquals(new Multiplicity.Concrete(1, null),
                c.properties().get(3).multiplicity());
    }

    @Test
    void exactRangeMultiplicity() {
        // [2..5] — engine accepts arbitrary lower..upper integers.
        ClassDefinition c = parseOneClass("Class P { x: Integer[2..5]; }");
        assertEquals(List.of(prop("x", "Integer", 2, 5)), c.properties());
    }

    @Test
    void qualifiedPropertyTypeIsCapturedVerbatim() {
        ClassDefinition c = parseOneClass("Class P { firm: my::model::Firm[1]; }");
        assertEquals(List.of(prop("firm", "my::model::Firm", 1, 1)), c.properties());
    }

    @Test
    void genericPropertyTypeCapturesAngleBrackets() {
        // Engine-lite parity: generic type arguments (e.g. List<Person>)
        // are captured as part of the type text. Depth-tracked parsing
        // handles nested generics and embedded function types too.
        ClassDefinition c = parseOneClass("Class P { items: List<Person>[*]; }");
        assertEquals(List.of(propT("items", tg("List", nr("Person")), 0, null)),
                c.properties());
    }

    @Test
    void nestedGenericPropertyType() {
        // 'Pair<List<A>, B>' -- nested angle brackets must balance.
        // A single-pass depth counter (vs. a regex) is what makes this
        // work; the test pins that depth tracking stays correct when
        // the inner generic closes first.
        ClassDefinition c = parseOneClass(
                "Class P { x: Pair<List<A>, B>[1]; }");
        assertEquals(
                List.of(propT("x", tg("Pair", tg("List", nr("A")), nr("B")), 1, 1)),
                c.properties());
    }

    @Test
    void functionPropertyTypeCapturesBraces() {
        // 'Function<{T[1]->U[1]}>' combines generics with an embedded
        // bare-function type. Engine-lite treats the inner '{...}' as
        // an opaque brace-balanced span; we match. The '->' arrow
        // inside is a lexer ARROW token that never breaks depth
        // tracking because it's not a brace or angle.
        ClassDefinition c = parseOneClass(
                "Class P { f: Function<{T[1]->U[1]}>[1]; }");
        TypeExpression fnTU = new FunctionType(
                List.of(tp(nr("T"), Multiplicity.exactly(1))),
                tp(nr("U"), Multiplicity.exactly(1)));
        assertEquals(List.of(propT("f", tg("Function", fnTU), 1, 1)),
                c.properties());
    }

    @Test
    void bareFunctionTypeAsPropertyType() {
        // '{T[1]->U[1]}' standalone (no 'Function<>' wrapper). Engine-
        // lite accepts; we match. The multiplicity follows the
        // function-type span, not the inner return multiplicity.
        ClassDefinition c = parseOneClass(
                "Class P { g: {T[1]->U[1]}[1]; }");
        TypeExpression fnTU = new FunctionType(
                List.of(tp(nr("T"), Multiplicity.exactly(1))),
                tp(nr("U"), Multiplicity.exactly(1)));
        assertEquals(List.of(propT("g", fnTU, 1, 1)), c.properties());
    }

    @Test
    void genericTypeInExtendsClause() {
        // 'extends List<Bar>' -- generic superclass. The 'extends'
        // arm also goes through parseType (see parseClassDefinition),
        // so this pins the same depth-tracking behaviour applies to
        // superclass parsing too, not just property types.
        ClassDefinition c = parseOneClass(
                "Class my::Foo extends List<Bar> { x: Integer[1]; }");
        assertEquals(List.of(tg("List", nr("Bar"))), c.superClasses());
    }

    @Test
    void genericTypeInAssociationEnd() {
        // Association ends also parseType -- pin the generic-support
        // carries through.
        ParsedModel m = ElementParser.parse(
                "Association my::A { left: List<B>[*]; right: A[1]; }");
        AssociationDefinition a = (AssociationDefinition) m.elements().get(0);
        assertEquals(tg("List", nr("B")), a.property1().targetClass());
    }

    @Test
    void genericTypeInFunctionParameterAndReturn() {
        // Function parameters and the return type both go through
        // parseType. Both must accept generics.
        ParsedModel m = ElementParser.parse(
                "function my::f(x: List<String>[*]): Pair<String, Integer>[1] { $x }");
        FunctionDefinition f = (FunctionDefinition) m.elements().get(0);
        assertEquals(tg("List", nr("String")), f.parameters().get(0).type());
        assertEquals(tg("Pair", nr("String"), nr("Integer")), f.returnType());
    }

    // ---------------------------------------------------------------
    // Type parameters and superclasses
    // ---------------------------------------------------------------

    @Test
    void typeParametersListedInDeclaredOrder() {
        ClassDefinition c = parseOneClass("Class Pair<A, B, C> {}");
        assertEquals(List.of("A", "B", "C"), c.typeParams());
        assertTrue(c.superClasses().isEmpty(), "type params must not leak into superClasses");
    }

    @Test
    void multipleSuperclassesIncludingQualifiedNames() {
        ClassDefinition c = parseOneClass("Class Hybrid extends A, b::c::D {}");
        assertEquals(List.of(nr("A"), nr("b::c::D")), c.superClasses());
        assertTrue(c.typeParams().isEmpty(), "extends must not leak into typeParams");
    }

    @Test
    void compositeClassWithEverythingCombined() {
        ClassDefinition c = parseOneClass("""
                Class <<doc::D.deprecated>>
                  { doc::D.author = 'Alice' }
                  my::pkg::Big<T, U>
                  extends my::pkg::Base, OtherBase
                {
                  payload: T[1];
                  count: Integer[0..*];
                }
                """);
        assertEquals("my::pkg::Big", c.qualifiedName());
        assertEquals(List.of("T", "U"), c.typeParams());
        assertEquals(List.of(nr("my::pkg::Base"), nr("OtherBase")), c.superClasses());
        assertEquals(List.of(new StereotypeApplication("doc::D", "deprecated")),
                c.stereotypes());
        assertEquals(List.of(new TaggedValue("doc::D", "author", "Alice")),
                c.taggedValues());
        assertEquals(
                List.of(prop("payload", "T", 1, 1),
                        prop("count", "Integer", 0, null)),
                c.properties());
        assertFalse(c.isNative());
    }

    // ---------------------------------------------------------------
    // Annotations
    // ---------------------------------------------------------------

    @Test
    void multipleStereotypesPreserveOrder() {
        ClassDefinition c = parseOneClass(
                "Class <<a::P.s1, a::P.s2, b::Q.s3>> Foo {}");
        assertEquals(
                List.of(
                        new StereotypeApplication("a::P", "s1"),
                        new StereotypeApplication("a::P", "s2"),
                        new StereotypeApplication("b::Q", "s3")),
                c.stereotypes());
    }

    @Test
    void taggedValueQuotesAreStripped() {
        ClassDefinition c = parseOneClass(
                "Class { doc::D.author = 'Alice', doc::D.note = 'multi word value' } Foo {}");
        assertEquals(
                List.of(
                        new TaggedValue("doc::D", "author", "Alice"),
                        new TaggedValue("doc::D", "note", "multi word value")),
                c.taggedValues());
    }

    @Test
    void propertyWithBothStereotypeAndTaggedValue() {
        ClassDefinition c = parseOneClass(
                "Class P { <<doc::D.required>> { doc::D.note = 'pk' } id: String[1]; }");
        PropertyDefinition expected = new PropertyDefinition(
                "id", nr("String"), Multiplicity.exactly(1),
                List.of(new StereotypeApplication("doc::D", "required")),
                List.of(new TaggedValue("doc::D", "note", "pk")));
        assertEquals(List.of(expected), c.properties());
    }

    // ---------------------------------------------------------------
    // End-to-end
    // ---------------------------------------------------------------

    @Test
    void importsPlusMultipleClassesFullModel() {
        String src = """
                import my::model::*;
                import my::store::Firm;

                Class my::model::Person {
                  firstName: String[1];
                  lastName: String[1];
                  age: Integer[0..1];
                }

                Class my::model::Firm extends LegalEntity {
                  name: String[1];
                  ceo: Person[0..1];
                }
                """;
        ParsedModel m = ElementParser.parse(src);

        ImportScope expectedImports = new ImportScope(
                List.of("my::model"),
                Map.of("Firm", "my::store::Firm"));

        ClassDefinition expectedPerson = new ClassDefinition(
                "my::model::Person",
                List.of(), List.of(),
                List.of(
                        prop("firstName", "String", 1, 1),
                        prop("lastName", "String", 1, 1),
                        prop("age", "Integer", 0, 1)),
                List.of(), List.of(), List.of(), List.of(), false);

        ClassDefinition expectedFirm = new ClassDefinition(
                "my::model::Firm",
                List.of(), List.of(nr("LegalEntity")),
                List.of(
                        prop("name", "String", 1, 1),
                        prop("ceo", "Person", 0, 1)),
                List.of(), List.of(), List.of(), List.of(), false);

        assertEquals(
                new ParsedModel(List.of(expectedPerson, expectedFirm), expectedImports),
                m,
                "full ParsedModel must match expected record graph exactly");
    }

    // ---------------------------------------------------------------
    // Immutability — defensive structural invariants
    // ---------------------------------------------------------------

    @Test
    void parsedListsRejectMutation() {
        ClassDefinition c = parseOneClass("Class P { x: Integer[1]; }");
        assertThrows(UnsupportedOperationException.class,
                () -> c.properties().add(prop("y", "String", 1, 1)),
                "properties list must be immutable");
        assertThrows(UnsupportedOperationException.class,
                () -> c.typeParams().add("X"),
                "typeParams list must be immutable");
        assertThrows(UnsupportedOperationException.class,
                () -> c.superClasses().add(nr("X")),
                "superClasses list must be immutable");
    }

    @Test
    void importScopeMapsRejectMutation() {
        ParsedModel m = ElementParser.parse("import a::b::*; import c::d::E;");
        assertThrows(UnsupportedOperationException.class,
                () -> m.imports().wildcards().add("evil::pkg"));
        assertThrows(UnsupportedOperationException.class,
                () -> m.imports().typeImports().put("X", "evil::X"));
    }

    // ---------------------------------------------------------------
    // Errors
    // ---------------------------------------------------------------

    @Test
    void unsupportedTopLevelKeywordFailsLoudly() {
        // 'Measure' is still unsupported as of B.4b. The dispatcher must
        // name the offending token rather than silently skipping it.
        ParseException e = assertThrows(ParseException.class,
                () -> ElementParser.parse("Measure my::M ( )"));
        assertTrue(e.getMessage().toLowerCase().contains("unsupported"),
                () -> "expected 'unsupported' in message, got: " + e.getMessage());
        assertTrue(e.getMessage().contains("MEASURE"),
                () -> "error should name the offending token type, got: " + e.getMessage());
    }

    @Test
    void missingSemicolonAfterPropertyFails() {
        // 'name: String[1]' followed immediately by '}' — expected SEMI_COLON, got BRACE_CLOSE.
        ParseException e = assertThrows(ParseException.class,
                () -> ElementParser.parse("Class P { name: String[1] }"));
        assertTrue(e.getMessage().contains("SEMI_COLON"),
                () -> "expected SEMI_COLON in message, got: " + e.getMessage());
    }

    @Test
    void missingClosingBraceFails() {
        assertThrows(ParseException.class,
                () -> ElementParser.parse("Class P { name: String[1];"));
    }

    @Test
    void errorCarriesAccurateLineAndColumn() {
        // Layout (1-indexed lines):
        //   1: Class P
        //   2: {
        //   3:   name String[1];
        //   4: }
        // Error fires at 'String' (col 7 in 0-indexed) where ':' was expected.
        ParseException e = assertThrows(ParseException.class,
                () -> ElementParser.parse("Class P\n{\n  name String[1];\n}"));
        assertEquals(3, e.line(), "line should point to the offending token");
        assertTrue(e.column() >= 6 && e.column() <= 8,
                () -> "column should be near start of 'String' (~7), got " + e.column());
        assertTrue(e.getMessage().startsWith("[3:"),
                () -> "formatted message should embed [line:col], got: " + e.getMessage());
    }

    // ---------------------------------------------------------------
    // API surface — text vs token-stream overloads must agree
    // ---------------------------------------------------------------

    @Test
    void parseTokenStreamOverloadAndStringOverloadProduceEqualResults() {
        String src = "import a::b::*; Class Foo { x: Integer[1]; y: String[0..*]; }";
        var stream = com.legend.lexer.Lexer.tokenize(src);
        ParsedModel viaStream = ElementParser.parse(stream);
        ParsedModel viaString = ElementParser.parse(src);
        assertEquals(viaString, viaStream,
                "TokenStream overload must return a ParsedModel record-equal to the String overload");
    }

    // ===============================================================
    // B.2 — derived properties
    // ===============================================================

    @Test
    void derivedPropertyZeroArgsCapturesBodyText() {
        ClassDefinition c = parseOneClass(
                "Class P { fullName() { 'a' + 'b' }: String[1]; }");
        assertEquals(1, c.derivedProperties().size());
        DerivedPropertyDefinition d = c.derivedProperties().get(0);
        assertEquals("fullName", d.name());
        assertEquals(List.of(), d.parameters());
        // Body parses eagerly to ValueSpec AST (C.8). 'a' + 'b' desugars
        // to plus(CString("a"), CString("b")) per SpecParser's operator desugaring.
        assertEquals(1, d.expression().size());
        AppliedFunction plus = assertInstanceOf(AppliedFunction.class, d.expression().get(0));
        assertEquals("plus", plus.function());
        assertEquals(List.of(new CString("a"), new CString("b")), plus.parameters());
        assertEquals(nr("String"), d.type());
        assertEquals(Multiplicity.exactly(1), d.multiplicity());
        // Regular properties list stays empty.
        assertTrue(c.properties().isEmpty());
    }

    @Test
    void derivedPropertyWithParametersAndBraces() {
        ClassDefinition c = parseOneClass(
                "Class P { tax(rate: Float[1], country: String[0..1]) { $this.price * $rate }: Float[1]; }");
        assertEquals(1, c.derivedProperties().size());
        DerivedPropertyDefinition d = c.derivedProperties().get(0);
        assertEquals("tax", d.name());
        assertEquals(
                List.of(
                        new ParameterDefinition("rate", nr("Float"), Multiplicity.exactly(1)),
                        new ParameterDefinition("country", nr("String"), Multiplicity.range(0, 1))),
                d.parameters());
        // $this.price * $rate desugars to times(AppliedProperty($this, price), Variable(rate)).
        assertEquals(1, d.expression().size());
        AppliedFunction times = assertInstanceOf(AppliedFunction.class, d.expression().get(0));
        assertEquals("times", times.function());
        assertEquals(2, times.parameters().size());
        AppliedProperty thisPrice = assertInstanceOf(AppliedProperty.class, times.parameters().get(0));
        assertEquals("price", thisPrice.property());
        assertEquals(new Variable("this"), thisPrice.receiver());
        assertEquals(new Variable("rate"), times.parameters().get(1));
        assertEquals(nr("Float"), d.type());
    }

    @Test
    void derivedAndRegularPropertiesInSameClass() {
        ClassDefinition c = parseOneClass("""
                Class P {
                  first: String[1];
                  greeting() { 'hi ' + $this.first }: String[1];
                  last: String[1];
                }
                """);
        assertEquals(
                List.of(prop("first", "String", 1, 1), prop("last", "String", 1, 1)),
                c.properties(),
                "regular properties parsed and ordered correctly");
        assertEquals(1, c.derivedProperties().size());
        assertEquals("greeting", c.derivedProperties().get(0).name());
    }

    // ===============================================================
    // B.2 — class-level constraints
    // ===============================================================

    @Test
    void namedConstraintCapturesExpressionText() {
        ClassDefinition c = parseOneClass("Class P [validAge: $this.age >= 0] {}");
        assertEquals(1, c.constraints().size());
        ConstraintDefinition cn = c.constraints().get(0);
        assertEquals("validAge", cn.name());
        // $this.age >= 0 desugars to greaterThanEqual(AppliedProperty($this, age), CInteger(0)).
        AppliedFunction gte = assertInstanceOf(AppliedFunction.class, cn.expression());
        assertEquals("greaterThanEqual", gte.function());
        assertEquals(2, gte.parameters().size());
        AppliedProperty thisAge = assertInstanceOf(AppliedProperty.class, gte.parameters().get(0));
        assertEquals("age", thisAge.property());
        assertEquals(new Variable("this"), thisAge.receiver());
        assertEquals(new CInteger(0L), gte.parameters().get(1));
    }

    @Test
    void unnamedConstraintGetsDefaultName() {
        ClassDefinition c = parseOneClass("Class P [$this.age >= 0] {}");
        assertEquals(1, c.constraints().size());
        ConstraintDefinition cn = c.constraints().get(0);
        assertEquals("unnamed", cn.name());
        // Same body shape as namedConstraintCapturesExpressionText — verifies that
        // omitting the name doesn't disturb body parsing.
        AppliedFunction gte = assertInstanceOf(AppliedFunction.class, cn.expression());
        assertEquals("greaterThanEqual", gte.function());
        assertEquals(new CInteger(0L), gte.parameters().get(1));
    }

    @Test
    void multipleConstraintsPreserveOrder() {
        ClassDefinition c = parseOneClass(
                "Class P [a: $this.x > 0, b: $this.y < 10] {}");
        assertEquals(2, c.constraints().size());
        assertEquals("a", c.constraints().get(0).name());
        assertEquals("b", c.constraints().get(1).name());
    }

    // ===============================================================
    // B.2 — Association
    // ===============================================================

    @Test
    void associationBasicTwoEnds() {
        ParsedModel m = ElementParser.parse(
                "Association my::ns::Person_Firm { firm: Firm[1]; employees: Person[*]; }");
        assertEquals(1, m.elements().size());
        AssociationDefinition a = assertInstanceOf(AssociationDefinition.class,
                m.elements().get(0));
        assertEquals("my::ns::Person_Firm", a.qualifiedName());
        assertEquals(new AssociationEndDefinition("firm", nr("Firm"), Multiplicity.exactly(1)),
                a.property1());
        assertEquals(new AssociationEndDefinition("employees", nr("Person"), Multiplicity.zeroMany()),
                a.property2());
    }

    @Test
    void associationMustHaveExactlyTwoEnds() {
        ParseException one = assertThrows(ParseException.class,
                () -> ElementParser.parse("Association A { only: B[1]; }"));
        assertTrue(one.getMessage().contains("exactly 2"),
                () -> "expected 'exactly 2' in message, got: " + one.getMessage());

        ParseException three = assertThrows(ParseException.class,
                () -> ElementParser.parse(
                        "Association A { a: B[1]; b: C[1]; c: D[1]; }"));
        assertTrue(three.getMessage().contains("exactly 2"));
    }

    // ===============================================================
    // B.2 — Enum
    // ===============================================================

    @Test
    void enumWithSingleValue() {
        ParsedModel m = ElementParser.parse("Enum my::S { ONLY }");
        EnumDefinition e = assertInstanceOf(EnumDefinition.class,
                m.elements().get(0));
        assertEquals(new EnumDefinition("my::S", List.of("ONLY")), e);
    }

    @Test
    void enumWithMultipleValuesPreservesOrder() {
        ParsedModel m = ElementParser.parse(
                "Enum my::Status { ACTIVE, INACTIVE, PENDING }");
        EnumDefinition e = assertInstanceOf(EnumDefinition.class,
                m.elements().get(0));
        assertEquals(List.of("ACTIVE", "INACTIVE", "PENDING"), e.values());
    }

    @Test
    void enumEmptyBodyFailsLoudly() {
        ParseException ex = assertThrows(ParseException.class,
                () -> ElementParser.parse("Enum my::S {}"));
        assertTrue(ex.getMessage().contains("at least one value"));
    }

    // ===============================================================
    // B.2 — Profile
    // ===============================================================

    @Test
    void profileWithStereotypesAndTags() {
        ParsedModel m = ElementParser.parse("""
                Profile doc::Documentation {
                  stereotypes: [deprecated, legacy, experimental];
                  tags: [author, since, description];
                }
                """);
        ProfileDefinition p = assertInstanceOf(ProfileDefinition.class,
                m.elements().get(0));
        assertEquals(new ProfileDefinition(
                        "doc::Documentation",
                        List.of("deprecated", "legacy", "experimental"),
                        List.of("author", "since", "description")),
                p);
    }

    @Test
    void profileStereotypesOnly() {
        ParsedModel m = ElementParser.parse(
                "Profile my::P { stereotypes: [a, b]; }");
        ProfileDefinition p = assertInstanceOf(ProfileDefinition.class,
                m.elements().get(0));
        assertEquals(List.of("a", "b"), p.stereotypes());
        assertEquals(List.of(), p.tags());
    }

    @Test
    void profileEmptyBody() {
        ParsedModel m = ElementParser.parse("Profile my::P { }");
        ProfileDefinition p = assertInstanceOf(ProfileDefinition.class,
                m.elements().get(0));
        assertEquals(new ProfileDefinition("my::P", List.of(), List.of()), p);
    }

    // ===============================================================
    // B.2 — sealed-root coverage: all four element kinds in one file
    // ===============================================================

    @Test
    void mixedElementKindsAllAppearAsPackageableElements() {
        ParsedModel m = ElementParser.parse("""
                Class my::Person { name: String[1]; }
                Association my::P_F { person: my::Person[1]; firms: my::Firm[*]; }
                Enum my::Status { ACTIVE, INACTIVE }
                Profile my::Doc { stereotypes: [draft]; tags: [author]; }
                """);
        assertEquals(4, m.elements().size());
        assertInstanceOf(ClassDefinition.class,       m.elements().get(0));
        assertInstanceOf(AssociationDefinition.class, m.elements().get(1));
        assertInstanceOf(EnumDefinition.class,        m.elements().get(2));
        assertInstanceOf(ProfileDefinition.class,     m.elements().get(3));
    }

    // ===============================================================
    // B.3 — function
    // ===============================================================

    @Test
    void functionZeroArgsReturnSimpleType() {
        ParsedModel m = ElementParser.parse(
                "function my::pkg::greet(): String[1] { 'hello' }");
        FunctionDefinition f = assertInstanceOf(FunctionDefinition.class, m.elements().get(0));
        assertEquals("my::pkg::greet", f.qualifiedName());
        assertEquals(List.of(), f.parameters());
        assertEquals(nr("String"), f.returnType());
        assertEquals(Multiplicity.exactly(1), f.returnMultiplicity());
        // 'hello' parses to a single CString literal.
        assertEquals(List.of(new CString("hello")), f.body());
        assertEquals(List.of(), f.stereotypes());
        assertEquals(List.of(), f.taggedValues());
    }

    @Test
    void functionWithMultipleParametersAndOptionalMultiplicity() {
        ParsedModel m = ElementParser.parse(
                "function my::add(a: Integer[1], b: Integer[0..1]): Integer[1] { $a + $b }");
        FunctionDefinition f = assertInstanceOf(FunctionDefinition.class, m.elements().get(0));
        assertEquals(
                List.of(
                        new FunctionDefinition.ParameterDefinition("a", nr("Integer"), Multiplicity.exactly(1)),
                        new FunctionDefinition.ParameterDefinition("b", nr("Integer"), Multiplicity.range(0, 1))),
                f.parameters());
        assertEquals(nr("Integer"), f.returnType());
        // $a + $b desugars to plus(Variable(a), Variable(b)).
        assertEquals(
                List.of(new AppliedFunction("plus",
                        List.of(new Variable("a"), new Variable("b")))),
                f.body());
    }

    @Test
    void functionBodyParsesNestedLambdaBraces() {
        // The body contains a nested braced lambda — the brace-balancing
        // scan must locate the outer '}' correctly so SpecParser sees the
        // full body. Two statements: a let-binding (which contains the
        // nested '{x | ...}' lambda) and an arrow-eval.
        ParsedModel m = ElementParser.parse(
                "function my::f(): Integer[1] { let inc = {x | $x + 1}; $inc->eval(1) }");
        FunctionDefinition f = assertInstanceOf(FunctionDefinition.class, m.elements().get(0));
        assertEquals(2, f.body().size(),
                () -> "two-statement body should parse to two ValueSpecs, got: " + f.body());
        // Stmt 0: 'let inc = ...' desugars to letFunction(CString("inc"), <lambda>).
        AppliedFunction letFn = assertInstanceOf(AppliedFunction.class, f.body().get(0));
        assertEquals("letFunction", letFn.function());
        assertEquals(2, letFn.parameters().size());
        assertEquals(new CString("inc"), letFn.parameters().get(0));
        // Stmt 1: '$inc->eval(1)' desugars to eval(Variable(inc), CInteger(1)).
        AppliedFunction evalCall = assertInstanceOf(AppliedFunction.class, f.body().get(1));
        assertEquals("eval", evalCall.function());
        assertEquals(
                List.of(new Variable("inc"), new CInteger(1L)),
                evalCall.parameters());
    }

    @Test
    void functionUpperBoundStarBecomesConcreteUnbounded() {
        FunctionDefinition f = assertInstanceOf(FunctionDefinition.class,
                ElementParser.parse("function my::all(): Person[*] { Person.all() }")
                        .elements().get(0));
        assertEquals(Multiplicity.zeroMany(), f.returnMultiplicity(),
                "[*] must parse to Concrete(0, null), not a parameter");
        assertEquals(new Multiplicity.Concrete(0, null), f.returnMultiplicity());
    }

    @Test
    void functionWithGenericTypeParameters() {
        FunctionDefinition f = assertInstanceOf(FunctionDefinition.class,
                ElementParser.parse(
                        "function my::id<T>(x: T[1]): T[1] { $x }")
                        .elements().get(0));
        assertEquals(List.of("T"), f.typeParameters());
        assertEquals(List.of(), f.multiplicityParameters());
        assertEquals(nr("T"), f.parameters().get(0).type());
        assertEquals(nr("T"), f.returnType());
    }

    @Test
    void functionWithTypeAndMultiplicityParameters() {
        FunctionDefinition f = assertInstanceOf(FunctionDefinition.class,
                ElementParser.parse(
                        "function my::pick<T,V|m,n>(a: T[m], b: V[n]): T[m] { $a }")
                        .elements().get(0));
        assertEquals(List.of("T", "V"), f.typeParameters());
        assertEquals(List.of("m", "n"), f.multiplicityParameters());
    }

    @Test
    void functionWithMultiplicityParametersOnly() {
        FunctionDefinition f = assertInstanceOf(FunctionDefinition.class,
                ElementParser.parse(
                        "function my::take<|m>(xs: Any[m]): Any[m] { $xs }")
                        .elements().get(0));
        assertEquals(List.of(), f.typeParameters());
        assertEquals(List.of("m"), f.multiplicityParameters());
    }

    @Test
    void userFunctionWithSubsetConstraintInGeneric() {
        // User functions inherit the native-signature grammar: a subset
        // constraint <X\u2286T> inside a parameter's generic must parse,
        // captured verbatim in the type text.
        FunctionDefinition f = assertInstanceOf(FunctionDefinition.class,
                ElementParser.parse(
                        "function my::pickCols<X,T>(rel: Relation<T>[1], "
                                + "info: SortInfo<X\u2286T>[*]): Relation<T>[1] "
                                + "{ $rel }")
                        .elements().get(0));
        assertEquals(List.of("X", "T"), f.typeParameters());
        assertEquals(tg("Relation", nr("T")), f.parameters().get(0).type());
        assertEquals(tg("SortInfo", sa(nr("X"), Op.SUBSET, nr("T"))),
                f.parameters().get(1).type());
        assertEquals(Multiplicity.zeroMany(), f.parameters().get(1).multiplicity());
        assertEquals(tg("Relation", nr("T")), f.returnType());
    }

    @Test
    void userFunctionWithSchemaAlgebraInReturnType() {
        // Schema add (T+Z) in return type position.
        FunctionDefinition f = assertInstanceOf(FunctionDefinition.class,
                ElementParser.parse(
                        "function my::ext<T,Z>(r: Relation<T>[1]): Relation<T+Z>[1] "
                                + "{ $r }")
                        .elements().get(0));
        assertEquals(tg("Relation", sa(nr("T"), Op.UNION, nr("Z"))),
                f.returnType());
        assertEquals(Multiplicity.exactly(1), f.returnMultiplicity());
    }

    @Test
    void userFunctionWithSchemaDropAddInReturnType() {
        // Schema drop-then-add (T-Z+V) in return type position.
        FunctionDefinition f = assertInstanceOf(FunctionDefinition.class,
                ElementParser.parse(
                        "function my::ren<T,Z,V>(r: Relation<T>[1]): Relation<T-Z+V>[1] "
                                + "{ $r }")
                        .elements().get(0));
        assertEquals(
                tg("Relation",
                        sa(sa(nr("T"), Op.DIFFERENCE, nr("Z")), Op.UNION, nr("V"))),
                f.returnType());
    }

    @Test
    void userFunctionWithRenameDslAndSubsetInGeneric() {
        // The hairiest form from Pure stdlib: ColSpec<Z=(?:K)\u2286T> means
        // 'rename Z to K, where K is a subset of T'. Must parse verbatim.
        FunctionDefinition f = assertInstanceOf(FunctionDefinition.class,
                ElementParser.parse(
                        "function my::ren2<T,Z,K>(r: Relation<T>[1], "
                                + "spec: ColSpec<Z=(?:K)\u2286T>[1]): Relation<T>[1] "
                                + "{ $r }")
                        .elements().get(0));
        // Z=(?:K)⊆T parses to SUBSET wrapping EQUAL; wildcard column (?:K)
        // becomes a RelationType with one Column named "?".
        TypeExpression wildcardOfK = rel(col("?", nr("K"), Multiplicity.exactly(1)));
        TypeExpression zEqQK_subT = sa(
                sa(nr("Z"), Op.EQUAL, wildcardOfK),
                Op.SUBSET, nr("T"));
        assertEquals(tg("ColSpec", zEqQK_subT), f.parameters().get(1).type());
    }

    @Test
    void userFunctionWithNestedMultiArgFunctionTypeParameter() {
        // Function type with three input arrows (matches extend-with-window
        // shape). Captured as raw text by the signature parser.
        FunctionDefinition f = assertInstanceOf(FunctionDefinition.class,
                ElementParser.parse(
                        "function my::win<T,R>(r: Relation<T>[1], "
                                + "f: FuncColSpec<{Relation<T>[1],_Window<T>[1],T[1]->Any[0..1]},R>[1]"
                                + "): Relation<T+R>[1] { $r }")
                        .elements().get(0));
        TypeExpression innerFn = new FunctionType(
                List.of(
                        tp(tg("Relation", nr("T")), Multiplicity.exactly(1)),
                        tp(tg("_Window", nr("T")), Multiplicity.exactly(1)),
                        tp(nr("T"), Multiplicity.exactly(1))),
                tp(nr("Any"), Multiplicity.range(0, 1)));
        assertEquals(tg("FuncColSpec", innerFn, nr("R")),
                f.parameters().get(1).type());
        assertEquals(tg("Relation", sa(nr("T"), Op.UNION, nr("R"))),
                f.returnType());
    }

    @Test
    void userFunctionWithUnderscorePrefixedTypeName() {
        // _Window<T> and _Traversal exist as user-visible types; their
        // leading underscore must not trip the identifier rule.
        FunctionDefinition f = assertInstanceOf(FunctionDefinition.class,
                ElementParser.parse(
                        "function my::w<T>(w: _Window<T>[1], t: _Traversal[1]): T[1] "
                                + "{ $w->cast(@T) }")
                        .elements().get(0));
        assertEquals(tg("_Window", nr("T")), f.parameters().get(0).type());
        assertEquals(nr("_Traversal"), f.parameters().get(1).type());
    }

    // ===============================================================
    // Native function declarations (C.5)
    // ===============================================================

    @Test
    void nativeFunctionSimple() {
        NativeFunctionDefinition f = assertInstanceOf(NativeFunctionDefinition.class,
                ElementParser.parse(
                        "native function meta::pure::functions::math::abs(x: Number[1]): Number[1];")
                        .elements().get(0));
        assertEquals("meta::pure::functions::math::abs", f.qualifiedName());
        assertEquals(List.of(), f.typeParameters());
        assertEquals(List.of(), f.multiplicityParameters());
        assertEquals(
                List.of(new FunctionDefinition.ParameterDefinition(
                        "x", nr("Number"), Multiplicity.exactly(1))),
                f.parameters());
        assertEquals(nr("Number"), f.returnType());
        assertEquals(Multiplicity.exactly(1), f.returnMultiplicity());
    }

    @Test
    void nativeFunctionWithStereotypeAndGenerics() {
        NativeFunctionDefinition f = assertInstanceOf(NativeFunctionDefinition.class,
                ElementParser.parse(
                        "native function <<PCT.function>> meta::pure::functions::lang::cast<T|m>"
                                + "(source: Any[m], object: T[1]): T[m];")
                        .elements().get(0));
        assertEquals("meta::pure::functions::lang::cast", f.qualifiedName());
        assertEquals(List.of("T"), f.typeParameters());
        assertEquals(List.of("m"), f.multiplicityParameters());
        assertEquals(1, f.stereotypes().size());
        assertEquals(2, f.parameters().size());
        assertEquals(nr("T"), f.returnType());
    }

    @Test
    void nativeFunctionWithFunctionTypeParameter() {
        NativeFunctionDefinition f = assertInstanceOf(NativeFunctionDefinition.class,
                ElementParser.parse(
                        "native function meta::pure::functions::collection::map<T,V>"
                                + "(value: T[*], func: Function<{T[1]->V[*]}>[1]): V[*];")
                        .elements().get(0));
        assertEquals(List.of("T", "V"), f.typeParameters());
        // Function type captured as raw text (structural parse deferred to the model layer).
        assertEquals(
                List.of(
                        new FunctionDefinition.ParameterDefinition("value", nr("T"), Multiplicity.zeroMany()),
                        new FunctionDefinition.ParameterDefinition(
                                "func",
                                tg("Function", new FunctionType(
                                        List.of(tp(nr("T"), Multiplicity.exactly(1))),
                                        tp(nr("V"), Multiplicity.zeroMany()))),
                                Multiplicity.exactly(1))),
                f.parameters());
        assertEquals(nr("V"), f.returnType());
        assertEquals(Multiplicity.zeroMany(), f.returnMultiplicity());
    }

    @Test
    void nativeFunctionWithRelationAndSubsetConstraint() {
        // sort<X,T>(rel:Relation<T>[1], sortInfo:SortInfo<X⊆T>[*]):Relation<T>[1]
        NativeFunctionDefinition f = assertInstanceOf(NativeFunctionDefinition.class,
                ElementParser.parse(
                        "native function meta::pure::functions::relation::sort<X,T>"
                                + "(rel: Relation<T>[1], sortInfo: SortInfo<X\u2286T>[*]): Relation<T>[1];")
                        .elements().get(0));
        assertEquals(List.of("X", "T"), f.typeParameters());
        assertEquals(tg("Relation", nr("T")), f.parameters().get(0).type());
        // SUBSET (⊆) parses to a structured SchemaAlgebra.
        assertEquals(tg("SortInfo", sa(nr("X"), Op.SUBSET, nr("T"))),
                f.parameters().get(1).type());
        assertEquals(tg("Relation", nr("T")), f.returnType());
    }

    @Test
    void nativeFunctionWithSchemaAlgebraReturnType() {
        // extend<T,K,V,R>(r:Relation<T>[1], agg:AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<T+R>[1]
        NativeFunctionDefinition f = assertInstanceOf(NativeFunctionDefinition.class,
                ElementParser.parse(
                        "native function meta::pure::functions::relation::extend<T,K,V,R>"
                                + "(r: Relation<T>[1], "
                                + "agg: AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]"
                                + "): Relation<T+R>[1];")
                        .elements().get(0));
        assertEquals(List.of("T", "K", "V", "R"), f.typeParameters());
        assertEquals(tg("Relation", sa(nr("T"), Op.UNION, nr("R"))),
                f.returnType());
    }

    @Test
    void nativeFunctionWithFullSchemaAlgebraRename() {
        // rename<T,Z,K,V>(r:Relation<T>[1], old:ColSpec<Z=(?:K)⊆T>[1], new:ColSpec<V=(?:K)>[1]):Relation<T-Z+V>[1]
        NativeFunctionDefinition f = assertInstanceOf(NativeFunctionDefinition.class,
                ElementParser.parse(
                        "native function meta::pure::functions::relation::rename<T,Z,K,V>"
                                + "(r: Relation<T>[1], "
                                + "old: ColSpec<Z=(?:K)\u2286T>[1], "
                                + "new: ColSpec<V=(?:K)>[1]"
                                + "): Relation<T-Z+V>[1];")
                        .elements().get(0));
        assertEquals(List.of("T", "Z", "K", "V"), f.typeParameters());
        // All four algebra operators (=, ⊆, +, -) survive as structured
        // SchemaAlgebra nodes; wildcard column (?:K) is a RelationType.
        TypeExpression wildcardOfK = rel(col("?", nr("K"), Multiplicity.exactly(1)));
        assertEquals(
                tg("ColSpec", sa(sa(nr("Z"), Op.EQUAL, wildcardOfK), Op.SUBSET, nr("T"))),
                f.parameters().get(1).type());
        assertEquals(
                tg("ColSpec", sa(nr("V"), Op.EQUAL, wildcardOfK)),
                f.parameters().get(2).type());
        assertEquals(
                tg("Relation",
                        sa(sa(nr("T"), Op.DIFFERENCE, nr("Z")), Op.UNION, nr("V"))),
                f.returnType());
    }

    @Test
    void nativeFunctionWithSchemaLiteralAndSubset() {
        // Mimics user-level signatures like meta::pure::functions::relation::filter<T>
        //   (rel:Relation<(age:Integer)⊆T>[1], f:Function<{T[1]->Boolean[1]}>[1]):Relation<T>[1]
        NativeFunctionDefinition f = assertInstanceOf(NativeFunctionDefinition.class,
                ElementParser.parse(
                        "native function my::filterAdults<T>"
                                + "(rel: Relation<(age:Integer)\u2286T>[1], "
                                + "f: Function<{T[1]->Boolean[1]}>[1]"
                                + "): Relation<T>[1];")
                        .elements().get(0));
        assertEquals(List.of("T"), f.typeParameters());
        // (age:Integer)⊆T parses as SchemaAlgebra(RelationType([age:Integer[1]]), SUBSET, T).
        TypeExpression ageColSubT = sa(
                rel(col("age", nr("Integer"), Multiplicity.exactly(1))),
                Op.SUBSET, nr("T"));
        assertEquals(tg("Relation", ageColSubT), f.parameters().get(0).type());
        assertEquals(tg("Relation", nr("T")), f.returnType());
    }

    @Test
    void nativeFunctionWithMultipleStereotypesAndTaggedValues() {
        NativeFunctionDefinition f = assertInstanceOf(NativeFunctionDefinition.class,
                ElementParser.parse(
                        "native function "
                                + "<<PCT.function, functionType.SideEffectFunction>> "
                                + "{ doc.doc = 'writes a relation' } "
                                + "meta::pure::functions::relation::write<T>"
                                + "(rel: Relation<T>[1], "
                                + "acc: RelationElementAccessor<T>[1]): Integer[1];")
                        .elements().get(0));
        assertEquals(2, f.stereotypes().size());
        assertEquals(1, f.taggedValues().size());
        assertEquals(nr("Integer"), f.returnType());
    }

    @Test
    void nativeFunctionMissingSemicolonFails() {
        ParseException ex = assertThrows(ParseException.class,
                () -> ElementParser.parse(
                        "native function my::oops(x: Integer[1]): Integer[1]"));
        assertTrue(ex.getMessage().toLowerCase().contains("semi")
                        || ex.getMessage().contains(";")
                        || ex.getMessage().toLowerCase().contains("expected"),
                () -> "expected diagnostic to reference missing ';' but got: " + ex.getMessage());
    }

    @Test
    void nativeAndConcreteFunctionsMixInSameModel() {
        ParsedModel m = ElementParser.parse(
                "native function my::n(x: Integer[1]): Integer[1];\n"
                        + "function my::u<T>(x: T[1]): T[1] { $x }\n");
        assertEquals(2, m.elements().size());
        assertInstanceOf(NativeFunctionDefinition.class, m.elements().get(0));
        FunctionDefinition user = assertInstanceOf(FunctionDefinition.class, m.elements().get(1));
        assertEquals(List.of("T"), user.typeParameters());
    }

    @Test
    void functionMultiplicityParameterCaptured() {
        // <|m> declares 'm'; Any[m] references it. The parser must store
        // it as Multiplicity.Parameter, not silently coerce to [*].
        FunctionDefinition f = assertInstanceOf(FunctionDefinition.class,
                ElementParser.parse(
                        "function my::take<|m>(xs: Any[m]): Any[m] { $xs }")
                        .elements().get(0));
        Multiplicity paramMult = f.parameters().get(0).multiplicity();
        Multiplicity retMult = f.returnMultiplicity();
        assertInstanceOf(Multiplicity.Parameter.class, paramMult);
        assertInstanceOf(Multiplicity.Parameter.class, retMult);
        assertEquals("m", ((Multiplicity.Parameter) paramMult).name());
        assertEquals("m", ((Multiplicity.Parameter) retMult).name());
    }

    @Test
    void nativeFunctionMultiplicityParameterCaptured() {
        // The motivating case: native function signatures use multiplicity
        // parameters to express "preserve cardinality from input to output".
        NativeFunctionDefinition f = assertInstanceOf(NativeFunctionDefinition.class,
                ElementParser.parse(
                        "native function meta::pure::functions::collection::cast<T|m>"
                                + "(source: Any[m], object: T[1]): T[m];")
                        .elements().get(0));
        assertInstanceOf(Multiplicity.Parameter.class, f.parameters().get(0).multiplicity());
        assertInstanceOf(Multiplicity.Parameter.class, f.returnMultiplicity());
        assertEquals("m", ((Multiplicity.Parameter) f.returnMultiplicity()).name());
        // The second parameter still uses a concrete [1].
        assertEquals(Multiplicity.exactly(1), f.parameters().get(1).multiplicity());
    }

    @Test
    void concreteMultiplicityShapesRoundTrip() {
        // Exercise each concrete shape end-to-end.
        FunctionDefinition f = assertInstanceOf(FunctionDefinition.class,
                ElementParser.parse(
                        "function my::shapes("
                                + "one: String[1], "
                                + "opt: String[0..1], "
                                + "many: String[*], "
                                + "oneMany: String[1..*], "
                                + "range: String[2..5]"
                                + "): String[1] { 'x' }")
                        .elements().get(0));
        assertEquals(Multiplicity.exactly(1), f.parameters().get(0).multiplicity());
        assertEquals(Multiplicity.range(0, 1), f.parameters().get(1).multiplicity());
        assertEquals(Multiplicity.zeroMany(), f.parameters().get(2).multiplicity());
        assertEquals(Multiplicity.range(1, null), f.parameters().get(3).multiplicity());
        assertEquals(Multiplicity.range(2, 5), f.parameters().get(4).multiplicity());
    }

    @Test
    void nativeFunctionImplementsSealedFunctionMarker() {
        // The sealed Function marker lets downstream consumers handle both
        // variants uniformly without instanceof per-record.
        ParsedModel m = ElementParser.parse(
                "native function my::n(x: Integer[1]): Integer[1];\n"
                        + "function my::u(x: Integer[1]): Integer[1] { $x }");
        com.legend.parser.element.Function n =
                assertInstanceOf(com.legend.parser.element.Function.class, m.elements().get(0));
        com.legend.parser.element.Function u =
                assertInstanceOf(com.legend.parser.element.Function.class, m.elements().get(1));
        assertEquals("my::n", n.qualifiedName());
        assertEquals("my::u", u.qualifiedName());
        assertEquals(nr("Integer"), n.returnType());
        assertEquals(nr("Integer"), u.returnType());
    }

    // ===============================================================
    // B.3 — RelationalDatabaseConnection
    // ===============================================================

    @Test
    void connectionMinimalInMemoryNoAuth() {
        ParsedModel m = ElementParser.parse("""
                RelationalDatabaseConnection store::C
                {
                  store: store::PersonDb;
                  type: DuckDB;
                  specification: InMemory {};
                  auth: NoAuth {};
                }
                """);
        ConnectionDefinition c = assertInstanceOf(ConnectionDefinition.class,
                m.elements().get(0));
        assertEquals(
                new ConnectionDefinition(
                        "store::C", "store::PersonDb",
                        ConnectionDefinition.DatabaseType.DuckDB,
                        new ConnectionSpecification.InMemory(),
                        new AuthenticationSpec.NoAuth()),
                c);
    }

    @Test
    void connectionStaticDatasourceWithUsernamePassword() {
        ParsedModel m = ElementParser.parse("""
                RelationalDatabaseConnection store::Prod
                {
                  store: store::PersonDb;
                  type: Postgres;
                  specification: Static { host: 'db.example.com'; port: 5432; database: 'prod'; };
                  auth: UsernamePassword { username: 'svc'; passwordVaultRef: 'vault::prod_pw'; };
                }
                """);
        ConnectionDefinition c = (ConnectionDefinition) m.elements().get(0);
        assertEquals(ConnectionDefinition.DatabaseType.Postgres, c.databaseType());
        assertEquals(new ConnectionSpecification.StaticDatasource("db.example.com", 5432, "prod"),
                c.specification());
        assertEquals(new AuthenticationSpec.UsernamePassword("svc", "vault::prod_pw"),
                c.authentication());
    }

    @Test
    void connectionLocalFileSpec() {
        ParsedModel m = ElementParser.parse("""
                RelationalDatabaseConnection store::F
                {
                  store: store::S;
                  type: DuckDB;
                  specification: LocalFile { path: '/tmp/db.duckdb'; };
                  auth: NoAuth {};
                }
                """);
        ConnectionDefinition c = (ConnectionDefinition) m.elements().get(0);
        assertEquals(new ConnectionSpecification.LocalFile("/tmp/db.duckdb"), c.specification());
    }

    @Test
    void connectionRejectsUnknownDatabaseType() {
        ParseException ex = assertThrows(ParseException.class, () -> ElementParser.parse("""
                RelationalDatabaseConnection store::X
                {
                  store: store::S;
                  type: MariaDB;
                  specification: InMemory {};
                  auth: NoAuth {};
                }
                """));
        assertTrue(ex.getMessage().contains("MariaDB"),
                () -> "should name the unknown type, got: " + ex.getMessage());
    }

    @Test
    void connectionRejectsUnknownTopLevelKey() {
        // Strict-mode divergence from engine (D-2).
        ParseException ex = assertThrows(ParseException.class, () -> ElementParser.parse("""
                RelationalDatabaseConnection store::X
                {
                  store: store::S;
                  type: DuckDB;
                  specification: InMemory {};
                  auth: NoAuth {};
                  futureKey: 'value';
                }
                """));
        assertTrue(ex.getMessage().contains("futureKey")
                        && ex.getMessage().toLowerCase().contains("unknown"),
                () -> "should name the offending key, got: " + ex.getMessage());
    }

    // ===============================================================
    // B.3 — Runtime
    // ===============================================================

    @Test
    void runtimeMappingsAndConnectionBindings() {
        ParsedModel m = ElementParser.parse("""
                Runtime my::R
                {
                  mappings: [ my::M1, my::M2 ];
                  connections:
                  [
                    store::DbA: [ id: store::ConnA ],
                    store::DbB: [ id: store::ConnB ]
                  ];
                }
                """);
        RuntimeDefinition r = assertInstanceOf(RuntimeDefinition.class, m.elements().get(0));
        assertEquals(List.of("my::M1", "my::M2"), r.mappings());
        assertEquals(
                Map.of("store::DbA", "store::ConnA", "store::DbB", "store::ConnB"),
                r.connectionBindings());
        assertEquals(List.of(), r.jsonConnections());
    }

    @Test
    void runtimeEmbeddedJsonModelConnection() {
        ParsedModel m = ElementParser.parse(
                "Runtime my::R { mappings: [my::M]; connections: ["
                + "ModelStore: [ json: #{ JsonModelConnection { class: model::Raw;"
                + " url: 'data:application/json,[]'; } }# ] ]; }");
        RuntimeDefinition r = (RuntimeDefinition) m.elements().get(0);
        assertEquals(1, r.jsonConnections().size());
        assertEquals(new JsonModelConnection("model::Raw", "data:application/json,[]"),
                r.jsonConnections().get(0));
    }

    @Test
    void runtimeRejectsUnknownTopLevelKey() {
        ParseException ex = assertThrows(ParseException.class, () -> ElementParser.parse(
                "Runtime my::R { mappings: [my::M]; futureKey: foo; }"));
        assertTrue(ex.getMessage().contains("futureKey"));
    }

    @Test
    void singleConnectionRuntimeAcceptsEmptyBody() {
        // Engine's stub behavior: build a minimal runtime, body is skipped.
        ParsedModel m = ElementParser.parse(
                "SingleConnectionRuntime my::SCR { whatever: foo; }");
        RuntimeDefinition r = (RuntimeDefinition) m.elements().get(0);
        assertEquals("my::SCR", r.qualifiedName());
        assertEquals(List.of(), r.mappings());
        assertEquals(Map.of(), r.connectionBindings());
    }

    // ===============================================================
    // B.3 — Service
    // ===============================================================

    @Test
    void serviceMinimalWithExecutionBlock() {
        ParsedModel m = ElementParser.parse("""
                Service my::api::GetPerson
                {
                  pattern: '/api/person/{id}';
                  documentation: 'fetch by id';
                  execution: Single
                  {
                    query: |Person.all();
                    mapping: my::PersonMapping;
                    runtime: my::PersonRuntime;
                  }
                }
                """);
        ServiceDefinition s = assertInstanceOf(ServiceDefinition.class, m.elements().get(0));
        assertEquals("my::api::GetPerson", s.qualifiedName());
        assertEquals("/api/person/{id}", s.pattern());
        assertEquals("fetch by id", s.documentation());
        assertEquals("my::PersonMapping", s.mappingRef());
        assertEquals("my::PersonRuntime", s.runtimeRef());
        // 'Person.all()' desugars to getAll(PackageableElementPtr("Person")) —
        // SpecParser uses the 'get'-prefixed stdlib overload name (see
        // SpecParser.parseAllCall) to match the resolved-overload binding.
        AppliedFunction getAll = assertInstanceOf(AppliedFunction.class, s.functionBody());
        assertEquals("getAll", getAll.function());
        assertEquals(List.of(new PackageableElementPtr("Person")), getAll.parameters());
        assertEquals(null, s.testSuitesSource(), "testSuites absent → null");
    }

    @Test
    void serviceCapturesTestSuitesAsRawText() {
        // D-3: testSuites block preserved as raw text for B.4 to parse.
        // Pin both ends + every salient inner fragment so a capture bug that
        // trims, extends, or otherwise distorts the slice fails loudly.
        ParsedModel m = ElementParser.parse("""
                Service my::S
                {
                  pattern: '/x';
                  execution: Single { query: |1; mapping: my::M; runtime: my::R; }
                  testSuites: [ { suite_1: { setup: 'x'; } } ];
                }
                """);
        ServiceDefinition s = (ServiceDefinition) m.elements().get(0);
        String captured = s.testSuitesSource();
        assertNotNull(captured, "testSuites raw text should be captured");
        assertTrue(captured.startsWith("["),
                () -> "outer '[' must be in capture, got: " + captured);
        assertTrue(captured.endsWith("]"),
                () -> "outer ']' must be in capture, got: " + captured);
        // Every nested token must round-trip — guards against partial slices.
        for (String fragment : List.of("suite_1", "setup", "'x'")) {
            assertTrue(captured.contains(fragment),
                    () -> "missing fragment '" + fragment + "' in captured testSuites: " + captured);
        }
        // The capture must not bleed into following content (no execution-block tokens).
        assertFalse(captured.contains("execution"),
                () -> "capture must not bleed into preceding/following keys, got: " + captured);
    }

    @Test
    void servicePatternDefaultsToSlashWhenAbsent() {
        ParsedModel m = ElementParser.parse(
                "Service my::S { execution: Single { query: |1; mapping: my::M; runtime: my::R; } }");
        ServiceDefinition s = (ServiceDefinition) m.elements().get(0);
        assertEquals("/", s.pattern());
    }

    @Test
    void serviceRejectsUnknownTopLevelKey() {
        ParseException ex = assertThrows(ParseException.class, () -> ElementParser.parse(
                "Service my::S { pattern: '/x'; futureKey: 'value'; }"));
        assertTrue(ex.getMessage().contains("futureKey"));
    }

    // ===============================================================
    // B.3 — sealed-root coverage extended to 8 element kinds
    // ===============================================================

    // ===============================================================
    // B.4a — Database basics
    // ===============================================================

    @Test
    void databaseSingleTableColumns() {
        DatabaseDefinition db = (DatabaseDefinition) ElementParser.parse("""
                Database store::Db
                (
                  Table T_PERSON
                  (
                    ID INTEGER PRIMARY KEY,
                    NAME VARCHAR(100) NOT NULL,
                    SCORE DECIMAL(10,2)
                  )
                )
                """).elements().get(0);
        assertEquals("store::Db", db.qualifiedName());
        assertEquals(1, db.tables().size());
        TableDefinition t = db.tables().get(0);
        assertEquals("T_PERSON", t.name());
        assertEquals(
                List.of(
                        new ColumnDefinition("ID", new RelationalDataType.Integer_(), true, true),     // PK ⇒ NOT NULL
                        new ColumnDefinition("NAME", new RelationalDataType.Varchar(100), false, true),
                        new ColumnDefinition("SCORE", new RelationalDataType.Decimal(10, 2), false, false)),
                t.columns());
    }

    @Test
    void databaseQuotedTableAndColumnIdentifiers() {
        DatabaseDefinition db = (DatabaseDefinition) ElementParser.parse(
                "Database s::Db ( Table \"Mixed Case\" ( \"Col One\" INTEGER ) )")
                .elements().get(0);
        TableDefinition t = db.tables().get(0);
        assertEquals("Mixed Case", t.name(), "quotes must be stripped from table name");
        assertEquals("Col One", t.columns().get(0).name(), "quotes must be stripped from column name");
    }

    @Test
    void databaseIncludeStatements() {
        DatabaseDefinition db = (DatabaseDefinition) ElementParser.parse(
                "Database s::Child ( include s::Parent1 include other::P2 Table T ( C INTEGER ) )")
                .elements().get(0);
        assertEquals(List.of("s::Parent1", "other::P2"), db.includes());
        assertEquals(1, db.tables().size());
    }

    @Test
    void databaseSchemaMirrorsTablesAndViewsToFlatLists() {
        DatabaseDefinition db = (DatabaseDefinition) ElementParser.parse("""
                Database s::Db
                (
                  Schema S1
                  (
                    Table T1 ( C INTEGER )
                    Table T2 ( C INTEGER )
                  )
                  Table T3 ( C INTEGER )
                )
                """).elements().get(0);
        assertEquals(1, db.schemas().size());
        SchemaDefinition s = db.schemas().get(0);
        assertEquals("S1", s.name());
        assertEquals(2, s.tables().size(), "schema tables tracked under the schema");
        assertEquals(3, db.tables().size(),
                "flat tables list mirrors schema tables (engine parity)");
    }

    @Test
    void databaseRejectsUnknownTopLevelElement() {
        ParseException ex = assertThrows(ParseException.class,
                () -> ElementParser.parse("Database s::Db ( Foo X ( ) )"));
        assertTrue(ex.getMessage().contains("Foo"),
                () -> "should name unknown element, got: " + ex.getMessage());
    }

    // ===============================================================
    // B.4a — Join definitions (relational expression tree)
    // ===============================================================

    @Test
    void joinSimpleEquiCondition() {
        DatabaseDefinition db = (DatabaseDefinition) ElementParser.parse(
                "Database s::Db ( Table A ( ID INTEGER ) Table B ( A_ID INTEGER ) "
                + "Join AB(A.ID = B.A_ID) )").elements().get(0);
        assertEquals(1, db.joins().size());
        JoinDefinition j = db.joins().get(0);
        assertEquals("AB", j.name());
        Comparison c = (Comparison) j.operation();
        assertEquals(new ColumnRef(null, "A", "ID"), c.left());
        assertEquals(ComparisonOp.EQ, c.op());
        assertEquals(new ColumnRef(null, "B", "A_ID"), c.right());
    }

    @Test
    void joinCompoundAndExpression() {
        DatabaseDefinition db = (DatabaseDefinition) ElementParser.parse(
                "Database s::Db ( Table A ( X INTEGER, Y INTEGER ) "
                + "Table B ( X INTEGER, Y INTEGER ) "
                + "Join AB(A.X = B.X and A.Y = B.Y) )").elements().get(0);
        JoinDefinition j = db.joins().get(0);
        BooleanOp bo = (BooleanOp) j.operation();
        assertEquals(LogicalOp.AND, bo.op());
        // Right-recursive structure: BooleanOp(eq(A.X,B.X), and, eq(A.Y,B.Y))
        assertInstanceOf(Comparison.class, bo.left());
        assertInstanceOf(Comparison.class, bo.right());
    }

    @Test
    void joinSelfWithTargetColumn() {
        DatabaseDefinition db = (DatabaseDefinition) ElementParser.parse(
                "Database s::Db ( Table T ( ID INTEGER, PARENT_ID INTEGER ) "
                + "Join SelfJ(T.PARENT_ID = {target}.ID) )").elements().get(0);
        JoinDefinition j = db.joins().get(0);
        Comparison c = (Comparison) j.operation();
        assertEquals(new ColumnRef(null, "T", "PARENT_ID"), c.left());
        assertEquals(new RelationalOperation.TargetColumnRef("ID"), c.right());
    }

    // ===============================================================
    // B.4a — Filter and MultiGrainFilter
    // ===============================================================

    @Test
    void filterWithFunctionCallAndLiteral() {
        DatabaseDefinition db = (DatabaseDefinition) ElementParser.parse(
                "Database s::Db ( Table T ( N VARCHAR(50) ) "
                + "Filter NameFilter(upper(T.N) = 'BOB') )").elements().get(0);
        FilterDefinition f = db.filters().get(0);
        assertEquals("NameFilter", f.name());
        Comparison c = (Comparison) f.condition();
        FunctionCall fc = (FunctionCall) c.left();
        assertEquals("upper", fc.name());
        assertEquals(List.of(new ColumnRef(null, "T", "N")), fc.args());
        assertEquals(new Literal("BOB"), c.right());
    }

    @Test
    void filterRejectsBareIdentifierMatchingEngine() {
        // FINOS legend-engine (RelationalParseTreeWalker.generateTableAlias)
        // throws "Missing table or alias for column 'X'" when a bare
        // identifier appears in a Database-context expression. There is no
        // implicit-table column-ref shape in either engine or core/'s AST.
        ParseException ex = assertThrows(ParseException.class,
                () -> ElementParser.parse(
                        "Database s::Db ( Table T_PERSON ( IS_ACTIVE INTEGER ) "
                        + "Filter ActiveFilter(IS_ACTIVE = 1) )"));
        assertTrue(ex.getMessage().contains("Missing table or alias for column 'IS_ACTIVE'"),
                () -> "expected engine-parity error message, got: " + ex.getMessage());
    }

    @Test
    void filterIsNullAndIsNotNull() {
        DatabaseDefinition db = (DatabaseDefinition) ElementParser.parse(
                "Database s::Db ( Table T ( X INTEGER, Y INTEGER ) "
                + "Filter Fa(T.X is null) Filter Fb(T.Y is not null) )")
                .elements().get(0);
        assertInstanceOf(RelationalOperation.IsNull.class,
                db.filters().get(0).condition());
        assertInstanceOf(RelationalOperation.IsNotNull.class,
                db.filters().get(1).condition());
    }

    @Test
    void multiGrainFilterTrackedSeparately() {
        DatabaseDefinition db = (DatabaseDefinition) ElementParser.parse(
                "Database s::Db ( Table T ( X INTEGER ) MultiGrainFilter MGF(T.X = 1) )")
                .elements().get(0);
        assertEquals(0, db.filters().size());
        assertEquals(1, db.multiGrainFilters().size());
        assertEquals("MGF", db.multiGrainFilters().get(0).name());
    }

    // ===============================================================
    // B.4a — Views
    // ===============================================================

    @Test
    void viewMinimalColumnMapping() {
        DatabaseDefinition db = (DatabaseDefinition) ElementParser.parse(
                "Database s::Db ( Table T ( X INTEGER ) "
                + "View V ( a: T.X PRIMARY KEY, b: T.X ) )").elements().get(0);
        assertEquals(1, db.views().size());
        ViewDefinition v = db.views().get(0);
        assertEquals("V", v.name());
        assertEquals(null, v.filter(), "no ~filter clause was written");
        assertFalse(v.distinct());
        assertEquals(2, v.columnMappings().size());
        assertTrue(v.columnMappings().get(0).primaryKey(),
                "first column has PRIMARY KEY marker");
        assertFalse(v.columnMappings().get(1).primaryKey());
    }

    @Test
    void viewFilterDirectLocal() {
        // ~filter F → Direct(Local("F"))
        DatabaseDefinition db = (DatabaseDefinition) ElementParser.parse(
                "Database s::Db ( Table T ( X INTEGER, Y INTEGER ) "
                + "Filter F(T.X = 1) "
                + "View V ( ~filter F ~groupBy(T.Y) ~distinct  a: T.X, b: T.Y ) )")
                .elements().get(0);
        ViewDefinition v = db.views().get(0);
        FilterMapping.Direct direct = (FilterMapping.Direct) v.filter();
        FilterPointer.Local local = (FilterPointer.Local) direct.filter();
        assertEquals("F", local.name());
        assertEquals(List.of(new ColumnRef(null, "T", "Y")), v.groupByColumns());
        assertTrue(v.distinct());
    }

    @Test
    void viewFilterDirectCross() {
        // ~filter [other::Db] F → Direct(Cross("other::Db", "F"))
        DatabaseDefinition db = (DatabaseDefinition) ElementParser.parse(
                "Database s::Db ( Table T ( X INTEGER ) "
                + "View V ( ~filter [other::Db] RemoteFilter  a: T.X ) )")
                .elements().get(0);
        FilterMapping.Direct direct = (FilterMapping.Direct) db.views().get(0).filter();
        FilterPointer.Cross cross = (FilterPointer.Cross) direct.filter();
        assertEquals("other::Db", cross.db());
        assertEquals("RemoteFilter", cross.name());
    }

    @Test
    void viewFilterJoinMediatedLocalTarget() {
        // ~filter [s::Db] @J1 > @J2 | F → JoinMediated("s::Db", [J1,J2], Local("F"))
        DatabaseDefinition db = (DatabaseDefinition) ElementParser.parse(
                "Database s::Db ( Table T ( X INTEGER ) "
                + "Join J1(T.X = T.X) Join J2(T.X = T.X) "
                + "Filter F(T.X = 1) "
                + "View V ( ~filter [s::Db] @J1 > @J2 | F  a: T.X ) )")
                .elements().get(0);
        FilterMapping.JoinMediated jm = (FilterMapping.JoinMediated) db.views().get(0).filter();
        assertEquals("s::Db", jm.sourceDb());
        assertEquals(2, jm.joins().size());
        assertEquals("J1", jm.joins().get(0).joinName());
        assertEquals("J2", jm.joins().get(1).joinName());
        FilterPointer.Local local = (FilterPointer.Local) jm.filter();
        assertEquals("F", local.name());
    }

    @Test
    void viewFilterJoinMediatedCrossTarget() {
        // ~filter [s::Db] @J | [other::Db] F → JoinMediated(.., Cross("other::Db", "F"))
        DatabaseDefinition db = (DatabaseDefinition) ElementParser.parse(
                "Database s::Db ( Table T ( X INTEGER ) "
                + "Join J(T.X = T.X) "
                + "View V ( ~filter [s::Db] @J | [other::Db] RemoteFilter  a: T.X ) )")
                .elements().get(0);
        FilterMapping.JoinMediated jm = (FilterMapping.JoinMediated) db.views().get(0).filter();
        assertEquals("s::Db", jm.sourceDb());
        assertEquals(1, jm.joins().size());
        FilterPointer.Cross cross = (FilterPointer.Cross) jm.filter();
        assertEquals("other::Db", cross.db());
        assertEquals("RemoteFilter", cross.name());
    }

    @Test
    void joinMediatedFilterRequiresSourceDbQualifier() {
        // ~filter @J | F is invalid — engine grammar requires [DB1] before the
        // join sequence. Without it, our parser cannot tell join-mediation
        // from a malformed direct reference.
        ParseException ex = assertThrows(ParseException.class,
                () -> ElementParser.parse(
                        "Database s::Db ( Table T ( X INTEGER ) Join J(T.X = T.X) "
                        + "View V ( ~filter @J | F  a: T.X ) )"));
        assertTrue(ex.getMessage().contains("[DB]"),
                () -> "expected error about missing [DB] qualifier, got: " + ex.getMessage());
    }

    @Test
    void filterMappingJoinMediatedRejectsEmptyJoins() {
        // Direct construction sanity check: the sealed contract makes empty
        // joins on JoinMediated a programmer error, not a runtime state.
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new FilterMapping.JoinMediated(
                        "s::Db", List.of(), new FilterPointer.Local("F")));
        assertTrue(ex.getMessage().contains("at least one join hop"),
                () -> "JoinMediated must reject empty joins; got: " + ex.getMessage());
    }

    // ===============================================================
    // B.4a — Join navigation (multi-hop chains)
    // ===============================================================

    @Test
    void joinNavigationMultiHopWithTerminal() {
        // Database-context join navigation: @J1 > @J2 | T.COL
        // Engine permits this in filters; we test parser only here.
        DatabaseDefinition db = (DatabaseDefinition) ElementParser.parse(
                "Database s::Db ( Table T ( COL INTEGER ) "
                + "Join J1(T.COL = T.COL) Join J2(T.COL = T.COL) "
                + "Filter F(@J1 > @J2 | T.COL = 1) )").elements().get(0);
        FilterDefinition f = db.filters().get(0);  // joins go in db.joins(), filters in db.filters()
        // Filter condition is a JoinNavigation directly; the comparison
        // T.COL = 1 lives inside its terminal.
        RelationalOperation.JoinNavigation jn = (RelationalOperation.JoinNavigation) f.condition();
        assertEquals(2, jn.chain().size());
        assertEquals("J1", jn.chain().get(0).joinName());
        assertEquals("J2", jn.chain().get(1).joinName());
        assertInstanceOf(Comparison.class, jn.terminal(),
                "terminal after | is parsed as a comparison expression");
    }

    // ===============================================================
    // B.4a — sealed-root coverage extended to 9 element kinds
    // ===============================================================

    @Test
    void allNineB4aElementKindsParseInOneSource() {
        ParsedModel m = ElementParser.parse("""
                Class my::Person { name: String[1]; }
                Association my::A { x: my::Person[1]; y: my::Firm[*]; }
                Enum my::E { A, B }
                Profile my::P { stereotypes: [s]; }
                function my::f(): String[1] { 'hi' }
                RelationalDatabaseConnection store::C { store: store::Db; type: DuckDB;
                  specification: InMemory {}; auth: NoAuth {}; }
                Runtime my::R { mappings: [my::M]; connections: []; }
                Service my::Svc { pattern: '/x';
                  execution: Single { query: |1; mapping: my::M; runtime: my::R; } }
                Database store::Db ( Table T ( ID INTEGER PRIMARY KEY ) )
                """);
        assertEquals(9, m.elements().size());
        assertInstanceOf(DatabaseDefinition.class, m.elements().get(8));
    }

    @Test
    void allEightB3ElementKindsParseInOneSource() {
        ParsedModel m = ElementParser.parse("""
                Class my::Person { name: String[1]; }
                Association my::A { x: my::Person[1]; y: my::Firm[*]; }
                Enum my::E { A, B }
                Profile my::P { stereotypes: [s]; }
                function my::f(): String[1] { 'hi' }
                RelationalDatabaseConnection store::C { store: store::S; type: DuckDB;
                  specification: InMemory {}; auth: NoAuth {}; }
                Runtime my::R { mappings: [my::M]; connections: []; }
                Service my::Svc { pattern: '/x';
                  execution: Single { query: |1; mapping: my::M; runtime: my::R; } }
                """);
        assertEquals(8, m.elements().size());
        assertInstanceOf(ClassDefinition.class,       m.elements().get(0));
        assertInstanceOf(AssociationDefinition.class, m.elements().get(1));
        assertInstanceOf(EnumDefinition.class,        m.elements().get(2));
        assertInstanceOf(ProfileDefinition.class,     m.elements().get(3));
        assertInstanceOf(FunctionDefinition.class,    m.elements().get(4));
        assertInstanceOf(ConnectionDefinition.class,  m.elements().get(5));
        assertInstanceOf(RuntimeDefinition.class,     m.elements().get(6));
        assertInstanceOf(ServiceDefinition.class,     m.elements().get(7));
    }

    // ===============================================================
    // B.4b — Mapping (relational class mappings)
    // ===============================================================

    private static ClassMapping.Relational firstRelationalClassMapping(String src) {
        MappingDefinition md = (MappingDefinition) ElementParser.parse(src).elements().get(0);
        return (ClassMapping.Relational) md.classMappings().get(0);
    }

    @Test
    void mappingEmpty() {
        MappingDefinition md = (MappingDefinition)
                ElementParser.parse("Mapping my::M ( )").elements().get(0);
        assertEquals("my::M", md.qualifiedName());
        assertTrue(md.includes().isEmpty());
        assertTrue(md.classMappings().isEmpty());
    }

    @Test
    void mappingIncludeWithoutSubstitutions() {
        MappingDefinition md = (MappingDefinition)
                ElementParser.parse("Mapping my::M ( include other::Base )").elements().get(0);
        assertEquals(1, md.includes().size());
        assertEquals("other::Base", md.includes().get(0).mappingPath());
        assertTrue(md.includes().get(0).substitutions().isEmpty());
    }

    @Test
    void mappingIncludeWithStoreSubstitutions() {
        MappingDefinition md = (MappingDefinition)
                ElementParser.parse(
                        "Mapping my::M ( include other::Base "
                        + "[ store::OldDb -> store::NewDb, store::Old2 -> store::New2 ] )")
                .elements().get(0);
        var subs = md.includes().get(0).substitutions();
        assertEquals(2, subs.size());
        assertEquals("store::OldDb", subs.get(0).originalStore());
        assertEquals("store::NewDb", subs.get(0).replacementStore());
        assertEquals("store::Old2", subs.get(1).originalStore());
        assertEquals("store::New2", subs.get(1).replacementStore());
    }

    @Test
    void relationalClassMappingMinimal() {
        var cm = firstRelationalClassMapping(
                "Mapping my::M ( *model::Person: Relational { ~mainTable [db::DB] PERSON } )");
        assertEquals("model::Person", cm.className());
        assertNull(cm.setId());
        assertNull(cm.extendsSetId());
        assertTrue(cm.root());
        assertEquals(new MappingDefinition.TableReference("db::DB", "PERSON"), cm.mainTable());
        assertNull(cm.filter());
        assertFalse(cm.distinct());
        assertTrue(cm.groupBy().isEmpty());
        assertTrue(cm.primaryKey().isEmpty());
        assertTrue(cm.propertyMappings().isEmpty());
    }

    @Test
    void relationalClassMappingSetIdAndExtends() {
        var cm = firstRelationalClassMapping(
                "Mapping my::M ( model::Person[employees] extends [parentSet]: Relational "
                + "{ ~mainTable [db::DB] PERSON } )");
        assertEquals("employees", cm.setId());
        assertEquals("parentSet", cm.extendsSetId());
        assertFalse(cm.root(), "no leading * was written");
    }

    @Test
    void relationalClassMappingMainTableWithSchema() {
        // ~mainTable [db::DB] SCHEMA.TABLE  — schema-qualified main table
        var cm = firstRelationalClassMapping(
                "Mapping my::M ( *model::Person: Relational { ~mainTable [db::DB] HR.PERSON } )");
        assertEquals("HR.PERSON", cm.mainTable().table());
    }

    @Test
    void relationalClassMappingTildeModifiers() {
        // Legend-engine canonical clause order (RelationalParserGrammar.g4):
        //   ~filter? ~distinct? ~groupBy? ~primaryKey? ~mainTable?
        //   (propertyMapping,...)
        // ~mainTable goes LAST among the tilde-commands -- writing it
        // before ~distinct/~groupBy/~primaryKey is a grammar violation.
        var cm = firstRelationalClassMapping(
                "Mapping my::M ( *model::Person: Relational { "
                + "~distinct "
                + "~groupBy([db::DB]PERSON.DEPT_ID) "
                + "~primaryKey([db::DB]PERSON.ID) "
                + "~mainTable [db::DB] PERSON "
                + "} )");
        assertTrue(cm.distinct());
        assertEquals("db::DB", ((ColumnRef) cm.groupBy().get(0)).databaseName());
        assertEquals("PERSON", ((ColumnRef) cm.groupBy().get(0)).table());
        assertEquals("DEPT_ID", ((ColumnRef) cm.groupBy().get(0)).column());
        assertEquals("db::DB", ((ColumnRef) cm.primaryKey().get(0)).databaseName());
        assertEquals("PERSON", ((ColumnRef) cm.primaryKey().get(0)).table());
        assertEquals("ID", ((ColumnRef) cm.primaryKey().get(0)).column());
    }

    @Test
    void relationalClassMappingFilterDirectLocal() {
        // ~filter precedes ~mainTable per legend-engine grammar.
        var cm = firstRelationalClassMapping(
                "Mapping my::M ( *model::Person: Relational { "
                + "~filter ActivePersonFilter "
                + "~mainTable [db::DB] PERSON "
                + "} )");
        assertInstanceOf(FilterMapping.Direct.class, cm.filter(),
                "~filter <Name> must produce Direct, not JoinMediated");
        var direct = (FilterMapping.Direct) cm.filter();
        assertInstanceOf(FilterPointer.Local.class, direct.filter(),
                "bare filter name must be Local (no cross-db prefix)");
        var local = (FilterPointer.Local) direct.filter();
        assertEquals("ActivePersonFilter", local.name());
    }

    @Test
    void relationalClassMappingMainTableIsOptional() {
        // Per legend-engine grammar, mappingMainTable? is 0..1 --
        // a Relational class mapping MAY have no main table (e.g.
        // when extending another set-id that supplies it, or when
        // every property mapping self-qualifies with [DB]T.col).
        // Our previous implementation hard-rejected this; legend-
        // engine accepts.
        var cm = firstRelationalClassMapping(
                "Mapping my::M ( *model::Person: Relational { "
                + "firstName: [db::DB]PERSON.FIRST_NAME "
                + "} )");
        assertNull(cm.mainTable(), "no ~mainTable -> mainTable is null");
        assertEquals(1, cm.propertyMappings().size());
    }

    @Test
    void relationalClassMappingClauseOrderIsEnforced() {
        // ~mainTable before ~primaryKey is a legend-engine grammar
        // violation; we must reject with a diagnostic that names
        // the offending clause and the canonical order. A generic
        // 'unexpected token' would not help users port from
        // engine-lite (which accepts any order).
        ParseException ex = assertThrows(ParseException.class,
                () -> ElementParser.parse("Mapping my::M ( *model::Person: Relational { "
                        + "~mainTable [db::DB] PERSON "
                        + "~primaryKey([db::DB]PERSON.ID) "
                        + "} )"));
        assertTrue(ex.getMessage().contains("~primaryKey"),
                () -> "error must name the offending clause, got: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("out of order"),
                () -> "error must say 'out of order', got: " + ex.getMessage());
    }

    @Test
    void relationalClassMappingDuplicateTildeClauseRejected() {
        // Each ~clause is 0..1 per grammar. Two ~distinct in a row
        // is duplicated; the second one is an out-of-order violation.
        ParseException ex = assertThrows(ParseException.class,
                () -> ElementParser.parse("Mapping my::M ( *model::Person: Relational { "
                        + "~distinct "
                        + "~distinct "
                        + "~mainTable [db::DB] PERSON "
                        + "} )"));
        assertTrue(ex.getMessage().contains("~distinct"),
                () -> "want duplicated-clause diagnostic, got: " + ex.getMessage());
    }

    @Test
    void relationalClassMappingFilterJoinMediated() {
        // ~filter precedes ~mainTable per legend-engine grammar.
        var cm = firstRelationalClassMapping(
                "Mapping my::M ( *model::Person: Relational { "
                + "~filter [db::DB] @PersonFirm | ActiveFirm "
                + "~mainTable [db::DB] PERSON "
                + "} )");
        var jm = (FilterMapping.JoinMediated) cm.filter();
        assertEquals("db::DB", jm.sourceDb());
        assertEquals(1, jm.joins().size());
        assertEquals("PersonFirm", jm.joins().get(0).joinName());
    }

    @Test
    void propertyMappingPlainColumn() {
        var cm = firstRelationalClassMapping(
                "Mapping my::M ( *model::Person: Relational { "
                + "~mainTable [db::DB] PERSON "
                + "firstName: PERSON.FIRST_NAME "
                + "} )");
        var col = (PropertyMapping.Column) cm.propertyMappings().get(0);
        assertEquals("firstName", col.propertyName());
        assertEquals("db::DB", col.database());
        assertEquals("PERSON", col.table());
        assertEquals("FIRST_NAME", col.column());
    }

    @Test
    void propertyMappingBareIdentifierResolvesToMainTable() {
        // FIRST_NAME (no T.) — engine ScopeInfo parity: parse-time resolves
        // to mainTable.FIRST_NAME, not a sentinel/null table.
        var cm = firstRelationalClassMapping(
                "Mapping my::M ( *model::Person: Relational { "
                + "~mainTable [db::DB] PERSON "
                + "firstName: FIRST_NAME "
                + "} )");
        var expr = (PropertyMapping.Expression) cm.propertyMappings().get(0);
        var ref = (ColumnRef) expr.expression();
        assertEquals("db::DB", ref.databaseName());
        assertEquals("PERSON", ref.table());
        assertEquals("FIRST_NAME", ref.column());
    }

    @Test
    void propertyMappingColumnWithExplicitDbOverride() {
        var cm = firstRelationalClassMapping(
                "Mapping my::M ( *model::Person: Relational { "
                + "~mainTable [db::DB] PERSON "
                + "remoteField: [other::Db] EXTRA.NAME "
                + "} )");
        var col = (PropertyMapping.Column) cm.propertyMappings().get(0);
        assertEquals("other::Db", col.database(),
                "explicit [DB] overrides the main table's database");
        assertEquals("EXTRA", col.table());
        assertEquals("NAME", col.column());
    }

    @Test
    void propertyMappingEnumeratedColumn() {
        var cm = firstRelationalClassMapping(
                "Mapping my::M ( *model::Person: Relational { "
                + "~mainTable [db::DB] PERSON "
                + "status: EnumerationMapping StatusMapping : PERSON.STATUS "
                + "} )");
        var enumCol = (PropertyMapping.EnumeratedColumn) cm.propertyMappings().get(0);
        assertEquals("StatusMapping", enumCol.enumMappingId());
        assertEquals("PERSON", enumCol.table());
        assertEquals("STATUS", enumCol.column());
    }

    @Test
    void propertyMappingSingleHopJoin() {
        var cm = firstRelationalClassMapping(
                "Mapping my::M ( *model::Person: Relational { "
                + "~mainTable [db::DB] PERSON "
                + "firm: @Person_Firm "
                + "} )");
        var j = (PropertyMapping.Join) cm.propertyMappings().get(0);
        assertEquals("db::DB", j.database());
        assertEquals(1, j.joins().size());
        assertEquals("Person_Firm", j.joins().get(0).joinName());
    }

    @Test
    void propertyMappingMultiHopJoinWithTerminalColumn() {
        var cm = firstRelationalClassMapping(
                "Mapping my::M ( *model::Person: Relational { "
                + "~mainTable [db::DB] PERSON "
                + "cityName: @Person_Address > @Address_City | CITY.NAME "
                + "} )");
        var jt = (PropertyMapping.JoinTerminalColumn) cm.propertyMappings().get(0);
        assertEquals(2, jt.joins().size());
        assertEquals("Person_Address", jt.joins().get(0).joinName());
        assertEquals("Address_City", jt.joins().get(1).joinName());
        var ref = (ColumnRef) jt.terminalColumn();
        assertEquals("CITY", ref.table());
        assertEquals("NAME", ref.column());
    }

    @Test
    void qualifiedColumnRefInsideExpressionHasNullDatabase() {
        // Engine parity: at parse time, a qualified T.COL without an
        // explicit [DB] qualifier leaves database null even inside a
        // mapping body. T may live in mainTable's db OR in any included
        // db; Phase D resolves. (The bare-id case is different: it's
        // unambiguously the main table and IS resolved eagerly.)
        var cm = firstRelationalClassMapping(
                "Mapping my::M ( *model::Person: Relational { "
                + "~mainTable [db::DB] PERSON "
                + "fullName: concat(PERSON.FIRST_NAME, PERSON.LAST_NAME) "
                + "} )");
        var expr = (PropertyMapping.Expression) cm.propertyMappings().get(0);
        var fn = (FunctionCall) expr.expression();
        var firstArg = (ColumnRef) fn.args().get(0);
        assertNull(firstArg.databaseName(),
                "qualified T.COL must leave db null at parse time");
        assertEquals("PERSON", firstArg.table());
    }

    @Test
    void propertyMappingStructuredExpression() {
        var cm = firstRelationalClassMapping(
                "Mapping my::M ( *model::Person: Relational { "
                + "~mainTable [db::DB] PERSON "
                + "fullName: concat(PERSON.FIRST_NAME, ' ', PERSON.LAST_NAME) "
                + "} )");
        var expr = (PropertyMapping.Expression) cm.propertyMappings().get(0);
        var fn = (FunctionCall) expr.expression();
        assertEquals("concat", fn.name());
        assertEquals(3, fn.args().size());
    }

    @Test
    void mappingMissingMainTableThrows() {
        ParseException e = assertThrows(ParseException.class, () ->
                ElementParser.parse(
                        "Mapping my::M ( *model::Person: Relational { firstName: PERSON.FIRST_NAME } )"));
        assertTrue(e.getMessage().toLowerCase().contains("~maintable"),
                () -> "expected error about missing ~mainTable, got: " + e.getMessage());
    }

    @Test
    void mappingUnsupportedClassMappingTypeThrows() {
        // 'Operation' class mappings aren't part of B.4b's scope.
        ParseException e = assertThrows(ParseException.class, () ->
                ElementParser.parse(
                        "Mapping my::M ( *model::P: SomethingElse { x: 1 } )"));
        assertTrue(e.getMessage().toLowerCase().contains("unsupported"),
                () -> "expected 'unsupported' message, got: " + e.getMessage());
    }

    // ===============================================================
    // B.4c — Association mappings
    // ===============================================================

    private static AssociationMapping.Relational firstAssociationMapping(String src) {
        MappingDefinition md = (MappingDefinition) ElementParser.parse(src).elements().get(0);
        return (AssociationMapping.Relational) md.associationMappings().get(0);
    }

    @Test
    void associationMappingSingleProperty() {
        var am = firstAssociationMapping(
                "Mapping my::M ( "
                + "my::Person_Firm: Relational { AssociationMapping ( "
                + "firm: [db::DB] @Person_Firm "
                + ") } )");
        assertEquals("my::Person_Firm", am.associationName());
        assertEquals(1, am.propertyMappings().size());
        var apm = am.propertyMappings().get(0);
        assertNull(apm.sourceSetId());
        assertNull(apm.targetSetId());
        assertEquals("firm", apm.propertyName());
        var join = (PropertyMapping.Join) apm.body();
        assertEquals("db::DB", join.database());
        assertEquals("Person_Firm", join.joins().get(0).joinName());
    }

    @Test
    void associationMappingTwoEndsCommaSeparated() {
        var am = firstAssociationMapping(
                "Mapping my::M ( "
                + "my::Person_Firm: Relational { AssociationMapping ( "
                + "firm: [db::DB] @Person_Firm, "
                + "person: [db::DB] @Person_Firm "
                + ") } )");
        assertEquals(2, am.propertyMappings().size());
        assertEquals("firm", am.propertyMappings().get(0).propertyName());
        assertEquals("person", am.propertyMappings().get(1).propertyName());
    }

    @Test
    void associationMappingWithSetIdBrackets() {
        var am = firstAssociationMapping(
                "Mapping my::M ( "
                + "my::Person_Firm: Relational { AssociationMapping ( "
                + "firm[employees, mainFirms]: [db::DB] @Person_Firm "
                + ") } )");
        var apm = am.propertyMappings().get(0);
        assertEquals("employees", apm.sourceSetId());
        assertEquals("mainFirms", apm.targetSetId());
    }

    @Test
    void associationMappingMultiHopJoinChain() {
        var am = firstAssociationMapping(
                "Mapping my::M ( "
                + "my::Person_City: Relational { AssociationMapping ( "
                + "city: [db::DB] @Person_Address > @Address_City "
                + ") } )");
        var join = (PropertyMapping.Join) am.propertyMappings().get(0).body();
        assertEquals(
                List.of("Person_Address", "Address_City"),
                join.joins().stream().map(JoinChainElement::joinName).toList(),
                "join chain must preserve hop names and order");
        assertEquals("db::DB", join.database());
    }

    @Test
    void associationMappingRejectsBareIdentifier() {
        // No mainTable in association context → bare-id must error
        // (engine: "Missing table or alias for column 'X'").
        ParseException e = assertThrows(ParseException.class, () ->
                ElementParser.parse(
                        "Mapping my::M ( my::A: Relational { AssociationMapping ( "
                        + "firm: BARE_NAME ) } )"));
        assertTrue(e.getMessage().toLowerCase().contains("missing table or alias"),
                () -> "expected bare-id rejection, got: " + e.getMessage());
    }

    @Test
    void associationMappingRejectsLeadingStar() {
        ParseException e = assertThrows(ParseException.class, () ->
                ElementParser.parse(
                        "Mapping my::M ( *my::A: Relational { AssociationMapping ( "
                        + "firm: [db::DB] @J ) } )"));
        assertTrue(e.getMessage().toLowerCase().contains("association"),
                () -> "expected association-related error, got: " + e.getMessage());
    }

    @Test
    void associationMappingPropertyJoinRequiresDb() {
        // Plain @J without [db::DB] and no main table → must error.
        ParseException e = assertThrows(ParseException.class, () ->
                ElementParser.parse(
                        "Mapping my::M ( my::A: Relational { AssociationMapping ( "
                        + "firm: @SomeJoin ) } )"));
        assertTrue(e.getMessage().toLowerCase().contains("requires a database"),
                () -> "expected db-required error, got: " + e.getMessage());
    }

    @Test
    void mappingMixesClassAndAssociationMappings() {
        MappingDefinition md = (MappingDefinition) ElementParser.parse(
                "Mapping my::M ( "
                + "*model::Person: Relational { ~mainTable [db::DB] PERSON  name: PERSON.NAME } "
                + "my::Person_Firm: Relational { AssociationMapping ( firm: [db::DB] @P_F ) } "
                + ")").elements().get(0);
        assertEquals(1, md.classMappings().size());
        assertEquals(1, md.associationMappings().size());
        assertEquals("model::Person",
                ((ClassMapping.Relational) md.classMappings().get(0)).className());
        assertEquals("my::Person_Firm",
                ((AssociationMapping.Relational) md.associationMappings().get(0)).associationName());
    }

    @Test
    void associationPropertyMappingRejectsHalfSetId() {
        // sourceSetId without targetSetId — the data-model invariant
        // catches this at construction. Parser can't produce it directly
        // (grammar requires both), but the invariant is documented and
        // pinned here.
        assertThrows(IllegalArgumentException.class, () ->
                new AssociationPropertyMapping("src", null,
                        new PropertyMapping.Join("p", "db", List.of(
                                new JoinChainElement("J", null, "db", false)))));
    }

    // ===============================================================
    // B.4d — Enumeration mapping declarations
    // ===============================================================

    private static EnumerationMapping firstEnumerationMapping(String src) {
        MappingDefinition md = (MappingDefinition) ElementParser.parse(src).elements().get(0);
        return md.enumerationMappings().get(0);
    }

    @Test
    void enumerationMappingWithIdAndSingleStringSources() {
        var em = firstEnumerationMapping(
                "Mapping my::M ( "
                + "model::OrderStatus: EnumerationMapping StatusMap { "
                + "PENDING: 'P', "
                + "SHIPPED: 'S' "
                + "} )");
        assertEquals("model::OrderStatus", em.enumName());
        assertEquals("StatusMap", em.mappingId());
        assertEquals(2, em.valueMappings().size());
        var pending = em.valueMappings().get(0);
        assertEquals("PENDING", pending.enumValue());
        var src = (EnumerationMapping.SourceValue.StringValue) pending.sourceValues().get(0);
        assertEquals("P", src.value());
    }

    @Test
    void enumerationMappingWithoutId() {
        var em = firstEnumerationMapping(
                "Mapping my::M ( "
                + "model::TaskStatus: EnumerationMapping { "
                + "TODO: 'TODO' "
                + "} )");
        assertNull(em.mappingId(),
                "engine allows omitting the mapping ID; generated synthetically later");
    }

    @Test
    void enumerationMappingBracketedMultipleStringSources() {
        var em = firstEnumerationMapping(
                "Mapping my::M ( "
                + "model::S: EnumerationMapping Mid { "
                + "PENDING: ['P', 'PEND', 'WAITING'] "
                + "} )");
        var sources = em.valueMappings().get(0).sourceValues();
        assertEquals(3, sources.size());
        assertEquals("P", ((EnumerationMapping.SourceValue.StringValue) sources.get(0)).value());
        assertEquals("PEND", ((EnumerationMapping.SourceValue.StringValue) sources.get(1)).value());
        assertEquals("WAITING", ((EnumerationMapping.SourceValue.StringValue) sources.get(2)).value());
    }

    @Test
    void enumerationMappingIntegerSources() {
        var em = firstEnumerationMapping(
                "Mapping my::M ( "
                + "model::S: EnumerationMapping Mid { "
                + "BUY: 1, "
                + "SELL: [2, 3] "
                + "} )");
        var buyVal = (EnumerationMapping.SourceValue.IntegerValue)
                em.valueMappings().get(0).sourceValues().get(0);
        assertEquals(1L, buyVal.value());
        var sellSources = em.valueMappings().get(1).sourceValues();
        assertEquals(2, sellSources.size());
        assertEquals(2L, ((EnumerationMapping.SourceValue.IntegerValue) sellSources.get(0)).value());
        assertEquals(3L, ((EnumerationMapping.SourceValue.IntegerValue) sellSources.get(1)).value());
    }

    @Test
    void enumerationMappingCrossEnumSource() {
        // Map our enum values to values of another (already-defined) enum.
        var em = firstEnumerationMapping(
                "Mapping my::M ( "
                + "model::Local: EnumerationMapping Mid { "
                + "FAST: other::Speed.HIGH, "
                + "SLOW: other::Speed.LOW "
                + "} )");
        var fastSrc = (EnumerationMapping.SourceValue.EnumRef)
                em.valueMappings().get(0).sourceValues().get(0);
        assertEquals("other::Speed", fastSrc.enumPath());
        assertEquals("HIGH", fastSrc.enumValueName());
    }

    @Test
    void enumerationMappingMixedSourceKinds() {
        var em = firstEnumerationMapping(
                "Mapping my::M ( "
                + "model::S: EnumerationMapping Mid { "
                + "X: ['STR', 42, other::E.V] "
                + "} )");
        var sources = em.valueMappings().get(0).sourceValues();
        assertInstanceOf(EnumerationMapping.SourceValue.StringValue.class, sources.get(0));
        assertInstanceOf(EnumerationMapping.SourceValue.IntegerValue.class, sources.get(1));
        assertInstanceOf(EnumerationMapping.SourceValue.EnumRef.class, sources.get(2));
    }

    @Test
    void enumerationMappingTrailingCommaTolerated() {
        // Regression: trailing comma must yield N entries, not N+1 with a
        // phantom empty entry silently appended.
        var em = firstEnumerationMapping(
                "Mapping my::M ( "
                + "model::S: EnumerationMapping Mid { "
                + "A: 'a', "
                + "B: 'b', "
                + "} )");
        assertEquals(2, em.valueMappings().size(),
                "trailing comma must not produce a phantom value mapping");
        assertEquals("A", em.valueMappings().get(0).enumValue());
        assertEquals("B", em.valueMappings().get(1).enumValue());
        // Last entry's source value must be the real 'b', not empty/null.
        var lastSource = em.valueMappings().get(1).sourceValues().get(0);
        assertEquals("b",
                ((EnumerationMapping.SourceValue.StringValue) lastSource).value());
    }

    @Test
    void enumerationMappingEmptyBracketsRejected() {
        ParseException e = assertThrows(ParseException.class, () ->
                ElementParser.parse(
                        "Mapping my::M ( "
                        + "model::S: EnumerationMapping Mid { X: [] } )"));
        assertTrue(e.getMessage().toLowerCase().contains("at least one source value"),
                () -> "expected empty-brackets error, got: " + e.getMessage());
    }

    @Test
    void enumerationMappingRejectsLeadingStar() {
        ParseException e = assertThrows(ParseException.class, () ->
                ElementParser.parse(
                        "Mapping my::M ( *model::S: EnumerationMapping Mid { X: 'x' } )"));
        assertTrue(e.getMessage().toLowerCase().contains("enumeration"),
                () -> "expected enumeration-related error, got: " + e.getMessage());
    }

    @Test
    void mappingWithClassAndEnumerationMappingTogether() {
        MappingDefinition md = (MappingDefinition) ElementParser.parse(
                "Mapping my::M ( "
                + "*model::Order: Relational { ~mainTable [db::DB] ORDERS "
                + "  status: EnumerationMapping StatusMap : ORDERS.STATUS } "
                + "model::OrderStatus: EnumerationMapping StatusMap { "
                + "  PENDING: 'P', SHIPPED: 'S' } "
                + ")").elements().get(0);
        assertEquals(1, md.classMappings().size());
        assertEquals(1, md.enumerationMappings().size());
        // The class mapping's status property references the enum mapping by id.
        var cm = (ClassMapping.Relational) md.classMappings().get(0);
        var enumCol = (PropertyMapping.EnumeratedColumn) cm.propertyMappings().get(0);
        assertEquals("StatusMap", enumCol.enumMappingId());
        assertEquals("StatusMap", md.enumerationMappings().get(0).mappingId());
    }

    // ===============================================================
    // B.4e — Pure (model-to-model) class mappings
    // ===============================================================

    private static ClassMapping.Pure firstPureClassMapping(String src) {
        MappingDefinition md = (MappingDefinition) ElementParser.parse(src).elements().get(0);
        return (ClassMapping.Pure) md.classMappings().get(0);
    }

    @Test
    void pureClassMappingMinimal() {
        var cm = firstPureClassMapping(
                "Mapping my::M ( "
                + "*model::Person: Pure { "
                + "~src model::RawPerson "
                + "fullName: $src.firstName + ' ' + $src.lastName "
                + "} )");
        assertEquals("model::Person", cm.className());
        assertTrue(cm.root());
        assertEquals("model::RawPerson", cm.sourceClass());
        assertNull(cm.filter(), "no ~filter means cm.filter() is null");
        assertEquals(1, cm.propertyBindings().size());
        var b = cm.propertyBindings().get(0);
        assertEquals("fullName", b.propertyName());
        // $src.firstName + ' ' + $src.lastName parses left-associative:
        // plus(plus($src.firstName, ' '), $src.lastName).
        AppliedFunction outerPlus = assertInstanceOf(AppliedFunction.class, b.expression());
        assertEquals("plus", outerPlus.function());
        assertEquals(2, outerPlus.parameters().size());
        AppliedFunction innerPlus = assertInstanceOf(AppliedFunction.class, outerPlus.parameters().get(0));
        assertEquals("plus", innerPlus.function());
        AppliedProperty srcFirst = assertInstanceOf(AppliedProperty.class, innerPlus.parameters().get(0));
        assertEquals("firstName", srcFirst.property());
        assertEquals(new Variable("src"), srcFirst.receiver());
        assertEquals(new CString(" "), innerPlus.parameters().get(1));
        AppliedProperty srcLast = assertInstanceOf(AppliedProperty.class, outerPlus.parameters().get(1));
        assertEquals("lastName", srcLast.property());
        assertEquals(new Variable("src"), srcLast.receiver());
    }

    @Test
    void pureClassMappingMultipleProperties() {
        var cm = firstPureClassMapping(
                "Mapping my::M ( "
                + "*model::Person: Pure { "
                + "~src model::RawPerson "
                + "firstName: $src.firstName, "
                + "lastName:  $src.lastName, "
                + "fullName:  $src.firstName + ' ' + $src.lastName "
                + "} )");
        assertEquals(3, cm.propertyBindings().size());
        assertEquals("firstName", cm.propertyBindings().get(0).propertyName());
        assertEquals("lastName",  cm.propertyBindings().get(1).propertyName());
        assertEquals("fullName",  cm.propertyBindings().get(2).propertyName());
    }

    @Test
    void pureClassMappingWithFilter() {
        var cm = firstPureClassMapping(
                "Mapping my::M ( "
                + "*model::ActivePerson: Pure { "
                + "~src model::RawPerson "
                + "~filter $src.isActive == true "
                + "firstName: $src.firstName "
                + "} )");
        // ~filter $src.isActive == true desugars to
        // equal(AppliedProperty($src, isActive), CBoolean(true)).
        AppliedFunction eq = assertInstanceOf(AppliedFunction.class, cm.filter());
        assertEquals("equal", eq.function());
        assertEquals(2, eq.parameters().size());
        AppliedProperty srcActive = assertInstanceOf(AppliedProperty.class, eq.parameters().get(0));
        assertEquals("isActive", srcActive.property());
        assertEquals(new Variable("src"), srcActive.receiver());
        assertEquals(1, cm.propertyBindings().size());
    }

    @Test
    void pureClassMappingBodyCanContainCommasInsideCalls() {
        // Commas inside if(...) must NOT split the property binding.
        var cm = firstPureClassMapping(
                "Mapping my::M ( "
                + "*model::Person: Pure { "
                + "~src model::RawPerson "
                + "ageGroup: if($src.age < 18, |'Minor', |if($src.age < 65, |'Adult', |'Senior')), "
                + "firstName: $src.firstName "
                + "} )");
        assertEquals(2, cm.propertyBindings().size(),
                "commas inside if(...) must not split the property binding");
        var ageGroup = cm.propertyBindings().get(0);
        assertEquals("ageGroup", ageGroup.propertyName());
        // The whole if-expression parses to one AppliedFunction("if", ...) —
        // if the parser had treated an internal comma as a property separator
        // we'd see two property bindings (caught above) and a broken AST here.
        AppliedFunction ifCall = assertInstanceOf(AppliedFunction.class, ageGroup.expression());
        assertEquals("if", ifCall.function());
        assertEquals(3, ifCall.parameters().size(),
                "if(cond, then, else) takes three params after parse-time desugaring");
    }

    @Test
    void pureClassMappingArrowCallCaptured() {
        // The whole RHS expression must round-trip — not just a keyword fragment.
        // Whitespace normalised so the test isn't brittle to tokenizer spacing.
        var cm = firstPureClassMapping(
                "Mapping my::M ( "
                + "*model::Person: Pure { "
                + "~src model::RawPerson "
                + "upperLastName: $src.lastName->toUpper() "
                + "} )");
        var b = cm.propertyBindings().get(0);
        assertEquals("upperLastName", b.propertyName());
        // $src.lastName->toUpper() desugars to toUpper(AppliedProperty($src, lastName)).
        AppliedFunction toUpper = assertInstanceOf(AppliedFunction.class, b.expression());
        assertEquals("toUpper", toUpper.function());
        assertEquals(1, toUpper.parameters().size());
        AppliedProperty srcLast = assertInstanceOf(AppliedProperty.class, toUpper.parameters().get(0));
        assertEquals("lastName", srcLast.property());
        assertEquals(new Variable("src"), srcLast.receiver());
    }

    @Test
    void pureClassMappingWithSetIdAndExtends() {
        var cm = firstPureClassMapping(
                "Mapping my::M ( "
                + "model::Person[childSet] extends [baseSet]: Pure { "
                + "~src model::RawPerson "
                + "name: $src.name "
                + "} )");
        assertFalse(cm.root());
        assertEquals("childSet", cm.setId());
        assertEquals("baseSet", cm.extendsSetId());
    }

    @Test
    void pureClassMappingRequiresSrc() {
        ParseException e = assertThrows(ParseException.class, () ->
                ElementParser.parse(
                        "Mapping my::M ( *model::P: Pure { "
                        + "name: $src.name } )"));
        assertTrue(e.getMessage().contains("SRC"),
                () -> "expected ~src required error, got: " + e.getMessage());
    }

    @Test
    void pureClassMappingEmptyPropertyBodyRejected() {
        ParseException e = assertThrows(ParseException.class, () ->
                ElementParser.parse(
                        "Mapping my::M ( *model::P: Pure { "
                        + "~src model::Raw "
                        + "name: , other: $src.x "
                        + "} )"));
        assertTrue(e.getMessage().toLowerCase().contains("empty body"),
                () -> "expected empty-body error, got: " + e.getMessage());
    }

    @Test
    void pureClassMappingTrailingCommaTolerated() {
        var cm = firstPureClassMapping(
                "Mapping my::M ( "
                + "*model::Person: Pure { "
                + "~src model::Raw "
                + "name: $src.name, "
                + "} )");
        assertEquals(1, cm.propertyBindings().size(),
                "trailing comma must not produce a phantom property binding");
        var only = cm.propertyBindings().get(0);
        assertEquals("name", only.propertyName());
        // $src.name parses to AppliedProperty(Variable(src), "name") —
        // confirms the RHS terminator (trailing comma) didn't truncate or extend.
        AppliedProperty srcName = assertInstanceOf(AppliedProperty.class, only.expression());
        assertEquals("name", srcName.property());
        assertEquals(new Variable("src"), srcName.receiver());
    }

    @Test
    void mappingMixesPureAndRelationalClassMappings() {
        MappingDefinition md = (MappingDefinition) ElementParser.parse(
                "Mapping my::M ( "
                + "*model::Person: Pure { ~src model::RawPerson  name: $src.name } "
                + "*model::Firm: Relational { ~mainTable [db::DB] FIRMS  legalName: FIRMS.NAME } "
                + ")").elements().get(0);
        assertEquals(2, md.classMappings().size());
        assertInstanceOf(ClassMapping.Pure.class, md.classMappings().get(0));
        assertInstanceOf(ClassMapping.Relational.class, md.classMappings().get(1));
    }

    // ===============================================================
    // B.4g — Property mapping parity fillers
    //        (Embedded / InlineEmbedded / OtherwiseEmbedded / LocalProperty)
    // ===============================================================

    private static ClassMapping.Relational firstRelational(String src) {
        MappingDefinition md = (MappingDefinition) ElementParser.parse(src).elements().get(0);
        return (ClassMapping.Relational) md.classMappings().get(0);
    }

    @Test
    void embeddedPropertyMapping() {
        var cm = firstRelational(
                "Mapping my::M ( "
                + "*model::Person: Relational { "
                + "~mainTable [db::DB] T_PERSON "
                + "name: T_PERSON.NAME, "
                + "firm ( "
                + "  legalName: T_PERSON.FIRM_NAME, "
                + "  employeeCount: T_PERSON.FIRM_COUNT "
                + ") "
                + "} )");
        assertEquals(2, cm.propertyMappings().size());
        var firm = (PropertyMapping.Embedded) cm.propertyMappings().get(1);
        assertEquals("firm", firm.propertyName());
        assertEquals(2, firm.propertyMappings().size());
        assertEquals("legalName", firm.propertyMappings().get(0).propertyName());
        assertEquals("employeeCount", firm.propertyMappings().get(1).propertyName());
        assertInstanceOf(PropertyMapping.Column.class, firm.propertyMappings().get(0));
    }

    @Test
    void embeddedPropertyMappingNested() {
        // Embedded inside embedded — confirms recursive composition.
        var cm = firstRelational(
                "Mapping my::M ( "
                + "*model::Person: Relational { "
                + "~mainTable [db::DB] T_PERSON "
                + "address ( "
                + "  street: T_PERSON.STREET, "
                + "  city ( "
                + "    name: T_PERSON.CITY_NAME, "
                + "    zip: T_PERSON.CITY_ZIP "
                + "  ) "
                + ") "
                + "} )");
        var addr = (PropertyMapping.Embedded) cm.propertyMappings().get(0);
        var city = (PropertyMapping.Embedded) addr.propertyMappings().get(1);
        assertEquals("city", city.propertyName());
        assertEquals(2, city.propertyMappings().size());
        assertEquals("name", city.propertyMappings().get(0).propertyName());
    }

    @Test
    void inlineEmbeddedPropertyMapping() {
        var cm = firstRelational(
                "Mapping my::M ( "
                + "*model::Person: Relational { "
                + "~mainTable [db::DB] T_PERSON "
                + "name: T_PERSON.NAME, "
                + "firm() Inline[firm_set1] "
                + "} )");
        var firm = (PropertyMapping.InlineEmbedded) cm.propertyMappings().get(1);
        assertEquals("firm", firm.propertyName());
        assertEquals("firm_set1", firm.setId());
    }

    @Test
    void otherwiseEmbeddedPropertyMapping() {
        var cm = firstRelational(
                "Mapping my::M ( "
                + "*model::Person: Relational { "
                + "~mainTable [db::DB] T_PERSON "
                + "name: T_PERSON.NAME, "
                + "firm ( "
                + "  legalName: T_PERSON.FIRM_NAME "
                + ") Otherwise ([firm_set1]: [db::DB] @Person_Firm) "
                + "} )");
        var firm = (PropertyMapping.OtherwiseEmbedded) cm.propertyMappings().get(1);
        assertEquals("firm", firm.propertyName());
        assertEquals(1, firm.embedded().size());
        assertEquals("legalName", firm.embedded().get(0).propertyName());
        assertEquals("firm_set1", firm.fallbackSetId());
        var fallback = (PropertyMapping.Join) firm.fallback();
        assertEquals("db::DB", fallback.database());
        assertEquals(1, fallback.joins().size());
        assertEquals("Person_Firm", fallback.joins().get(0).joinName());
    }

    @Test
    void localMappingProperty() {
        var cm = firstRelational(
                "Mapping my::M ( "
                + "*model::Person: Relational { "
                + "~mainTable [db::DB] T_PERSON "
                + "name: T_PERSON.NAME, "
                + "+localProp: String[1]: [db::DB] T_PERSON.EXTRA "
                + "} )");
        assertEquals(2, cm.propertyMappings().size());
        var local = (PropertyMapping.LocalProperty) cm.propertyMappings().get(1);
        assertEquals("localProp", local.propertyName());
        assertEquals(nr("String"), local.type());
        assertEquals(Multiplicity.exactly(1), local.multiplicity());
        // Body is the regular Column binding.
        var col = (PropertyMapping.Column) local.body();
        assertEquals("db::DB", col.database());
        assertEquals("T_PERSON", col.table());
        assertEquals("EXTRA", col.column());
    }

    @Test
    void localMappingPropertyWithUnboundedMultiplicity() {
        var cm = firstRelational(
                "Mapping my::M ( "
                + "*model::Person: Relational { "
                + "~mainTable [db::DB] T_PERSON "
                + "+tags: String[*]: T_PERSON.TAGS "
                + "} )");
        var local = (PropertyMapping.LocalProperty) cm.propertyMappings().get(0);
        assertEquals(Multiplicity.zeroMany(), local.multiplicity(),
                "[*] must parse to Concrete(0, null), same as PropertyDefinition");
    }

    @Test
    void localMappingPropertyWithExpressionBody() {
        var cm = firstRelational(
                "Mapping my::M ( "
                + "*model::Person: Relational { "
                + "~mainTable [db::DB] T_PERSON "
                + "+computed: String[1]: concat(T_PERSON.A, ' ', T_PERSON.B) "
                + "} )");
        var local = (PropertyMapping.LocalProperty) cm.propertyMappings().get(0);
        assertInstanceOf(PropertyMapping.Expression.class, local.body());
    }

    @Test
    void embeddedAllowsTrailingComma() {
        var cm = firstRelational(
                "Mapping my::M ( "
                + "*model::P: Relational { "
                + "~mainTable [db::DB] T "
                + "firm ( legalName: T.NAME, ) "
                + "} )");
        var firm = (PropertyMapping.Embedded) cm.propertyMappings().get(0);
        assertEquals(1, firm.propertyMappings().size(),
                "trailing comma in embedded body must not produce a phantom sub-mapping");
        var only = (PropertyMapping.Column) firm.propertyMappings().get(0);
        assertEquals("legalName", only.propertyName());
        assertEquals("T", only.table());
        assertEquals("NAME", only.column());
    }

    @Test
    void embeddedAndLocalAndRegularMixedInOneClassMapping() {
        var cm = firstRelational(
                "Mapping my::M ( "
                + "*model::Person: Relational { "
                + "~mainTable [db::DB] T_PERSON "
                + "name: T_PERSON.NAME, "
                + "firm ( legalName: T_PERSON.FIRM_NAME ), "
                + "broker() Inline[broker_set], "
                + "+localTag: String[1]: T_PERSON.TAG "
                + "} )");
        assertEquals(4, cm.propertyMappings().size());
        assertInstanceOf(PropertyMapping.Column.class,         cm.propertyMappings().get(0));
        assertInstanceOf(PropertyMapping.Embedded.class,       cm.propertyMappings().get(1));
        assertInstanceOf(PropertyMapping.InlineEmbedded.class, cm.propertyMappings().get(2));
        assertInstanceOf(PropertyMapping.LocalProperty.class,  cm.propertyMappings().get(3));
    }

    // ===============================================================
    // B.4f — Mapping test suites (raw-text capture, D-3 deferred)
    // ===============================================================

    @Test
    void mappingWithoutTestSuitesHasNullSource() {
        MappingDefinition md = (MappingDefinition) ElementParser.parse(
                "Mapping my::M ( "
                + "*model::P: Relational { ~mainTable [db::DB] T  x: T.X } "
                + ")").elements().get(0);
        assertNull(md.testSuitesSource());
    }

    @Test
    void mappingTestSuitesBlockCapturedVerbatim() {
        MappingDefinition md = (MappingDefinition) ElementParser.parse(
                "Mapping my::M ( "
                + "*model::P: Relational { ~mainTable [db::DB] T  x: T.X } "
                + "testSuites: [ "
                + "  PersonSuite: { "
                + "    tests: [ "
                + "      Test1: { query: |model::P.all()->graphFetch(#{ x }#) } "
                + "    ] "
                + "  } "
                + "] "
                + ")").elements().get(0);
        assertNotNull(md.testSuitesSource());
        assertTrue(md.testSuitesSource().contains("PersonSuite"));
        assertTrue(md.testSuitesSource().contains("graphFetch"));
        // Outer brackets included in capture.
        assertTrue(md.testSuitesSource().startsWith("["));
        assertTrue(md.testSuitesSource().endsWith("]"));
    }

    @Test
    void mappingTestSuitesInteriorBracesBalanced() {
        // Nested braces and brackets must balance correctly inside capture.
        MappingDefinition md = (MappingDefinition) ElementParser.parse(
                "Mapping my::M ( "
                + "testSuites: [ "
                + "  S1: { tests: [ T1: { q: |[1, 2, 3]->size() } ] } "
                + "] "
                + "*model::P: Relational { ~mainTable [db::DB] T  x: T.X } "
                + ")").elements().get(0);
        assertNotNull(md.testSuitesSource());
        assertTrue(md.testSuitesSource().contains("[1, 2, 3]"),
                () -> "captured: " + md.testSuitesSource());
        // Class mapping after testSuites still parsed.
        assertEquals(1, md.classMappings().size());
    }

    @Test
    void mappingDuplicateTestSuitesRejected() {
        ParseException e = assertThrows(ParseException.class, () ->
                ElementParser.parse(
                        "Mapping my::M ( "
                        + "testSuites: [ A: {} ] "
                        + "testSuites: [ B: {} ] "
                        + ")"));
        assertTrue(e.getMessage().toLowerCase().contains("duplicate"),
                () -> "expected duplicate-testSuites error, got: " + e.getMessage());
    }

    @Test
    void mappingTwoClassMappingsWithCommonMainTable() {
        MappingDefinition md = (MappingDefinition) ElementParser.parse(
                "Mapping my::M ( "
                + "*model::Person: Relational { ~mainTable [db::DB] PERSON  name: PERSON.NAME } "
                + "*model::Firm: Relational { ~mainTable [db::DB] FIRM  legalName: FIRM.LEGAL_NAME } "
                + ")").elements().get(0);
        assertEquals(2, md.classMappings().size());
        var first = (ClassMapping.Relational) md.classMappings().get(0);
        var second = (ClassMapping.Relational) md.classMappings().get(1);
        assertEquals("model::Person", first.className());
        assertEquals("model::Firm", second.className());
    }
}
