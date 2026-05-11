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
import com.legend.parser.element.FilterMapping;
import com.legend.parser.element.FilterPointer;
import com.legend.parser.element.FunctionDefinition;
import com.legend.parser.element.JoinChainElement;
import com.legend.parser.element.JsonModelConnection;
import com.legend.parser.element.PackageableElement;
import com.legend.parser.element.RelationalOperation;
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
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
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

    /** Property with no annotations &mdash; the common shape. */
    private static PropertyDefinition prop(String name, String type, int lower, Integer upper) {
        return new PropertyDefinition(name, type, lower, upper, List.of(), List.of());
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
        // Defensive: confirm null upperBound is the encoding for unbounded.
        assertNull(c.properties().get(2).upperBound());
        assertNull(c.properties().get(3).upperBound());
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
        assertEquals(List.of("A", "b::c::D"), c.superClasses());
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
        assertEquals(List.of("my::pkg::Base", "OtherBase"), c.superClasses());
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
                "id", "String", 1, 1,
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
                List.of(), List.of("LegalEntity"),
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
                () -> c.superClasses().add("X"),
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
        // 'Mapping' is still unsupported as of B.2; B.4 will add it.
        ParseException e = assertThrows(ParseException.class,
                () -> ElementParser.parse("Mapping my::M ( )"));
        assertTrue(e.getMessage().toLowerCase().contains("unsupported"),
                () -> "expected 'unsupported' in message, got: " + e.getMessage());
        assertTrue(e.getMessage().contains("MAPPING"),
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
        // Body text reconstructed verbatim from source (whitespace preserved).
        assertEquals("'a' + 'b'", d.expression().trim());
        assertEquals("String", d.type());
        assertEquals(1, d.lowerBound());
        assertEquals(Integer.valueOf(1), d.upperBound());
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
                        new ParameterDefinition("rate", "Float", 1, 1),
                        new ParameterDefinition("country", "String", 0, 1)),
                d.parameters());
        assertTrue(d.expression().contains("$this.price * $rate"),
                () -> "body should capture the expression text, got: " + d.expression());
        assertEquals("Float", d.type());
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
        assertEquals("$this.age >= 0", cn.expression().trim());
    }

    @Test
    void unnamedConstraintGetsDefaultName() {
        ClassDefinition c = parseOneClass("Class P [$this.age >= 0] {}");
        assertEquals(1, c.constraints().size());
        assertEquals("unnamed", c.constraints().get(0).name());
        assertEquals("$this.age >= 0", c.constraints().get(0).expression().trim());
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
        assertEquals(new AssociationEndDefinition("firm", "Firm", 1, 1),
                a.property1());
        assertEquals(new AssociationEndDefinition("employees", "Person", 0, null),
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
        assertEquals("String", f.returnType());
        assertEquals(1, f.returnLowerBound());
        assertEquals(Integer.valueOf(1), f.returnUpperBound());
        assertEquals("'hello'", f.body().trim());
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
                        new FunctionDefinition.ParameterDefinition("a", "Integer", 1, 1),
                        new FunctionDefinition.ParameterDefinition("b", "Integer", 0, 1)),
                f.parameters());
        assertEquals("Integer", f.returnType());
        assertEquals("$a + $b", f.body().trim());
    }

    @Test
    void functionBodyPreservesNestedBraces() {
        // The body itself contains braces — depth-balance must keep them in.
        ParsedModel m = ElementParser.parse(
                "function my::f(): String[1] { let x = { 'inner' }; $x }");
        FunctionDefinition f = assertInstanceOf(FunctionDefinition.class, m.elements().get(0));
        assertTrue(f.body().contains("{ 'inner' }"),
                () -> "nested braces should be preserved in body, got: " + f.body());
    }

    @Test
    void functionUpperBoundStarBecomesNull() {
        FunctionDefinition f = assertInstanceOf(FunctionDefinition.class,
                ElementParser.parse("function my::all(): Person[*] { Person.all() }")
                        .elements().get(0));
        assertEquals(0, f.returnLowerBound());
        assertEquals(null, f.returnUpperBound(), "* upper bound must be null");
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
        assertTrue(s.functionBody().contains("Person.all()"),
                () -> "body should capture query text, got: " + s.functionBody());
        assertEquals(null, s.testSuitesSource(), "testSuites absent → null");
    }

    @Test
    void serviceCapturesTestSuitesAsRawText() {
        // D-3: testSuites block preserved as raw text for B.4 to parse.
        ParsedModel m = ElementParser.parse("""
                Service my::S
                {
                  pattern: '/x';
                  execution: Single { query: |1; mapping: my::M; runtime: my::R; }
                  testSuites: [ { suite_1: { setup: 'x'; } } ];
                }
                """);
        ServiceDefinition s = (ServiceDefinition) m.elements().get(0);
        assertTrue(s.testSuitesSource() != null && s.testSuitesSource().contains("suite_1"),
                () -> "testSuites raw text should be captured, got: " + s.testSuitesSource());
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
                        new ColumnDefinition("ID", "INTEGER", true, true),     // PK ⇒ NOT NULL
                        new ColumnDefinition("NAME", "VARCHAR(100)", false, true),
                        new ColumnDefinition("SCORE", "DECIMAL(10,2)", false, false)),
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
        assertEquals("=", c.op());
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
        assertEquals("and", bo.op());
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
}
