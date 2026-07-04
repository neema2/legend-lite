package com.legend.ide;

import com.legend.lexer.Lexer;
import com.legend.lexer.TokenStream;
import com.legend.parser.ElementKind;
import com.legend.parser.ElementParser;
import com.legend.parser.ParseException;
import com.legend.parser.ParsedModel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link ModelIndexer}.
 *
 * <p>The indexer's job is purely to find element ranges and FQNs without
 * parsing bodies. Tests assert FQN-set parity with the full parser, and
 * pin a few specific shapes that the scanner has to handle correctly
 * (function decorations, mapping parens, native classes, multiple
 * elements, imports in arbitrary positions).
 */
final class ModelIndexerTest {

    private static ModelIndex scan(String src) {
        return ModelIndexer.scan(Lexer.tokenize(src));
    }

    // ----- happy paths ---------------------------------------------------

    @Test
    void emptySourceHasEmptyIndex() {
        ModelIndex idx = scan("");
        assertEquals(0, idx.size());
        assertTrue(idx.imports().isEmpty());
    }

    @Test
    void singleClass() {
        ModelIndex idx = scan("Class my::Person { name: String[1]; }");
        assertEquals(1, idx.size());
        ModelIndex.Entry e = idx.get("my::Person");
        assertNotNull(e);
        assertEquals(ElementKind.CLASS, e.kind());
        assertEquals(0, e.startTokenInclusive());
        // Range covers the whole source.
        assertTrue(e.tokenCount() > 5, "range should span 'Class my::Person { name : String [ 1 ] ; }'");
    }

    @Test
    void multipleClassesPreserveSourceOrder() {
        ModelIndex idx = scan(
                "Class my::A { a: String[1]; } "
                + "Class my::B { b: Integer[1]; } "
                + "Class my::C { c: Boolean[1]; }");
        assertEquals(List.of("my::A", "my::B", "my::C"), List.copyOf(idx.fqns()));
    }

    @Test
    void nativeClassRecognised() {
        ModelIndex idx = scan("native Class my::Foo {}");
        ModelIndex.Entry e = idx.get("my::Foo");
        assertNotNull(e);
        assertEquals(ElementKind.CLASS, e.kind());
        assertEquals(0, e.startTokenInclusive(),
                "native prefix is part of the element range");
    }

    @Test
    void associationEnumProfileRecognised() {
        ModelIndex idx = scan(
                "Association my::A { left: model::L[1]; right: model::R[1]; } "
                + "Enum my::E { X, Y, Z } "
                + "Profile my::P { stereotypes: [s1]; tags: [t1]; }");
        assertEquals(ElementKind.ASSOCIATION, idx.get("my::A").kind());
        assertEquals(ElementKind.ENUM, idx.get("my::E").kind());
        assertEquals(ElementKind.PROFILE, idx.get("my::P").kind());
    }

    @Test
    void databaseUsesParenBody() {
        ModelIndex idx = scan(
                "Database my::DB ( Table T_PERSON ( ID INTEGER, NAME VARCHAR(120) ) )");
        ModelIndex.Entry e = idx.get("my::DB");
        assertNotNull(e);
        assertEquals(ElementKind.DATABASE, e.kind());
    }

    @Test
    void mappingUsesParenBody() {
        ModelIndex idx = scan(
                "Mapping my::M ( *model::P: Relational { ~mainTable [my::DB] T_PERSON  name: T_PERSON.NAME } )");
        ModelIndex.Entry e = idx.get("my::M");
        assertNotNull(e);
        assertEquals(ElementKind.MAPPING, e.kind());
    }

    @Test
    void functionWithParamsAndReturnAndBody() {
        ModelIndex idx = scan(
                "function my::greet(p: String[1]): String[1] { $p->toUpper() }");
        ModelIndex.Entry e = idx.get("my::greet");
        assertNotNull(e);
        assertEquals(ElementKind.FUNCTION, e.kind());
        // Range should include both the params (...) and body {...}.
        TokenStream tokens = Lexer.tokenize(
                "function my::greet(p: String[1]): String[1] { $p->toUpper() }");
        assertEquals(tokens.count(), e.endTokenExclusive(),
                "function range should reach to end of stream");
    }

    @Test
    void functionWithStereotypeBeforeFqn() {
        String src = "function <<svc::profile.Test>> my::test::run(): String[1] { 'hi' }";
        TokenStream tokens = Lexer.tokenize(src);
        ModelIndex idx = ModelIndexer.scan(tokens);

        ModelIndex.Entry e = idx.get("my::test::run");
        assertNotNull(e, "FQN must be extracted past the <<...>> stereotype block");
        assertEquals(ElementKind.FUNCTION, e.kind());
        assertEquals(0, e.startTokenInclusive(),
                "element range must include the 'function' keyword");
        assertEquals(tokens.count(), e.endTokenExclusive(),
                "element range must extend through the body '}'");
    }

    @Test
    void functionWithStereotypeAndTaggedValuesBeforeFqn() {
        // Exercises ModelIndexer.looksLikeTaggedValueBlock — the '{tag=val}'
        // decoration appears between '<<...>>' and the FQN and must not be
        // mistaken for the element body.
        String src = "function <<svc::profile.Test>> { meta::pure::profiles::doc.doc = 'hello' } "
                + "my::test::run(): String[1] { 'hi' }";
        TokenStream tokens = Lexer.tokenize(src);
        ModelIndex idx = ModelIndexer.scan(tokens);

        ModelIndex.Entry e = idx.get("my::test::run");
        assertNotNull(e, "FQN must be extracted past stereotype + tagged-value decorations");
        assertEquals(ElementKind.FUNCTION, e.kind());
        assertEquals(tokens.count(), e.endTokenExclusive(),
                "range must cover the real body, not stop at the tagged-value block");
    }

    @Test
    void classWithTaggedValuesBeforeFqn() {
        String src = "Class { meta::pure::profiles::doc.doc = 'a person' } "
                + "my::Person { name: String[1]; }";
        ModelIndex idx = scan(src);
        ModelIndex.Entry e = idx.get("my::Person");
        assertNotNull(e, "FQN must survive a tagged-value decoration on Class");
        assertEquals(ElementKind.CLASS, e.kind());
    }

    // ----- looksLikeTaggedValueBlock predicate (positive + negative) ----

    /**
     * Direct positive/negative coverage of the shared
     * {@code ElementParser.looksLikeTaggedValueBlock} predicate. The
     * indexer and the parser both consult it; drift between the two would
     * be silent and hard to diagnose, so the predicate is pinned
     * independently of either consumer.
     */
    @Test
    void taggedValuePredicatePositive() {
        // The classic tagged-value shape: '{' QN '.' IDENT '=' STRING '}'.
        TokenStream tokens = Lexer.tokenize("{ meta::pure::profiles::doc.doc = 'd' }");
        assertTrue(ElementParser.looksLikeTaggedValueBlock(tokens, 0),
                "tagged-value block should match the predicate");
    }

    @Test
    void taggedValuePredicateRejectsClassBody() {
        // A class body must NOT be mistaken for a tagged-value block;
        // otherwise the scanner would consume the body and crash on FQN.
        TokenStream tokens = Lexer.tokenize("{ name: String[1]; }");
        assertFalse(ElementParser.looksLikeTaggedValueBlock(tokens, 0),
                "property declarations are not tagged values");
    }

    @Test
    void taggedValuePredicateRejectsEmptyBraces() {
        TokenStream tokens = Lexer.tokenize("{}");
        assertFalse(ElementParser.looksLikeTaggedValueBlock(tokens, 0),
                "empty braces are not tagged values");
    }

    @Test
    void taggedValuePredicateRejectsBareIdentifierInBraces() {
        // No '.IDENT=' — just a bare path inside braces. Not a tagged value.
        TokenStream tokens = Lexer.tokenize("{ my::path }");
        assertFalse(ElementParser.looksLikeTaggedValueBlock(tokens, 0),
                "{path} without '.IDENT=' is not a tagged value");
    }

    @Test
    void taggedValuePredicateRejectsNonBraceStart() {
        // The predicate is positional: it must return false if the token
        // at the given index is not '{'.
        TokenStream tokens = Lexer.tokenize("Class my::Foo {}");
        assertFalse(ElementParser.looksLikeTaggedValueBlock(tokens, 0),
                "position 0 here is 'Class', not '{' — predicate must reject");
    }

    @Test
    void serviceRuntimeAndConnection() {
        ModelIndex idx = scan(
                "Service my::S { pattern: '/x'; "
                + "execution: Single { query: |1; mapping: my::M; runtime: my::R; } } "
                + "Runtime my::R { mappings: [my::M]; } "
                + "RelationalDatabaseConnection my::C { store: my::DB; type: DuckDB; "
                + "specification: InMemory {}; auth: NoAuth {}; }");
        assertEquals(ElementKind.SERVICE, idx.get("my::S").kind());
        assertEquals(ElementKind.RUNTIME, idx.get("my::R").kind());
        assertEquals(ElementKind.CONNECTION, idx.get("my::C").kind());
    }

    // ----- imports -------------------------------------------------------

    @Test
    void leadingImportsRecorded() {
        ModelIndex idx = scan(
                "import my::model::*;\n"
                + "import other::*;\n"
                + "Class my::X {}");
        assertEquals(2, idx.imports().size());
        assertEquals(1, idx.size());
        assertNotNull(idx.get("my::X"));
    }

    @Test
    void importsInterleavedWithElements() {
        ModelIndex idx = scan(
                "Class my::A {} "
                + "import my::store::*; "
                + "Class my::B {}");
        assertEquals(1, idx.imports().size());
        assertEquals(List.of("my::A", "my::B"), List.copyOf(idx.fqns()));
    }

    // ----- range correctness --------------------------------------------

    @Test
    void elementRangesAreContiguousAndCoverEverything() {
        // Two elements + one import. Every token in the source must be
        // covered by exactly one indexed range \u2014 no gaps, no overlaps.
        // A sum-only check would let A-overlaps-B-by-N + C-short-by-N
        // pathologies slip through, so we do per-token coverage instead.
        String src = "import my::*; Class my::A {} Class my::B {}";
        TokenStream tokens = Lexer.tokenize(src);
        ModelIndex idx = scan(src);

        int n = tokens.count();
        int[] owners = new int[n];
        java.util.Arrays.fill(owners, -1);
        int rangeId = 0;
        for (ModelIndex.ImportEntry imp : idx.imports()) {
            markRange(owners, imp.startTokenInclusive(), imp.endTokenExclusive(), rangeId++);
        }
        for (ModelIndex.Entry e : idx.entries()) {
            markRange(owners, e.startTokenInclusive(), e.endTokenExclusive(), rangeId++);
        }
        for (int i = 0; i < n; i++) {
            assertTrue(owners[i] >= 0,
                    () -> "token uncovered by any indexed range");
        }
    }

    /** Asserts no token in [start, end) is already owned; then claims them. */
    private static void markRange(int[] owners, int start, int end, int id) {
        for (int i = start; i < end; i++) {
            final int idx = i;
            assertEquals(-1, owners[i],
                    () -> "token " + idx + " covered by two ranges (overlap)");
            owners[i] = id;
        }
    }

    // ----- parity with full parser --------------------------------------

    /**
     * Property: for any Pure source that the full element parser accepts,
     * {@link ModelIndexer#scan(TokenStream)} must produce the same FQN set
     * in the same source order. Run across a diverse corpus covering every
     * element kind plus combinations and decorations.
     */
    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("shallowDeepParityCorpus")
    void shallowAndDeepFqnsAgreeAcrossDiverseSources(String label, String src) {
        ModelIndex idx = scan(src);
        ParsedModel pm = ElementParser.parse(src);

        List<String> shallowFqns = List.copyOf(idx.fqns());
        List<String> deepFqns = pm.elements().stream().map(ModelIndexerTest::fqnOf).toList();
        assertEquals(deepFqns, shallowFqns,
                () -> "shallow scan FQN set must equal eager parser element FQN set for: " + label);
    }

    private static java.util.stream.Stream<org.junit.jupiter.params.provider.Arguments> shallowDeepParityCorpus() {
        return java.util.stream.Stream.of(
                org.junit.jupiter.params.provider.Arguments.of("empty source", ""),
                org.junit.jupiter.params.provider.Arguments.of("single class",
                        "Class my::Foo { x: String[1]; }"),
                org.junit.jupiter.params.provider.Arguments.of("native class",
                        "native Class my::Foo {}"),
                org.junit.jupiter.params.provider.Arguments.of("class with extends",
                        "Class my::Foo extends my::Bar { x: String[1]; } Class my::Bar { y: Integer[1]; }"),
                org.junit.jupiter.params.provider.Arguments.of("class with constraints",
                        "Class my::Foo [ $this.x > 0 ] { x: Integer[1]; }"),
                org.junit.jupiter.params.provider.Arguments.of("class with stereotype",
                        "Class <<meta::pure::profiles::testProfile.Skip>> my::Foo { x: String[1]; }"),
                org.junit.jupiter.params.provider.Arguments.of("class with tagged value",
                        "Class { meta::pure::profiles::doc.doc = 'd' } my::Foo { x: String[1]; }"),
                org.junit.jupiter.params.provider.Arguments.of("association",
                        "Association my::A { left: model::L[1]; right: model::R[1]; }"),
                org.junit.jupiter.params.provider.Arguments.of("enum",
                        "Enum my::E { X, Y, Z }"),
                org.junit.jupiter.params.provider.Arguments.of("profile",
                        "Profile my::P { stereotypes: [s1, s2]; tags: [t1]; }"),
                org.junit.jupiter.params.provider.Arguments.of("function with multi-statement body",
                        "function my::f(p: String[1]): String[1] { let x = $p; $x->toUpper() }"),
                org.junit.jupiter.params.provider.Arguments.of("function with stereotype + tagged values",
                        "function <<test::profile.Skip>> { meta::pure::profiles::doc.doc = 'd' } "
                                + "my::f(): String[1] { 'hi' }"),
                org.junit.jupiter.params.provider.Arguments.of("database with table and join",
                        "Database my::DB ( Table T_P ( ID INTEGER, NAME VARCHAR(120) ) "
                                + "Join J(T_P.ID = T_P.ID) )"),
                org.junit.jupiter.params.provider.Arguments.of("mapping with includes",
                        "Mapping my::M ( include my::Base "
                                + "*my::P: Relational { ~mainTable [my::DB] T  x: T.X } )"
                                + " Mapping my::Base ( *my::Q: Relational { ~mainTable [my::DB] U  y: U.Y } )"),
                org.junit.jupiter.params.provider.Arguments.of("service + runtime + connection",
                        "Service my::S { pattern: '/x'; "
                                + "execution: Single { query: |1; mapping: my::M; runtime: my::R; } } "
                                + "Runtime my::R { mappings: [my::M]; } "
                                + "RelationalDatabaseConnection my::C { store: my::DB; type: DuckDB; "
                                + "specification: InMemory {}; auth: NoAuth {}; }"),
                org.junit.jupiter.params.provider.Arguments.of("imports before and between elements",
                        "import my::*; Class my::A {} import my::store::*; Class my::B {}"),
                org.junit.jupiter.params.provider.Arguments.of("all element kinds mixed",
                        """
                        import my::*;
                        Class my::C { x: String[1]; }
                        Association my::A { l: my::C[1]; r: my::C[1]; }
                        Enum my::E { X, Y }
                        Profile my::P { stereotypes: [s]; tags: [t]; }
                        function my::f(): String[1] { 'hi' }
                        Database my::DB ( Table T ( X INTEGER ) )
                        Mapping my::M ( *my::C: Relational { ~mainTable [my::DB] T  x: T.X } )
                        Runtime my::R { mappings: [my::M]; }
                        RelationalDatabaseConnection my::Conn { store: my::DB; type: DuckDB;
                          specification: InMemory {}; auth: NoAuth {}; }
                        """)
        );
    }

    private static String fqnOf(com.legend.parser.element.PackageableElement e) {
        return switch (e) {
            case com.legend.parser.element.ClassDefinition c -> c.qualifiedName();
            case com.legend.parser.element.AssociationDefinition a -> a.qualifiedName();
            case com.legend.parser.element.EnumDefinition en -> en.qualifiedName();
            case com.legend.parser.element.ProfileDefinition p -> p.qualifiedName();
            case com.legend.parser.element.FunctionDefinition f -> f.qualifiedName();
            case com.legend.parser.element.ServiceDefinition s -> s.qualifiedName();
            case com.legend.parser.element.RuntimeDefinition r -> r.qualifiedName();
            case com.legend.parser.element.ConnectionDefinition co -> co.qualifiedName();
            case com.legend.parser.element.DatabaseDefinition d -> d.qualifiedName();
            case com.legend.parser.element.LegacyMappingDefinition m -> m.qualifiedName();
            default -> throw new AssertionError("unknown element kind: " + e.getClass());
        };
    }

    @Test
    void shallowAndDeepAgreeOnFqnSet() {
        String src = """
                import my::*;

                Class my::Person extends my::LegalEntity {
                  name: String[1];
                  age: Integer[0..1];
                }

                Class my::LegalEntity {
                  legalName: String[1];
                }

                Enum my::Status { ACTIVE, RETIRED }

                Association my::Person_Org {
                  emp: my::Person[*];
                  org: my::LegalEntity[1];
                }

                function my::computeAge(p: my::Person[1]): Integer[1] { 42 }

                Database my::DB (
                  Table PERSON ( ID INTEGER, NAME VARCHAR(120) )
                )

                Mapping my::M (
                  *my::Person: Relational {
                    ~mainTable [my::DB] PERSON
                    name: PERSON.NAME
                  }
                )

                Runtime my::R {
                  mappings: [my::M];
                }
                """;
        ModelIndex idx = scan(src);
        ParsedModel pm = ElementParser.parse(src);

        // Same FQN set, same order.
        List<String> shallowFqns = List.copyOf(idx.fqns());
        List<String> deepFqns = pm.elements().stream()
                .map(e -> {
                    // Each element record has its own FQN accessor; cover all.
                    return switch (e) {
                        case com.legend.parser.element.ClassDefinition c -> c.qualifiedName();
                        case com.legend.parser.element.AssociationDefinition a -> a.qualifiedName();
                        case com.legend.parser.element.EnumDefinition en -> en.qualifiedName();
                        case com.legend.parser.element.ProfileDefinition p -> p.qualifiedName();
                        case com.legend.parser.element.FunctionDefinition f -> f.qualifiedName();
                        case com.legend.parser.element.ServiceDefinition s -> s.qualifiedName();
                        case com.legend.parser.element.RuntimeDefinition r -> r.qualifiedName();
                        case com.legend.parser.element.ConnectionDefinition co -> co.qualifiedName();
                        case com.legend.parser.element.DatabaseDefinition d -> d.qualifiedName();
                        case com.legend.parser.element.LegacyMappingDefinition m -> m.qualifiedName();
                        default -> throw new AssertionError("unknown element: " + e.getClass());
                    };
                })
                .toList();
        assertEquals(deepFqns, shallowFqns,
                "shallow scan must agree with eager parser on declared FQNs and order");
    }

    // ----- errors --------------------------------------------------------

    @Test
    void unknownTopLevelTokenThrows() {
        ParseException ex = assertThrows(ParseException.class,
                () -> scan("Banana my::X {}"));
        assertTrue(ex.getMessage().contains("unsupported"),
                () -> "want unsupported-keyword error, got: " + ex.getMessage());
        // Indexer errors must carry line/column info, not just byte offsets:
        // the offending token sits at line 1, column 0 (start of file).
        assertEquals(1, ex.line(), "unsupported keyword should be reported with line number");
        assertEquals(0, ex.column(), "unsupported keyword starts at column 0");
    }

    @Test
    void duplicateFqnThrows() {
        ParseException ex = assertThrows(ParseException.class,
                () -> scan("Class my::X {} Class my::X {}"));
        assertTrue(ex.getMessage().contains("duplicate"),
                () -> "want duplicate-FQN error, got: " + ex.getMessage());
        // The duplicate is reported at the start of the SECOND 'Class my::X',
        // not at the first occurrence.
        assertEquals(1, ex.line());
        assertTrue(ex.column() > 0,
                () -> "duplicate should point at the second declaration, got col " + ex.column());
    }

    @Test
    void indexerErrorsCarryLineNumberForMultiLineSources() {
        // Pin the line-number computation specifically: a malformed token
        // on line 3 should be reported with line == 3.
        String src = "Class my::A {}\n"
                + "Class my::B {}\n"
                + "Banana my::C {}\n";
        ParseException ex = assertThrows(ParseException.class, () -> scan(src));
        assertEquals(3, ex.line(),
                () -> "error on line 3 should report line 3, got: " + ex.getMessage());
    }

    @Test
    void databaseWithBraceBodyIsRejected() {
        // Database body must be '(...)'. Encountering '{' in the header
        // phase is the scanner's signal that the source is malformed for
        // this kind \u2014 fail fast, don't paper over it.
        ParseException ex = assertThrows(ParseException.class,
                () -> scan("Database my::DB { Table T (X INTEGER) }"));
        assertTrue(ex.getMessage().contains("DATABASE")
                        || ex.getMessage().toLowerCase().contains("database"),
                () -> "error should mention DATABASE kind, got: " + ex.getMessage());
    }

    @Test
    void truncatedElementWithoutBodyIsRejected() {
        // FQN present, body never opens. The scanner falls out of phase 1
        // at EOF and must error rather than return a zero-width range.
        ParseException ex = assertThrows(ParseException.class,
                () -> scan("Class my::Foo"));
        assertTrue(ex.getMessage().toLowerCase().contains("missing body"),
                () -> "want missing-body error, got: " + ex.getMessage());
    }

    @Test
    void unbalancedCloseInHeaderIsRejected() {
        // A stray ')' in the header phase is a structural error; the
        // scanner must not absorb it into the range or silently skip it.
        ParseException ex = assertThrows(ParseException.class,
                () -> scan("Class my::Foo ) { x: String[1]; }"));
        assertTrue(ex.getMessage().toLowerCase().contains("unbalanced"),
                () -> "want unbalanced-delimiter error, got: " + ex.getMessage());
    }

    @Test
    void garbageBetweenElementsIsRejected() {
        // Per the no-fallbacks rule: unknown tokens between well-formed
        // elements must fail fast, not be silently absorbed into either
        // element's range. After the first element's body closes, the
        // outer loop is positioned at the garbage token and the unknown
        // top-level keyword check fires.
        ParseException ex = assertThrows(ParseException.class,
                () -> scan("Class my::A {} garbageToken Class my::B {}"));
        assertTrue(ex.getMessage().toLowerCase().contains("unsupported"),
                () -> "want unsupported-keyword error, got: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("garbageToken"),
                () -> "error should name the offending token, got: " + ex.getMessage());
    }
}
