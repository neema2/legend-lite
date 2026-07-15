package com.legend.compiler.element;

import com.legend.builtin.Pure;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.parser.ElementParser;
import com.legend.model.NormalizedModel;
import com.legend.model.ParsedModel;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("PureModelContext — manifest classification + lazy materialization")
class PureModelContextTest {

    private static final String STRING = Type.Primitive.STRING.qualifiedName();
    private static final String INTEGER = Type.Primitive.INTEGER.qualifiedName();
    private static final String NUMBER = Type.Primitive.NUMBER.qualifiedName();

    /** Type references are written FQN (post-NameResolver form) to isolate this unit. */
    private static PureModelContext fixture() {
        ParsedModel parsed = ElementParser.parse(
                "Class model::Address { street: " + STRING + "[1]; }\n"
              + "Class model::Person {\n"
              + "  name: " + STRING + "[1];\n"
              + "  age: " + INTEGER + "[1];\n"
              + "  home: model::Address[0..1];\n"
              + "}\n"
              + "Enum model::Color { RED, GREEN }\n");
        return PureModelContext.from(asNormalized(parsed));
    }

    /**
     * Treat already-final elements as a {@link NormalizedModel} without running
     * Phase E. These are Phase-F unit fixtures: the elements are written in
     * post-NameResolver FQN form with no body sites to lift, so normalization
     * would be a no-op &mdash; this adapter isolates {@code PureModelContext}
     * from the earlier phases while satisfying the Phase-F type gate.
     */
    private static NormalizedModel asNormalized(ParsedModel parsed) {
        return new NormalizedModel(parsed.elements(), parsed.imports());
    }

    // ====================================================================
    // findType — the kind manifest (Knowledge)
    // ====================================================================

    @Test
    @DisplayName("classifies user class, user enum, primitives, and native enum")
    void findTypeClassifiesEveryKind() {
        PureModelContext ctx = fixture();

        assertInstanceOf(Type.ClassType.class, ctx.findType("model::Person").orElseThrow());
        assertInstanceOf(Type.EnumType.class, ctx.findType("model::Color").orElseThrow());

        assertSame(Type.Primitive.STRING, ctx.findType(STRING).orElseThrow());
        assertSame(Type.Primitive.INTEGER, ctx.findType(INTEGER).orElseThrow());

        // Native enum from the bootstrap Pure catalog (not user-declared).
        assertInstanceOf(Type.EnumType.class,
                ctx.findType(Pure.MONTH.qualifiedName()).orElseThrow());
    }

    @Test
    @DisplayName("an unknown FQN is not classified (empty), never guessed")
    void findTypeUnknownIsEmpty() {
        assertTrue(fixture().findType("model::DoesNotExist").isEmpty());
    }

    // ====================================================================
    // Lazy structure materialization (Work)
    // ====================================================================

    @Test
    @DisplayName("class properties materialize with uniformly kinded types")
    void findClassMaterializesProperties() {
        TypedClass person = fixture().findClass("model::Person").orElseThrow();

        assertEquals(3, person.properties().size());

        Property name = person.properties().get(0);
        assertEquals("name", name.name());
        assertSame(Type.Primitive.STRING, name.type());
        assertEquals(Multiplicity.Bounded.ONE, name.multiplicity());

        Property home = person.properties().get(2);
        assertEquals("home", home.name());
        assertInstanceOf(Type.ClassType.class, home.type());
        assertEquals("model::Address", ((Type.ClassType) home.type()).fqn());
        assertEquals(Multiplicity.Bounded.ZERO_ONE, home.multiplicity());
    }

    @Test
    @DisplayName("findClass is memoized — repeated lookups return the same instance")
    void findClassMemoized() {
        PureModelContext ctx = fixture();
        assertSame(ctx.findClass("model::Person").orElseThrow(),
                   ctx.findClass("model::Person").orElseThrow());
    }

    @Test
    @DisplayName("enum materializes to its ordered values")
    void findEnumMaterializesValues() {
        TypedEnum color = fixture().findEnum("model::Color").orElseThrow();
        assertEquals(List.of("RED", "GREEN"), color.values());
    }

    @Test
    @DisplayName("findProperty walks the class then its supertypes")
    void findPropertyResolvesOnClass() {
        Property home = fixture().findProperty("model::Person", "home").orElseThrow();
        assertEquals("home", home.name());
        assertInstanceOf(Type.ClassType.class, home.type());
    }

    // ====================================================================
    // Subtyping via the bootstrap native lattice
    // ====================================================================

    @Test
    @DisplayName("isSubtype walks native class superclass chains (Integer < Number)")
    void isSubtypeWalksNativeLattice() {
        PureModelContext ctx = fixture();
        assertTrue(ctx.isSubtype(INTEGER, NUMBER));
        assertTrue(ctx.isSubtype(INTEGER, Pure.ANY.qualifiedName()));
    }

    // ====================================================================
    // No fallback (AGENTS.md invariant 4)
    // ====================================================================

    @Test
    @DisplayName("a property referencing an unknown type fails EAGERLY at construction")
    void unknownPropertyTypeThrows() {
        // F.a is eager + whole-model: the dangling reference fails compileModel
        // itself — an invalid model never becomes a queryable context, even if
        // nothing ever demands the bad class (pipeline stage-failure finding).
        ParsedModel parsed = ElementParser.parse(
                "Class model::Bad { x: not::a::RealType[1]; }");
        com.legend.error.ModelException ex = assertThrows(com.legend.error.ModelException.class,
                () -> PureModelContext.from(asNormalized(parsed)));
        assertTrue(ex.getMessage().contains("Unknown type"));
        assertTrue(ex.getMessage().contains("not::a::RealType"));
    }

    // ====================================================================
    // F.a — binding integrity (a bound function must exist)
    // ====================================================================

    @Test
    @DisplayName("F.a: a Door-4 derived property bound to a missing function fails, naming the site")
    void derivedPropertyBoundToMissingFunctionThrows() {
        // ModelIntegrity is eager: the dangling realizer fails compileModel itself.
        com.legend.error.ModelException ex = assertThrows(com.legend.error.ModelException.class,
                () -> com.legend.Compiler.compileModel(
                        "Class model::Person { name: String[1]; "
                      + "  fullName() { model::funcs::missing }: String[1]; }"));
        assertTrue(ex.getMessage().contains("binds to unknown function"),
                () -> "got: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("derived property 'fullName' of model::Person"),
                () -> "error names the user-facing site, not the $-FQN; got: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("model::funcs::missing"),
                () -> "error names the missing function; got: " + ex.getMessage());
    }

    @Test
    @DisplayName("F.a: a Door-4 constraint bound to a missing predicate fails")
    void constraintBoundToMissingFunctionThrows() {
        com.legend.error.ModelException ex = assertThrows(com.legend.error.ModelException.class,
                () -> com.legend.Compiler.compileModel(
                        "Class model::Person [adult: model::funcs::missingPredicate] { age: Integer[1]; }"));
        assertTrue(ex.getMessage().contains("binds to unknown function")
                        && ex.getMessage().contains("constraint 'adult' of model::Person"),
                () -> "got: " + ex.getMessage());
    }

    @Test
    @DisplayName("F.a: a Door-4 binding to a PRESENT function compiles, pointing at the user fn")
    void derivedPropertyBoundToPresentFunctionCompiles() {
        var ctx = com.legend.Compiler.compileModel(
                "Class model::Person { firstName: String[1]; "
              + "  fullName() { model::funcs::fullName }: String[1]; } "
              + "function model::funcs::fullName(p: model::Person[1]): String[1] { $p.firstName }");
        var person = ctx.findClass("model::Person").orElseThrow();   // does not throw
        var fullName = person.properties().stream()
                .filter(p -> p instanceof com.legend.compiler.element.Property.Derived d
                        && d.name().equals("fullName"))
                .map(p -> (com.legend.compiler.element.Property.Derived) p)
                .findFirst().orElseThrow();
        assertEquals("model::funcs::fullName", fullName.bodyFunctionFqn());
    }

    @Test
    @DisplayName("F.a: a sugar derived property compiles — its lifted $prop$ function is present")
    void sugarDerivedPropertyPassesIntegrity() {
        var ctx = com.legend.Compiler.compileModel(
                "Class model::Person { name: String[1]; "
              + "  greeting() { $this.name }: String[1]; }");
        // The lifted model::Person$prop$greeting exists, so integrity passes.
        assertTrue(ctx.findClass("model::Person").isPresent());
    }

    // ====================================================================
    // F.b — mapping binding integrity
    // ====================================================================

    @Test
    @DisplayName("F.b: a clean-sheet mapping bound to a missing function fails at build")
    void mappingClassBindingToMissingFunctionThrows() {
        // compileModel builds the context (and validates mappings) eagerly.
        com.legend.error.ModelException ex = assertThrows(com.legend.error.ModelException.class,
                () -> com.legend.Compiler.compileModel(
                        "Class model::Person { name: String[1]; } "
                      + "Mapping my::M ( *model::Person: Pure { my::funcs::missing } )"));
        assertTrue(ex.getMessage().contains("class binding for 'model::Person'")
                        && ex.getMessage().contains("unknown function"),
                () -> "got: " + ex.getMessage());
    }

    @Test
    @DisplayName("F.b: a valid clean-sheet mapping compiles (bound function present)")
    void validCleanSheetMappingCompiles() {
        var ctx = com.legend.Compiler.compileModel(
                "Class model::Person { name: String[1]; } "
              + "function my::funcs::personMapping(): model::Person[*] { model::Person.all() } "
              + "Mapping my::M ( *model::Person: Pure { my::funcs::personMapping } )");
        // No throw at build; the class still resolves.
        assertTrue(ctx.findClass("model::Person").isPresent());
    }

    @Test
    @DisplayName("F.b: a legacy mapping passes integrity — its lifted $class$ function is present")
    void legacyMappingPassesIntegrity() {
        var ctx = com.legend.Compiler.compileModel(
                "Class model::Person { name: String[1]; } "
              + "Class model::RawPerson { name: String[1]; } "
              + "Mapping my::M ( *model::Person: Pure { ~src model::RawPerson name: $src.name } )");
        // The lifted my::M$class$model::Person exists, so build doesn't throw.
        assertTrue(ctx.findClass("model::Person").isPresent());
    }

    // ====================================================================
    // F.c — structural signature-shape checks (no subtyping; that is G)
    // ====================================================================

    @Test
    @DisplayName("F.c: a constraint bound to a non-Boolean function fails")
    void constraintBoundToNonBooleanFunctionThrows() {
        com.legend.error.ModelException ex = assertThrows(com.legend.error.ModelException.class,
                () -> com.legend.Compiler.compileModel(
                        "Class model::Person { age: Integer[1]; } "
                      + "function my::funcs::wrong(p: model::Person[1]): String[1] { 'x' } "
                      + "Class model::P2 [adult: my::funcs::wrong] { x: Integer[1]; }"));
        assertTrue(ex.getMessage().contains("constraint 'adult' of model::P2")
                        && ex.getMessage().contains("returning Boolean[1]"),
                () -> "got: " + ex.getMessage());
    }

    @Test
    @DisplayName("F.c: a clean-sheet mapping bound to a non-Class[*] function fails")
    void mappingBoundToNonClassManyFunctionThrows() {
        com.legend.error.ModelException ex = assertThrows(com.legend.error.ModelException.class,
                () -> com.legend.Compiler.compileModel(
                        "Class model::Person { name: String[1]; } "
                      + "function my::funcs::notAClass(): String[1] { 'x' } "
                      + "Mapping my::M ( *model::Person: Pure { my::funcs::notAClass } )"));
        assertTrue(ex.getMessage().contains("class binding for 'model::Person'")
                        && ex.getMessage().contains("(): Class[*]"),
                () -> "got: " + ex.getMessage());
    }

    @Test
    @DisplayName("F.c: a well-shaped Door-4 constraint compiles")
    void wellShapedConstraintCompiles() {
        var ctx = com.legend.Compiler.compileModel(
                "Class model::Person { age: Integer[1]; } "
              + "function my::funcs::isAdult(p: model::Person[1]): Boolean[1] { true } "
              + "Class model::P2 [adult: my::funcs::isAdult] { x: Integer[1]; }");
        assertTrue(ctx.findClass("model::P2").isPresent());   // does not throw
    }
}
