package com.gs.legend.parser;

import com.gs.legend.model.def.ClassDefinition;
import com.gs.legend.model.def.PackageableElement;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Parser coverage for the phase-2.5e grammar additions:
 * <ul>
 *   <li>Generic class headers: {@code Class Foo<T>}, {@code Class Foo<U, V> extends Bar}</li>
 *   <li>Native class stubs: {@code native Class FQN<T> {}}</li>
 * </ul>
 *
 * <p>Non-generic / non-native classes are covered by the existing parser test suite; this
 * file pins the new surface only.
 */
class GenericClassParserTest {

    // ===== Generic class headers =====

    @Test
    void singleTypeParam() {
        String source = """
                Class model::Box<T>
                {
                    value: T[1];
                }
                """;
        ClassDefinition cd = parseSingleClass(source);
        assertEquals("model::Box", cd.qualifiedName());
        assertEquals(List.of("T"), cd.typeParams());
        assertFalse(cd.isNative());
        // Property type is stored as the raw String "T"; type-var scoping happens in a later phase.
        assertEquals("T", cd.properties().get(0).type());
    }

    @Test
    void multipleTypeParams() {
        String source = """
                Class model::Pair<U, V>
                {
                    first: U[1];
                    second: V[1];
                }
                """;
        ClassDefinition cd = parseSingleClass(source);
        assertEquals(List.of("U", "V"), cd.typeParams());
        assertEquals(2, cd.properties().size());
    }

    @Test
    void threeTypeParamsWithExtends() {
        String source = """
                Class model::AggColSpec<Z, V, T> extends meta::pure::metamodel::type::Any
                {
                }
                """;
        ClassDefinition cd = parseSingleClass(source);
        assertEquals(List.of("Z", "V", "T"), cd.typeParams());
        assertEquals(List.of("meta::pure::metamodel::type::Any"), cd.superClasses());
    }

    // ===== Generic property types (commit-2 critical) =====

    /**
     * Builtin bodies use generic references like {@code SortInfo<Any>} in property types.
     * The parser must capture the full text verbatim — {@code parseType}'s depth-skip over
     * {@code <...>} is what makes this work, and if that interacts badly with the new
     * class-header generic grammar it would break every builtin in commit 2.
     */
    @Test
    void propertyTypeWithGenericReferenceRoundTrips() {
        String source = """
                Class model::_Window<T>
                {
                    partition: String[*];
                    sortInfo: SortInfo<Any>[*];
                }
                """;
        ClassDefinition cd = parseSingleClass(source);
        assertEquals(List.of("T"), cd.typeParams());
        assertEquals(2, cd.properties().size());
        assertEquals("SortInfo<Any>", cd.properties().get(1).type(),
                "Property type must preserve the full generic reference verbatim");
    }

    /**
     * Nested generics — {@code Function<Relation<T>>} — must not confuse the depth counter
     * in {@code parseType}. Exercises the parser's ability to nest {@code <...>} correctly.
     */
    @Test
    void propertyTypeWithNestedGenericRoundTrips() {
        String source = """
                Class model::Holder<T>
                {
                    fn: Function<Relation<T>>[1];
                }
                """;
        ClassDefinition cd = parseSingleClass(source);
        assertEquals("Function<Relation<T>>", cd.properties().get(0).type());
    }

    /**
     * A generic super type like {@code extends Bar<T>} sits right after the optional
     * class-header {@code <T>}. If the header grammar and the extends grammar don't cleanly
     * separate, one side will over-consume the other's angle brackets.
     */
    @Test
    void genericSuperTypeParsesAfterGenericHeader() {
        String source = """
                Class model::Child<T> extends model::Parent<T>
                {
                }
                """;
        ClassDefinition cd = parseSingleClass(source);
        assertEquals(List.of("T"), cd.typeParams());
        assertEquals(List.of("model::Parent<T>"), cd.superClasses());
    }

    // ===== Native class stubs =====

    @Test
    void nativeClassWithTypeParams() {
        String source = """
                native Class meta::pure::metamodel::relation::Relation<T>
                {
                }
                """;
        ClassDefinition cd = parseSingleClass(source);
        assertEquals("meta::pure::metamodel::relation::Relation", cd.qualifiedName());
        assertEquals(List.of("T"), cd.typeParams());
        assertTrue(cd.isNative());
        assertEquals(0, cd.properties().size());
    }

    /**
     * Native classes are meant to be empty bootstrap stubs, but the current grammar
     * doesn't enforce that — properties inside a native body are still captured. This test
     * pins the status quo so a future enforcement change is a conscious decision, not an
     * accidental regression.
     */
    @Test
    void nativeClassCapturesBodyPropertiesAsStatusQuo() {
        String source = """
                native Class model::WithBody<T>
                {
                    x: String[1];
                }
                """;
        ClassDefinition cd = parseSingleClass(source);
        assertTrue(cd.isNative());
        assertEquals(1, cd.properties().size(),
                "Grammar currently permits native-class bodies; if this changes, update commit-2 seeding accordingly");
    }

    @Test
    void nativeRequiresClassKeywordAfter() {
        String source = "native Enum model::Bad { A, B }";
        PureParseException e = assertThrows(PureParseException.class,
                () -> PureParser.parseModel(source));
        assertTrue(e.getMessage().contains("expected 'Class' after 'native'"),
                "Expected error about missing 'Class' keyword, got: " + e.getMessage());
    }

    // ===== Malformed type-param list error paths =====

    /** Empty type-param list: {@code Class Foo<> { }} should fail cleanly, not silently accept. */
    @Test
    void emptyTypeParamListRejected() {
        String source = "Class model::Foo<> { }";
        assertThrows(PureParseException.class,
                () -> PureParser.parseModel(source));
    }

    /** Unterminated type-param list: {@code Class Foo<T { }} should fail, not run off into the body. */
    @Test
    void unterminatedTypeParamListRejected() {
        String source = "Class model::Foo<T { }";
        assertThrows(PureParseException.class,
                () -> PureParser.parseModel(source));
    }

    // ===== Helpers =====

    private static ClassDefinition parseSingleClass(String source) {
        List<PackageableElement> defs = PureParser.parseModel(source);
        assertEquals(1, defs.size(), "Expected exactly one definition");
        return assertInstanceOf(ClassDefinition.class, defs.get(0));
    }
}
