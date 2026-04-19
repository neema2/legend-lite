package com.gs.legend.model;

import com.gs.legend.model.m3.Primitive;
import com.gs.legend.model.m3.Property;
import com.gs.legend.model.m3.PureClass;
import com.gs.legend.model.m3.Type;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end coverage for the phase-2.5e Stage A work in {@link PureModelBuilder}:
 * <ul>
 *   <li>Class property types are now parsed structurally via
 *       {@code PureModelParser.parsePureType}, not via whole-string kind lookup.</li>
 *   <li>Inside a generic class body, bare type-parameter references
 *       ({@code x: T[1]} inside {@code Class Foo<T>}) resolve to {@link Type.TypeVar}
 *       rather than failing as unknown type names.</li>
 *   <li>All other property types continue to resolve to primitives / classes / enums;
 *       unresolvable names throw per the no-fallback invariant.</li>
 * </ul>
 *
 * <p>Commit 2 prerequisite: these shapes are what the builtin catalog (commit 2c) relies
 * on to land {@code Pair<U,V>}, {@code SortInfo<T>}, etc. without ad-hoc string hacks.
 */
class GenericClassBuilderTest {

    @Test
    void bareTypeParamResolvesToTypeVar() {
        String source = """
                Class model::Box<T>
                {
                    value: T[1];
                }
                """;
        PureClass box = buildClass(source, "model::Box");
        assertEquals(List.of("T"), box.typeParams());

        Property value = box.findLocalProperty("value").orElseThrow();
        Type.TypeVar tv = assertInstanceOf(Type.TypeVar.class, value.type());
        assertEquals("T", tv.name());
    }

    @Test
    void multipleTypeParamsResolveIndependently() {
        String source = """
                Class model::Pair<U, V>
                {
                    first: U[1];
                    second: V[1];
                }
                """;
        PureClass pair = buildClass(source, "model::Pair");
        assertEquals(List.of("U", "V"), pair.typeParams());

        Type firstType = pair.findLocalProperty("first").orElseThrow().type();
        Type secondType = pair.findLocalProperty("second").orElseThrow().type();
        assertEquals("U", ((Type.TypeVar) firstType).name());
        assertEquals("V", ((Type.TypeVar) secondType).name());
    }

    @Test
    void nonTypeParamPropertiesStillResolveToPrimitivesAndClasses() {
        String source = """
                Class model::Mixed<T>
                {
                    id: T[1];
                    label: String[1];
                    count: Integer[1];
                }
                """;
        PureClass mixed = buildClass(source, "model::Mixed");

        Type idType = mixed.findLocalProperty("id").orElseThrow().type();
        assertInstanceOf(Type.TypeVar.class, idType,
                "id should be a TypeVar, got " + idType.getClass().getSimpleName());
        assertEquals("T", ((Type.TypeVar) idType).name(),
                "id's TypeVar must bind the declared type-param 'T'");

        // String and Integer resolve to Primitive leaves, unchanged by the type-var pass.
        assertEquals(Primitive.STRING, mixed.findLocalProperty("label").orElseThrow().type());
        assertEquals(Primitive.INTEGER, mixed.findLocalProperty("count").orElseThrow().type());
    }

    @Test
    void nonGenericClassUnaffectedByTypeParamPath() {
        // Regression guard: pre-phase-2.5e user classes with zero type-params must keep
        // resolving property types exactly as before. Structural parse + substitute pipeline
        // must be a no-op transformation for the common case.
        String source = """
                Class model::Person
                {
                    firstName: String[1];
                    age: Integer[0..1];
                }
                """;
        PureClass person = buildClass(source, "model::Person");
        assertEquals(List.of(), person.typeParams());
        assertEquals(Primitive.STRING, person.findLocalProperty("firstName").orElseThrow().type());
        assertEquals(Primitive.INTEGER, person.findLocalProperty("age").orElseThrow().type());
    }

    @Test
    void typeParameterShadowsPrimitiveOfSameName() {
        // Subtle contract: inside a generic class body, a type-parameter whose name
        // collides with a primitive (or any other in-scope type) wins. Standard
        // lexical-scoping behaviour for generics — `Class Foo<String>` declares a
        // type variable literally named "String" that shadows the primitive within
        // the class body.
        //
        // This invariant is encoded by the ordering in substituteTypeVarsAndClassify:
        // the typeParams check runs BEFORE findType lookup. Flipping that order
        // would silently resolve `x: String[1]` to Primitive.STRING instead of
        // TypeVar("String"), producing a quietly wrong type without any error.
        String source = """
                Class model::Shadow<String>
                {
                    x: String[1];
                    realString: Integer[1];
                }
                """;
        PureClass shadow = buildClass(source, "model::Shadow");
        assertEquals(List.of("String"), shadow.typeParams());

        Type xType = shadow.findLocalProperty("x").orElseThrow().type();
        Type.TypeVar tv = assertInstanceOf(Type.TypeVar.class, xType,
                "Inside Class Shadow<String>, 'x: String[1]' must bind to the type-param, "
                        + "not the primitive. Got: " + xType);
        assertEquals("String", tv.name());

        // Sanity: Integer is not a type-param here, so it still resolves to the primitive.
        // (Guards against a degenerate implementation that treats ALL names as type-params.)
        assertEquals(Primitive.INTEGER,
                shadow.findLocalProperty("realString").orElseThrow().type());
    }

    @Test
    void unresolvableTypeThrowsWithPropertyContext() {
        // No-fallback invariant: an unknown type in a property position must throw clearly,
        // carrying the property name in the error for diagnostic value.
        String source = """
                Class model::Broken
                {
                    bad: NonExistentType[1];
                }
                """;
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> new PureModelBuilder().addSource(source));
        assertTrue(ex.getMessage().contains("NonExistentType"),
                "Error should name the missing type, got: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("bad"),
                "Error should name the offending property, got: " + ex.getMessage());
    }

    // ===== Helpers =====

    private static PureClass buildClass(String source, String fqn) {
        PureModelBuilder builder = new PureModelBuilder().addSource(source);
        return builder.findClass(fqn).orElseThrow(
                () -> new AssertionError("Class not found: " + fqn));
    }
}
