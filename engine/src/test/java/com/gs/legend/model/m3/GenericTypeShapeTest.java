package com.gs.legend.model.m3;

import com.gs.legend.model.def.ClassDefinition;
import com.gs.legend.model.def.ClassDefinition.PropertyDefinition;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the phase-2.5e data-shape additions:
 * <ul>
 *   <li>{@link Type.GenericType} — structured replacement for {@link Type.Parameterized},
 *       whose {@code rawType} is itself a {@link Type} rather than a raw {@link String}.</li>
 *   <li>{@link PureClass#typeParams()} / {@link PureClass#isNative()} — new record fields
 *       with backward-compatible constructors so every pre-2.5e call site keeps compiling.</li>
 *   <li>{@link ClassDefinition#typeParams()} / {@link ClassDefinition#isNative()} — same on the AST side.</li>
 * </ul>
 */
class GenericTypeShapeTest {

    // ===== Type.GenericType =====

    @Test
    void genericTypeCarriesRawTypeAndArgs() {
        Type rawRef = new Type.NameRef("Pair");
        Type.GenericType gt = new Type.GenericType(rawRef,
                List.of(Primitive.STRING, Primitive.INTEGER));
        assertSame(rawRef, gt.rawType());
        assertEquals(2, gt.typeArgs().size());
        assertEquals("Pair", gt.typeName(), "typeName delegates to rawType.typeName()");
    }

    /**
     * After {@code NameResolver} runs, {@code rawType} is typically a {@link Type.ClassType}
     * carrying a full FQN. The user-facing {@code typeName()} must strip to the simple name
     * — error messages and debug output depend on this.
     */
    @Test
    void genericTypeNameStripsFqnWhenRawIsClassType() {
        Type.ClassType pairClass = new Type.ClassType("meta::pure::functions::collection::Pair");
        Type.GenericType gt = new Type.GenericType(pairClass,
                List.of(Primitive.STRING, Primitive.INTEGER));
        assertEquals("Pair", gt.typeName(),
                "typeName() must delegate to rawType.typeName(), which strips FQN to simple name");
    }

    /**
     * Nested generics — {@code Relation<Pair<String, Integer>>} — must survive a round-trip
     * through the record without the inner {@link Type.GenericType} being flattened or lost.
     */
    @Test
    void genericTypeNestsAnotherGenericTypeCleanly() {
        Type inner = new Type.GenericType(
                new Type.ClassType("meta::pure::functions::collection::Pair"),
                List.of(Primitive.STRING, Primitive.INTEGER));
        Type.GenericType outer = new Type.GenericType(
                new Type.ClassType("meta::pure::metamodel::relation::Relation"),
                List.of(inner));
        assertEquals(1, outer.typeArgs().size());
        assertSame(inner, outer.typeArgs().get(0));
        assertTrue(outer.typeArgs().get(0) instanceof Type.GenericType);
    }

    @Test
    void genericTypeTypeArgsAreImmutable() {
        java.util.ArrayList<Type> mutable = new java.util.ArrayList<>();
        mutable.add(Primitive.STRING);
        Type.GenericType gt = new Type.GenericType(new Type.NameRef("List"), mutable);
        // Downstream mutation of the input list must not leak into the record.
        mutable.add(Primitive.INTEGER);
        assertEquals(1, gt.typeArgs().size());
    }

    @Test
    void genericTypeAllowsClassTypeAsRaw() {
        // Post-NameResolver shape: rawType is a classified variant.
        Type.ClassType pairClass = new Type.ClassType("meta::pure::functions::collection::Pair");
        Type.GenericType gt = new Type.GenericType(pairClass, List.of(Primitive.STRING, Primitive.INTEGER));
        assertSame(pairClass, gt.rawType());
    }

    // ===== PureClass backward-compat =====

    @Test
    void pureClassPreservesLegacySixArgConstructor() {
        // The pre-phase-2.5e six-arg shape must still compile and produce typeParams=[] + isNative=false.
        PureClass pc = new PureClass(
                "model",
                "Person",
                List.<String>of(),
                List.<Property>of(),
                List.of(),
                List.of());
        assertEquals(List.of(), pc.typeParams());
        assertFalse(pc.isNative());
    }

    @Test
    void pureClassFullConstructorCarriesTypeParamsAndIsNative() {
        PureClass pc = new PureClass(
                "meta::pure::metamodel::relation",
                "Relation",
                List.of("T"),
                List.<String>of(),
                List.<Property>of(),
                List.of(),
                List.of(),
                true);
        assertEquals(List.of("T"), pc.typeParams());
        assertTrue(pc.isNative());
    }

    // ===== ClassDefinition backward-compat =====

    @Test
    void classDefinitionPreservesLegacySevenArgConstructor() {
        // The pre-phase-2.5e seven-arg shape must still compile and default new fields.
        ClassDefinition cd = new ClassDefinition(
                "model::Person",
                List.<String>of(),
                List.<PropertyDefinition>of(),
                List.of(),
                List.of(),
                List.of(),
                List.of());
        assertEquals(List.of(), cd.typeParams());
        assertFalse(cd.isNative());
    }

    @Test
    void classDefinitionFullConstructorCarriesTypeParamsAndIsNative() {
        ClassDefinition cd = new ClassDefinition(
                "meta::pure::functions::collection::Pair",
                List.of("U", "V"),
                List.<String>of(),
                List.<PropertyDefinition>of(),
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                false);
        assertEquals(List.of("U", "V"), cd.typeParams());
        assertFalse(cd.isNative());
    }
}
