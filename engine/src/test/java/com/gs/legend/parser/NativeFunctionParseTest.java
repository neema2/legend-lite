package com.gs.legend.parser;
import com.gs.legend.model.m3.Type;

import com.gs.legend.compiler.NativeFunctionDef;
import com.gs.legend.model.m3.Multiplicity;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link PureNativeSignatureParser} — validates that the hand-rolled
 * native-signature parser produces correct {@link NativeFunctionDef} records
 * from Pure signature strings.
 */
class NativeFunctionParseTest {

    // ===== Scalar functions (no type params) =====

    @Test
    void testSimpleScalar_toLower() {
        var fn = PureNativeSignatureParser.parse(
                "native function toLower(source:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
        assertEquals("toLower", fn.name());
        assertEquals(List.of(), fn.typeParams());
        assertEquals(1, fn.params().size());
        assertEquals("source", fn.params().get(0).name());
        assertPrimitive("String", fn.params().get(0).type());
        assertPrimitive("String", fn.returnType());
        assertEquals(Multiplicity.ONE, fn.returnMult());
    }

    @Test
    void testSimpleScalar_abs() {
        var fn = PureNativeSignatureParser.parse(
                "native function abs(int:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[1];");
        assertEquals("abs", fn.name());
        assertPrimitive("Integer", fn.returnType());
    }

    @Test
    void testScalar_substring() {
        var fn = PureNativeSignatureParser.parse(
                "native function substring(str:meta::pure::metamodel::type::String[1], start:meta::pure::metamodel::type::Integer[1], end:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::String[1];");
        assertEquals("substring", fn.name());
        assertEquals(3, fn.params().size());
        assertEquals("str", fn.params().get(0).name());
        assertEquals("start", fn.params().get(1).name());
        assertEquals("end", fn.params().get(2).name());
        assertPrimitive("String", fn.returnType());
    }

    // ===== Generic functions with type params =====

    @Test
    void testGeneric_filter() {
        var fn = PureNativeSignatureParser.parse(
                "native function filter<T>(rel:meta::pure::metamodel::relation::Relation<T>[1], " +
                "f:meta::pure::metamodel::function::Function<{T[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):meta::pure::metamodel::relation::Relation<T>[1];");
        assertEquals("filter", fn.name());
        assertEquals(List.of("T"), fn.typeParams());
        assertEquals(2, fn.params().size());

        // param 0: rel:meta::pure::metamodel::relation::Relation<T>[1]
        assertPlatformClass("Relation", fn.params().get(0).type());

        // param 1: f:meta::pure::metamodel::function::Function<{T[1]->Boolean[1]}>[1]
        assertPlatformClass("Function", fn.params().get(1).type());

        // return: meta::pure::metamodel::relation::Relation<T>[1]
        var ret = assertPlatformClass("Relation", fn.returnType());
        assertEquals(1, ret.typeArgs().size());
        assertTypeVar("T", ret.typeArgs().get(0));
    }

    @Test
    void testGeneric_join() {
        // Join uses schema algebra T+V in return type — must be pre-processed
        var registry = new com.gs.legend.compiler.BuiltinRegistry();
        registry.registerSignature("join",
                "native function join<T,V>(rel1:meta::pure::metamodel::relation::Relation<T>[1], rel2:meta::pure::metamodel::relation::Relation<V>[1], " +
                "joinKind:meta::pure::functions::relation::JoinKind[1], f:meta::pure::metamodel::function::Function<{T[1],V[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):meta::pure::metamodel::relation::Relation<T+V>[1];");
        var fn = registry.resolve("join").get(0);
        assertEquals("join", fn.name());
        assertEquals(List.of("T", "V"), fn.typeParams());
        assertEquals(4, fn.params().size());
        // After pre-processing: meta::pure::metamodel::relation::Relation<T+V> → meta::pure::metamodel::relation::Relation<T> (algebra stripped)
        assertPlatformClass("Relation", fn.returnType());
        // rawSignature preserves the original T+V for schema inference
        assertTrue(fn.rawSignature().contains("T+V"));
    }

    @Test
    void testGeneric_exists() {
        var fn = PureNativeSignatureParser.parse(
                "native function exists<T>(value:T[*], func:meta::pure::metamodel::function::Function<{T[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):meta::pure::metamodel::type::Boolean[1];");
        assertEquals("exists", fn.name());
        assertEquals(List.of("T"), fn.typeParams());
        assertTypeVar("T", fn.params().get(0).type());
        assertEquals(Multiplicity.MANY, fn.params().get(0).multiplicity());
        assertPrimitive("Boolean", fn.returnType());
    }

    // ===== Multiplicity variables =====

    @Test
    void testMultiplicityVar_sort() {
        var fn = PureNativeSignatureParser.parse(
                "native function sort<T|m>(col:T[m]):T[m];");
        assertEquals("sort", fn.name());
        assertEquals(List.of("T"), fn.typeParams());
        assertEquals(List.of("m"), fn.multParams());
        assertTypeVar("T", fn.returnType());
        assertInstanceOf(Multiplicity.Var.class, fn.returnMult());
        assertEquals("m", ((Multiplicity.Var) fn.returnMult()).name());
        assertInstanceOf(Multiplicity.Var.class, fn.params().get(0).multiplicity());
    }

    // ===== Window functions =====

    @Test
    void testWindow_rank() {
        var fn = PureNativeSignatureParser.parse(
                "native function rank<T>(rel:meta::pure::metamodel::relation::Relation<T>[1], w:meta::pure::functions::relation::_Window<T>[1], row:T[1]):meta::pure::metamodel::type::Integer[1];");
        assertEquals("rank", fn.name());
        assertEquals(3, fn.params().size());
        assertPlatformClass("_Window", fn.params().get(1).type());
        assertPrimitive("Integer", fn.returnType());
    }

    @Test
    void testWindow_first() {
        var fn = PureNativeSignatureParser.parse(
                "native function first<T>(w:meta::pure::metamodel::relation::Relation<T>[1], f:meta::pure::functions::relation::_Window<T>[1], r:T[1]):T[0..1];");
        assertEquals("first", fn.name());
        assertTypeVar("T", fn.returnType());
        assertEquals(Multiplicity.ZERO_ONE, fn.returnMult());
    }

    // ===== Schema constraints (via pre-processing) =====

    @Test
    void testSubsetConstraint_select() {
        // ⊆ is stripped by normalizeSignature — constraint preserved in rawSignature
        var registry = new com.gs.legend.compiler.BuiltinRegistry();
        registry.registerSignature("select",
                "native function select<T,Z>(r:meta::pure::metamodel::relation::Relation<T>[1], cols:meta::pure::metamodel::relation::ColSpecArray<Z⊆T>[1]):meta::pure::metamodel::relation::Relation<Z>[1];");
        var fn = registry.resolve("select").get(0);
        assertEquals("select", fn.name());
        assertEquals(List.of("T", "Z"), fn.typeParams());

        // After normalization: meta::pure::metamodel::relation::ColSpecArray<Z⊆T> → meta::pure::metamodel::relation::ColSpecArray<Z>
        var colsType = assertPlatformClass("ColSpecArray", fn.params().get(1).type());
        assertTypeVar("Z", colsType.typeArgs().get(0));

        // rawSignature preserves constraint for later analysis
        assertTrue(fn.rawSignature().contains("Z⊆T"));
    }

    @Test
    void testTypeMatchConstraint_rename() {
        // =(?:K) and ⊆ stripped by normalizeSignature
        var registry = new com.gs.legend.compiler.BuiltinRegistry();
        registry.registerSignature("rename",
                "native function rename<T,Z,K,V>(r:meta::pure::metamodel::relation::Relation<T>[1], " +
                "old:meta::pure::metamodel::relation::ColSpec<Z=(?:K)⊆T>[1], new:meta::pure::metamodel::relation::ColSpec<V=(?:K)>[1]):meta::pure::metamodel::relation::Relation<T-Z+V>[1];");
        var fn = registry.resolve("rename").get(0);
        assertEquals("rename", fn.name());
        assertEquals(List.of("T", "Z", "K", "V"), fn.typeParams());

        // After normalization: meta::pure::metamodel::relation::ColSpec<Z=(?:K)⊆T> → meta::pure::metamodel::relation::ColSpec<Z>, meta::pure::metamodel::relation::Relation<T-Z+V> → meta::pure::metamodel::relation::Relation<T>
        assertPlatformClass("ColSpec", fn.params().get(1).type());

        // rawSignature preserves both constraints and schema algebra
        assertTrue(fn.rawSignature().contains("Z=(?:K)⊆T"));
        assertTrue(fn.rawSignature().contains("T-Z+V"));
    }

    // ===== Function types (lambda params) =====

    @Test
    void testFunctionType_map() {
        var fn = PureNativeSignatureParser.parse(
                "native function map<T,V>(value:T[*], func:meta::pure::metamodel::function::Function<{T[1]->V[*]}>[1]):V[*];");
        assertEquals("map", fn.name());

        var funcType = assertPlatformClass("Function", fn.params().get(1).type());
        // Function type arg should be a FunctionType
        assertFalse(funcType.typeArgs().isEmpty());
        assertInstanceOf(Type.FunctionType.class, funcType.typeArgs().get(0));

        var ft = (Type.FunctionType) funcType.typeArgs().get(0);
        assertEquals(1, ft.params().size());
        assertTypeVar("V", ft.returnType());
    }

    // ===== Aggregate functions =====

    @Test
    void testAggregate_groupBy() {
        // groupBy uses both ⊆ and schema algebra Z+R — must be pre-processed
        var registry = new com.gs.legend.compiler.BuiltinRegistry();
        registry.registerSignature("groupBy",
                "native function groupBy<T,Z,K,V,R>(r:meta::pure::metamodel::relation::Relation<T>[1], " +
                "cols:meta::pure::metamodel::relation::ColSpecArray<Z⊆T>[1], " +
                "agg:meta::pure::metamodel::relation::AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):meta::pure::metamodel::relation::Relation<Z+R>[1];");
        var fn = registry.resolve("groupBy").get(0);
        assertEquals("groupBy", fn.name());
        assertEquals(List.of("T", "Z", "K", "V", "R"), fn.typeParams());
        assertEquals(3, fn.params().size());

        // After normalization: meta::pure::metamodel::relation::ColSpecArray<Z⊆T> → meta::pure::metamodel::relation::ColSpecArray<Z>
        assertPlatformClass("AggColSpec", fn.params().get(2).type());
        assertTrue(fn.rawSignature().contains("Z⊆T"));
        assertTrue(fn.rawSignature().contains("Z+R"));
    }

    // ===== All registered signatures parse =====

    @Test
    void testAllRegisteredSignaturesParse() {
        var registry = com.gs.legend.compiler.BuiltinRegistry.instance();
        int count = 0;
        for (var entry : registry.allRegistered().entrySet()) {
            for (NativeFunctionDef def : entry.getValue()) {
                assertNotNull(def.name(), "Name should not be null");
                assertNotNull(def.returnType(), "Return type should not be null for: " + def.name());
                count++;
            }
        }
        assertTrue(count > 100, "Should have parsed 100+ signatures, got: " + count);
    }

    // ===== Helpers =====

    /**
     * Asserts the parsed type is a built-in primitive matching the given Pure simple name
     * (e.g., "Integer", "String"). Native signatures resolve primitives eagerly at parse
     * time via FQN, so the actual type is a {@link com.gs.legend.model.m3.Primitive}.
     */
    private static void assertPrimitive(String expected, Type actual) {
        assertInstanceOf(com.gs.legend.model.m3.Primitive.class, actual,
                "Expected Primitive(" + expected + ") but got: " + actual);
        assertEquals(expected, ((com.gs.legend.model.m3.Primitive) actual).pureName());
    }

    private static void assertTypeVar(String expected, Type actual) {
        assertInstanceOf(Type.TypeVar.class, actual, "Expected TypeVar(" + expected + ") but got: " + actual);
        assertEquals(expected, ((Type.TypeVar) actual).name());
    }

    /**
     * Asserts the type is {@code GenericType(LClass.X, ...)} where {@code X.typeName()} matches
     * {@code expected}. Returns the {@link Type.GenericType} so callers can chain assertions on
     * {@link Type.GenericType#typeArgs()}.
     */
    private static Type.GenericType assertPlatformClass(String expected, Type actual) {
        Type.GenericType gt = assertInstanceOf(Type.GenericType.class, actual,
                "Expected GenericType(LClass." + expected + ", ...) but got: " + actual);
        com.gs.legend.model.m3.LClass lc = assertInstanceOf(com.gs.legend.model.m3.LClass.class,
                gt.rawType(),
                "Expected GenericType rawType to be LClass." + expected + " but got: " + gt.rawType());
        assertEquals(expected, lc.typeName(),
                "Expected LClass with simple name '" + expected + "' but got '" + lc.typeName() + "'");
        return gt;
    }
}
