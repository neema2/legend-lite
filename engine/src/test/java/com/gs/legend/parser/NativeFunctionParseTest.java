package com.gs.legend.parser;

import com.gs.legend.compiler.NativeFunctionDef;
import com.gs.legend.compiler.PType;
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
                "native function toLower(source:String[1]):String[1];");
        assertEquals("toLower", fn.name());
        assertEquals(List.of(), fn.typeParams());
        assertEquals(1, fn.params().size());
        assertEquals("source", fn.params().get(0).name());
        assertConcrete("String", fn.params().get(0).type());
        assertConcrete("String", fn.returnType());
        assertEquals(Multiplicity.ONE, fn.returnMult());
    }

    @Test
    void testSimpleScalar_abs() {
        var fn = PureNativeSignatureParser.parse(
                "native function abs(int:Integer[1]):Integer[1];");
        assertEquals("abs", fn.name());
        assertConcrete("Integer", fn.returnType());
    }

    @Test
    void testScalar_substring() {
        var fn = PureNativeSignatureParser.parse(
                "native function substring(str:String[1], start:Integer[1], end:Integer[1]):String[1];");
        assertEquals("substring", fn.name());
        assertEquals(3, fn.params().size());
        assertEquals("str", fn.params().get(0).name());
        assertEquals("start", fn.params().get(1).name());
        assertEquals("end", fn.params().get(2).name());
        assertConcrete("String", fn.returnType());
    }

    // ===== Generic functions with type params =====

    @Test
    void testGeneric_filter() {
        var fn = PureNativeSignatureParser.parse(
                "native function filter<T>(rel:Relation<T>[1], " +
                "f:Function<{T[1]->Boolean[1]}>[1]):Relation<T>[1];");
        assertEquals("filter", fn.name());
        assertEquals(List.of("T"), fn.typeParams());
        assertEquals(2, fn.params().size());

        // param 0: rel:Relation<T>[1]
        assertParameterized("Relation", fn.params().get(0).type());

        // param 1: f:Function<{T[1]->Boolean[1]}>[1]
        assertParameterized("Function", fn.params().get(1).type());

        // return: Relation<T>[1]
        var ret = assertParameterized("Relation", fn.returnType());
        assertEquals(1, ret.typeArgs().size());
        assertTypeVar("T", ret.typeArgs().get(0));
    }

    @Test
    void testGeneric_join() {
        // Join uses schema algebra T+V in return type — must be pre-processed
        var registry = new com.gs.legend.compiler.BuiltinFunctionRegistry();
        registry.registerSignature("join",
                "native function join<T,V>(rel1:Relation<T>[1], rel2:Relation<V>[1], " +
                "joinKind:JoinKind[1], f:Function<{T[1],V[1]->Boolean[1]}>[1]):Relation<T+V>[1];");
        var fn = registry.resolve("join").get(0);
        assertEquals("join", fn.name());
        assertEquals(List.of("T", "V"), fn.typeParams());
        assertEquals(4, fn.params().size());
        // After pre-processing: Relation<T+V> → Relation<T> (algebra stripped)
        assertParameterized("Relation", fn.returnType());
        // rawSignature preserves the original T+V for schema inference
        assertTrue(fn.rawSignature().contains("T+V"));
    }

    @Test
    void testGeneric_exists() {
        var fn = PureNativeSignatureParser.parse(
                "native function exists<T>(value:T[*], func:Function<{T[1]->Boolean[1]}>[1]):Boolean[1];");
        assertEquals("exists", fn.name());
        assertEquals(List.of("T"), fn.typeParams());
        assertTypeVar("T", fn.params().get(0).type());
        assertEquals(Multiplicity.MANY, fn.params().get(0).mult());
        assertConcrete("Boolean", fn.returnType());
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
        assertInstanceOf(Multiplicity.Var.class, fn.params().get(0).mult());
    }

    // ===== Window functions =====

    @Test
    void testWindow_rank() {
        var fn = PureNativeSignatureParser.parse(
                "native function rank<T>(rel:Relation<T>[1], w:_Window<T>[1], row:T[1]):Integer[1];");
        assertEquals("rank", fn.name());
        assertEquals(3, fn.params().size());
        assertParameterized("_Window", fn.params().get(1).type());
        assertConcrete("Integer", fn.returnType());
    }

    @Test
    void testWindow_first() {
        var fn = PureNativeSignatureParser.parse(
                "native function first<T>(w:Relation<T>[1], f:_Window<T>[1], r:T[1]):T[0..1];");
        assertEquals("first", fn.name());
        assertTypeVar("T", fn.returnType());
        assertEquals(Multiplicity.ZERO_ONE, fn.returnMult());
    }

    // ===== Schema constraints (via pre-processing) =====

    @Test
    void testSubsetConstraint_select() {
        // ⊆ is stripped by normalizeSignature — constraint preserved in rawSignature
        var registry = new com.gs.legend.compiler.BuiltinFunctionRegistry();
        registry.registerSignature("select",
                "native function select<T,Z>(r:Relation<T>[1], cols:ColSpecArray<Z⊆T>[1]):Relation<Z>[1];");
        var fn = registry.resolve("select").get(0);
        assertEquals("select", fn.name());
        assertEquals(List.of("T", "Z"), fn.typeParams());

        // After normalization: ColSpecArray<Z⊆T> → ColSpecArray<Z>
        var colsType = assertParameterized("ColSpecArray", fn.params().get(1).type());
        assertTypeVar("Z", colsType.typeArgs().get(0));

        // rawSignature preserves constraint for later analysis
        assertTrue(fn.rawSignature().contains("Z⊆T"));
    }

    @Test
    void testTypeMatchConstraint_rename() {
        // =(?:K) and ⊆ stripped by normalizeSignature
        var registry = new com.gs.legend.compiler.BuiltinFunctionRegistry();
        registry.registerSignature("rename",
                "native function rename<T,Z,K,V>(r:Relation<T>[1], " +
                "old:ColSpec<Z=(?:K)⊆T>[1], new:ColSpec<V=(?:K)>[1]):Relation<T-Z+V>[1];");
        var fn = registry.resolve("rename").get(0);
        assertEquals("rename", fn.name());
        assertEquals(List.of("T", "Z", "K", "V"), fn.typeParams());

        // After normalization: ColSpec<Z=(?:K)⊆T> → ColSpec<Z>, Relation<T-Z+V> → Relation<T>
        assertParameterized("ColSpec", fn.params().get(1).type());

        // rawSignature preserves both constraints and schema algebra
        assertTrue(fn.rawSignature().contains("Z=(?:K)⊆T"));
        assertTrue(fn.rawSignature().contains("T-Z+V"));
    }

    // ===== Function types (lambda params) =====

    @Test
    void testFunctionType_map() {
        var fn = PureNativeSignatureParser.parse(
                "native function map<T,V>(value:T[*], func:Function<{T[1]->V[*]}>[1]):V[*];");
        assertEquals("map", fn.name());

        var funcType = assertParameterized("Function", fn.params().get(1).type());
        // Function type arg should be a FunctionType
        assertFalse(funcType.typeArgs().isEmpty());
        assertInstanceOf(PType.FunctionType.class, funcType.typeArgs().get(0));

        var ft = (PType.FunctionType) funcType.typeArgs().get(0);
        assertEquals(1, ft.paramTypes().size());
        assertTypeVar("V", ft.returnType());
    }

    // ===== Aggregate functions =====

    @Test
    void testAggregate_groupBy() {
        // groupBy uses both ⊆ and schema algebra Z+R — must be pre-processed
        var registry = new com.gs.legend.compiler.BuiltinFunctionRegistry();
        registry.registerSignature("groupBy",
                "native function groupBy<T,Z,K,V,R>(r:Relation<T>[1], " +
                "cols:ColSpecArray<Z⊆T>[1], " +
                "agg:AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<Z+R>[1];");
        var fn = registry.resolve("groupBy").get(0);
        assertEquals("groupBy", fn.name());
        assertEquals(List.of("T", "Z", "K", "V", "R"), fn.typeParams());
        assertEquals(3, fn.params().size());

        // After normalization: ColSpecArray<Z⊆T> → ColSpecArray<Z>
        assertParameterized("AggColSpec", fn.params().get(2).type());
        assertTrue(fn.rawSignature().contains("Z⊆T"));
        assertTrue(fn.rawSignature().contains("Z+R"));
    }

    // ===== All registered signatures parse =====

    @Test
    void testAllRegisteredSignaturesParse() {
        var registry = com.gs.legend.compiler.BuiltinFunctionRegistry.instance();
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

    private static void assertConcrete(String expected, PType actual) {
        assertInstanceOf(PType.Concrete.class, actual, "Expected Concrete(" + expected + ") but got: " + actual);
        assertEquals(expected, ((PType.Concrete) actual).name());
    }

    private static void assertTypeVar(String expected, PType actual) {
        assertInstanceOf(PType.TypeVar.class, actual, "Expected TypeVar(" + expected + ") but got: " + actual);
        assertEquals(expected, ((PType.TypeVar) actual).name());
    }

    private static PType.Parameterized assertParameterized(String expected, PType actual) {
        assertInstanceOf(PType.Parameterized.class, actual,
                "Expected Parameterized(" + expected + ") but got: " + actual);
        var p = (PType.Parameterized) actual;
        assertEquals(expected, p.rawType());
        return p;
    }
}
