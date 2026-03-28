package com.gs.legend.compiler;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the BuiltinFunctionRegistry: ensures all functions are registered,
 * correctly named, and retrievable.
 */
class BuiltinFunctionRegistryTest {

    private final BuiltinFunctionRegistry registry = BuiltinFunctionRegistry.instance();

    // ===== Existence checks =====

    @Test
    void registrySingleton_isNotNull() {
        assertNotNull(registry);
    }

    @Test
    void hasRelationFunctions() {
        // Tier 1: every relation function should be registered
        var expected = List.of(
                "filter", "sort", "limit", "drop", "slice", "concatenate", "size",
                "distinct", "select", "rename",
                "extend", "groupBy", "aggregate",
                "join", "asOfJoin",
                "pivot", "project", "flatten",
                "first", "last", "nth", "offset", "rowNumber", "ntile",
                "rank", "denseRank", "percentRank", "cumulativeDistribution",
                "lag", "lead"
        );
        for (String name : expected) {
            assertTrue(registry.isRegistered(name),
                    "Missing relation function: " + name);
        }
    }

    @Test
    void hasScalarFunctions() {
        // Tier 2: key scalar functions should be registered
        var expected = List.of(
                "toLower", "toUpper", "trim", "ltrim", "rtrim",
                "substring", "indexOf", "startsWith", "endsWith", "reverseString",
                "abs", "ceiling", "floor", "round", "sqrt", "pow", "exp", "log", "log10", "sign",
                "sin", "tan",
                "plus", "minus", "times",
                "dateDiff", "year", "monthNumber", "hour", "minute", "second",
                "parseInteger", "parseFloat", "parseDate", "parseBoolean",
                "toDecimal", "toFloat",
                "toOne", "toOneMany", "reverse",
                "take", "exists", "forAll", "find", "map", "zip",
                "instanceOf"
        );
        for (String name : expected) {
            assertTrue(registry.isRegistered(name),
                    "Missing scalar function: " + name);
        }
    }

    // ===== Overload counts =====

    @Test
    void extendHasMultipleOverloads() {
        var overloads = registry.resolve("extend");
        assertTrue(overloads.size() >= 6,
                "extend should have at least 6 overloads, found: " + overloads.size());
    }

    @Test
    void absHasGenericOverload() {
        var overloads = registry.resolve("abs");
        assertEquals(1, overloads.size(),
                "abs should have 1 generic overload (TypeVar T)");
        assertInstanceOf(PType.TypeVar.class, overloads.getFirst().returnType());
    }

    @Test
    void selectHasThreeOverloads() {
        var overloads = registry.resolve("select");
        assertEquals(3, overloads.size());
    }

    @Test
    void lagHasTwoOverloads() {
        var overloads = registry.resolve("lag");
        assertEquals(2, overloads.size());
    }

    @Test
    void leadHasTwoOverloads() {
        var overloads = registry.resolve("lead");
        assertEquals(2, overloads.size());
    }



    // ===== NativeFunctionDef =====

    @Test
    void resolvedDef_hasCorrectArity() {
        var defs = registry.resolve("filter");
        assertEquals(2, defs.size());
        // Real parser: filter<T>(rel, f) has arity 2
        assertEquals(2, defs.getFirst().arity());
    }

    @Test
    void resolvedDef_hasRawSignature() {
        var defs = registry.resolve("toLower");
        assertEquals(1, defs.size());
        assertTrue(defs.getFirst().rawSignature().contains("toLower"));
    }

    // ===== Registry stats =====

    @Test
    void registryHasReasonableCounts() {
        // Tier 1 (~20 unique names) + Tier 2 (~40+ unique names)
        assertTrue(registry.functionCount() >= 50,
                "Expected at least 50 unique function names, got: " + registry.functionCount());
        // Total overloads should be much higher
        assertTrue(registry.overloadCount() >= 80,
                "Expected at least 80 total overloads, got: " + registry.overloadCount());
    }

    @Test
    void unknownFunctionReturnsEmptyList() {
        assertTrue(registry.resolve("doesNotExist").isEmpty());
        assertFalse(registry.isRegistered("doesNotExist"));
    }

    // ===== Mult parsing =====

    @Test
    void multParse_fixedValues() {
        assertEquals(Mult.ONE, Mult.parse("1"));
        assertEquals(Mult.ZERO_MANY, Mult.parse("*"));
        assertEquals(Mult.ZERO_ONE, Mult.parse("0..1"));
        assertEquals(Mult.ONE_MANY, Mult.parse("1..*"));
    }

    @Test
    void multParse_variable() {
        var m = Mult.parse("m");
        assertInstanceOf(Mult.Var.class, m);
        assertEquals("m", ((Mult.Var) m).name());
    }

    @Test
    void multToString() {
        assertEquals("1", Mult.ONE.toString());
        assertEquals("*", Mult.ZERO_MANY.toString());
        assertEquals("0..1", Mult.ZERO_ONE.toString());
        assertEquals("1..*", Mult.ONE_MANY.toString());
        assertEquals("m", new Mult.Var("m").toString());
    }

    // ===== PType basic checks =====

    @Test
    void ptype_concrete() {
        var t = new PType.Concrete("String");
        assertEquals("String", t.name());
        assertEquals("String", t.toString());
    }

    @Test
    void ptype_typeVar() {
        var t = new PType.TypeVar("T");
        assertEquals("T", t.name());
        assertEquals("T", t.toString());
    }

    @Test
    void ptype_parameterized() {
        var t = new PType.Parameterized("Relation",
                List.of(new PType.TypeVar("T")));
        assertEquals("Relation", t.rawType());
        assertEquals("Relation<T>", t.toString());
    }

    @Test
    void ptype_functionType() {
        var ft = new PType.FunctionType(
                List.of(new PType.Param("x", new PType.Concrete("String"), Mult.ONE)),
                new PType.Concrete("Boolean"),
                Mult.ONE
        );
        assertInstanceOf(PType.FunctionType.class, ft);
        assertEquals(1, ft.paramTypes().size());
        assertEquals("{x:String[1]->Boolean[1]}", ft.toString());
    }
}
