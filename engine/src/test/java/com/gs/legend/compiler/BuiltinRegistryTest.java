package com.gs.legend.compiler;
import com.gs.legend.model.m3.Type;

import com.gs.legend.model.m3.Multiplicity;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the BuiltinRegistry: ensures all functions are registered,
 * correctly named, and retrievable.
 */
class BuiltinRegistryTest {

    private final BuiltinRegistry registry = BuiltinRegistry.instance();

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
        assertInstanceOf(Type.TypeVar.class, overloads.getFirst().returnType());
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

    // ===== Multiplicity toString =====
    //
    // The m3 Multiplicity type is intentionally a pure data model with no text-parsing
    // surface — parsers (PureQueryParser, PureModelParser, ValueSpecificationBuilder) build
    // Multiplicity directly from tokens/grammar contexts. What's round-trippable here is the
    // toString form, so that's what we pin.

    @Test
    void multToString() {
        assertEquals("[1]", Multiplicity.ONE.toString());
        assertEquals("[*]", Multiplicity.MANY.toString());
        assertEquals("[0..1]", Multiplicity.ZERO_ONE.toString());
        assertEquals("[1..*]", Multiplicity.ONE_MANY.toString());
        assertEquals("m", new Multiplicity.Var("m").toString());
    }

    // ===== Type basic checks =====

    @Test
    void type_nameRef_carriesFqn() {
        var t = new Type.NameRef("meta::pure::metamodel::type::String");
        assertEquals("meta::pure::metamodel::type::String", t.qualifiedName());
    }

    @Test
    void type_typeVar_carriesName() {
        var t = new Type.TypeVar("T");
        assertEquals("T", t.name());
    }

    @Test
    void type_parameterized_carriesRawTypeAndArgs() {
        var t = new Type.GenericType(com.gs.legend.model.m3.LClass.RELATION,
                List.of(new Type.TypeVar("T")));
        assertEquals(com.gs.legend.model.m3.LClass.RELATION, t.rawType());
        assertEquals(1, t.typeArgs().size());
        assertInstanceOf(Type.TypeVar.class, t.typeArgs().get(0));
    }

    @Test
    void type_functionType_carriesParamsAndReturn() {
        var ft = new Type.FunctionType(
                List.of(new Type.Parameter("x", com.gs.legend.model.m3.Primitive.STRING, Multiplicity.ONE)),
                com.gs.legend.model.m3.Primitive.BOOLEAN,
                Multiplicity.ONE
        );
        assertEquals(1, ft.params().size());
        assertEquals("x", ft.params().get(0).name());
        assertEquals(com.gs.legend.model.m3.Primitive.BOOLEAN, ft.returnType());
    }
}
