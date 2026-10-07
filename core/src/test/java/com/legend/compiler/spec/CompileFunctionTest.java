package com.legend.compiler.spec;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * <strong>Ported from engine's {@code compiler/CompileFunctionTest.java}</strong> &mdash; the
 * type-only, function-based behavioral spec for the Phase-G function-compile path
 * ({@link SpecCompiler#compile}). It is the corpus that matches this work: engine's
 * {@code *CheckerTest} are DuckDB integration tests (typecheck &rarr; SQL &rarr; execute) and
 * can't run in a type-only phase; this one asserts only on inferred types.
 *
 * <p>Translation: engine {@code TypeChecker.check(PureFunction)} &rarr; core
 * {@code SpecCompiler.compile(TypedFunction)}; {@code compiled.returnType()} &rarr;
 * {@code cf.signature().returnType()}; {@code body.hir().type()} &rarr; {@code cf.result().info().type()}.
 */
class CompileFunctionTest {

    private static CompiledFunction compile(String model, String fnFqn) {
        ModelContext ctx = Compiler.compileModel(model);
        return new SpecCompiler(ctx).compile(ctx.findFunction(fnFqn).get(0));
    }

    @Test
    void producesTypedFunctionBody() {
        CompiledFunction cf = compile(
                "function test::add(a: Integer[1], b: Integer[1]): Integer[1] { $a + $b }", "test::add");
        TypedFunction sig = cf.signature();
        assertEquals("test::add", sig.qualifiedName());
        assertEquals(2, sig.parameters().size());
        assertEquals("a", sig.parameters().get(0).name());
        assertEquals(Type.Primitive.INTEGER, sig.parameters().get(0).type());
        assertEquals(Multiplicity.Bounded.ONE, sig.parameters().get(0).multiplicity());
        assertEquals(Type.Primitive.INTEGER, sig.returnType());
        assertEquals(Multiplicity.Bounded.ONE, sig.returnMultiplicity());
        assertNotNull(cf.result().info(), "Function body must carry a typed HIR root");
    }

    @Test
    void fromOverAFunctionTypedParameterRunsTheFunction() {
        // from(FunctionDefinition<{->T[m]}>, Runtime) beats from<T|m>(T[m], Runtime) for a function value,
        // as in legend-pure: the result is the function's Integer[*], not the function (audit B1, 2026-10-07)
        String model = "function test::viaParam(q: FunctionDefinition<{->Integer[*]}>[1]): Integer[*]"
                + " { $q->from(^meta::core::runtime::Runtime())->map(x|$x + 1) }\n"
                + "function test::viaParamToOne(q: FunctionDefinition<{->Integer[*]}>[1]): Integer[*]"
                + " { $q->from(^meta::core::runtime::Runtime()) }\n";
        for (String fn : java.util.List.of("test::viaParam", "test::viaParamToOne")) {
            CompiledFunction cf = compile(model, fn);
            assertEquals(Type.Primitive.INTEGER, cf.result().info().type(), fn);
            assertEquals(Multiplicity.Bounded.ZERO_MANY, cf.result().info().multiplicity(), fn);
        }
    }

    @Test
    void evalOnFunctionTypedParameter() {
        // $f is a function-typed PARAMETER — the concrete lambda arrives per call site,
        // so eval types from the declared function type's result (engine EvalChecker's
        // Variable branch). Exercises the element-side classify of Function<{…}> params
        // AND EvalChecker.variableEval end-to-end.
        CompiledFunction cf = compile(
                "function test::apply(f: Function<{Integer[1]->Integer[1]}>[1]): Integer[1] { $f->eval(2) }",
                "test::apply");
        assertEquals(Type.Primitive.INTEGER, cf.result().info().type());
        assertEquals(Multiplicity.Bounded.ONE, cf.result().info().multiplicity());
    }

    @Test
    void evalOnFunctionTypedParameterRejectsWrongArgType() {
        assertThrows(TypeInferenceException.class, () -> compile(
                "function test::apply(f: Function<{Integer[1]->Integer[1]}>[1]): Integer[1] { $f->eval('x') }",
                "test::apply"));
    }

    @Test
    void multiStatementBody() {
        CompiledFunction cf = compile(
                "function test::addOne(a: Integer[1]): Integer[1] { let x = $a + 1; $x; }", "test::addOne");
        assertEquals(Type.Primitive.INTEGER, cf.result().info().type(),
                "Multi-statement body's last statement must type-check to Integer");
    }

    @Test
    void multiValuedLetPreservesMultiplicity() {
        // let xs = [1,2,3] : Integer[*]; binding must stay many — IMPOSSIBLE under the old
        // letFunction(String[1], T[1]):T[1] signature (forced [1]). The corrected real-legend-pure
        // letFunction(String[1], T[m]):T[m] makes the standard pipeline preserve the value's [m].
        CompiledFunction cf = compile(
                "function test::xs(): Integer[*] { let xs = [1, 2, 3]; $xs; }", "test::xs");
        assertEquals(Type.Primitive.INTEGER, cf.result().info().type());
        assertNotEquals(Multiplicity.Bounded.ONE, cf.result().info().multiplicity(),
                "a multi-valued let binding must not collapse to [1]");
    }

    @Test
    void returnTypeMismatchFailsLoudly() {
        TypeInferenceException ex = assertThrows(TypeInferenceException.class, () -> compile(
                "function test::mismatch(a: Integer[1]): String[1] { $a + 1 }", "test::mismatch"));
        assertTrue(String.valueOf(ex.getMessage()).contains("String") && String.valueOf(ex.getMessage()).contains("Integer"),
                "Error must mention both the declared return type and the actual body type. Got: "
                        + ex.getMessage());
    }

    @Test
    void memoizesByFqn() {
        ModelContext ctx = Compiler.compileModel("function test::greeting(): String[1] { 'hello' }");
        SpecCompiler sc = new SpecCompiler(ctx);
        TypedFunction fn = ctx.findFunction("test::greeting").get(0);
        assertSame(sc.compile(fn), sc.compile(fn),
                "Repeat compile of the same function must return the cached CompiledFunction");
    }

    @Test
    void identityFunctionBodyTypesToString() {
        CompiledFunction cf = compile(
                "function test::identity(s: String[1]): String[1] { $s }", "test::identity");
        assertEquals(Type.Primitive.STRING, cf.result().info().type());
    }

    @Test
    void zeroParamFunction() {
        CompiledFunction cf = compile("function test::pi(): Float[1] { 3.14 }", "test::pi");
        assertEquals(0, cf.signature().parameters().size());
        assertEquals(Type.Primitive.FLOAT, cf.signature().returnType());
    }

    @Test
    void classReturnTypeIsPreserved() {
        CompiledFunction cf = compile(
                "Class test::Box { value: String[1]; }\n"
              + "function test::makeBox(): test::Box[1] { ^test::Box(value='hi') }", "test::makeBox");
        assertTrue(cf.signature().returnType().typeName().contains("Box"),
                "Return type must reference the user class by name. Got: " + cf.signature().returnType());
    }

    // ===== function-value classifiers (m3.pure hierarchy: LambdaFunction /
    // ConcreteFunctionDefinition extend FunctionDefinition extends Function) =====

    @Test
    void lambdaLiteralClassifiesAsLambdaFunction() {
        // A lambda literal's m3 classifier is LambdaFunction<ft>, never the
        // bare structural FunctionType (engine stamps
        // LambdaFunction<{->Integer[1]}> on the instance).
        CompiledFunction cf = compile(
                "function test::t(): Any[1] { {x:Integer[1]|$x + 1} }", "test::t");
        Type t = cf.result().info().type();
        assertTrue(t instanceof Type.GenericType g
                        && com.legend.compiler.element.type.PlatformTypes
                                .LAMBDA_FUNCTION.equals(g.rawFqn())
                        && g.arguments().get(0) instanceof Type.FunctionType,
                "lambda literal must classify as LambdaFunction<ft>, got " + t.typeName());
    }

    @Test
    void functionDefinitionParamAcceptsLambdaLiteral() {
        // pkOfFunc shape (engine pkInferenceTests.pure): a
        // FunctionDefinition<Any>[1] formal accepts a lambda literal —
        // LambdaFunction ≤ FunctionDefinition on the class lattice.
        CompiledFunction cf = compile(
                "function test::pk(func: FunctionDefinition<Any>[1]): Integer[1] { 1 }\n"
              + "function test::t(): Integer[1] { test::pk({|2}) }", "test::t");
        assertEquals(Type.Primitive.INTEGER, cf.result().info().type());
    }

    @Test
    void functionDefinitionParamAcceptsConcreteFunctionReference() {
        // A mangled reference to a body-bearing user function classifies as
        // ConcreteFunctionDefinition<ft> ≤ FunctionDefinition (the engine
        // call shape: pkOfFunc(pkTestBare__Relation_1_)).
        CompiledFunction cf = compile(
                "function test::inc(i: Integer[1]): Integer[1] { $i + 1 }\n"
              + "function test::pk(func: FunctionDefinition<Any>[1]): Integer[1] { 1 }\n"
              + "function test::t(): Integer[1] { test::pk(test::inc_Integer_1__Integer_1_) }",
                "test::t");
        assertEquals(Type.Primitive.INTEGER, cf.result().info().type());
    }

    @Test
    void prevalAcceptsLambdaVerbatimSignature() {
        // preval<T>(f:FunctionDefinition<T>[1], ...) — engine-verbatim
        // (preeval.pure:53; audit R4 fix): the lambda self-types against
        // the nominal carrier, T binds the whole FunctionType.
        CompiledFunction cf = compile(
                "function test::t(): Any[1] { meta::pure::router::preeval::preval({|1}, []) }",
                "test::t");
        Type t = cf.result().info().type();
        assertTrue(t instanceof Type.GenericType g
                        && com.legend.compiler.element.type.PlatformTypes
                                .FUNCTION_DEFINITION.equals(g.rawFqn()),
                "preval must return FunctionDefinition<T> resolved, got " + t.typeName());
    }

    @Test
    void routerExecuteAcceptsLambdaLiteral() {
        // router execute<T|y>(f:FunctionDefinition<{->T[y]}>[1], ...) —
        // engine-verbatim (router_entry.pure:20; audit R4 fix): a lambda
        // conforms (LambdaFunction ≤ FunctionDefinition).
        CompiledFunction cf = compile(
                // upstream's signature (batch 5): a Mapping and a Runtime, spelled as
                // the instances real pure builds — the lambda is the point of the test
                "function test::t(): Any[1] { meta::pure::router::execute({|1},"
                        + " ^meta::pure::mapping::Mapping(name='m'), ^meta::core::runtime::Runtime(), []) }",
                "test::t");
        assertTrue(cf.result().info().type().typeName().contains("Result"),
                "router execute must produce a Result, got "
                        + cf.result().info().type().typeName());
    }

    @Test
    void routerExecuteRejectsFunctionTypedVariable() {
        // The verbatim FunctionDefinition formal must REJECT a value only
        // known to be a Function — the supertype direction (audit R4/R5).
        assertThrows(TypeInferenceException.class, () -> compile(
                "function test::t(f: Function<{->Integer[1]}>[1]): Any[1]"
                + " { meta::pure::router::execute($f, 'm', 'r', []) }",
                "test::t"));
    }

    @Test
    void lambdaFunctionFormalRejectsFunctionCarrier() {
        // concatenateTemporalTdsQueries(lfs:LambdaFunction<{->TDS[1]}>[*])
        // — the nominal gate judges BEFORE the structural unwrap (audit
        // R5's hole, closed): a Function-carrier value with a MATCHING
        // signature still does not conform to a LambdaFunction formal.
        assertThrows(TypeInferenceException.class, () -> compile(
                "function test::t(f: Function<{->meta::pure::tds::TabularDataSet[1]}>[1]): Any[1]"
                + " { meta::relational::milestoning::concatenateTemporalTdsQueries($f) }",
                "test::t"));
    }

    @Test
    void functionDefinitionParamRejectsFunctionTypedVariable() {
        // A Function<{…}>-DECLARED parameter is only known to be a Function —
        // Function is the SUPERTYPE of FunctionDefinition, so it must NOT
        // conform (native-function references are Functions but not
        // FunctionDefinitions; the lattice direction is load-bearing).
        assertThrows(TypeInferenceException.class, () -> compile(
                "function test::pk(func: FunctionDefinition<Any>[1]): Integer[1] { 1 }\n"
              + "function test::t(f: Function<{Integer[1]->Integer[1]}>[1]): Integer[1] { test::pk($f) }",
                "test::t"));
    }
}
