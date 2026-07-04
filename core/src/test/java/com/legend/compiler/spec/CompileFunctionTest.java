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
                "function test::addOne(a: Integer[1]): Integer[1] { let x = $a + 1; $x }", "test::addOne");
        assertEquals(Type.Primitive.INTEGER, cf.result().info().type(),
                "Multi-statement body's last statement must type-check to Integer");
    }

    @Test
    void multiValuedLetPreservesMultiplicity() {
        // let xs = [1,2,3] : Integer[*]; binding must stay many — IMPOSSIBLE under the old
        // letFunction(String[1], T[1]):T[1] signature (forced [1]). The corrected real-legend-pure
        // letFunction(String[1], T[m]):T[m] makes the standard pipeline preserve the value's [m].
        CompiledFunction cf = compile(
                "function test::xs(): Integer[*] { let xs = [1, 2, 3]; $xs }", "test::xs");
        assertEquals(Type.Primitive.INTEGER, cf.result().info().type());
        assertNotEquals(Multiplicity.Bounded.ONE, cf.result().info().multiplicity(),
                "a multi-valued let binding must not collapse to [1]");
    }

    @Test
    void returnTypeMismatchFailsLoudly() {
        TypeInferenceException ex = assertThrows(TypeInferenceException.class, () -> compile(
                "function test::mismatch(a: Integer[1]): String[1] { $a + 1 }", "test::mismatch"));
        assertTrue(ex.getMessage().contains("String") && ex.getMessage().contains("Integer"),
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
}
