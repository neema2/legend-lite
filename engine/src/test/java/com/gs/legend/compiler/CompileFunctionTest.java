package com.gs.legend.compiler;

import com.gs.legend.compiled.CompiledFunction;
import com.gs.legend.model.PureModelBuilder;
import com.gs.legend.model.m3.Multiplicity;
import com.gs.legend.model.m3.Primitive;
import com.gs.legend.model.m3.PureFunction;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Contract tests for {@link TypeChecker#check(PureFunction)}.
 *
 * <p>Covers the typed-metamodel compile path: a {@link PureFunction} (built by
 * {@link PureModelBuilder}) compiles to a fully typed {@link CompiledFunction} via the
 * shared {@code compileBodyInContext} primitive, wrapping a
 * {@link com.gs.legend.compiled.CompiledExpression} whose TypeInfo side table is
 * the same instance shared across the {@link TypeChecker}'s lifetime.
 */
class CompileFunctionTest {

    @Test
    void producesTypedFunctionBody() {
        var built = build("""
                function test::add(a: Integer[1], b: Integer[1]): Integer[1]
                {
                    $a + $b
                }
                """);
        var tc = new TypeChecker(built.modelCtx());

        CompiledFunction compiled = tc.check(built.pf("test::add"));

        assertEquals("test::add", compiled.qualifiedName());
        assertEquals(2, compiled.parameters().size());
        assertEquals("a", compiled.parameters().get(0).name());
        assertSame(Primitive.INTEGER, compiled.parameters().get(0).type(),
                "Parameter type promoted to Primitive.INTEGER at NameResolver");
        assertEquals(Multiplicity.ONE, compiled.parameters().get(0).multiplicity());
        assertSame(Primitive.INTEGER, compiled.returnType());
        assertEquals(Multiplicity.ONE, compiled.returnMultiplicity());

        // Body is stamped — root has TypeInfo.
        var body = compiled.body();
        assertNotNull(body.typeInfoFor(body.ast()),
                "Function body root must be stamped with TypeInfo");
    }

    @Test
    void multiStatementBody() {
        var built = build("""
                function test::addOne(a: Integer[1]): Integer[1]
                {
                    let x = $a + 1;
                    $x
                }
                """);
        var tc = new TypeChecker(built.modelCtx());

        CompiledFunction compiled = tc.check(built.pf("test::addOne"));

        // Final statement's type is the function's result — the $x reference.
        var ast = compiled.body().ast();
        var rootInfo = compiled.body().typeInfoFor(ast);
        assertSame(Primitive.INTEGER, rootInfo.type(),
                "Multi-statement body's last statement must type-check to Integer");
    }

    @Test
    void returnTypeMismatchFailsLoudly() {
        var built = build("""
                function test::mismatch(a: Integer[1]): String[1]
                {
                    $a + 1
                }
                """);
        var tc = new TypeChecker(built.modelCtx());

        var ex = assertThrows(PureCompileException.class,
                () -> tc.check(built.pf("test::mismatch")));
        assertTrue(ex.getMessage().contains("String") && ex.getMessage().contains("Integer"),
                "Error must mention both the declared return type and the actual body type. Got: "
                        + ex.getMessage());
    }

    @Test
    void memoizesByFqn() {
        var built = build("""
                function test::greeting(): String[1]
                {
                    'hello'
                }
                """);
        var tc = new TypeChecker(built.modelCtx());

        var first = tc.check(built.pf("test::greeting"));
        var second = tc.check(built.pf("test::greeting"));

        assertSame(first, second,
                "Repeat check(pf) on same PureFunction must return the cached CompiledFunction");
    }

    @Test
    void pureFunctionSharesBodyWithCompiled() {
        var built = build("""
                function test::identity(s: String[1]): String[1]
                {
                    $s
                }
                """);
        var tc = new TypeChecker(built.modelCtx());

        CompiledFunction compiled = tc.check(built.pf("test::identity"));

        // CompiledFunction body must wrap the exact AST instance PureFunction holds —
        // no copy, no rebuild.
        var pureFn = built.modelCtx().findFunction("test::identity").get(0);
        assertSame(pureFn.body().get(pureFn.body().size() - 1), compiled.body().ast(),
                "CompiledExpression.ast must be the same instance as PureFunction.body's last statement");
    }

    @Test
    void zeroParamFunction() {
        var built = build("""
                function test::pi(): Float[1]
                {
                    3.14
                }
                """);
        var tc = new TypeChecker(built.modelCtx());

        CompiledFunction compiled = tc.check(built.pf("test::pi"));

        assertEquals(0, compiled.parameters().size(),
                "Zero-param function must compile with empty parameter list");
        assertSame(Primitive.FLOAT, compiled.returnType());
    }

    @Test
    void classReturnTypeIsPreserved() {
        var built = build("""
                Class test::Box { value: String[1]; }

                function test::makeBox(): test::Box[1]
                {
                    ^test::Box(value='hi')
                }
                """);
        var tc = new TypeChecker(built.modelCtx());

        CompiledFunction compiled = tc.check(built.pf("test::makeBox"));

        // User classes stay as NameRef through the typed metamodel to preserve lazy loading
        // (AGENTS.md §5). CompiledFunction.returnType reflects what PureFunction carries.
        var rt = compiled.returnType();
        assertTrue(rt.typeName().contains("Box"),
                "Return type must reference the user class by name. Got: " + rt);
    }

    // ---- fixture ----

    private Fixture build(String source) {
        var builder = new PureModelBuilder().addSource(source);
        return new Fixture(builder);
    }

    private record Fixture(PureModelBuilder builder) {
        com.gs.legend.model.ModelContext modelCtx() { return builder; }

        PureFunction pf(String name) {
            var list = builder.findFunction(name);
            assertNotNull(list, "No PureFunction list for: " + name);
            assertTrue(!list.isEmpty(), "No PureFunction entries for: " + name);
            return list.get(0);
        }
    }
}
