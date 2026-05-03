package com.gs.legend.test;

import com.gs.legend.compiler.ExpressionType;
import com.gs.legend.compiler.HirRewriter;
import com.gs.legend.compiler.typed.*;
import com.gs.legend.model.m3.Multiplicity;
import com.gs.legend.model.m3.Primitive;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link HirRewriter#alphaRename(TypedSpec, java.util.function.UnaryOperator)}.
 *
 * <p>α-rename ("alpha-conversion") deep-clones a typed-HIR subtree, renaming
 * every {@link TypedLambda}'s parameters to fresh names and rewriting every
 * reference to those parameters inside the lambda's body. References to
 * outer free variables MUST pass through unchanged. These tests verify each
 * of those properties on synthetic micro-trees.
 */
final class HirRewriterAlphaRenameTest {

    /** Stable namer that appends a counter, so output is deterministic. */
    private static java.util.function.UnaryOperator<String> counter() {
        AtomicLong ctr = new AtomicLong(0);
        return old -> old + "$" + ctr.getAndIncrement();
    }

    private static ExpressionType intInfo() {
        return new ExpressionType(Primitive.INTEGER, Multiplicity.ONE);
    }

    private static TypedVariable var(String name) {
        return new TypedVariable(name, Role.LAMBDA_PARAM, intInfo());
    }

    private static TypedParam param(String name) {
        return new TypedParam(name, Primitive.INTEGER, Multiplicity.ONE);
    }

    /** Single-param lambda body referencing its param: $p → $p$0 in body. */
    @Test
    void renamesSingleLambdaParameter() {
        TypedLambda lam = new TypedLambda(
                List.of(param("p")),
                List.of(var("p")),
                intInfo());

        TypedLambda renamed = (TypedLambda) HirRewriter.alphaRename(lam, counter());

        assertEquals(1, renamed.parameters().size());
        assertEquals("p$0", renamed.parameters().get(0).name(),
                "lambda parameter renamed");
        assertEquals(1, renamed.body().size());
        assertInstanceOf(TypedVariable.class, renamed.body().get(0));
        assertEquals("p$0", ((TypedVariable) renamed.body().get(0)).name(),
                "body reference rewritten to fresh name");
    }

    /** Two-param lambda: each param gets its own fresh name. */
    @Test
    void renamesMultipleLambdaParameters() {
        // {$a, $b | $a} - body refs $a only
        TypedLambda lam = new TypedLambda(
                List.of(param("a"), param("b")),
                List.of(var("a")),
                intInfo());

        TypedLambda renamed = (TypedLambda) HirRewriter.alphaRename(lam, counter());

        assertEquals("a$0", renamed.parameters().get(0).name());
        assertEquals("b$1", renamed.parameters().get(1).name());
        assertEquals("a$0", ((TypedVariable) renamed.body().get(0)).name());
    }

    /** Free variables (not bound by any lambda in the tree) pass through unchanged. */
    @Test
    void freeVariablesPassThrough() {
        // $outer is free here — no enclosing lambda binds it
        TypedVariable free = var("outer");
        TypedSpec renamed = HirRewriter.alphaRename(free, counter());

        assertSame(free, renamed, "free variable identity-preserved");
    }

    /**
     * Lambda inside a lambda. Inner $p shadows outer $p. After rename,
     * inner and outer get distinct fresh names; references inside the
     * inner lambda's body bind to the inner's fresh name, not the outer's.
     */
    @Test
    void nestedLambdasShadowingHandledCorrectly() {
        // Outer: {$p | <inner-lambda>}
        // Inner: {$p | $p}  ← inner $p shadows outer $p
        TypedLambda inner = new TypedLambda(
                List.of(param("p")),
                List.of(var("p")),
                intInfo());
        TypedLambda outer = new TypedLambda(
                List.of(param("p")),
                List.of(inner),
                intInfo());

        TypedLambda renamed = (TypedLambda) HirRewriter.alphaRename(outer, counter());

        // Outer p renamed first: p$0
        assertEquals("p$0", renamed.parameters().get(0).name());
        TypedLambda renamedInner = (TypedLambda) renamed.body().get(0);
        // Inner p renamed second: p$1
        assertEquals("p$1", renamedInner.parameters().get(0).name());
        // Inner body's $p binds to INNER's fresh name (p$1), not outer's (p$0)
        assertEquals("p$1", ((TypedVariable) renamedInner.body().get(0)).name(),
                "inner lambda body binds to inner's renamed param, not outer's");
    }

    /**
     * Outer lambda's param IS referenced inside the inner lambda's body
     * (since the inner lambda doesn't shadow the outer's name). Should
     * resolve to the OUTER's fresh name.
     */
    @Test
    void innerLambdaSeesOuterParameterRename() {
        // Outer: {$x | <inner>}
        // Inner: {$y | $x}  ← references outer's $x, not inner's $y
        TypedLambda inner = new TypedLambda(
                List.of(param("y")),
                List.of(var("x")),
                intInfo());
        TypedLambda outer = new TypedLambda(
                List.of(param("x")),
                List.of(inner),
                intInfo());

        TypedLambda renamed = (TypedLambda) HirRewriter.alphaRename(outer, counter());

        assertEquals("x$0", renamed.parameters().get(0).name());
        TypedLambda renamedInner = (TypedLambda) renamed.body().get(0);
        assertEquals("y$1", renamedInner.parameters().get(0).name());
        // Inner body's $x binds to OUTER's fresh name (x$0), unchanged-by inner
        assertEquals("x$0", ((TypedVariable) renamedInner.body().get(0)).name(),
                "reference to outer param rewritten to outer's fresh name");
    }

    /**
     * Subtrees with NO lambdas should be returned identity-unchanged,
     * preserving the kernel's allocation-free guarantee.
     */
    @Test
    void noLambdaSubtreesAreReferenceIdentity() {
        TypedSpec literal = new TypedCInteger(42L, intInfo());
        TypedSpec renamed = HirRewriter.alphaRename(literal, counter());
        assertSame(literal, renamed);
    }

    /**
     * Lambda with no parameters: nothing to rename. Result should be the
     * same lambda (identity).
     */
    @Test
    void parameterlessLambdaIsIdentityPreserved() {
        TypedLambda lam = new TypedLambda(
                List.of(),
                List.of(new TypedCInteger(7L, intInfo())),
                intInfo());
        TypedSpec renamed = HirRewriter.alphaRename(lam, counter());
        assertSame(lam, renamed,
                "no parameters means no rename means no allocation");
    }

    /**
     * Renamed variable's role and info come from the original
     * {@link TypedVariable} reference — only the name changes. Verifies
     * no info-corruption from α-rename.
     */
    @Test
    void renamedVariablePreservesRoleAndInfo() {
        ExpressionType bigInfo = intInfo();
        TypedVariable origRef = new TypedVariable("p", Role.LAMBDA_PARAM, bigInfo);
        TypedLambda lam = new TypedLambda(
                List.of(param("p")),
                List.of(origRef),
                intInfo());

        TypedLambda renamed = (TypedLambda) HirRewriter.alphaRename(lam, counter());

        TypedVariable renamedRef = (TypedVariable) renamed.body().get(0);
        assertEquals("p$0", renamedRef.name(), "name renamed");
        assertEquals(Role.LAMBDA_PARAM, renamedRef.role(), "role preserved");
        assertSame(bigInfo, renamedRef.info(), "info preserved");
    }
}
